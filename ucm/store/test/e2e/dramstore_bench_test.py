# -*- coding: utf-8 -*-
#
# MIT License
#
# Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without including without limitation the rights to use, copy,
# modify, merge, publish, distribute, sublicense, and/or sell copies of the
# Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
"""DramStore bench, modelled on ``cache_on_empty_test.py``.

Self-contained: loads the ``ucmpipelinestore`` pybind module by path
(without the ``ucm`` package init's vllm dependency) and stacks DramStore
via ``PipelineStore.Stack("Dram", libdramstore.so, config)``. KV buffers
are host ``ctypes`` regions that DramStore registers and the ``drampool``
daemon reads (dump) / writes (load) over the configured one-sided transport.

Prerequisites (not started by this script):
  * A ``drampool`` daemon at ``--node-control-endpoints`` started with the
    transport protocol + NIC matching ``--transport-protocol`` /
    ``--ibverbs-device``, and a yaml whose
    ``transport.endpoints[].one_sided`` matches ``--node-transport-manager-ids``.
  * ``ucmpipelinestore*.so`` + ``libdramstore.so`` + ``libucm_p2p_transport.so``
    on ``--so-dir``/``--lib-dir`` (and LD_LIBRARY_PATH pointing at lib-dir).
  * The chosen NIC reachable from both this host and the drampool host.

Run (from a node with the built modules):
  python3 dramstore_bench_test.py --so-dir /tmp --lib-dir /tmp \
    --local-host <client-ip> --ibverbs-device <nic> \
    --node-control-endpoints <pool-ip>:9000 \
    --node-transport-manager-ids <pool-ip>:4501
"""
import argparse
import array
import ctypes
import importlib.util
import os
import secrets
import statistics
import struct
import sys
import time

import numpy as np


def load_ucmpipelinestore(so_dir):
    """Load the ucmpipelinestore pybind .so by path, avoiding the ucm package init."""
    candidates = []
    for name in os.listdir(so_dir):
        if name.startswith("ucmpipelinestore") and name.endswith(".so"):
            candidates.append(os.path.join(so_dir, name))
    if not candidates:
        raise FileNotFoundError(f"ucmpipelinestore*.so not found in {so_dir}")
    path = sorted(candidates, key=os.path.getmtime)[-1]
    spec = importlib.util.spec_from_file_location("ucmpipelinestore", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_ucmdramstore(so_dir):
    # Backward-compat alias; the DramStore direct-link pybind was merged into
    # the pipeline pattern. Callers now load ucmpipelinestore and Stack DramStore.
    return load_ucmpipelinestore(so_dir)


class BareDramStore:
    """Thin wrapper over ucmpipelinestore.PipelineStore with DramStore stacked;
    mirrors the pipeline connector's Dram path."""

    def __init__(self, config, ucmpipelinestore, libdramstore_path=None):
        self._ds = ucmpipelinestore.PipelineStore()
        if libdramstore_path is None:
            # Testbed convention: libdramstore.so lives next to the pybind .so.
            libdramstore_path = os.path.join(
                os.path.dirname(getattr(ucmpipelinestore, "__file__", "") or ""),
                "libdramstore.so",
            )
        self._ds.Stack("Dram", libdramstore_path, dict(config))

    def lookup(self, block_ids):
        res = self._ds.Lookup(b"".join(block_ids))
        return [bool(b) for b in res]

    def lookup_on_prefix(self, block_ids):
        return self._ds.LookupOnPrefix(b"".join(block_ids))

    def wait(self, task_id):
        self._ds.Wait(task_id)

    def need_register_kv_caches(self):
        return bool(self._ds.NeedRegisterKVCaches())

    def register_kv_caches(self, addr_size_pairs):
        buf = bytearray()
        for addr, size in addr_size_pairs:
            buf += struct.pack("<QQ", int(addr), int(size))
        self._ds.RegisterKVCaches(bytes(buf))

    def dump(self, block_ids, shard_index, addrs):
        return self._ds.Dump(b"".join(block_ids), array.array("Q", shard_index),
                             addrs, 0)

    def load(self, block_ids, shard_index, addrs):
        return self._ds.Load(b"".join(block_ids), array.array("Q", shard_index),
                             addrs)


def cmp_and_print_diff(dst_ptrs_sizes, expected_bytes):
    for r, ((ptr, size), want) in enumerate(zip(dst_ptrs_sizes, expected_bytes)):
        got = ctypes.string_at(ptr, size)
        if got != want:
            for c, (xa, xb) in enumerate(zip(got, want)):
                if xa != xb:
                    print(f"DIFF at [{r}][{c}]  a={xa} b={xb}")
                    assert False
            if len(got) != len(want):
                print(f"DIFF at [{r}] length {len(got)} vs {len(want)}")
                assert False


def make_dram_config(args, role: str) -> dict:
    if role == "worker":
        local_control = f"{args.local_host}:{args.local_control_port}"
        local_mgr = f"{args.local_host}:{args.local_manager_port}"
    else:
        local_control = f"{args.local_host}:{args.local_control_port + 1}"
        local_mgr = f"{args.local_host}:{args.local_manager_port + 1}"
    return {
        "local_control_endpoint": local_control,
        "local_host": args.local_host,
        "local_transport_manager_id": local_mgr,
        "transport_device_id": args.transport_device_id,
        "router_type": args.router_type,
        "tensor_size_list": [args.tensor_size],
        "max_io_entries": 256,
        "transport_worker_count": 1,
        "node_ids": args.node_ids,
        "node_control_endpoints": args.node_control_endpoints,
        "node_transport_manager_ids": args.node_transport_manager_ids,
        "lookup_timeout_ms": args.lookup_timeout_ms,
        "dump_timeout_ms": args.dump_timeout_ms,
        "load_timeout_ms": args.load_timeout_ms,
        "transport_protocol": args.transport_protocol,
        "ibverbs_device_name": args.ibverbs_device,
        "kv_cache_memory_type": args.kv_cache_memory_type,
    }


class Region:
    """A KV buffer region: `ptr` is a usable local pointer (fill/verify); `addr`
    is the value passed as the Operation's remote_addr. For host memory these
    are the same (the host pointer)."""
    __slots__ = ("ptr", "size", "addr", "_keep")

    def __init__(self, ptr, size, addr):
        self.ptr = ptr
        self.size = size
        self.addr = addr


def build_host_regions(tensor_size, shards, request_size):
    """Host-memory model: ctypes buffers; ptr == addr == the host pointer.
    The buffers are kept alive (returned alongside via Region closure) so their
    addresses remain valid for the bench lifetime."""
    regions = []
    for _ in range(request_size * shards):
        b = ctypes.create_string_buffer(tensor_size)
        regions.append(Region(ctypes.addressof(b), ctypes.sizeof(b), ctypes.addressof(b)))
        regions[-1]._keep = b  # prevent GC of the ctypes buffer
    return regions


def fill_random(regions):
    for r in regions:
        ctypes.memmove(r.ptr, secrets.token_bytes(r.size), r.size)


def zero(regions):
    for r in regions:
        ctypes.memset(r.ptr, 0, r.size)


def region_addrs(regions, shards):
    """Return a 2-D uint64 ndarray of shape (n_blocks, shards) for the C++ binding."""
    flat = [r.addr for r in regions]
    n_blocks = len(flat) // shards
    return np.array(flat, dtype=np.uint64).reshape(n_blocks, shards)


def e2e_test(worker, scheduler, src_regs, dst_regs, args):
    chunk_block_ids = [secrets.token_bytes(16) for _ in range(args.request_size)]
    shard_indexes = [0 for _ in range(args.request_size)]
    shards = args.layer_size * args.chunk_size

    founds = scheduler.lookup(chunk_block_ids)
    assert not any(founds), "expected all-miss before dump"

    fill_random(src_regs)
    zero(dst_regs)
    expected = [ctypes.string_at(r.ptr, r.size) for r in src_regs]
    print(f"[debug] src[0][:4]={[b for b in expected[0][:4]]} ptr={hex(src_regs[0].ptr)} addr={hex(src_regs[0].addr)}", file=sys.stderr)

    t0 = time.perf_counter()
    task = worker.dump(chunk_block_ids, shard_indexes, region_addrs(src_regs, shards))
    worker.wait(task)
    dump_dt = time.perf_counter() - t0

    founds = scheduler.lookup(chunk_block_ids)
    assert all(founds), "expected all-hit after dump"

    t0 = time.perf_counter()
    task = worker.load(chunk_block_ids, shard_indexes, region_addrs(dst_regs, shards))
    worker.wait(task)
    load_dt = time.perf_counter() - t0

    dst_flat = [(r.ptr, r.size) for r in dst_regs]
    print(f"[debug] dst first bytes: {[ctypes.string_at(r.ptr, 1)[0] for r in dst_regs[:8]]}", file=sys.stderr)
    cmp_and_print_diff(dst_flat, expected)

    per_block = args.tensor_size * args.layer_size * args.chunk_size
    return dump_dt, load_dt, args.request_size * per_block


def main():
    parser = argparse.ArgumentParser(description="DramStore bench")
    parser.add_argument("--so-dir", default="/tmp", help="dir containing ucmdramstore*.so")
    parser.add_argument("--tensor-size", type=int, default=4096, help="bytes per shard")
    parser.add_argument("--layer-size", type=int, default=1)
    parser.add_argument("--chunk-size", type=int, default=1)
    parser.add_argument("--request-size", type=int, default=32, help="blocks per batch")
    parser.add_argument("--batch-number", type=int, default=32)
    parser.add_argument("--warmup-batches", type=int, default=2)
    parser.add_argument("--transport-device-id", type=int, default=0)
    parser.add_argument("--router-type", default="ring_hash")
    parser.add_argument("--local-host", default="127.0.0.1")
    parser.add_argument("--local-control-port", type=int, default=4702)
    parser.add_argument("--local-manager-port", type=int, default=4502)
    parser.add_argument("--node-ids", type=int, nargs="+", default=[1])
    parser.add_argument("--node-control-endpoints", nargs="+", default=["127.0.0.1:9000"])
    parser.add_argument("--node-transport-manager-ids", nargs="+", default=["127.0.0.1:4501"])
    parser.add_argument("--transport-protocol", default="ibverbs", choices=["ibverbs", "hixl"])
    parser.add_argument("--ibverbs-device", default="",
                        help="NIC device name passed to drampool --nics; "
                             "empty lets the transport pick its default")
    parser.add_argument("--kv-cache-memory-type", default="host", choices=["host", "device"])
    parser.add_argument("--lookup-timeout-ms", type=int, default=500)
    parser.add_argument("--dump-timeout-ms", type=int, default=2000)
    parser.add_argument("--load-timeout-ms", type=int, default=2000)
    args = parser.parse_args()

    args.shard_size = args.tensor_size * args.layer_size * args.chunk_size
    args.block_size = args.shard_size
    os.environ.setdefault("UC_LOGGER_LEVEL", "info")

    ucmdramstore = load_ucmdramstore(args.so_dir)
    worker = BareDramStore(make_dram_config(args, "worker"), ucmdramstore)
    scheduler = BareDramStore(make_dram_config(args, "scheduler"), ucmdramstore)

    shards = args.layer_size * args.chunk_size
    count = args.request_size * shards
    # Host memory: ctypes buffers; ptr == addr == host pointer.
    src_regs = build_host_regions(args.tensor_size, shards, args.request_size)
    dst_regs = build_host_regions(args.tensor_size, shards, args.request_size)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src_regs + dst_regs])
    print(f"[debug] registered {len(src_regs) + len(dst_regs)} regions", file=sys.stderr)

    per_block = args.tensor_size * args.layer_size * args.chunk_size
    print(f"[bench] blocks/batch={args.request_size} shards={shards} per_shard={args.tensor_size} B "
          f"per_block={per_block} B transport={args.transport_protocol}/{args.ibverbs_device} "
          f"batches={args.batch_number} (warmup={args.warmup_batches})")

    dump_times, load_times, data_bytes = [], [], 0
    for i in range(args.batch_number + args.warmup_batches):
        dt, lt, db = e2e_test(worker, scheduler, src_regs, dst_regs, args)
        if i < args.warmup_batches:
            continue
        dump_times.append(dt)
        load_times.append(lt)
        data_bytes = db

    n = len(dump_times)
    dump_avg = statistics.mean(dump_times)
    load_avg = statistics.mean(load_times)
    dump_bw = data_bytes / dump_avg / 1e6 if dump_avg > 0 else 0.0
    load_bw = data_bytes / load_avg / 1e6 if load_avg > 0 else 0.0

    print("\n================ DramStore bench ================")
    print(f"batches measured : {n}")
    print(f"data per batch   : {data_bytes} B ({data_bytes / 1e6:.2f} MB)")
    print(f"dump avg latency : {dump_avg * 1000:.2f} ms   bw {dump_bw:.2f} MB/s")
    print(f"load avg latency : {load_avg * 1000:.2f} ms   bw {load_bw:.2f} MB/s")
    if n > 1:
        print(f"dump p50/p99     : {statistics.median(dump_times) * 1000:.2f}"
              f" / {sorted(dump_times)[int(0.99 * (n - 1))] * 1000:.2f} ms")
        print(f"load p50/p99     : {statistics.median(load_times) * 1000:.2f}"
              f" / {sorted(load_times)[int(0.99 * (n - 1))] * 1000:.2f} ms")
    print("round-trip       : {:.2f} ms".format((dump_avg + load_avg) * 1000))


if __name__ == "__main__":
    main()
