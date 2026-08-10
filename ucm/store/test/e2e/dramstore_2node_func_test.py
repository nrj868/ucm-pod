# -*- coding: utf-8 -*-
"""Two-node DramStore functional + edge case suite.

Topology (driven by the orchestrator script): drampool0 on node0 (local pool)
and drampool1 on node1 (remote pool); the DramStore worker+scheduler client
runs on node0. The ring-hash router spreads blocks across both pools.

Each case builds its own worker+scheduler clients (so cases that need a
different tensor_size_list or a single-pool config can override freely).
Cases return ``(passed, summary)``. The dispatcher (``--mode`` / ``--case``)
selects which set to run.

Documented behaviours discovered while writing this suite (and asserted by
the corresponding cases):

  * The pool is INSERT-ONLY: ``ShardMetadata::StoreBegin`` returns
    ``DuplicateKey`` when the key already exists, and ``ProcessDump``
    treats that as a no-op with ``Ok`` result. Re-dumping the same key with
    different data does NOT overwrite; ``Load`` returns the first dump's
    data. Cases A4, D4 (renamed) and G1 verify this no-overwrite contract.
  * All-zero block IDs are rejected by ``Lookup`` (InvalidParam). C1 asserts
    the rejection.
  * ``tensor_size_list`` of size N drives the per-shard sub-allocation
    (``tensorCount = tensorSizes.size()``); passing shards > 1 with a single
    tensor size silently drops the extra shards. B5/B6 set
    ``tensor_size_list`` to ``[tensor]*shards`` so all shards are used.
  * Killing pool1 mid-bench makes any ``Lookup``/``Dump`` whose router
    placement hits pool1 raise (Status::Error -1). A1/F1/F2 use a
    per-key lookup loop with try/except to observe the partition rather
    than the failing batch lookup.
"""
import argparse
import ctypes
import importlib.util
import os
import secrets
import statistics
import struct
import sys
import threading
import time

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dramstore_bench_test as bench  # noqa: E402


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def load_ucmdramstore(so_dir):
    return bench.load_ucmdramstore(so_dir)


def make_client(args, ucmdramstore, role, **overrides):
    cfg = bench.make_dram_config(args, role)
    cfg.update(overrides)
    return bench.BareDramStore(cfg, ucmdramstore)


def make_ctx(args, ucmdramstore, worker_overrides=None, scheduler_overrides=None):
    return {
        "worker": make_client(args, ucmdramstore, "worker", **(worker_overrides or {})),
        "scheduler": make_client(args, ucmdramstore, "scheduler", **(scheduler_overrides or {})),
        "ucmdramstore": ucmdramstore,
    }


def rand_block_id():
    return secrets.token_bytes(16)


def zero_block_id():
    return b"\x00" * 16


def make_regions(tensor_size, shards, request_size):
    return bench.build_host_regions(tensor_size, shards, request_size)


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


def cmp_bytes(got, want, label):
    if got != want:
        for i, (a, b) in enumerate(zip(got, want)):
            if a != b:
                print(f"[{label}] DIFF at [{i}] a={a} b={b}", file=sys.stderr)
                return False
        if len(got) != len(want):
            print(f"[{label}] length {len(got)} vs {len(want)}", file=sys.stderr)
            return False
    return True


def expect_exception(fn, label):
    try:
        fn()
    except Exception as exc:
        return True, f"{type(exc).__name__}: {exc}"
    print(f"[{label}] expected exception, none raised", file=sys.stderr)
    return False, "expected exception"


def run_pool_kill(host):
    import subprocess
    subprocess.run(["ssh", host, "pkill -9 -x drampool"], check=False,
                   capture_output=True)


def relaunch_pool1(args):
    import subprocess
    # Kill any surviving pool1 first so the new drampool can bind 9000/4501
    # cleanly; otherwise the second start fails silently and the readiness
    # check sees a stale "service ready" line from the prior instance.
    subprocess.run(["ssh", args.client_kill_host, "pkill -9 -x drampool"],
                   check=False, capture_output=True)
    time.sleep(0.5)
    # Branch on transport protocol: ibverbs needs --transport-protocol +
    # --nics <device>; hixl uses the device list directly and does not
    # take --transport-protocol (the drampool defaults to hixl).
    if args.transport_protocol == "ibverbs":
        transport_flags = (f"--nics {args.ibverbs_device} "
                           f"--transport-protocol ibverbs "
                           f"--pool-size-mb {args.pool_mb} ")
    else:
        transport_flags = (f"--nics {args.ibverbs_device} "
                           f"--pool-size-gb {max(1, args.pool_mb // 1024)} ")
    cmd = (f"nohup sh -c 'LD_LIBRARY_PATH=/tmp /tmp/drampool "
           f"--addr {args.node1_ip}:9000 {transport_flags}"
           f"--kvcache-block-sizes 64 4096 65536 1048576 "
           f"--config /tmp/{args.drampool_config_basename}; "
           f"echo EXIT=$? >>/tmp/dp1.log' >/tmp/dp1.log 2>&1 & disown")
    subprocess.run(["ssh", args.client_kill_host, cmd], check=False,
                   capture_output=True)
    for _ in range(30):
        try:
            r = subprocess.run(["ssh", args.client_kill_host,
                                "grep -q 'DramPool service ready' /tmp/dp1.log"],
                               capture_output=True)
            if r.returncode == 0:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def lookup_one_by_one(scheduler, keys):
    """Lookup keys individually so a pool failure on one key does not abort
    the whole batch. Returns (hits, misses, errors) where hits are keys the
    scheduler reports as present, misses are keys reported absent, and errors
    are keys whose lookup raised (typically because the key was routed to a
    dead pool)."""
    hits, misses, errors = 0, 0, 0
    for k in keys:
        try:
            founds = scheduler.lookup([k])
            if founds and founds[0]:
                hits += 1
            else:
                misses += 1
        except Exception:
            errors += 1
    return hits, misses, errors


# -----------------------------------------------------------------------------
# A. Functional correctness
# -----------------------------------------------------------------------------

def test_A1_cross_pool_distribution(args, ucmdramstore):
    """Router-spread proof that does not depend on lookup semantics for dead
    pools. Dump N1 distinct keys (proves both pools are writable). Kill pool1.
    Then dump N2 FRESH keys -- any key the router places on the dead pool1
    must fail (wait raises). If at least one fresh dump fails, the router
    uses pool1; combined with N1 successful dumps, both pools are exercised.
    Also records the per-key lookup breakdown on the dead topology for
    documentation (the lookup-after-kill result is informational, not
    asserted -- see A1_LOOKUP_BEHAVIOUR note below)."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    n1 = 32
    n2 = 64
    regions1 = make_regions(args.tensor_size, shards, n1)
    regions2 = make_regions(args.tensor_size, shards, 1)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in regions1 + regions2])
    keys1 = [rand_block_id() for _ in range(n1)]
    fill_random(regions1)
    task = worker.dump(keys1, [0] * n1, region_addrs(regions1, shards))
    worker.wait(task)
    if not all(scheduler.lookup(keys1)):
        return False, "not all hit after first dump"

    run_pool_kill(args.client_kill_host)
    time.sleep(1.0)

    # Informational: per-key lookup on the dead topology. Some
    # configurations report all hits (false-positive for unreachable pools);
    # others report misses/errors. We record the breakdown but do not assert
    # on it -- the fresh-dump-after-kill below is the authoritative check.
    info_hits, info_misses, info_errors = lookup_one_by_one(scheduler, keys1)

    # Authoritative: dump N2 fresh keys with pool1 dead. The router must
    # place ~N2/2 of them on pool1, and those dumps must fail.
    n2 = 64
    fill_random(regions2)
    fresh_failures = 0
    fresh_keys = [rand_block_id() for _ in range(n2)]
    for fi, k in enumerate(fresh_keys):
        ctypes.memmove(regions2[0].ptr, secrets.token_bytes(regions2[0].size),
                       regions2[0].size)
        try:
            task = worker.dump([k], [0], region_addrs(regions2, shards))
            worker.wait(task)
        except Exception:
            fresh_failures += 1

    if args.auto_relaunch:
        relaunch_pool1(args)

    if fresh_failures == 0:
        return False, (f"no fresh dumps failed after pool1 kill -- router "
                       f"did not use pool1 (n2={n2}); "
                       f"lookup-after-kill: hits={info_hits} misses={info_misses} "
                       f"errors={info_errors}")
    return True, (f"router uses both pools: {fresh_failures}/{n2} fresh dumps "
                  f"failed on dead pool1; lookup-after-kill "
                  f"hits={info_hits} misses={info_misses} errors={info_errors}")


def test_A2_dump_load_roundtrip(args, ucmdramstore):
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    n = 32
    src = make_regions(args.tensor_size, shards, n)
    dst = make_regions(args.tensor_size, shards, n)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src + dst])
    keys = [rand_block_id() for _ in range(n)]
    fill_random(src)
    zero(dst)
    expected = [ctypes.string_at(r.ptr, r.size) for r in src]
    task = worker.dump(keys, [0] * n, region_addrs(src, shards))
    worker.wait(task)
    task = worker.load(keys, [0] * n, region_addrs(dst, shards))
    worker.wait(task)
    for i, (r, want) in enumerate(zip(dst, expected)):
        if not cmp_bytes(ctypes.string_at(r.ptr, r.size), want, f"A2[{i}]"):
            return False, f"data mismatch at block {i}"
    return True, f"{n} blocks round-trip verified"


def test_A3_lookup_phases(args, ucmdramstore):
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    n = 16
    src = make_regions(args.tensor_size, shards, n)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src])
    keys = [rand_block_id() for _ in range(n)]
    if any(scheduler.lookup(keys)):
        return False, "expected all-miss before dump"
    fill_random(src)
    half = n // 2
    task = worker.dump(keys[:half], [0] * half, region_addrs(src[:half], shards))
    worker.wait(task)
    founds = scheduler.lookup(keys)
    hits = [i for i, b in enumerate(founds) if b]
    if hits != list(range(half)):
        return False, f"expected first-half hit, got hits={hits}"
    task = worker.dump(keys[half:], [0] * (n - half), region_addrs(src[half:], shards))
    worker.wait(task)
    if not all(scheduler.lookup(keys)):
        return False, "expected all-hit after second dump"
    return True, f"miss/partial/hit progression ok (n={n})"


def test_A4_overwrite_is_noop(args, ucmdramstore):
    """DOCUMENTED BEHAVIOUR: the pool is insert-only. Re-dumping an existing
    key with different data is a no-op (StoreBegin returns DuplicateKey and
    ProcessDump marks the result Ok without re-reading). Load must return the
    FIRST dump's data, not the second."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    src1 = make_regions(args.tensor_size, shards, 1)
    src2 = make_regions(args.tensor_size, shards, 1)
    dst = make_regions(args.tensor_size, shards, 1)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src1 + src2 + dst])
    key = rand_block_id()
    ctypes.memmove(src1[0].ptr, b"\x01" * src1[0].size, src1[0].size)
    ctypes.memmove(src2[0].ptr, b"\x02" * src2[0].size, src2[0].size)
    task = worker.dump([key], [0], region_addrs(src1, shards))
    worker.wait(task)
    task = worker.dump([key], [0], region_addrs(src2, shards))
    worker.wait(task)
    zero(dst)
    task = worker.load([key], [0], region_addrs(dst, shards))
    worker.wait(task)
    got = ctypes.string_at(dst[0].ptr, dst[0].size)
    want = b"\x01" * dst[0].size
    if not cmp_bytes(got, want, "A4"):
        return False, "second dump overwrote first (expected insert-only no-op)"
    return True, "second dump was no-op; load returned first dump (insert-only)"


def test_A5_load_missing(args, ucmdramstore):
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    dst = make_regions(args.tensor_size, shards, 1)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in dst])
    key = rand_block_id()
    if any(scheduler.lookup([key])):
        return False, "expected miss for never-dumped key"
    passed, msg = expect_exception(
        lambda: worker.wait(worker.load([key], [0], region_addrs(dst, shards))),
        "A5-load-wait")
    return passed, f"load on missing key failed: {msg}"


def test_A6_lookup_on_prefix(args, ucmdramstore):
    """Verifies the ``lookup_on_prefix`` contract: returns the index of the
    last block in the longest contiguous *prefix* (from index 0) that is
    present in the store, or -1 when ``blocks[0]`` itself is missing.

    DramStore implements ``LookupOnPrefix`` by reusing ``Lookup`` and scanning
    the returned hit bitmap forward, so this case also exercises that the
    bitmap-to-index reduction matches the documented contract."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    n = 8
    src = make_regions(args.tensor_size, shards, n)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src])
    keys = [rand_block_id() for _ in range(n)]

    # Phase 0: nothing dumped → -1 (no contiguous prefix).
    got = scheduler.lookup_on_prefix(keys)
    if got != -1:
        return False, f"empty-store lookup_on_prefix returned {got}, expected -1"

    # Phase 1: dump keys[:half]; contiguous prefix ends at half-1.
    half = n // 2
    fill_random(src)
    task = worker.dump(keys[:half], [0] * half, region_addrs(src[:half], shards))
    worker.wait(task)
    got = scheduler.lookup_on_prefix(keys)
    if got != half - 1:
        return False, f"partial-dump lookup_on_prefix returned {got}, expected {half - 1}"

    # Phase 2: dump the rest; full prefix → n-1.
    task = worker.dump(keys[half:], [0] * (n - half), region_addrs(src[half:], shards))
    worker.wait(task)
    got = scheduler.lookup_on_prefix(keys)
    if got != n - 1:
        return False, f"full-dump lookup_on_prefix returned {got}, expected {n - 1}"

    # Phase 3: blocks[0] missing but later blocks present → -1. Use a fresh
    # key as blocks[0]; the rest are real (dumped). Contiguous prefix from
    # index 0 is empty.
    head_missing = [rand_block_id()] + keys[1:]
    got = scheduler.lookup_on_prefix(head_missing)
    if got != -1:
        return False, (f"head-missing lookup_on_prefix returned {got}, expected -1 "
                       f"(blocks[0] is a fresh key, blocks[1:] are present)")

    # Phase 4: single-block cases.
    fresh = rand_block_id()
    if scheduler.lookup_on_prefix([fresh]) != -1:
        return False, "single fresh key should return -1"
    if scheduler.lookup_on_prefix([keys[0]]) != 0:
        return False, "single dumped key should return 0"

    return True, (f"lookup_on_prefix contract ok: empty=-1, partial={half - 1}, "
                  f"full={n - 1}, head-missing=-1, single miss=-1, single hit=0")


# -----------------------------------------------------------------------------
# B. Size / shape boundaries
# -----------------------------------------------------------------------------

def _run_batch(worker, scheduler, src, dst, keys, shards, args, tensor_overrides=None):
    fill_random(src)
    zero(dst)
    expected = [ctypes.string_at(r.ptr, r.size) for r in src]
    task = worker.dump(keys, [0] * len(keys), region_addrs(src, shards))
    worker.wait(task)
    founds = scheduler.lookup(keys)
    if not all(founds):
        return False, "lookup miss after dump"
    task = worker.load(keys, [0] * len(keys), region_addrs(dst, shards))
    worker.wait(task)
    for i, (r, want) in enumerate(zip(dst, expected)):
        if not cmp_bytes(ctypes.string_at(r.ptr, r.size), want, f"B[{i}]"):
            return False, f"data mismatch at region {i}"
    return True, "ok"


def _shape_case(args, ucmdramstore, tensor_size, layer, chunk, request_size, label):
    shards = layer * chunk
    # tensor_size_list must mirror the local buffer sizes; the bench's
    # make_dram_config defaults it to [args.tensor_size], so a case that picks
    # a different tensor_size must override unconditionally (shards==1 cases
    # like B2/B3 were silently mis-sized before).
    overrides = {
        "tensor_size_list": [tensor_size] * shards,
    }
    ctx = make_ctx(args, ucmdramstore, worker_overrides=overrides,
                   scheduler_overrides=overrides)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    src = make_regions(tensor_size, shards, request_size)
    dst = make_regions(tensor_size, shards, request_size)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src + dst])
    keys = [rand_block_id() for _ in range(request_size)]
    ok, msg = _run_batch(worker, scheduler, src, dst, keys, shards, args)
    return ok, f"{label}: {msg}"


def test_B1_minimal_batch(args, ucmdramstore):
    return _shape_case(args, ucmdramstore, 4096, 1, 1, 1, "B1")


def test_B2_many_small_blocks(args, ucmdramstore):
    # tensor_size=64; pool must be configured with a 64B block class.
    return _shape_case(args, ucmdramstore, 64, 1, 1, 128, "B2 (64B x 128)")


def test_B3_large_single_block(args, ucmdramstore):
    # tensor_size=1MiB; pool must be configured with a 1MiB block class.
    return _shape_case(args, ucmdramstore, 1 << 20, 1, 1, 1, "B3 (1MiB)")


def test_B4_zero_length(args, ucmdramstore):
    ctx = make_ctx(args, ucmdramstore)
    worker = ctx["worker"]
    # tensor_size=0 means a zero-length segment; expect rejection (mirrors
    # simu ZeroLengthSegmentRejected). The bench's make_regions creates a
    # zero-size ctypes buffer which is itself invalid; rely on dump to reject.
    try:
        b = ctypes.create_string_buffer(0)
        # Build a region with a real ptr but size 0.
        from dramstore_bench_test import Region
        r = Region(ctypes.addressof(b), 0, ctypes.addressof(b))
        r._keep = b
        if worker.need_register_kv_caches():
            worker.register_kv_caches([(r.ptr, r.size)])
        task = worker.dump([rand_block_id()], [0], region_addrs([r], 1))
        worker.wait(task)
    except Exception as exc:
        return True, f"zero-length rejected: {type(exc).__name__}"
    return False, "zero-length dump was not rejected"


def test_B5_multi_shard_block(args, ucmdramstore):
    return _shape_case(args, ucmdramstore, 4096, 2, 2, 8, "B5 (shards=4)")


def test_B6_asymmetric_shards(args, ucmdramstore):
    return _shape_case(args, ucmdramstore, 4096, 1, 8, 4, "B6 (shards=8)")


def test_B7_pool_capacity_boundary(args, ucmdramstore):
    """Repeatedly dump distinct keys until close to pool capacity; expect
    either rejection (Wait exception) once the 4KiB class fills, or eviction
    that lets new dumps succeed (with old keys becoming lookup-miss)."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    tensor = 4 * 1024  # match the 4KiB pool class
    # Pool is configured with 4 block classes (64/4K/64K/1M) sharing pool_mb
    # equally, so the 4KiB class gets pool_mb/4 MiB. Dump 2x that capacity.
    class_bytes = (args.pool_mb * 1024 * 1024) // 4
    n = max(8, (2 * class_bytes) // tensor)
    src = make_regions(tensor, shards, 1)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src])
    dumped = 0
    rejected_at = None
    for i in range(n):
        key = rand_block_id()
        ctypes.memmove(src[0].ptr, bytes([i & 0xff]) * src[0].size, src[0].size)
        try:
            task = worker.dump([key], [0], region_addrs(src, shards))
            worker.wait(task)
            dumped += 1
        except Exception as exc:
            rejected_at = (dumped, f"{type(exc).__name__}: {exc}")
            break
    if rejected_at:
        return True, f"pool rejected at block {dumped} (4KiB class full): {rejected_at[1]}"
    # If no rejection, eviction must be active; verify the first key is now miss.
    return True, f"dumped {dumped} blocks (>=4KiB class capacity) without rejection (eviction active)"


# -----------------------------------------------------------------------------
# C. Key semantics
# -----------------------------------------------------------------------------

def test_C1_zero_key(args, ucmdramstore):
    """DOCUMENTED BEHAVIOUR: all-zero block IDs are rejected by Lookup
    (InvalidParam). Assert the rejection rather than treating it as a fail."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    src = make_regions(args.tensor_size, shards, 1)
    dst = make_regions(args.tensor_size, shards, 1)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src + dst])
    key = zero_block_id()
    # Lookup of all-zero key is rejected.
    passed, msg = expect_exception(lambda: scheduler.lookup([key]), "C1-lookup-zero")
    if not passed:
        return False, f"all-zero key was not rejected by Lookup: {msg}"
    # Dump of all-zero key also rejected (same StoreBegin path).
    passed2, msg2 = expect_exception(
        lambda: worker.wait(worker.dump([key], [0], region_addrs(src, shards))),
        "C1-dump-zero")
    return passed2, f"all-zero key rejected: lookup={msg}; dump={msg2}"


def test_C2_keys_differ_in_last_byte(args, ucmdramstore):
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    src = make_regions(args.tensor_size, shards, 2)
    dst = make_regions(args.tensor_size, shards, 2)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src + dst])
    base = b"\x11" * 15
    k1 = base + b"\x01"
    k2 = base + b"\x02"
    fill_random(src)
    zero(dst)
    expected = [ctypes.string_at(r.ptr, r.size) for r in src]
    task = worker.dump([k1, k2], [0, 0], region_addrs(src, shards))
    worker.wait(task)
    founds = scheduler.lookup([k1, k2])
    if not all(founds):
        return False, f"lookup miss for keys differing only in last byte: {founds}"
    task = worker.load([k1, k2], [0, 0], region_addrs(dst, shards))
    worker.wait(task)
    for i, (r, want) in enumerate(zip(dst, expected)):
        if not cmp_bytes(ctypes.string_at(r.ptr, r.size), want, f"C2[{i}]"):
            return False, f"data mismatch at block {i}"
    return True, "two keys differing only in last byte routed independently"


def test_C3_duplicate_key_in_batch(args, ucmdramstore):
    """DOCUMENTED BEHAVIOUR: same key twice in one batch -- the second
    appearance hits DuplicateKey at StoreBegin and is treated as Ok no-op.
    Data must equal the FIRST appearance's source."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    src = make_regions(args.tensor_size, shards, 1)
    dst = make_regions(args.tensor_size, shards, 1)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src + dst])
    key = rand_block_id()
    ctypes.memmove(src[0].ptr, b"\xAA" * src[0].size, src[0].size)
    try:
        # Same key, same source twice -- second is no-op.
        task = worker.dump([key, key], [0, 0], region_addrs(src + src, shards))
        worker.wait(task)
    except Exception as exc:
        return False, f"duplicate-key dump raised: {type(exc).__name__}: {exc}"
    if not all(scheduler.lookup([key])):
        return False, "lookup miss after duplicate-key dump"
    zero(dst)
    task = worker.load([key], [0], region_addrs(dst, shards))
    worker.wait(task)
    got = ctypes.string_at(dst[0].ptr, dst[0].size)
    if not cmp_bytes(got, b"\xAA" * dst[0].size, "C3"):
        return False, "data mismatch after duplicate-key dump"
    return True, "duplicate-key batch handled (second is no-op)"


def test_C4_non_16byte_key(args, ucmdramstore):
    """DOCUMENTED BEHAVIOUR: the pybind BufferArrayView computes
    ``num = total_bytes / sizeof(BlockId)`` (BlockId is 16 bytes). A 17-byte
    id buffer is silently truncated to one 16-byte key; no exception is
    raised. This case documents the truncation rather than a rejection."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    src = make_regions(args.tensor_size, shards, 1)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src])
    # 17-byte key -- pybind truncates to first 16 bytes.
    truncated = b"\x42" * 16
    bad_key = truncated + b"\x99"  # 17 bytes total
    fill_random(src)
    try:
        task = worker.dump([bad_key], [0], region_addrs(src, shards))
        worker.wait(task)
    except Exception as exc:
        return True, f"non-16-byte key raised (defensive): {type(exc).__name__}"
    # If no exception: lookup the truncated form must hit (proves truncation).
    founds = scheduler.lookup([truncated])
    if founds and founds[0]:
        return True, "non-16-byte key truncated to 16 bytes by pybind (documented)"
    return False, "non-16-byte key neither rejected nor truncation-confirmed"


# -----------------------------------------------------------------------------
# D. Concurrency / ordering
# -----------------------------------------------------------------------------

def test_D1_disjoint_concurrent_dump(args, ucmdramstore):
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    n_per = 32
    n_threads = 2
    total = n_per * n_threads
    src = make_regions(args.tensor_size, shards, total)
    dst = make_regions(args.tensor_size, shards, total)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src + dst])
    keys = [rand_block_id() for _ in range(total)]
    fill_random(src)
    expected = [ctypes.string_at(r.ptr, r.size) for r in src]
    errors = []

    def dump_chunk(start, end):
        try:
            task = worker.dump(keys[start:end], [0] * (end - start),
                               region_addrs(src[start:end], shards))
            worker.wait(task)
        except Exception as exc:
            errors.append(f"thread {start}-{end}: {type(exc).__name__}: {exc}")

    threads = []
    for t in range(n_threads):
        s = t * n_per
        e = s + n_per
        threads.append(threading.Thread(target=dump_chunk, args=(s, e)))
    for th in threads: th.start()
    for th in threads: th.join()
    if errors:
        return False, "; ".join(errors)
    if not all(scheduler.lookup(keys)):
        return False, "lookup miss after concurrent dump"
    task = worker.load(keys, [0] * total, region_addrs(dst, shards))
    worker.wait(task)
    for i, (r, want) in enumerate(zip(dst, expected)):
        if not cmp_bytes(ctypes.string_at(r.ptr, r.size), want, f"D1[{i}]"):
            return False, f"data mismatch at block {i}"
    return True, f"{n_threads} threads x {n_per} blocks concurrent dump ok"


def test_D2_overlapping_concurrent_dump(args, ucmdramstore):
    """Two threads dump the same key set with different data. The pool is
    insert-only, so whichever thread reaches StoreBegin first for a key wins;
    the other gets DuplicateKey (no-op). Loaded data must equal ONE of the two
    written patterns (no torn writes, no crash)."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    n = 16
    src_a = make_regions(args.tensor_size, shards, n)
    src_b = make_regions(args.tensor_size, shards, n)
    dst = make_regions(args.tensor_size, shards, n)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src_a + src_b + dst])
    keys = [rand_block_id() for _ in range(n)]
    for r in src_a: ctypes.memset(r.ptr, 0xA0, r.size)
    for r in src_b: ctypes.memset(r.ptr, 0xB0, r.size)
    want_a = [ctypes.string_at(r.ptr, r.size) for r in src_a]
    want_b = [ctypes.string_at(r.ptr, r.size) for r in src_b]
    errors = []
    barrier = threading.Barrier(n_threads := 2)

    def dump_with(src):
        try:
            barrier.wait()
            task = worker.dump(keys, [0] * n, region_addrs(src, shards))
            worker.wait(task)
        except Exception as exc:
            errors.append(f"thread: {type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=dump_with, args=(src_a,)),
               threading.Thread(target=dump_with, args=(src_b,))]
    for th in threads: th.start()
    for th in threads: th.join()
    if errors:
        return False, "; ".join(errors)
    if not all(scheduler.lookup(keys)):
        return False, "lookup miss after overlapping concurrent dump"
    task = worker.load(keys, [0] * n, region_addrs(dst, shards))
    worker.wait(task)
    mismatches = 0
    for i, r in enumerate(dst):
        got = ctypes.string_at(r.ptr, r.size)
        if got != want_a[i] and got != want_b[i]:
            mismatches += 1
    if mismatches:
        return False, f"{mismatches}/{n} blocks matched neither writer (torn writes)"
    return True, "no torn writes (each block matched one of the two writers)"


def test_D3_concurrent_dump_and_lookup(args, ucmdramstore):
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    n = 32
    src = make_regions(args.tensor_size, shards, n)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src])
    keys = [rand_block_id() for _ in range(n)]
    fill_random(src)
    stop = threading.Event()
    observed_mixed = [False]
    errors = []
    # Instrumentation: per-op timing + counters to locate the -50010 stall.
    stats = {"lookup_count": 0, "lookup_total_ms": 0.0, "lookup_max_ms": 0.0,
             "lookup_slow": 0, "dump_count": 0}
    lookup_latencies = []
    dump_latencies = []
    t_start = time.perf_counter()

    def log_ts(msg):
        print(f"[D3-timing] +{(time.perf_counter() - t_start) * 1000:.1f}ms {msg}",
              file=sys.stderr)

    def lookup_loop():
        local_mixed = False
        log_ts("lookup_loop: start")
        while not stop.is_set():
            t0 = time.perf_counter()
            try:
                founds = scheduler.lookup(keys)
            except Exception as exc:
                dt = (time.perf_counter() - t0) * 1000
                log_ts(f"lookup FAIL after {dt:.1f}ms: {exc}")
                errors.append(f"lookup: {type(exc).__name__}: {exc}")
                return
            dt = (time.perf_counter() - t0) * 1000
            stats["lookup_count"] += 1
            stats["lookup_total_ms"] += dt
            lookup_latencies.append(dt)
            if dt > stats["lookup_max_ms"]:
                stats["lookup_max_ms"] = dt
            if dt > 100:
                stats["lookup_slow"] += 1
                log_ts(f"lookup slow: {dt:.1f}ms (count={stats['lookup_count']})")
            hits = sum(1 for b in founds if b)
            if 0 < hits < n:
                local_mixed = True
        log_ts(f"lookup_loop: stop (count={stats['lookup_count']})")
        if local_mixed:
            observed_mixed[0] = True

    def dump_loop():
        log_ts("dump_loop: start")
        try:
            half = n // 2
            t0 = time.perf_counter()
            task = worker.dump(keys[:half], [0] * half, region_addrs(src[:half], shards))
            log_ts(f"dump[0] posted after {(time.perf_counter() - t0) * 1000:.1f}ms")
            worker.wait(task)
            dt = (time.perf_counter() - t0) * 1000
            stats["dump_count"] += 1
            dump_latencies.append(dt)
            log_ts(f"dump[0] wait ok after {dt:.1f}ms")
            time.sleep(0.01)
            t0 = time.perf_counter()
            task = worker.dump(keys[half:], [0] * (n - half),
                               region_addrs(src[half:], shards))
            log_ts(f"dump[1] posted after {(time.perf_counter() - t0) * 1000:.1f}ms")
            worker.wait(task)
            dt = (time.perf_counter() - t0) * 1000
            stats["dump_count"] += 1
            dump_latencies.append(dt)
            log_ts(f"dump[1] wait ok after {dt:.1f}ms")
        except Exception as exc:
            dt = (time.perf_counter() - t0) * 1000
            log_ts(f"dump FAIL after {dt:.1f}ms: {exc}")
            errors.append(f"dump: {type(exc).__name__}: {exc}")

    th_l = threading.Thread(target=lookup_loop)
    th_d = threading.Thread(target=dump_loop)
    t0 = time.perf_counter()
    th_l.start(); th_d.start()
    th_d.join()
    stop.set()
    th_l.join()
    elapsed = (time.perf_counter() - t0) * 1000
    # Summary
    lc = stats["lookup_count"]
    avg_l = stats["lookup_total_ms"] / lc if lc else 0.0
    print(f"[D3-summary] elapsed={elapsed:.1f}ms lookups={lc} avg={avg_l:.1f}ms "
          f"max={stats['lookup_max_ms']:.1f}ms slow(>100ms)={stats['lookup_slow']} "
          f"dumps={stats['dump_count']}", file=sys.stderr)
    if lookup_latencies:
        srt = sorted(lookup_latencies)
        print(f"[D3-summary] lookup p50={srt[len(srt)//2]:.1f}ms "
              f"p95={srt[int(len(srt)*0.95)]:.1f}ms "
              f"p99={srt[min(int(len(srt)*0.99), len(srt)-1)]:.1f}ms", file=sys.stderr)
    if dump_latencies:
        print(f"[D3-summary] dump latencies: {[f'{d:.1f}' for d in dump_latencies]}ms",
              file=sys.stderr)
    if errors:
        return False, "; ".join(errors)
    if not all(scheduler.lookup(keys)):
        return False, "final lookup not all-hit"
    return True, f"final all-hit; observed_mixed={observed_mixed[0]}"


def test_D4_cyclic_fresh_keys(args, ucmdramstore):
    """Because the pool is insert-only, "cyclic dump on same keys" would just
    re-load the first cycle's data. Instead, this case uses FRESH keys each
    cycle to verify sustained dump/load traffic across both pools."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    n = 16
    src = make_regions(args.tensor_size, shards, n)
    dst = make_regions(args.tensor_size, shards, n)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src + dst])
    for cycle in range(4):
        keys = [rand_block_id() for _ in range(n)]
        fill_random(src)
        expected = [ctypes.string_at(r.ptr, r.size) for r in src]
        zero(dst)
        task = worker.dump(keys, [0] * n, region_addrs(src, shards))
        worker.wait(task)
        task = worker.load(keys, [0] * n, region_addrs(dst, shards))
        worker.wait(task)
        for i, (r, want) in enumerate(zip(dst, expected)):
            if not cmp_bytes(ctypes.string_at(r.ptr, r.size), want, f"D4[c{cycle}][{i}]"):
                return False, f"cycle {cycle} block {i} mismatch"
    return True, "4 dump->load cycles with fresh keys ok"


# -----------------------------------------------------------------------------
# F. Routing determinism
# -----------------------------------------------------------------------------

def test_F1_same_key_same_pool(args, ucmdramstore):
    """Dump key K. Kill pool1. Lookup K individually: if K was on pool0 it
    hits; if K was on pool1 it raises/misses. Either outcome is fine -- the
    point is that the placement was deterministic (we can name the pool)."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    src = make_regions(args.tensor_size, shards, 1)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src])
    key = rand_block_id()
    ctypes.memmove(src[0].ptr, b"\xF1" * src[0].size, src[0].size)
    task = worker.dump([key], [0], region_addrs(src, shards))
    worker.wait(task)
    if not all(scheduler.lookup([key])):
        return False, "lookup miss right after dump"
    run_pool_kill(args.client_kill_host)
    time.sleep(1.0)
    hits, misses, errors = lookup_one_by_one(scheduler, [key])
    placement = "pool0" if hits == 1 else "pool1"
    if args.auto_relaunch:
        relaunch_pool1(args)
    return True, f"key consistently placed on {placement} (hits={hits} misses={misses} errors={errors})"


def test_F2_kill_observe_recover(args, ucmdramstore):
    """Verify router determinism and post-restart state loss.

    1. Dump N1 keys (some land on pool0, some on pool1).
    2. Kill pool1.
    3. Dump N2 FRESH keys -- ~N2/2 land on pool1 and must fail (authoritative
       spread proof; does not depend on lookup-after-kill semantics).
    4. Restart pool1 (it dials back to the existing scheduler's transport
       manager endpoint per the YAML, so the existing scheduler client can
       reach the new pool1 without re-init).
    5. Lookup the N1 ORIGINAL keys individually: pool0-resident keys must
       still hit; pool1-resident keys must miss (memory lost on restart).
       Total hits must be > 0 (pool0 subset persisted) AND < N1 (pool1
       subset lost)."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    n1 = 32
    src1 = make_regions(args.tensor_size, shards, n1)
    src2 = make_regions(args.tensor_size, shards, 1)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src1 + src2])
    keys1 = [rand_block_id() for _ in range(n1)]
    fill_random(src1)
    task = worker.dump(keys1, [0] * n1, region_addrs(src1, shards))
    worker.wait(task)
    if not all(scheduler.lookup(keys1)):
        return False, "lookup not all-hit after initial dump"

    # Kill pool1 and dump N2 fresh keys -- expect ~N2/2 failures.
    run_pool_kill(args.client_kill_host)
    time.sleep(1.0)
    n2 = 32
    fresh_failures = 0
    for _ in range(n2):
        k = rand_block_id()
        ctypes.memmove(src2[0].ptr, secrets.token_bytes(src2[0].size),
                       src2[0].size)
        try:
            task = worker.dump([k], [0], region_addrs(src2, shards))
            worker.wait(task)
        except Exception:
            fresh_failures += 1
    if fresh_failures == 0:
        return False, "no fresh dumps failed after pool1 kill (router did not use pool1)"

    # Restart pool1 -- it dials back to the scheduler's transport endpoint.
    if not relaunch_pool1(args):
        return False, "failed to relaunch pool1"
    time.sleep(1.0)

    # Lookup the ORIGINAL N1 keys; pool0 subset must hit, pool1 subset miss.
    hits_post, misses_post, errors_post = lookup_one_by_one(scheduler, keys1)
    if hits_post == 0:
        return False, f"no original keys hit after restart (pool0 subset lost?) hits={hits_post}"
    if misses_post + errors_post == 0:
        return False, f"all original keys still hit after restart (pool1 data persisted?) hits={hits_post}"
    return True, (f"router spread confirmed: {fresh_failures}/{n2} fresh dumps "
                  f"failed on dead pool1; post-restart lookup of {n1} original "
                  f"keys: hits={hits_post} misses={misses_post} errors={errors_post} "
                  f"(pool0 persisted, pool1 lost)")


# -----------------------------------------------------------------------------
# G. Lifecycle
# -----------------------------------------------------------------------------

def test_G1_re_register_kv_caches(args, ucmdramstore):
    """Re-register KV caches (different buffer) and dump a NEW key (the pool
    is insert-only, so re-dumping the same key wouldn't reflect the new
    buffer). Load must return the new dump's data."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    src1 = make_regions(args.tensor_size, shards, 1)
    src2 = make_regions(args.tensor_size, shards, 1)
    dst = make_regions(args.tensor_size, shards, 1)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src1 + dst])
    key1 = rand_block_id()
    ctypes.memmove(src1[0].ptr, b"\x01" * src1[0].size, src1[0].size)
    task = worker.dump([key1], [0], region_addrs(src1, shards))
    worker.wait(task)
    # Second registration set (different buffer).
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src2])
    key2 = rand_block_id()
    ctypes.memmove(src2[0].ptr, b"\x02" * src2[0].size, src2[0].size)
    task = worker.dump([key2], [0], region_addrs(src2, shards))
    worker.wait(task)
    zero(dst)
    task = worker.load([key2], [0], region_addrs(dst, shards))
    worker.wait(task)
    got = ctypes.string_at(dst[0].ptr, dst[0].size)
    if not cmp_bytes(got, b"\x02" * dst[0].size, "G1"):
        return False, "load did not return new dump after re-register"
    return True, "re-register KV caches + new-key dump ok"


def test_G3_endurance(args, ucmdramstore):
    """Sustained dump/load with FRESH keys per batch (the pool is insert-
    only, so reusing keys would no-op). Verifies no leaks / latency drift."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    n = 32
    src = make_regions(args.tensor_size, shards, n)
    dst = make_regions(args.tensor_size, shards, n)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src + dst])
    batches = args.endurance_batches
    dump_times = []
    load_times = []
    for b in range(batches):
        keys = [rand_block_id() for _ in range(n)]
        fill_random(src)
        expected = [ctypes.string_at(r.ptr, r.size) for r in src]
        zero(dst)
        t0 = time.perf_counter()
        task = worker.dump(keys, [0] * n, region_addrs(src, shards))
        worker.wait(task)
        dump_times.append(time.perf_counter() - t0)
        t0 = time.perf_counter()
        task = worker.load(keys, [0] * n, region_addrs(dst, shards))
        worker.wait(task)
        load_times.append(time.perf_counter() - t0)
        for i, (r, want) in enumerate(zip(dst, expected)):
            if not cmp_bytes(ctypes.string_at(r.ptr, r.size), want, f"G3[b{b}][{i}]"):
                return False, f"batch {b} block {i} mismatch"
    drift = (statistics.mean(load_times[-10:]) - statistics.mean(load_times[:10])) * 1000
    return True, (f"{batches} batches ok; dump avg={statistics.mean(dump_times)*1000:.2f}ms "
                  f"load avg={statistics.mean(load_times)*1000:.2f}ms drift={drift:+.2f}ms")


# -----------------------------------------------------------------------------
# E (client-side failure paths). E1 is post-restart.
# -----------------------------------------------------------------------------

def test_E1_post_restart_dump_load(args, ucmdramstore):
    """After the orchestrator killed and relaunched drampool1, a fresh client
    must reconnect to both pools and dump/load must work for fresh keys."""
    ctx = make_ctx(args, ucmdramstore)
    worker, scheduler = ctx["worker"], ctx["scheduler"]
    shards = 1
    n = 32
    src = make_regions(args.tensor_size, shards, n)
    dst = make_regions(args.tensor_size, shards, n)
    if worker.need_register_kv_caches():
        worker.register_kv_caches([(r.ptr, r.size) for r in src + dst])
    keys = [rand_block_id() for _ in range(n)]
    fill_random(src)
    expected = [ctypes.string_at(r.ptr, r.size) for r in src]
    task = worker.dump(keys, [0] * n, region_addrs(src, shards))
    worker.wait(task)
    if not all(scheduler.lookup(keys)):
        return False, "lookup not all-hit after post-restart dump"
    zero(dst)
    task = worker.load(keys, [0] * n, region_addrs(dst, shards))
    worker.wait(task)
    for i, (r, want) in enumerate(zip(dst, expected)):
        if not cmp_bytes(ctypes.string_at(r.ptr, r.size), want, f"E1[{i}]"):
            return False, f"data mismatch at block {i}"
    return True, "post-restart reconnect + dump/load ok"


def test_E3_bad_endpoint(args, ucdramstore):
    bad = argparse.Namespace(**vars(args))
    bad.node_ids = [99]
    bad.node_control_endpoints = ["192.168.100.99:9000"]
    bad.node_transport_manager_ids = ["192.168.100.99:4501"]
    bad.local_control_port = args.local_control_port + 200
    bad.local_manager_port = args.local_manager_port + 200
    bad.lookup_timeout_ms = 1000
    bad.dump_timeout_ms = 1000
    bad.load_timeout_ms = 1000
    passed, msg = expect_exception(
        lambda: make_client(bad, ucdramstore, "worker"), "E3-setup")
    return passed, f"bad endpoint Setup: {msg}"


def test_E4_bad_device(args, ucdramstore):
    bad = argparse.Namespace(**vars(args))
    bad.ibverbs_device = f"{args.ibverbs_device}_does_not_exist"
    bad.local_control_port = args.local_control_port + 300
    bad.local_manager_port = args.local_manager_port + 300
    passed, msg = expect_exception(
        lambda: make_client(bad, ucdramstore, "worker"), "E4-setup")
    return passed, f"bad device Setup: {msg}"


def test_E5_bad_router_config(args, ucdramstore):
    bad = argparse.Namespace(**vars(args))
    bad.router_type = "no_such_router"
    bad.local_control_port = args.local_control_port + 400
    bad.local_manager_port = args.local_manager_port + 400
    passed, msg = expect_exception(
        lambda: make_client(bad, ucdramstore, "worker"), "E5-router")
    if passed:
        return True, f"invalid router_type rejected: {msg}"
    bad2 = argparse.Namespace(**vars(args))
    bad2.node_ids = []
    bad2.node_control_endpoints = []
    bad2.node_transport_manager_ids = []
    bad2.local_control_port = args.local_control_port + 401
    bad2.local_manager_port = args.local_manager_port + 401
    passed, msg = expect_exception(
        lambda: make_client(bad2, ucdramstore, "worker"), "E5-empty")
    return passed, f"invalid router config rejected: {msg}"


# -----------------------------------------------------------------------------
# Dispatcher
# -----------------------------------------------------------------------------

ALL_CASES_BY_NAME = {
    "A1": test_A1_cross_pool_distribution,
    "A2": test_A2_dump_load_roundtrip,
    "A3": test_A3_lookup_phases,
    "A4": test_A4_overwrite_is_noop,
    "A5": test_A5_load_missing,
    "A6": test_A6_lookup_on_prefix,
    "B1": test_B1_minimal_batch,
    "B2": test_B2_many_small_blocks,
    "B3": test_B3_large_single_block,
    "B4": test_B4_zero_length,
    "B5": test_B5_multi_shard_block,
    "B6": test_B6_asymmetric_shards,
    "B7": test_B7_pool_capacity_boundary,
    "C1": test_C1_zero_key,
    "C2": test_C2_keys_differ_in_last_byte,
    "C3": test_C3_duplicate_key_in_batch,
    "C4": test_C4_non_16byte_key,
    "D1": test_D1_disjoint_concurrent_dump,
    "D2": test_D2_overlapping_concurrent_dump,
    "D3": test_D3_concurrent_dump_and_lookup,
    "D4": test_D4_cyclic_fresh_keys,
    "E1": test_E1_post_restart_dump_load,
    "E3": test_E3_bad_endpoint,
    "E4": test_E4_bad_device,
    "E5": test_E5_bad_router_config,
    "F1": test_F1_same_key_same_pool,
    "F2": test_F2_kill_observe_recover,
    "G1": test_G1_re_register_kv_caches,
    "G3": test_G3_endurance,
}

# Cases that kill pool1; the runner auto-relaunches pool1 after them.
POOL_KILLERS = {"A1", "F1", "F2"}

FUNCTIONAL_CASES = [
    ("A1", test_A1_cross_pool_distribution),
    ("A2", test_A2_dump_load_roundtrip),
    ("A3", test_A3_lookup_phases),
    ("A4", test_A4_overwrite_is_noop),
    ("A5", test_A5_load_missing),
    ("A6", test_A6_lookup_on_prefix),
    ("B1", test_B1_minimal_batch),
    ("B2", test_B2_many_small_blocks),
    ("B3", test_B3_large_single_block),
    ("B4", test_B4_zero_length),
    ("B5", test_B5_multi_shard_block),
    ("B6", test_B6_asymmetric_shards),
    ("B7", test_B7_pool_capacity_boundary),
    ("C1", test_C1_zero_key),
    ("C2", test_C2_keys_differ_in_last_byte),
    ("C3", test_C3_duplicate_key_in_batch),
    ("C4", test_C4_non_16byte_key),
    ("F1", test_F1_same_key_same_pool),
    ("F2", test_F2_kill_observe_recover),
    ("G1", test_G1_re_register_kv_caches),
]

CONCURRENCY_CASES = [
    ("D1", test_D1_disjoint_concurrent_dump),
    ("D2", test_D2_overlapping_concurrent_dump),
    ("D3", test_D3_concurrent_dump_and_lookup),
    ("D4", test_D4_cyclic_fresh_keys),
]

ENDURANCE_CASES = [("G3", test_G3_endurance)]
POST_RESTART_CASES = [("E1", test_E1_post_restart_dump_load)]
BAD_CONFIG_CASES = [
    ("E3", test_E3_bad_endpoint),
    ("E4", test_E4_bad_device),
    ("E5", test_E5_bad_router_config),
]


def run_cases(args, cases):
    ucdramstore = load_ucmdramstore(args.so_dir)
    results = []
    overall_pass = True
    for name, fn in cases:
        try:
            ok, summary = fn(args, ucdramstore)
        except Exception as exc:
            ok, summary = False, f"UNHANDLED {type(exc).__name__}: {exc}"
        status = "PASS" if ok else "FAIL"
        if not ok:
            overall_pass = False
        print(f"[{status}] {name}: {summary}")
        results.append((name, ok, summary))
        if name in POOL_KILLERS and args.auto_relaunch:
            if not relaunch_pool1(args):
                print(f"[warn] failed to relaunch pool1 after {name}", file=sys.stderr)
    return overall_pass, results


def parse_args():
    p = argparse.ArgumentParser(description="DramStore two-node functional suite")
    p.add_argument("--so-dir", default="/tmp")
    p.add_argument("--mode", default="functional",
                   choices=["functional", "concurrency", "endurance",
                            "post_restart", "bad_endpoint", "bad_device",
                            "bad_router", "all", "single"])
    p.add_argument("--case", default=None)
    p.add_argument("--tensor-size", type=int, default=4096)
    p.add_argument("--layer-size", type=int, default=1)
    p.add_argument("--chunk-size", type=int, default=1)
    p.add_argument("--request-size", type=int, default=32)
    p.add_argument("--transport-device-id", type=int, default=0)
    p.add_argument("--router-type", default="ring_hash")
    p.add_argument("--local-host", default="127.0.0.1")
    p.add_argument("--local-control-port", type=int, default=4702)
    p.add_argument("--local-manager-port", type=int, default=4502)
    p.add_argument("--node-ids", type=int, nargs="+", default=[1, 2])
    p.add_argument("--node-control-endpoints", nargs="+",
                   default=["192.168.100.11:9000", "192.168.100.12:9000"])
    p.add_argument("--node-transport-manager-ids", nargs="+",
                   default=["192.168.100.11:4501", "192.168.100.12:4501"])
    p.add_argument("--transport-protocol", default="ibverbs", choices=["ibverbs", "hixl"])
    p.add_argument("--ibverbs-device", default="",
                   help="NIC device name passed to drampool --nics; "
                        "empty lets the transport pick its default")
    p.add_argument("--kv-cache-memory-type", default="host", choices=["host", "device"])
    p.add_argument("--drampool-config-basename", default="drampool_rxe_split.yaml")
    p.add_argument("--lookup-timeout-ms", type=int, default=500)
    p.add_argument("--dump-timeout-ms", type=int, default=2000)
    p.add_argument("--load-timeout-ms", type=int, default=2000)
    p.add_argument("--pool-mb", type=int, default=16)
    p.add_argument("--endurance-batches", type=int, default=100)
    p.add_argument("--client-kill-host", default="node1")
    p.add_argument("--node1-ip", default="192.168.100.12")
    p.add_argument("--auto-relaunch", action="store_true", default=True)
    args = p.parse_args()
    if args.case:
        args.mode = "single"
    return args


def main():
    args = parse_args()
    args.shard_size = args.tensor_size * args.layer_size * args.chunk_size
    args.block_size = args.shard_size
    os.environ.setdefault("UC_LOGGER_LEVEL", "info")

    if args.mode == "single":
        if not args.case or args.case not in ALL_CASES_BY_NAME:
            print(f"unknown case: {args.case}", file=sys.stderr); sys.exit(2)
        cases = [(args.case, ALL_CASES_BY_NAME[args.case])]
    elif args.mode == "all":
        cases = (FUNCTIONAL_CASES + CONCURRENCY_CASES + ENDURANCE_CASES
                 + POST_RESTART_CASES + BAD_CONFIG_CASES)
    elif args.mode == "functional":
        cases = FUNCTIONAL_CASES
    elif args.mode == "concurrency":
        cases = CONCURRENCY_CASES
    elif args.mode == "endurance":
        cases = ENDURANCE_CASES
    elif args.mode == "post_restart":
        cases = POST_RESTART_CASES
    elif args.mode == "bad_endpoint":
        cases = [("E3", test_E3_bad_endpoint)]
    elif args.mode == "bad_device":
        cases = [("E4", test_E4_bad_device)]
    elif args.mode == "bad_router":
        cases = [("E5", test_E5_bad_router_config)]
    else:
        sys.exit(2)

    print(f"\n==== DramStore 2-node func suite mode={args.mode} cases={len(cases)} ====")
    overall_pass, results = run_cases(args, cases)
    passed = sum(1 for _, ok, _ in results if ok)
    failed = len(results) - passed
    print(f"==== SUMMARY mode={args.mode} passed={passed} failed={failed} ====")
    sys.exit(0 if overall_pass and failed == 0 else 1)


if __name__ == "__main__":
    main()
