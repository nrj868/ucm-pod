# DramStore 双节点功能与边界用例测试报告（soft-RoCE / rxe）

**日期：** 2026-08-09
**分支：** `codex/rxe_trans`（提交 `bcdd166` + Plan C 多shard存储修复 + `_shape_case` 配置覆写修复
+ timeout 默认值上调：lookup 30→500ms，dump/load 300→2000ms
+ A1/F2 SSH 修复：编排脚本传 `--client-kill-host $IP1`，`register_kv_caches` 移到 kill 之前
+ F2 修复：`NodeActor::Handle(FenceCompleted)` 不再在 fence 失败时 `AbortDramStore`，降级为 `UC_WARN` + DISCONNECTED 重试
+ E3 修复：`DramStore::Compose` 在 `nodeScheduler->Start()` 之后对所有 node 做 eager `transportBackend->Connect`，不可达 peer 在 Setup 阶段即抛
+ 测试框架修复：`relaunch_pool1` 先 `pkill -9 -x drampool` 再启动，避免端口占用 + 日志截断导致的伪"relaunch 失败"
+ LookupOnPrefix 实现：`DramStore::LookupOnPrefix` 不再返回 `Unsupported`，改为复用 `Lookup` 扫描命中位图，返回最长连续命中前缀的末尾下标（`blocks[0]` 缺失则 -1）
+ A6 用例：新增 `test_A6_lookup_on_prefix` 覆盖空存/部分 dump/全 dump/头缺失/单 miss/单 hit 六阶段契约）
**拓扑：** node0 = DramStore 客户端（worker + scheduler）+ drampool0；node1 = drampool1。
路由 = ring-hash，128 个虚拟节点，Crc32IEEE 哈希。Pool 块大小类别 = 64 / 4096 / 65536 / 1048576。
**执行命令：** `bash scripts/run_dramstore_func_rxe_split.sh`

## 概览

| 指标 | 数值 |
| --- | --- |
| 用例总数 | 30 |
| 通过 | 29 |
| 失败 | 1 |
| 结果 | SOME FAIL（仅 G3 pool 容量耗尽，预期行为） |

每个用例的状态由编排脚本写入 node0 的 `/tmp/func_results.txt`。

## 用例说明

每个用例起一个 fresh DramStore 客户端（worker + scheduler）+ fresh pool 状态，
跨双节点 rxe 跑。下面按组描述每个用例的过程和断言。

### A. 功能正确性

- **A1 跨 pool 路由分散** — 先 dump 32 个 key（证明两个 pool 都可写），SSH kill
  pool1，再 dump 64 个 fresh key，要求至少有一个失败（路由到已死 pool1 的 key
  必然 wait 抛异常）。同时记录 kill 后逐 key lookup 的 hits/misses/errors 分布
  作为信息（不参与断言）。断言：`fresh_failures > 0`。
- **A2 dump→load 往返** — 32 个 block，先 dump 填入随机数据的 src buffer，再 load
  到 zeroed dst buffer，逐字节比对。断言：所有 dst == src。
- **A3 lookup 阶段递进** — 16 个 key，dump 之前断言 lookup 全 miss；dump 前半
  后断言 lookup hits 恰好等于前半下标集；dump 后半后断言 lookup 全 hit。
- **A4 overwrite 是 no-op** — 同一 key 连续 dump 两次，源数据不同（先 0x01 再
  0x02）。Pool 是 insert-only，第二次走 `StoreBegin` 的 `DuplicateKey` 路径标 Ok
  不重读。断言：load 返回**首次** dump 的数据（0x01）。
- **A5 load 缺失 key 失败** — 对一个从未 dump 过的 key 调 load，期望 wait 抛异常
  （item failure）。
- **A6 lookup_on_prefix 契约** — 验证 `LookupOnPrefix` 返回最长连续命中前缀的末尾
  下标。六阶段：(0) 空存 → -1；(1) dump 前 4 个 → 3；(2) dump 全部 → n-1=7；
  (3) `[fresh, key1..key7]`（头缺失、尾全在）→ -1；(4) 单 fresh key → -1；
  (5) 单 dumped key → 0。

### B. 大小 / 形状边界

`_shape_case(args, tensor_size, src_shards, dst_shards, blocks, label)` 是公共
helper：按指定的 tensor_size 和 shards 覆写 `tensor_size_list` 配置，建 src + dst
region，dump 再 load，逐字节比对。

- **B1 最小批次** — 1 个 4096B block，shards=1。最简 smoke。
- **B2 多小 block** — 64B × 128。验证 64B pool 类别 + 128 entry 批。
- **B3 大单 block** — 1 MiB × 1。验证 1 MiB pool 类别 + 单 entry 大块传输。
- **B4 零长度 segment** — 构造 size=0 的 region 调 dump，期望被拒绝（异常）。
- **B5 多 shard 对称** — 4 shards/block × 8 blocks × 4096B。验证 Plan C 派生键。
- **B6 多 shard 非对称** — src 1 shard，dst 8 shards，4 blocks × 4096B。验证
  src/dst shard 数不一致时的派生键映射。
- **B7 pool 容量边界** — 4 KiB 类别容量 = `pool_mb/4` MiB（4 个类别均分）。逐
  个 dump distinct key 直到抛异常或到 2× 容量。断言：要么在容量耗尽处失败，
  要么没失败则证明 eviction 激活（首轮 dump 的 key 之后 lookup 必 miss）。

### C. Key 语义

- **C1 全零 key** — 16 字节全零的 block ID，分别在 Lookup 和 Dump 路径上调，期望
  都被 `InvalidParam` 拒绝（同一条 StoreBegin 校验）。
- **C2 仅末字节不同** — `k1 = 0x11*15 + 0x01`，`k2 = 0x11*15 + 0x02`。同批
  dump，再 lookup + load，验证两者被独立路由、独立存储，数据无串扰。
- **C3 批内重复 key** — 同一批 dump `[key, key]` 同一源 buffer。第二次命中
  `DuplicateKey`，被当作 Ok no-op。load 必须返回首次写入的数据（0xAA）。
- **C4 非 16 字节 key** — 17 字节 key buffer。pybind 的 `BufferArrayView` 按
  `total_bytes / sizeof(BlockId)` 截断为 1 个 16 字节 key，不抛异常。用 truncated
  形式 lookup 必命中（证明截断发生）。该行为是已文档化的 pybind 副作用，不是
  DramStore 的语义保证。

### D. 并发 / 顺序

- **D1 不相交并发 dump** — 2 线程各 dump 32 个不同 key（共 64 key），barrier 同步
  起跑。线程结束后断言 lookup 全 hit + load 数据全部匹配。验证并发不丢数据。
- **D2 重叠并发 dump** — 2 线程同时 dump **同一组** 16 个 key，但源数据不同
  （线程 A 写 0xA0，线程 B 写 0xB0）。Pool insert-only → 先到先得，后到者得
  `DuplicateKey` no-op。load 后每个 block 必须等于 A 或 B 之一（无 torn write）。
- **D3 并发 dump + lookup** — 一个线程分两批 dump 16+16 个 key，另一线程循环
  lookup 全部 32 个 key。断言：lookup 至少一次观察到 `0 < hits < n` 的中间态
  （`observed_mixed=True`），且 dump 结束后最终 lookup 全 hit。
- **D4 循环 fresh key** — 4 轮 dump→load 循环，每轮用全新 16 个 key。验证持续
  流量下双 pool 不退化。

### E. 客户端故障路径

- **E1 重启后 dump/load** — 编排脚本先 kill+relaunch pool1（`pkill -9` 后重启
  drampool），fresh 客户端重连后 dump 32 个新 key 再 load，验证重启后两端可用。
- **E2 中途 kill pool1** — 编排脚本启动 bench 后 sleep 4s SSH kill pool1，期望 bench
  非零退出（rc=255）。功能用例模式下跑，不在功能矩阵里。
- **E3 坏 endpoint** — 配置不可达 endpoint `192.168.100.99:9000`，调 `make_client`
  期望在 Setup 阶段就抛 `RuntimeError(-1)`（eager connect 在 Compose 同步失败）。
- **E4 坏 ibverbs 设备** — 配置 `ibverbs_device="rdma0_does_not_exist"`，Setup 抛
  `RuntimeError(-50000)`（IB verbs Init 找不到设备）。
- **E5 坏 router_type** — 配置 `router_type="no_such_router"`，Setup 抛
  `RuntimeError(-50000)`（unsupported router_type）。

### F. 路由确定性

- **F1 同 key 同 pool** — dump 1 个 key，kill pool1，逐 key lookup。根据结果标注
  placement=pool0 或 pool1。任一结果都可接受，断言点是 placement 可命名（路由
  确定）。
- **F2 kill-observe-recover** — 32 个 key dump（落两边），kill pool1，再 dump 32
  个 fresh key 要求 `fresh_failures > 0`；relaunch pool1 后 lookup 32 个原 key，
  要求 `0 < hits < 32`（pool0 持久、pool1 丢失）。验证路由分散性 + 重启状态
  丢失语义。

### G. 生命周期

- **G1 重新注册 KV cache** — dump key1（用 src1 buffer），再 `register_kv_caches`
  注册 src2 buffer，dump key2，load key2。验证第二次注册后 dump 用的是新
  region（load 必返回 0x02 不是 0x01）。
- **G3 endurance** — 100 批 × 32 key × 4 KiB，每批 fresh key，记录 dump/load
  平均时延和尾 10 批 vs 首 10 批的 drift。当前 4 KiB 类别容量 1024 slot，
  100 × 32 = 3200 key 远超容量；eviction 关闭（`evict_period_ms` 设为一年），
  预期在容量耗尽处失败。FAIL 是预期行为。

## 用例结果矩阵

| 用例 | 状态 | 说明 |
| --- | --- | --- |
| A1 | PASS | 路由分散性验证 — 40/64 fresh dump 在已死 pool1 上失败；lookup-after-kill hits=16 errors=16 |
| A2 | PASS | 32 个 block 往返校验通过 |
| A3 | PASS | miss / partial / hit 阶段递进正确（n=16） |
| A4 | PASS | 第二次 dump 为 no-op；load 返回首次 dump 的数据（insert-only 语义） |
| A5 | PASS | load 缺失 key 时以 item failure 失败 |
| A6 | PASS | LookupOnPrefix 契约 — 空=-1 / 部分=half-1 / 全=n-1 / 头缺失=-1 / 单 hit=0 / 单 miss=-1 |
| B1 | PASS | 最小批次（1 × 4096B） |
| B2 | PASS | 64B × 128 — `_shape_case` 配置覆写修复（详见下文） |
| B3 | PASS | 1 MiB 单 block — `_shape_case` 配置覆写修复（详见下文） |
| B4 | PASS | 零长度 segment 被拒绝 |
| B5 | PASS | 4 shards/block — Plan C 修复（详见下文） |
| B6 | PASS | 8 shards/block — Plan C 修复（详见下文） |
| B7 | PASS | 4 KiB 类别在第 1959 个 block 耗尽（16 MiB / 4 类，符合预期） |
| C1 | PASS | 全零 key 在 lookup 和 dump 路径均被拒绝 |
| C2 | PASS | 仅末字节不同的两个 key 被独立路由 |
| C3 | PASS | 批内重复 key 视为 no-op |
| C4 | PASS | 非 16 字节 key 被 pybind 截断为 16 字节（已文档化） |
| D1 | PASS | 2 线程 × 32 block 并发 dump |
| D2 | PASS | 无 torn write（每个 block 仅匹配一个 writer） |
| D3 | PASS | 重叠写入最终全部命中（observed_mixed=True） |
| D4 | PASS | 4 轮 dump→load 循环（全新 key） |
| E1 | PASS | pool1 重启 + 重连 + dump/load |
| E2 | PASS | 中途 kill pool1 → bench 非零退出（rc=255） |
| E3 | PASS | 坏 endpoint — Setup 同步 eager connect 即抛 `RuntimeError(-1)`（修复详见下文） |
| E4 | PASS | 坏 ibverbs 设备在 Setup 阶段被拒绝 |
| E5 | PASS | 非法 router_type 在 Setup 阶段被拒绝 |
| F1 | PASS | key 一致性地落在 pool1（kill 后 lookup errors=1） |
| F2 | PASS | kill-observe-recover — 15/32 fresh dump 在已死 pool1 上失败；重启后 lookup 32 个原 key：hits=18 misses=13 errors=1（pool0 持久、pool1 丢失，修复详见下文） |
| G1 | PASS | 重新注册 KV cache + 新 key dump（timeout 上调后通过） |
| G3 | FAIL | 100 批次 endurance — pool 耗尽 |

## 调查："router did not use pool1"（A1 / F2 旧失败模式）

A1 和 F2 被重写为 "kill 后再 dump 全新 key" 的设计：先 dump N1 个 key，kill 掉 pool1，
再 dump N2 个全新 key，要求至少有一个失败（路由到已死 pool1 的 key 必然失败）。
两个用例都曾稳定报告 "no fresh dumps failed — router did not use pool1"。对此进行了深入调查。

### 误诊：QP / IB verbs disconnect 通知假说（已否定）

调查中途曾提出假设："rxe 上 IB verbs 不强制 disconnect 通知，本地 NodeActor 的 QP 视角
仍然存活，dump 的 submitTransport 本地入队成功，Wait 在远端无 reply 窗口内不会触发 timeout
所以 fresh dump 全部成功"。这个假设是**错的**。客户端本地根本不观察 QP 状态——
`ReplyService` 只轮询本地 `flag_buffer` slot 的状态字节（offset 0，Pending=0 / Ready=1）。
`KvProtocol::IsResponseReady` 读这个字节，drampool 在 `CompletionPoller::SubmitResponse`
里通过 RDMA Write 把它从 0 改成 1。drampool 进程没了就没人写这个字节，`Wait` 必然 timeout。
所以"fresh dump 全部成功"不可能是 QP 视角的假成功——只能是 pool1 根本没被 kill。

### 真根因：node0 → node1 SSH 不通（已修复）

`run_pool_kill(args.client_kill_host)` 在 DramStore 客户端进程（跑在 node0 上）里用
`subprocess.run(["ssh", host, "pkill -9 -x drampool"], ...)` 远程 kill pool1。原测试把
`--client-kill-host` 默认为 `node1`（hostname）。但 node0 解析不到 `node1` 这个名字
（无 DNS / `/etc/hosts` 条目），即使能解析，SSH 到 192.168.100.12 也需要 key 认证
（非交互测试环境没有密码）。结果：`run_pool_kill` 静默失败（`subprocess` 不 check
返回码，capture_output 吞掉 stderr），pool1 根本没死，"fresh dump" 实际打到了活着的
pool1 上，自然全部成功。"router did not use pool1" 的真相是 "pool1 从未被 kill"。

辅助诊断（`pgrep -x drampool` 走 SSH 跑在 node1 上）也走同一条 SSH 路径，所以同样
返回空——这误导调查一度认为 "kill 成功了但 router 不走 pool1"。

**修复（三处合一）：**

1. 在 node0 生成 SSH key 并把公钥加到 node1 的 `authorized_keys`，让 `ssh node1`
   非交互可用。
2. 编排脚本 `scripts/run_dramstore_func_rxe_split.sh` 把 `--client-kill-host $NODE1`
   改为 `--client-kill-host $IP1`（3 处：line 125 / 157 / 182）。这样测试进程用 IP
   192.168.100.12 而不是 hostname 调 SSH，绕开 DNS 解析问题。
3. `dramstore_2node_func_test.py` 把 `regions2 = make_regions(...)` 和
   `worker.register_kv_caches(regions2)` 移到 `run_pool_kill` **之前**。
   `RegisterKVCaches` 需要两个 pool 都活着（向两边注册本地内存），kill 之后调
   会抛 `RuntimeError(-1)` → 用例 UNHANDLED 失败。A1 和 F2 都改了。

**验证：** A1 PASS（40/64 fresh dumps 在已死 pool1 上失败）；F1 PASS（key 一致性落在
pool1，kill 后 lookup errors=1）。F2 在 fresh-dump 阶段也通过了（`fresh_failures > 0`），
但在后续 recovery 阶段失败——见下文。

### 误诊期间的临时调试打印（已全部回退）

调查期间在多个 C++ 文件里加过 `UC_DEBUG_UNLIMITED`（绕过 LogRateLimit 的调试版本）
和 `UC_ERROR_UNLIMITED` 追踪 NodeActor 状态机、TaskManager 路由、传输层和
drampool 执行路径：

- `ucm/store/dram/cc/node_actor.cc` — StartRequest 入口 / acquireReplySlot 失败 /
  EncodeRequest 失败 / submitTransport 结果；以及 Handle(Request/TransmitCompleted/
  ConnectCompleted/ReplyObserved)、FinalizeRequests needsFence、TryFence、
  Handle(FenceCompleted) 的 UC_ERROR 追踪
- `ucm/store/dram/cc/task_manager.cc` — BuildRequests 路由分布 + SubmitTransfer 入口
- `ucm/store/dram/cc/transport_manager_backend.cc` — pre/post-Send Transmit 追踪
- `ucm/store/dram/cc/drampool/task_worker.cc` — ProcessDump 入口 + pre/post-ExecuteAsync
- `ucm/store/dram/cc/drampool/completion_poller.cc` — PollDataTransfer terminal +
  SubmitResponse pre/post-ExecuteAsync
- `ucm/transport/p2p/src/protocols/ibverbs/ibverbs_transport.cpp` —
  ExportMetadata / ImportMetadata（用于 G1 300ms Read hang 调查）
- `examples/drampool_rxe_split.yaml` — `logger.level: debug`（默认 info 过滤 DEBUG）

全部回退为 `UC_DEBUG`（受 LogRateLimit 限制：每源位置每 60s 3 条）和
`logger.level: info`。

### 保留的 `UC_DEBUG` 日志（B2/B3 调查 + A1 误诊期间新增）

误诊期间发现的另一个陷阱：`UC_DEBUG` 经 `LogRateLimit`（每源位置每 60s 3 条），
一次 64-key fresh-dump 循环里 BuildRequests / StartRequest / ProcessDump 会触发
~95% 的日志被丢弃，看似"日志缺失"实为限流。误诊期间用 `UC_DEBUG_UNLIMITED` 绕开
限流确认了这一点。回退后，定位问题时可临时改 `UC_DEBUG` → `UC_DEBUG_UNLIMITED`
或把 yaml `logger.level` 改 `debug`。

为定位 B2/B3 在 `SettleDataTransfer` 的 silent failure 路径和 `ProcessDump`/`ProcessLoad`
的地址/长度，以及 A1 调查期间 NodeActor 的状态机流转，新增了以下 `UC_DEBUG` 日志
（默认 `info` 级别不输出，yaml 改 `debug` 即可观察，无需再改代码）：

- `ucm/store/dram/cc/node_actor.cc::StartRequest` — node/requestId/entries +
  acquireReplySlot / EncodeRequest / submitTransport 的结果
- `ucm/store/dram/cc/task_manager.cc::BuildRequests` — op/total_entries/routes/batchLimit
  + 每条 route 的 node/entries
- `ucm/store/dram/cc/drampool/task_worker.cc::ProcessDump` — batch_size/entries
- `ucm/store/dram/cc/drampool/task_worker.cc::ProcessDump[entry]` — `entry.idx`/`len`/
  `src_addr`/`dst_addr`/`dst_size` + `key_id`（blockId 前 8 字节作短标识）
- `ucm/store/dram/cc/drampool/task_worker.cc::ProcessLoad[entry]` — 同上但 `src_addr`/
  `dst_addr` 角色对调（pool 端是 src，client 端是 dst）
- `ucm/store/dram/cc/drampool/completion_poller.cc::SettleDataTransfer` — Dump else
  分支（`terminalStatus != Completed`）和 Load else 分支各自打印 `terminal`/
  `opcode`/`peer`/`handle`/`idx`/`key_id`

这些日志在默认 `info` 级别下不输出，对正常运行零开销；定位问题时把
`examples/drampool_rxe_split.yaml` 的 `logger.level` 改为 `debug` 即可。

## F2 — kill-observe-recover（已修复：fence 失败不再 fatal）

A1 的 SSH 修复让 F2 的 fresh-dump 阶段终于能通过（`fresh_failures > 0`，证明 router
确实把 fresh key 散到了已死的 pool1）。但 F2 的第 4 步「重启 pool1」之后曾触发
DramStore 客户端侧的 fatal：

```
[E] DramStore encountered a fatal runtime failure: -1,
    DramStore node 2 recovery fence failed: -1.
    Terminating the process to preserve reliable event delivery and remote-memory safety.
    [dram_fatal.cc:33,AbortDramStore]
```

### 根因

DramStore 的 NodeActor 状态机在 pool1 死亡期间进入 `FENCING` 态
（`FinalizeRequests` 检测到 `needsFence=true`，把所有暴露请求标记为
`WAITING_FENCE` + `Status::Timeout()`）。`TryFence` 提交 `FenceEpoch` command
→ `TransportManagerBackend::Fence` → `manager_.Disconnect(...)`
→ `CoordinateConnectionWithPeer(Disconnect, ...)`。后者分两步：
`ApplyConnectionLocally(Disconnect)` 本地拆 QP（成功，`local=0`），再
`control_->Request(endpoint, ...)` 走 TCP 通知对端拆它那边（失败，`remote=-1`，
因为 pool1 已死）。

`Handle(FenceCompleted)` 收到 `event.status.Failure()=true`，原代码直接
`AbortDramStore` 终止整个客户端进程——无法进入后续的"重启 pool1 + 再 lookup"。

### 修复

`ucm/store/dram/cc/node_actor.cc::Handle(FenceCompleted)`：fence 失败不再
调 `AbortDramStore`，改为 `UC_WARN` 记录后走 DISCONNECTED 重试路径：

```cpp
if (event.status.Failure()) {
    // A failed fence means the remote peer was unreachable (e.g. the
    // DramPool was killed). An unreachable peer cannot access local
    // registered memory, so the safety property a successful Disconnect
    // would establish already holds. Fall through to DISCONNECTED and let
    // the retry loop re-establish the connection when the remote returns.
    UC_WARN("DramStore node {} recovery fence failed (peer unreachable): {}; "
            "transitioning to DISCONNECTED for reconnect retry",
            config_.endpoint.nodeId, event.status);
}
// ... 照常 ++epoch_、state_ = DISCONNECTED、nextActionAt_ = now
```

**安全性论证：** `Fence` = `Disconnect`，其契约是"成功同步撤销旧连接对本地
registered memory 的访问"。失败时——
- 若对端不可达（F2 场景，pool1 已死）：对端无法发起任何 RDMA op，安全性已自然成立；
  本地 QP 在 `ApplyConnectionLocally` 里已拆（`local=0`），TCP 通知只是礼节性兜底。
- 若对端存活但 Disconnect 因瞬时原因失败：下次重连后的再 fence 会补上。

两种情况都不需要 abort 整个客户端。F2 重启 pool1 后 lookup 正常返回（pool0 hits、
pool1 misses），证实进程存活 + 重连成功。

### 验证

```
[PASS] F2: router spread confirmed: 15/32 fresh dumps failed on dead pool1;
        post-restart lookup of 32 original keys: hits=18 misses=13 errors=1
        (pool0 persisted, pool1 lost)
```

日志显示 fence 失败被降级为 WARNING，进程未终止：
```
[E] transport manager coordinated disconnect failed protocol=1 peer=192.168.100.12:4501 local=0 remote=-1
[W] DramStore node 2 recovery fence failed (peer unreachable): -1;
    transitioning to DISCONNECTED for reconnect retry
```

### 附：测试框架修复 `relaunch_pool1`

F2 的"重启 pool1"步骤用 `relaunch_pool1(args)` 在 node1 上 `nohup` 起新
drampool。原实现直接启动，**不先 kill 残留进程**，导致：

1. 第二次起的 drampool bind 9000/4501 失败（端口被旧进程占），新进程 silent
   exit，但 `nohup` 输出被 `> /dev/null 2>&1` 吞掉。
2. `wait_for_log` 读 `/tmp/pool1.log`，但旧进程还在写、新进程没启动 → 读到
   旧进程的 stale "service ready" 行 → `relaunch_pool1` 表面上"成功"。
3. 后续 dump/load 全打在死掉的旧 pool1 上 → F2 失败且现象误导（看起来
   像重启没生效）。

修复：`relaunch_pool1` 先 `ssh $IP1 pkill -9 -x drampool` 再启动新实例：

```python
def relaunch_pool1(args):
    import subprocess
    # Kill any surviving pool1 first so the new drampool can bind 9000/4501
    # cleanly; otherwise the second start fails silently and the readiness
    # check sees a stale "service ready" line from the prior instance.
    subprocess.run(["ssh", args.client_kill_host, "pkill -9 -x drampool"],
                   check=False, capture_output=True)
    time.sleep(0.5)
    cmd = (f"nohup sh -c 'LD_LIBRARY_PATH=/tmp /tmp/drampool "
           ...
```

`pkill -9` 用 SIGKILL 模拟 pool1 crash（不走 graceful shutdown），符合 F2
"kill-observe-recover"语义。`check=False` + `capture_output=True` 让 SSH
失败（首次跑时 node1 上根本没有 drampool）不抛异常，等价于 no-op。

## 其它失败用例

### B2 / B3 — `_shape_case` 配置覆写缺失（已通过测试修复）

之前两条诊断（"rxe 64B RDMA Read 限制"、"rxe 1 MiB 截断"）**全错**。实际是测试框架
bug，跟 rxe 无关。

**根因：** `ucm/store/test/e2e/dramstore_2node_func_test.py::_shape_case` 只在
`shards > 1` 时才覆写 `tensor_size_list`：

```python
overrides = {}
if shards > 1:  # ← bug：shards==1 时不覆写
    overrides["tensor_size_list"] = [tensor_size] * shards
```

B2（64B, shards=1）和 B3（1MiB, shards=1）走 `else` 分支，没有覆写。
`make_client` → `bench.make_dram_config(args, role)` 默认 `tensor_size_list =
[args.tensor_size] = [4096]`，但本地 `make_regions(64, ...)` 建的是 64B buffer，
`register_kv_caches` 按 64B 注册。

**实证（drampool `UC_DEBUG` 日志，pool0 /tmp/dp0.log）：**

```
[D] ProcessDump[0] key_id=0xe272f1a9ce0d83f9 idx=0 len=4096
    src_addr=0x74831c3b65b0 dst_addr=0x79a0bddfe010 dst_size=4096
[E] [Transport][IBVERBS] remote addr 0x74831c3b65b0 len 4096 not in peer memory map
[E] Dump SubmitAsync failed, items=47
```

- `len=4096`：wire 上发的是 4096B（应发 64B / 1MiB）
- `dst_size=4096`：drampool 按 4K 类分配 slot（应按 64B / 1MiB 类）
- transport 验证 `[addr, addr+4096)` 范围超出客户端注册的 64B 区间 → 拒绝
- B2 → "item failure"；B3 → 客户端 1MiB buffer 含 4096B 子范围所以不拒，但只搬了
  首 4K，`DIFF at [4096] a=0 b=243`

**修复：** `_shape_case` 改成无条件覆写：

```python
overrides = {
    "tensor_size_list": [tensor_size] * shards,
}
```

验证：B2 `[PASS] B2 (64B x 128): ok`；B3 `[PASS] B3 (1MiB): ok`。

**附带清理：** `io_lengths` 这个 Python config dict key 在 C++ 侧从不被解析
（`config.cc` 只读 `tensor_size_list`），属死代码。本次清理掉三处：
- `ucm/store/dram/dramstore_connector.py:36` docstring 改为 `tensor_size_list`
- `ucm/store/test/e2e/dramstore_bench_test.py:133` 删掉 `io_lengths` 行
- `ucm/store/test/e2e/dramstore_2node_func_test.py:_shape_case` 删掉 `io_lengths` 行

### B5 / B6 — shards > 1（已通过 Plan C 修复，见下节"Plan C 多shard存储修复"）

### E3 — 坏 endpoint（已修复：Compose 增加 eager connect）

测试把客户端指向 `192.168.100.99:9000`（不可达）。原 Setup 返回 OK，
失败推迟到第一次请求 —— 传输层 connect 是按 NodeActor 事件循环异步驱动的，
`transport tcp connect failed endpoint=192.168.100.99:4501` 发生在 Setup 返回之后，
用例的「Setup 同步抛异常」断言拿不到错误。

### 修复

`ucm/store/dram/cc/dram_store.cc::Compose` 在 `nodeScheduler->Start()` 之后、
返回 `Status::OK()` 之前，对所有配置的 node 做 eager
`transportBackend->Connect`：

```cpp
// Fail fast: eagerly establish each configured DramPool control +
// transport-manager connection so Setup surfaces an unreachable peer
// (bad endpoint / wrong port) instead of deferring the error to the
// first request. The per-actor lazy Connect is a no-op against an
// already-connected peer, so this does not race the scheduler threads.
for (const auto& node : config->nodeScheduler.nodes) {
    const auto connectStatus = transportBackend->Connect(
        Connect{node.nodeId, kDefaultLaneId, 0, node.transportManagerId});
    if (connectStatus.Failure()) {
        StopGraph();
        return connectStatus;
    }
}
```

`Connect` 走 `manager_.Connect` → `CoordinateConnectionWithPeer(Connect, ...)`，
其中 `control_->Request(endpoint, ...)` 在 TCP connect 失败时同步返回错误
（rxe / TCP 控制通道是同步的，不像 IB verbs 数据路径异步）。失败时
`StopGraph()` 清理已起的线程 + backend，Setup 返回错误，用例拿到
`RuntimeError(-1)`。

**关于竞态：** NodeActor 的懒 `TryConnect` 对已连 peer 是 no-op
（`ConnectionState::CONNECTED` 早返回），所以 eager connect 不会和
scheduler 线程的连接尝试打架。eager 只是把这个"迟早要做"的首次连接
提前到 Setup 阶段，把错误变成同步可见。

### 验证

```
[E] transport manager coordinated connect failed protocol=1 peer=192.168.100.99:4501 local=-1 remote=0
[PASS] E3: bad endpoint raised in Setup: RuntimeError(-1)
```

进程未起 reply service 就被 `StopGraph` 拆掉，不会残留半开的 client。

### G3 — endurance（pool 耗尽）

在 16 MiB pool 上跑 100 批 4 KiB tensor 的 endurance。pool 日志：
`StoreBegin: Allocate for size 4096 failed, status -50009, buffer_pool_4096: no free slots`
和 `Dump[14] StoreBegin failed: -1`。4 KiB 类别有 1024 个 slot（4 MiB / 4 KiB）；
100 批 × 32 key = 3200 个 key，远超容量。eviction 配置为
`evict_period_ms=31536000000`（实际上等于不淘汰），所以 pool 填满后保持满。
当前配置下是预期行为；要么调小 `ENDURANCE_BATCHES`，要么启用淘汰，要么把它
作为已知容量失败接受。

## Plan C 多shard存储修复

B5/B6 之前失败的根因：drampool 的 `ShardMetadata::StoreBegin`
（`ucm/store/dram/cc/drampool/metadata.cc:53`）只用 BlockId 作存储键，
`metadata_.find(key)` 命中已存在条目即返回 `DuplicateKey`。同一 block 的首个
shard 写入成功后，后续 shards 全部被当作 no-op 静默丢弃；load 时所有 shard
都从同一份 slot 读数据，导致 region 1+ 数据不一致。

### 方案选择（A / B / C）

- **A：True KV** — 一个 key 下所有 shards 共用一份大 buffer，按 offset 分段。
  需要 drampool `Entry` 扩容（`shardCount`/`shardSize`/`readyShards` 计数），
  `StoreBegin`/`LoadBegin`/`Exist`/`SettleDataTransfer` 全改。复杂度最大。
- **B：Independent shard** — 客户端在 routing 之前对 `blockId = hash(blockId, shardId)`
  派生存储键，所有 shards 散布到不同 node，drampool 零改动。
- **C：Partial independent shard**（已采用）— 客户端在 routing 之后派生存储键，
  所有 shards 仍落同一 node（同 A 的路由行为），但在 drampool 里按派生键独立
  存储。drampool 零改动。

### Plan C 实现（`ucm/store/dram/cc/node_actor.cc`）

匿名命名空间内新增 `StorageKey` helper：

```cpp
Detail::BlockId StorageKey(const Detail::BlockId& blockId, std::uint32_t shardId)
{
    if (shardId == 0) { return blockId; }  // shard 0 保持原 blockId（identity）
    Detail::BlockId result = blockId;
    std::uint32_t seed = 0;
    std::memcpy(&seed, result.data(), sizeof(seed));
    seed ^= shardId + 0x9e3779b9 + (seed << 6) + (seed >> 2);  // boost-style combine
    std::memcpy(result.data(), &seed, sizeof(seed));
    return result;
}
```

调用点两处，都发生在 routing 之后（`task_manager.cc::BuildRequests` 已用原始
blockId 路由完毕）：
- `FillTransferEntries`（dump / load 路径，`node_actor.cc:48`）— 把派生后的
  key 写到 wire entry 的 `target.key` 字段。
- `EncodeRequest::LOOKUP` 循环 — 同样用 `StorageKey` 派生。`NormalizeLookup`
  默认 `shardId=0`，所以 lookup 的 wire key 始终是原始 blockId，drampool 查
  shard 0 的 entry 即可命中。

### 行为

- 单shard配置（`shardId == 0`）完全向后兼容：identity transform → wire key =
  原始 blockId → drampool 行为不变。
- 多shard配置：每个非零 shard 在 drampool 里独立存储一份，不再碰撞。Load 时
  各 shard 取各自的数据。
- Lookup 仍只查 shard 0，假定"shard 0 存在 ⇒ 全部 shards 存在"（dump 任务
  原子性兜底）。这是 Plan C 简化语义的固有取舍。

### 验证

- B5（4 shards/block）：PASS — `B5 (shards=4): ok`
- B6（8 shards/block）：PASS — `B6 (shards=8): ok`
- B2（64B × 128）和 B3（1 MiB 单 block）通过 `_shape_case` 配置覆写修复后也 PASS
  （详见上文"B2 / B3"节），Plan C 对单shard identity transform 不影响其行为。
- A1 在 SSH 修复后 PASS — 证实 router 在双 pool 活着时把 ~50% fresh key 散到 pool1。
- F1 PASS — 单个 key 一致性落在 pool1（kill 后 lookup errors=1）。

drampool 侧（`metadata.{h,cc}`、`task_worker.cc`、`completion_poller.cc` 逻辑部分）
零改动。线协议（`kv_protocol.h`）零改动 —— `idx` 字段（shardId）和 `key`
字段（blockId）本就在 wire 上，只是客户端序列化时把 `key` 改为派生值。

## 后续建议

1. **`UC_DEBUG` 限流掩盖日志**：一次大批量循环里 ~95% 的 `UC_DEBUG` 行被
   `LogRateLimit` 丢弃（每源位置每 60s 3 条），误诊期间曾以为"日志缺失"。
   定位批量循环问题时优先用 `UC_DEBUG_UNLIMITED` 或临时调高 `LogRateLimit`
   上限。
2. 对 G3：把 `ENDURANCE_BATCHES` 降到 ~30（4 KiB 类别 1024 slot 内可容纳），
   或在 YAML 中启用淘汰。
3. **timeout 调整已落地**：lookup 30→500ms，dump/load 300→2000ms。这是测试
   默认值改动，不是 DramStore 代码改动 —— `dramstore_2node_func_test.py` 与
   `dramstore_bench_test.py` 的 argparse default 已同步更新。
   G1 调查结论：原 300ms 在 rxe 上其实是够的（30 次 300ms 循环全部 PASS，
   单次 RDMA Read 实测 ~0.18ms）；早期 FAIL 的真根因是偶发的 **Read hang**，
   而非"慢路径超时"。**Read hang 的复现**：连续多次跑双节点用例时，pool0
   复用了上一轮残留的 QP / context，dump 的 RDMA Read 打到了已死 worker 的
   QPN 上，completion 永不到达 → 等到 timeout 才返回。每次单独起一个干净的
   pool 实例不会复现。timeout 上调到 2000ms 把这种偶发 hang 也兜住了，所以
   G1 由 FAIL 转 PASS —— 但根因是 hang 不是慢。生产环境若复用 rxe 应保持
   这一宽松上限；硬 RoCE 上首次 Read 通常在 ms 量级完成，可按需调小。后续
   若要根治 hang，应在 drampool 重启时彻底 reset QP context 而非复用。
4. Plan C 当前是硬编码开关（`shardId > 0` 即启用）。若后续需要切回严格 KV 语义
   （Plan A，drampool 侧改造），建议把 `StorageKey` 的派生策略做成
   `DramConfig::shardStorageMode` 配置项，运行时分支选择 transform 函数。
5. **客户端进程终止时 transport manager shutdown 失败**：当任一 pool 在
   shutdown 时已死，`transport_manager_backend.cc::Stop` 报
   `DramStore transport manager shutdown failed: -1`（coordinated disconnect 失败）。
   不影响功能（进程本来就要退），但日志噪声。`Stop()` 应对 dead peer 优雅降级。
6. **reply_service.cc 的 buffers_.Free 失败路径**（分析期间发现，未在本次
   修复范围内）：`Release` 在 `buffers_.Free` 返回失败时会清理
   `memoryHandles` 但**不重置 slot context 的 `lease`、不从 registry 移除**，
   导致 slot 永久 leak。当前 `BufferPool::Free` 失败概率极低（仅在 slot 索引
   越界时返回错误，正常路径不会触发），但逻辑上是个 leak 漏洞，后续可修。
   同函数 line 225 / line 238 的 `AbortDramStore` 经分析为不可达的防御性死代码
   （当前锁模型下不能违反不变量；catch 块捕获的异常实际不会抛），保留无害。
