# DramStore soft-RDMA (ibverbs) 移植与 e2e 测试报告

> 日期: 2026-08-06  
> 分支: `feat/pod`  
> 传输后端: libibverbs (Soft-RoCE / rxe)  
> 验证环境: node0 / node1

## 1. 背景

`feat/pod` 分支的 DramStore P2P 传输只有 hixl 后端(依赖 Ascend CANN),`TransportManagerBackend` 把协议硬编码为 Hixl。`debug/dram-soft-rdma` 分支曾实现基于 libibverbs 的 soft-RoCE(rxe)后端 `IbverbsTransport`(及纯软件 `SimuTransport`),但那套代码基于旧的 `one_sided/` 目录布局 + `metadata_codec`,无法直接用于 `feat/pod`。

本次工作把 ibverbs(+ simu)后端移植到 `feat/pod` 最新 transport 接口(扁平 `protocols/` 布局 + `binary_codec` + `ControlChannel` 双向连接模型),让 DramStore backend 可选 ibverbs,并在 rxe 模拟环境做功能/边界/并发验证。

## 2. 关键发现(移植依据)

- **Transport 抽象接口零变化**:两分支的 12 个纯虚函数签名逐字相同;`Operation`/`Segment`/`Metadata`/`MemoryRegion` 结构一致。
- **codec 兼容**:`binary_codec.h` 与旧 `metadata_codec.h` 的 `AppendU8/U16/U32/U64`、`ReadU8/...`、`AppendString/ReadString` API 完全一致(仅 `AppendMetadata`→`AppendBytes` 改名,ibverbs/simu 未用到)。
- **枚举/Attrs 被删**:`feat/pod` 的 `TransportProtocol` 仅剩 `Hixl`;`IbverbsInitAttrs`/`SimuInitAttrs` 不存在——需重新引入。
- **连接模型变化**:`feat/pod` 的 `Connect` 经 `CoordinateConnectionWithPeer` 自动让**对端**也 apply Connect(通过 `ControlChannel` 的 `ControlOperation::Connect` 请求),而非旧版只本地建连。这直接决定了 e2e 握手时序必须调整。

## 3. 实现内容

### 3.1 Transport 层(移植)

| 文件 | 改动 |
|---|---|
| `ucm/transport/p2p/include/core/transport.h` | `TransportProtocol` 枚举追加 `Ibverbs=1, Simu=2` |
| `ucm/transport/p2p/include/core/transport_init_attrs.h` | 追加 `IbverbsInitAttrs`、`SimuInitAttrs` |
| `include/protocols/ibverbs/ibverbs_transport.{h,cpp}` | 从 soft-rdma 分支移植;include `one_sided/`→扁平、`metadata_codec`→`binary_codec` |
| `include/protocols/simu/simu_transport.{h,cpp}` | 同上(simu 无依赖,常驻编译) |
| `src/core/transport_manager.cpp` | `CreateTransport` 加 ibverbs/simu 分支(`UCM_P2P_HAS_*`);`TransportForDirect` 改多协议优先映射;`FindTransport` 加 fallback |
| `ucm/transport/p2p/CMakeLists.txt` | `option(P2P_ENABLE_IBVERBS ...)` 默认 OFF;find libibverbs;simu 常驻 |

### 3.2 DramStore backend(协议可配置)

| 文件 | 改动 |
|---|---|
| `ucm/store/dram/cc/transport_manager_backend.h` | `TransportBackendProtocol{kHixl,kIbverbs}` + ibverbs 配置字段 |
| `ucm/store/dram/cc/transport_manager_backend.cc` | `Init()` 按 `options_.protocol` 构造 `IbverbsInitAttrs`/`HixlInitAttrs`;`Connect`/`Fence` 用配置协议 |
| `ucm/store/dram/CMakeLists.txt` | `ascend OR P2P_ENABLE_IBVERBS` 时编译 backend + 链 transport;新增 `BUILD_DRAM_IBVERBS_E2E` target |

> 生产路径默认 `kHixl`,行为不变(位置聚合初始化命中新字段默认值)。ibverbs 经 e2e/options 直接选择;YAML/Config 全量集成留作后续。

### 3.3 e2e 与脚本

| 文件 | 改动 |
|---|---|
| `ucm/store/dram/cc/ibverbs_dramstore_e2e.cpp` | 移植自 soft-rdma;`two_sided/tcp`→`channels/tcp`;适配新连接模型(server 不再 Connect,client ADDR 移到 Connect 之后);新增 `IBV_TEST_MODE` 场景(functional/large/concurrency) |
| `scripts/run_dramstore_ibverbs_e2e.sh` | 多模式循环,独立端口,同步 .so + `LD_LIBRARY_PATH`,输出汇总 |

## 4. 验证环境

| 节点 | IP | RDMA 设备 | MTU | libibverbs |
|---|---|---|---|---|
| node0 (server) | 192.168.100.11 | rxe0 (PORT_ACTIVE) | 1024 | 已装 |
| node1 (client) | 192.168.100.12 | rxe0 (PORT_ACTIVE) | 1024 | 已装 |

互 ping 正常(0% 丢包)。本机无 rxe,采用"本地构建 + rsync 二进制与 .so 到节点 + `LD_LIBRARY_PATH=/tmp` 启动"。

## 5. 用例矩阵与结果

### 5.1 simu 单元/边界/并发(本地,无 rxe,GoogleTest)

`ucm/transport/p2p/tests/simu/simu_transport_test.cc`,target `ucmp2p.test`。13 项全部 PASS(0.84s)。

| 类别 | 用例 | 结果 |
|---|---|---|
| 功能 | WriteSyncMirrorsLocalBuffer | PASS |
| 功能 | ReadSyncPullsRemoteIntoLocal | PASS |
| 功能 | AsyncWriteCompletesViaGetStatus | PASS |
| 边界 | ZeroLengthSegmentRejected | PASS |
| 边界 | UnregisteredLocalAddressRejected | PASS |
| 边界 | UnregisteredRemoteAddressRejected | PASS |
| 边界 | ExecuteBeforeConnectRejected | PASS |
| 边界 | ExecuteAfterDisconnectRejected | PASS |
| 边界 | ReconnectAfterDisconnect | PASS |
| 边界 | LargeTransferBeyondMtu (1 MiB) | PASS |
| 边界 | MultiSegmentSingleOperation | PASS |
| 并发 | ConcurrentAsyncTransfersAllComplete (8 线程 async) | PASS |
| 并发 | ConcurrentSyncTransfersNoCorruption (4 线程 sync) | PASS |

### 5.2 rxe e2e 功能/边界/并发(node0 ↔ node1,真实 RDMA)

`scripts/run_dramstore_ibverbs_e2e.sh`,server 裸 `TransportManager` + client `CreateTransportManagerBackend`(ibverbs)。

| 模式 | 描述 | server exit | client exit | VERIFY | 结果 |
|---|---|---|---|---|---|
| functional | 4 KiB RDMA Write,4KB pattern 校验 | 0 | 0 | PASS | PASS |
| large | 256 KiB RDMA Write(>> rxe MTU 1024,验证多包/SGE 路径) | 0 | 0 | PASS | PASS |
| concurrency | 8 路并发 `ExecuteAsync` 写入 64 KiB 的 8 个条带 + `GetStatus` 轮询 | 0 | 0 | PASS | PASS |

汇总输出 `ALL PASS`。关键日志(server): `RDMA write done`;(client): `backend Connect ok` / `VERIFY client buffer: PASS`。

## 6. 过程中发现的问题与修复

1. **e2e 连接时序不匹配新模型**:旧 e2e 的 server 主动调 `manager.Connect` 且 client 在 Connect 前发 ADDR。在 `feat/pod` 的双向协调模型下:server 主动 Connect 会向尚未 ExchangeMetadata 的 client 发控制请求,且 client 尚未注册 peer → 失败/死锁。**修复**:server 不再调 Connect/ExchangeMetadata(由 client 的 `backend.Connect` 统一驱动两端);client 的 `ADDR` 移到 `backend.Connect` 返回之后发,保证 server 收到 ADDR 时两端 QP 已 RTS。

2. **e2e 动态链接缺 .so**:`feat/pod` 的 `ucm_p2p_transport`/`dramstore` 为 SHARED,原脚本只 rsync 二进制导致节点找不到 .so。**修复**:脚本一并 rsync 两个 .so 到 `/tmp` 并以 `LD_LIBRARY_PATH=/tmp` 启动。

3. **多 protocol 路由**:`feat/pod` 的 `TransportForDirect` 仅映射 `RemoteDeviceHost→Hixl`,装 ibverbs 时找不到。移植 soft-rdma 已验证的多协议优先映射 + `FindTransport` fallback 解决。

4. **死代码告警**:`-Wall -Werror` 下,移除 server Connect 后 `exchangeMetadataWithRetry` 变未用 → 删除。

## 7. 回归

- 不传 `P2P_ENABLE_IBVERBS` 时,`UCM_P2P_HAS_IBVERBS` 不定义,`CreateTransport` 对 ibverbs 返回 nullptr(同现状);hixl/ascend 构建路径不受影响。
- `dramstore` 生产路径默认 `kHixl`,行为不变。

## 8. 结论

soft-RDMA(soft-RoCE/rxe)传输后端已成功移植到 `feat/pod` 最新 transport 接口,并通过 DramStore backend 工厂验证承载真实 RDMA 流量。功能、边界(零长度/未注册/未连接/断连重连/超大块/多段)、并发(多线程 async+轮询、多线程 sync)在 simu 单元层与 rxe e2e 层均全部通过。

## 9. simu 后端 bench(进程内,无 rxe/无 GPU)

simu 后端是纯软件进程内 memcpy 模型,要求两端同进程。新增 `ucmdrampool` pybind 在单 Python 进程内同进程托管 drampool server(`DramPoolServer::Init/Start`,跳过 daemon 的信号/阻塞循环),配合 DramStore client(simu),全程 localhost TCP 控制 + 进程内 memcpy 数据。

### 9.1 改动(本轮,simu 支持)

| 文件 | 改动 |
|---|---|
| `ucm/store/dram/cc/drampool/drampool_server.cc` | `StartTransportService` 加 simu 分支(`SimuInitAttrs`+`InstallTransport(Simu)`);`InitializeDeviceRuntime` simu 下跳过(x86 无设备运行时) |
| `ucm/store/dram/cc/transport_manager_backend.{h,cc}` | `TransportBackendProtocol` 加 `kSimu`;`ToTransportProtocol`+`Init()` 加 simu 分支 |
| `ucm/store/dram/cc/dram_store.cc` | `Compose` 加 `simu`→`kSimu` 映射 |
| `ucm/store/dram/cc/drampool/drampool_launch_config.cc` | `--transport-protocol` 接受 `simu` |
| `ucm/store/dram/CMakeLists.txt` | backend 构建条件加 `RUNTIME_ENVIRONMENT=simu`;新增 `ucmdrampool` target(链 `drampool_core`);`ucmdramstore` 改显式源(避免 glob 纳入 drampool.py.cc) |
| `ucm/transport/p2p/src/protocols/simu/simu_transport.cpp` | `ImportMetadata` 不再重置 `connected`(同 ibverbs 修复:metadata 刷新不应打断已建连) |
| `ucm/store/dram/cpy/drampool.py.cc` | **新增** `ucmdrampool` pybind:`start(argv)`/`stop()`/`ready_addr()` |
| `ucm/store/test/e2e/dramstore_bench_test.py` | `--transport-protocol simu` 模式:进程内 `ucmdrampool.start` + DramStore client,`try/finally` 保证 stop |
| `examples/drampool_simu.yaml` | 三方(127.0.0.1)静态路由 + 小池 |
| `scripts/run_dramstore_bench_simu.sh` | 单进程运行脚本 |

### 9.2 结果(本地单进程,32 batch × 32 块 × 4KiB)

| 指标 | 值 |
|---|---|
| dump 平均延迟 | 0.87 ms |
| load 平均延迟 | 0.87 ms |
| dump 带宽 | 151.20 MB/s |
| load 带宽 | 151.31 MB/s |
| round-trip | 1.73 ms |
| 数据校验 | PASS(无 DIFF) |

### 9.3 两种 bench 对比

| | simu(进程内) | ibverbs/rxe(node0↔node1) |
|---|---|---|
| 硬件 | 无(rxe/无 GPU) | rxe0 + 两节点 |
| 拓扑 | 单进程 | 双进程(ssh) |
| dump 带宽 | 151 MB/s | 27 MB/s |
| load 带宽 | 151 MB/s | 117 MB/s |
| round-trip | 1.73 ms | 5.90 ms |

simu(进程内 memcpy)更快(无 RDMA 协议栈/网络往返),适合本地快速回归;ibverbs/rxe 验证真实 RDMA 通路。两者数据校验均通过。

## 10. 两 node e2e bench(rxe,node0 双 pool + node1 client)

把 DramStore bench 从单 node 扩为两 node:node0 跑两个 drampool daemon(不同控制/transport 端口),node1 跑 DramStore client,ring-hash router 把块跨两个 pool 分发。验证多 node 路由 + dump/load/lookup 全链路。

> simu 进程内(memcpy)无法两 node(同进程仅一个 drampool,g_config 全局),两 node 用 rxe(跨进程真 RDMA)。

### 10.1 改动

| 文件 | 改动 |
|---|---|
| `examples/drampool_rxe_2node.yaml` | 三方静态路由:drampool0(9000→4501)、drampool1(9001→4503)、client worker(4702→4502)、client scheduler(4703→4503) |
| `scripts/run_dramstore_bench_rxe_2node.sh` | node0 起两 drampool(rxe0)+ node1 跑 bench(`--node-ids 1 2 --node-control-endpoints ...:9000 ...:9001 --node-transport-manager-ids ...:4501 ...:4503`) |

### 10.2 结果(32 batch × 32 块 × 4KiB,两 pool)

| 指标 | 两 node rxe | 单 node rxe(对照) |
|---|---|---|
| dump 平均延迟 | 3.70 ms | 4.78 ms |
| load 平均延迟 | 2.21 ms | 1.12 ms |
| dump 带宽 | 35.41 MB/s | 27.44 MB/s |
| load 带宽 | 59.23 MB/s | 116.75 MB/s |
| round-trip | 5.92 ms | 5.90 ms |
| 数据校验 | PASS(无 DIFF) | PASS |

bench 全块 lookup all-hit(dump 后),证明 ring-hash router 把块分发到了两个 pool 且两 pool 都正确存取。两 node 通过(rxe)。
