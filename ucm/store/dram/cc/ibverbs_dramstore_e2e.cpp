// DramStore ibverbs transport end-to-end across two Soft-RoCE (rxe) nodes.
//
// The CLIENT side exercises UC::Dram::CreateTransportManagerBackend with the
// ibverbs protocol -- the exact factory the DramStore store plugin uses. The
// SERVER side is a minimal transport::TransportManager peer that RDMA-writes a
// pattern into the client's registered buffer, proving the client's
// RegisterMemory + Connect (ExchangeMetadata + Connect) path carries real RDMA
// traffic end-to-end.
//
//   server: dramstore_ibverbs_e2e server
//   client: dramstore_ibverbs_e2e client
//
// Env (see scripts/run_dramstore_ibverbs_e2e.sh):
//   IBV_TEST_LOCAL_HOST / IBV_TEST_PEER_HOST / IBV_TEST_DEVICE
//   TRANSPORT_TEST_PORT_A/B (manager metadata ports, default 4501/4502)
//   TRANSPORT_CONTROL_PORT_A/B (standalone handshake ports, default 4601/4602)
//   DRAM_BACKEND_CONTROL_PORT (client backend's own TCP listener, default 4702)

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include "core/transport.h"
#include "core/transport_init_attrs.h"
#include "core/transport_manager.h"
#include "channels/tcp/tcp_message_channel.h"
#include "transport_executor.h"
#include "transport_manager_backend.h"

using namespace transport;

namespace {

const char* envText(const char* name, const char* fallback)
{
    const char* value = std::getenv(name);
    return value == nullptr || *value == '\0' ? fallback : value;
}

uint16_t envPort(const char* name, uint16_t fallback)
{
    const char* text = std::getenv(name);
    if (text == nullptr || *text == '\0') { return fallback; }
    const auto value = std::strtoul(text, nullptr, 10);
    return (value == 0 || value > UINT16_MAX) ? fallback : static_cast<uint16_t>(value);
}

int envInt(const char* name, int fallback)
{
    const char* text = std::getenv(name);
    if (text == nullptr || *text == '\0') { return fallback; }
    return static_cast<int>(std::strtol(text, nullptr, 10));
}

struct Config {
    std::string local_host = envText("IBV_TEST_LOCAL_HOST", "127.0.0.1");
    std::string peer_host = envText("IBV_TEST_PEER_HOST", "127.0.0.1");
    std::string device_name = envText("IBV_TEST_DEVICE", "rxe0");
    uint16_t server_manager_port = envPort("TRANSPORT_TEST_PORT_A", 4501);
    uint16_t client_manager_port = envPort("TRANSPORT_TEST_PORT_B", 4502);
    uint16_t server_control_port = envPort("TRANSPORT_CONTROL_PORT_A", 4601);
    uint16_t client_control_port = envPort("TRANSPORT_CONTROL_PORT_B", 4602);
    uint16_t client_backend_control_port = envPort("DRAM_BACKEND_CONTROL_PORT", 4702);
    int connect_timeout_ms = envInt("IBV_TEST_CONNECT_TIMEOUT_MS", 30000);
    int transfer_timeout_ms = envInt("IBV_TEST_TRANSFER_TIMEOUT_MS", 30000);
    int wait_attempts = envInt("IBV_TEST_WAIT_ATTEMPTS", 600);
    int wait_interval_ms = envInt("IBV_TEST_WAIT_RETRY_MS", 100);
};

// true when this process is the server side.
ManagerID makeManagerID(const std::string& host, uint16_t port) { return host + ":" + std::to_string(port); }

IbverbsInitAttrs makeIbverbsAttrs(const Config& c)
{
    IbverbsInitAttrs attrs;
    attrs.device_name = c.device_name;
    attrs.connect_timeout_ms = c.connect_timeout_ms;
    attrs.transfer_timeout_ms = c.transfer_timeout_ms;
    return attrs;
}

bool sendText(TcpMessageChannel& tcp, const Endpoint& peer, const std::string& text,
              const Config& c, const char* step)
{
    for (int attempt = 1; attempt <= c.wait_attempts; ++attempt) {
        if (tcp.Send(peer, text.data(), text.size()) == Status::OK()) { return true; }
        std::this_thread::sleep_for(std::chrono::milliseconds(c.wait_interval_ms));
    }
    std::cerr << step << " failed\n";
    return false;
}

bool receiveText(TcpMessageChannel& tcp, Endpoint& peer, std::string& text, const char* step)
{
    Metadata data;
    if (tcp.Receive(peer, data) != Status::OK()) {
        std::cerr << step << " failed\n";
        return false;
    }
    text.assign(data.begin(), data.end());
    return true;
}

bool receiveText(TcpMessageChannel& tcp, std::string& text, const char* step)
{
    Endpoint peer;
    return receiveText(tcp, peer, text, step);
}

bool allocHostBuffer(void*& ptr, size_t length)
{
    return posix_memalign(&ptr, 4096, length) == 0 && ptr != nullptr;
}

void fillPattern(unsigned char* p, size_t n)
{
    for (size_t i = 0; i < n; ++i) { p[i] = static_cast<unsigned char>(i & 0xff); }
}

bool verifyPattern(const unsigned char* p, size_t n, const char* label)
{
    for (size_t i = 0; i < n; ++i) {
        if (p[i] != static_cast<unsigned char>(i & 0xff)) {
            std::cerr << "VERIFY " << label << ": FAIL at index " << i
                      << " expect=" << int(i & 0xff) << " actual=" << int(p[i]) << "\n";
            return false;
        }
    }
    std::cout << "VERIFY " << label << ": PASS\n";
    return true;
}

// Scenario selected by IBV_TEST_MODE. functional = single 4KB RDMA write;
// large = single 256KB write (>> rxe MTU 1024, exercises SGE/multi-packet);
// concurrency = N concurrent async writes covering the buffer in equal stripes.
struct Scenario {
    size_t size = 4 * 1024;
    int concurrency = 1;
};

Scenario resolveScenario()
{
    const std::string m = envText("IBV_TEST_MODE", "functional");
    if (m == "large") { return Scenario{256 * 1024, 1}; }
    if (m == "concurrency") { return Scenario{64 * 1024, 8}; }
    return Scenario{4 * 1024, 1};
}

// -----------------------------------------------------------------------------
// Server: minimal ibverbs peer. RDMA-writes a pattern into the client buffer.
// -----------------------------------------------------------------------------
int runServer(const Config& c)
{
    const auto manager_id = makeManagerID(c.local_host, c.server_manager_port);
    const auto peer_manager_id = makeManagerID(c.peer_host, c.client_manager_port);
    std::cerr << "[server] manager_id=" << manager_id << " peer=" << peer_manager_id
              << " device=\"" << c.device_name << "\"\n";

    TcpMessageChannel control;
    if (control.Init({c.local_host, c.server_control_port}) != Status::OK()) {
        std::cerr << "server init standalone TCP failed\n";
        return 1;
    }

    TransportManager manager(manager_id);
    if (manager.Init() != Status::OK() ||
        manager.InstallTransport(TransportProtocol::Ibverbs, makeIbverbsAttrs(c)) != Status::OK()) {
        std::cerr << "server install ibverbs failed\n";
        return 1;
    }

    const auto sc = resolveScenario();
    void* buf = nullptr;
    if (!allocHostBuffer(buf, sc.size)) {
        std::cerr << "server alloc buffer failed\n";
        return 1;
    }
    fillPattern(static_cast<unsigned char*>(buf), sc.size);

    MemoryRegion region{buf, sc.size, MemoryType::Host, -1};
    MemoryHandle handle = kInvalidMemoryHandle;
    if (manager.RegisterMemory(region, handle) != Status::OK()) {
        std::cerr << "server register memory failed\n";
        return 1;
    }

    std::cerr << "[server] waiting READY from client\n";
    Endpoint client_control;
    std::string ready;
    if (!receiveText(control, client_control, ready, "server receives READY") || ready != "READY") {
        return 1;
    }
    std::cerr << "[server] client READY from " << client_control.host << ':'
              << client_control.port << '\n';

    // The client drives ExchangeMetadata + Connect on the ibverbs protocol via
    // its TransportManagerBackend. feat/pod's Connect coordinates both peers
    // (the local side applies Connect, then a control request makes this side
    // apply Connect too), so this server only needs to wait for the client to
    // finish that handshake (signalled by ADDR) before issuing the RDMA write.
    std::cerr << "[server] waiting client ADDR\n";
    std::string addr_msg;
    if (!receiveText(control, addr_msg, "server receives ADDR") ||
        addr_msg.rfind("ADDR ", 0) != 0) {
        std::cerr << "server: malformed client ADDR\n";
        return 1;
    }
    char* end = nullptr;
    const auto remote_addr = std::strtoull(addr_msg.c_str() + 5, &end, 16);
    if (end == nullptr || *end != '\0' || remote_addr == 0) {
        std::cerr << "server: failed to parse client ADDR\n";
        return 1;
    }
    std::cerr << "[server] client buffer addr = 0x" << std::hex << remote_addr << std::dec << '\n';

    const auto stripe = sc.size / static_cast<size_t>(sc.concurrency);
    if (sc.concurrency > 1) {
        std::vector<TransferHandle> handles;
        handles.reserve(static_cast<size_t>(sc.concurrency));
        for (int s = 0; s < sc.concurrency; ++s) {
            const auto off = static_cast<size_t>(s) * stripe;
            Operation op;
            op.target_manager = peer_manager_id;
            op.direct = OperationDirect::RemoteDeviceHost;
            op.opcode = Opcode::Write;
            op.ops.push_back(Segment{static_cast<char*>(buf) + off, remote_addr + off, stripe});
            TransferHandle handle = 0;
            if (manager.ExecuteAsync(op, handle) != Status::OK()) {
                std::cerr << "server RDMA async write " << s << " failed\n";
                return 1;
            }
            handles.push_back(handle);
        }
        for (int s = 0; s < sc.concurrency; ++s) {
            TransferStatus status = TransferStatus::Waiting;
            for (int attempt = 0; attempt < 5000 && status == TransferStatus::Waiting; ++attempt) {
                if (manager.GetStatus(handles[static_cast<size_t>(s)], status) != Status::OK()) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            if (status != TransferStatus::Completed) {
                std::cerr << "server RDMA async write " << s << " did not complete\n";
                return 1;
            }
        }
    } else {
        Operation op;
        op.target_manager = peer_manager_id;
        op.direct = OperationDirect::RemoteDeviceHost;
        op.opcode = Opcode::Write;
        op.ops.push_back(Segment{buf, remote_addr, sc.size});
        if (manager.ExecuteSync(op) != Status::OK()) {
            std::cerr << "server RDMA write into client buffer failed\n";
            return 1;
        }
    }
    std::cerr << "[server] RDMA write done\n";

    if (!sendText(control, client_control, "DONE", c, "server sends DONE")) { return 1; }
    // Let the client read DONE before tearing down the TCP channel.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    manager.Shutdown();
    control.Shutdown();
    std::free(buf);
    return 0;
}

// -----------------------------------------------------------------------------
// Client: uses the DramStore TransportManagerBackend factory (ibverbs).
// -----------------------------------------------------------------------------
int runClient(const Config& c)
{
    const auto local_manager_id = makeManagerID(c.local_host, c.client_manager_port);
    const auto peer_manager_id = makeManagerID(c.peer_host, c.server_manager_port);
    std::cerr << "[client] manager_id=" << local_manager_id << " peer=" << peer_manager_id
              << " device=\"" << c.device_name << "\"\n";

    TcpMessageChannel control;
    if (control.Init({c.local_host, c.client_control_port}) != Status::OK()) {
        std::cerr << "client init standalone TCP failed\n";
        return 1;
    }

    UC::Dram::TransportManagerBackendOptions opts;
    opts.localControlHost = c.local_host;
    opts.localControlPort = c.client_backend_control_port;
    opts.localTransportManagerId = local_manager_id;
    opts.localHost = c.local_host;
    opts.deviceId = 0;
    opts.connectTimeoutMs = c.connect_timeout_ms;
    opts.transferTimeoutMs = c.transfer_timeout_ms;
    opts.protocol = UC::Dram::TransportBackendProtocol::kIbverbs;
    opts.ibverbsDeviceName = c.device_name;
    opts.ibverbsPort = 1;
    opts.ibverbsGidIndex = -1;
    // One node entry so Init's non-empty validation passes; its id is unused
    // by the direct Connect path exercised here.
    UC::Dram::NodeEndpoint node;
    node.nodeId = 1;
    node.controlHost = c.peer_host;
    node.controlPort = c.server_control_port;
    node.transportManagerId = peer_manager_id;
    opts.nodes.push_back(node);

    auto created = UC::Dram::CreateTransportManagerBackend(std::move(opts));
    if (!created) {
        std::cerr << "client CreateTransportManagerBackend failed: " << created.Error().ToString()
                  << '\n';
        return 1;
    }
    auto backend = std::move(created).Value();

    const auto sc = resolveScenario();
    void* buf = nullptr;
    if (!allocHostBuffer(buf, sc.size)) {
        std::cerr << "client alloc buffer failed\n";
        return 1;
    }
    std::memset(buf, 0, sc.size);

    auto registered = backend->RegisterMemory(buf, sc.size, UC::Dram::MemoryRegionType::HOST);
    if (!registered) {
        std::cerr << "client RegisterMemory failed: " << registered.Error().ToString() << '\n';
        return 1;
    }

    const Endpoint server_control{c.peer_host, c.server_control_port};
    if (!sendText(control, server_control, "READY", c, "client sends READY")) { return 1; }

    // Connect drives ExchangeMetadata + Connect on the ibverbs protocol, which
    // coordinates both peers' QPs to RTS. Send ADDR only after Connect returns
    // so the server can issue the RDMA write against a connected QP.
    UC::Dram::Connect connect_cmd;
    connect_cmd.transportManagerId = peer_manager_id;
    const auto connect_status = backend->Connect(connect_cmd);
    if (connect_status.Failure()) {
        std::cerr << "client backend Connect failed: " << connect_status.ToString() << '\n';
        return 1;
    }
    std::cerr << "[client] backend Connect ok\n";

    std::ostringstream addr_msg;
    addr_msg << "ADDR 0x" << std::hex << reinterpret_cast<uintptr_t>(buf);
    if (!sendText(control, server_control, addr_msg.str(), c, "client sends ADDR")) { return 1; }

    std::string done;
    if (!receiveText(control, done, "client receives DONE") || done != "DONE") {
        std::cerr << "client: did not receive DONE\n";
        return 1;
    }

    const auto verify_ok = verifyPattern(static_cast<const unsigned char*>(buf), sc.size,
                                         "client buffer");

    backend->Stop();
    control.Shutdown();
    std::free(buf);
    return verify_ok ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv)
{
    const std::string mode = argc > 1 ? argv[1] : envText("IBV_TEST_ROLE", "");
    if (mode == "server" || mode == "B") { return runServer(Config{}); }
    if (mode == "client" || mode == "A") { return runClient(Config{}); }
    std::cerr << "usage: " << argv[0] << " server|client\n";
    return 1;
}
