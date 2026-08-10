#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <infiniband/verbs.h>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "core/transport.h"
#include "core/transport_init_attrs.h"

namespace transport {

// Pure libibverbs backend. Owns one device context, protection domain, and a
// shared completion queue drained by a background poller thread. Each peer gets
// a reliable-connection QP whose address handle is built from the RoCEv2 GID
// exchanged over the out-of-band MetadataChannel. Connection establishment
// (QP RESET -> INIT -> RTR -> RTS) and remote-key exchange are hidden inside
// ExportMetadata/ImportMetadata/Connect, mirroring the HIXL backend.
class IbverbsTransport final : public Transport {
public:
    IbverbsTransport();
    ~IbverbsTransport() override;

    IbverbsTransport(const IbverbsTransport&) = delete;
    IbverbsTransport& operator=(const IbverbsTransport&) = delete;

    TransportProtocol Protocol() const override;
    Status Init(const InitAttrs& attrs) override;
    Status Init(const IbverbsInitAttrs& attrs);
    Status Shutdown() override;
    Status RegisterMemory(const MemoryRegion& memory, MemoryHandle& handle) override;
    Status UnregisterMemory(MemoryHandle handle) override;
    Status ExportMetadata(const ManagerID& manager_id, Metadata& out) override;
    Status ImportMetadata(const ManagerID& manager_id, const Metadata& metadata) override;
    Status Connect(const ManagerID& manager_id) override;
    Status Disconnect(const ManagerID& manager_id) override;
    Status ExecuteSync(const Operation& request) override;
    Status ExecuteAsync(const Operation& request, TransferHandle& handle) override;
    Status GetStatus(TransferHandle handle, TransferStatus& status) override;

private:
    static constexpr uint8_t kGidSize = 16;

    struct RemoteMemory {
        uint64_t addr = 0;
        uint64_t length = 0;
        uint32_t rkey = 0;
    };

    struct Peer {
        // QP created lazily during ExportMetadata so its qpn can be advertised.
        ibv_qp* qp = nullptr;
        uint32_t qpn = 0;
        uint32_t psn = 0;
        // Local gid/gid_index used to build the address handle for this peer.
        uint8_t gid[kGidSize] = {};
        int32_t gid_index = 0;
        // Remote connection info captured during ImportMetadata.
        uint8_t remote_gid[kGidSize] = {};
        int32_t remote_gid_index = 0;
        uint32_t remote_qpn = 0;
        uint32_t remote_psn = 0;
        bool connected = false;
        std::mutex send_mutex;  // serializes ibv_post_send on this QP.
        std::vector<RemoteMemory> remote_mems;
    };

    struct LocalMemoryRecord {
        ibv_mr* mr = nullptr;
        MemoryRegion region;
    };

    struct PendingTransfer {
        uint64_t wr_id = 0;
    };

    Status ValidateAndResolveLocked(const Operation& request, const Peer& peer,
                                     size_t instance_index_unused) const;
    const LocalMemoryRecord* FindLocalMemory(uint64_t addr, uint64_t length) const;
    const RemoteMemory* FindRemoteMemory(const Peer& peer, uint64_t addr, uint64_t length) const;
    Status EnsurePeerQpLocked(Peer& peer);
    Status TransitionQpToConnected(Peer& peer);
    Status PostTransfer(Peer& peer, Opcode opcode, const std::vector<Segment>& segments,
                         uint64_t wr_id);
    void PollerMain();
    void RecordCompletion(uint64_t wr_id, TransferStatus status);

    ibv_context* context_ = nullptr;
    ibv_pd* pd_ = nullptr;
    ibv_cq* cq_ = nullptr;
    uint8_t port_ = 1;
    int32_t gid_index_ = -1;
    int32_t poll_interval_us_ = 50;
    int32_t transfer_timeout_ms_ = 30000;

    std::vector<ibv_device*> devices_;  // not owned; freed by ibv_free_device_list.

    std::thread poller_;
    std::atomic<bool> stopping_{false};
    std::mutex completion_mutex_;
    std::condition_variable completion_cv_;
    std::unordered_map<uint64_t, TransferStatus> completions_;
    std::atomic<uint64_t> next_wr_id_{1};

    mutable std::shared_mutex lifecycle_mutex_;
    mutable std::shared_mutex peers_mutex_;
    mutable std::shared_mutex memories_mutex_;
    mutable std::mutex pending_mutex_;
    std::unordered_map<ManagerID, std::unique_ptr<Peer>> peers_;
    std::unordered_map<MemoryHandle, std::unique_ptr<LocalMemoryRecord>> memories_;
    std::unordered_map<TransferHandle, PendingTransfer> pending_transfers_;
    TransferHandle next_transfer_handle_ = 1;
};

}  // namespace transport
