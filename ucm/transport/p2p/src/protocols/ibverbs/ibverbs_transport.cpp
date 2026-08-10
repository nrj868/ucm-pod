#include "protocols/ibverbs/ibverbs_transport.h"
#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <limits>
#include <thread>
#include <utility>
#include "common/binary_codec.h"
#include "logger/logger.h"

namespace transport {
namespace {

constexpr uint8_t kMetadataVersion = 1;
constexpr uint8_t kGidSize = 16;

// Recognize a RoCEv2 IPv4 GID: bytes 8..11 are 00 00 ff ff.
bool IsRocev2Ipv4Gid(const uint8_t gid[kGidSize]) {
    // Defined out of line via a local helper to keep the header free of logic.
    return gid[8] == 0x00 && gid[9] == 0x00 && gid[10] == 0xff && gid[11] == 0xff;
}

bool AnyGidByteSet(const uint8_t gid[kGidSize]) {
    for (uint8_t i = 0; i < kGidSize; ++i) {
        if (gid[i] != 0) { return true; }
    }
    return false;
}

void CopyGid(uint8_t dest[kGidSize], const ibv_gid& src) {
    std::memcpy(dest, src.raw, kGidSize);
}

void CopyGid(ibv_gid& dest, const uint8_t src[kGidSize]) {
    std::memcpy(dest.raw, src, kGidSize);
}

Status EncodeGid(Metadata& out, const uint8_t gid[kGidSize]) {
    for (uint8_t i = 0; i < kGidSize; ++i) {
        transport::detail::AppendU8(out, gid[i]);
    }
    return Status::OK();
}

Status DecodeGid(const Metadata& in, size_t& offset, uint8_t gid[kGidSize]) {
    for (uint8_t i = 0; i < kGidSize; ++i) {
        if (!transport::detail::ReadU8(in, offset, gid[i])) { return Status::InvalidParam(); }
    }
    return Status::OK();
}

const char* VerbsOpName(ibv_wr_opcode opcode) {
    switch (opcode) {
        case IBV_WR_RDMA_WRITE: return "RDMA_WRITE";
        case IBV_WR_RDMA_READ: return "RDMA_READ";
        default: return "UNKNOWN";
    }
}

}  // namespace

IbverbsTransport::IbverbsTransport() = default;

IbverbsTransport::~IbverbsTransport()
{
    if (Shutdown() != Status::OK()) {}
}

TransportProtocol IbverbsTransport::Protocol() const { return TransportProtocol::Ibverbs; }

Status IbverbsTransport::Init(const InitAttrs& attrs)
{
    const auto* ibv_attrs = dynamic_cast<const IbverbsInitAttrs*>(&attrs);
    return ibv_attrs == nullptr ? Status::InvalidParam() : Init(*ibv_attrs);
}

Status IbverbsTransport::Init(const IbverbsInitAttrs& attrs)
{
    std::unique_lock<std::shared_mutex> lock(lifecycle_mutex_);
    if (context_ != nullptr) { return Status::OK(); }

    int device_count = 0;
    ibv_device** device_list = ibv_get_device_list(&device_count);
    if (device_list == nullptr || device_count <= 0) {
        UC_ERROR("[Transport][IBVERBS] no RDMA devices found");
        return Status::Error();
    }

    ibv_device* selected = nullptr;
    for (int i = 0; i < device_count; ++i) {
        if (!attrs.device_name.empty() && attrs.device_name == device_list[i]->name) {
            selected = device_list[i];
            break;
        }
    }
    if (selected == nullptr) {
        if (!attrs.device_name.empty()) {
            UC_ERROR("[Transport][IBVERBS] device \"{}\" not found", attrs.device_name);
            ibv_free_device_list(device_list);
            return Status::InvalidParam();
        }
        selected = device_list[0];
    }

    context_ = ibv_open_device(selected);
    if (context_ == nullptr) {
        UC_ERROR("[Transport][IBVERBS] ibv_open_device(\"{}\") failed", selected->name);
        ibv_free_device_list(device_list);
        return Status::Error();
    }

    ibv_device_attr device_attr{};
    if (ibv_query_device(context_, &device_attr) != 0) {
        UC_ERROR("[Transport][IBVERBS] ibv_query_device failed");
        ibv_close_device(context_);
        context_ = nullptr;
        ibv_free_device_list(device_list);
        return Status::Error();
    }

    port_ = attrs.port == 0 ? 1 : attrs.port;
    ibv_port_attr port_attr{};
    if (ibv_query_port(context_, port_, &port_attr) != 0 || port_attr.state != IBV_PORT_ACTIVE) {
        UC_ERROR("[Transport][IBVERBS] port {} not active", static_cast<int>(port_));
        ibv_close_device(context_);
        context_ = nullptr;
        ibv_free_device_list(device_list);
        return Status::Error();
    }

    pd_ = ibv_alloc_pd(context_);
    if (pd_ == nullptr) {
        UC_ERROR("[Transport][IBVERBS] ibv_alloc_pd failed");
        ibv_close_device(context_);
        context_ = nullptr;
        ibv_free_device_list(device_list);
        return Status::Error();
    }

    cq_ = ibv_create_cq(context_, attrs.cq_depth, nullptr, nullptr, 0);
    if (cq_ == nullptr) {
        UC_ERROR("[Transport][IBVERBS] ibv_create_cq failed");
        ibv_dealloc_pd(pd_);
        pd_ = nullptr;
        ibv_close_device(context_);
        context_ = nullptr;
        ibv_free_device_list(device_list);
        return Status::Error();
    }

    // Choose the local GID index used for every address handle on this device.
    int32_t chosen_gid_index = attrs.gid_index;
    if (chosen_gid_index < 0) {
        chosen_gid_index = -1;
        const int gid_tbl_len = port_attr.gid_tbl_len;
        for (int g = 0; g < gid_tbl_len; ++g) {
            ibv_gid gid{};
            if (ibv_query_gid(context_, port_, g, &gid) != 0) { continue; }
            if (!AnyGidByteSet(gid.raw)) { continue; }
            if (IsRocev2Ipv4Gid(gid.raw)) {
                chosen_gid_index = g;
                break;
            }
            if (chosen_gid_index < 0) { chosen_gid_index = g; }
        }
        if (chosen_gid_index < 0) {
            UC_ERROR("[Transport][IBVERBS] no usable GID on device \"{}\" port {}",
                      selected->name, static_cast<int>(port_));
            ibv_destroy_cq(cq_);
            cq_ = nullptr;
            ibv_dealloc_pd(pd_);
            pd_ = nullptr;
            ibv_close_device(context_);
            context_ = nullptr;
            ibv_free_device_list(device_list);
            return Status::Error();
        }
    }
    gid_index_ = chosen_gid_index;

    poll_interval_us_ = attrs.poll_interval_us > 0 ? attrs.poll_interval_us : 50;
    (void)attrs.recv_wr_depth;
    (void)attrs.send_wr_depth;
    (void)attrs.sge_depth;
    transfer_timeout_ms_ = attrs.transfer_timeout_ms;

    stopping_.store(false);
    poller_ = std::thread(&IbverbsTransport::PollerMain, this);

    UC_INFO("[Transport][IBVERBS] init success device={} port={} gid_index={}",
            selected->name, static_cast<int>(port_), gid_index_);
    ibv_free_device_list(device_list);
    return Status::OK();
}

Status IbverbsTransport::Shutdown()
{
    std::unique_lock<std::shared_mutex> lock(lifecycle_mutex_);
    stopping_.store(true);
    completion_cv_.notify_all();
    if (poller_.joinable()) { poller_.join(); }

    Status result = Status::OK();
    for (auto& item : peers_) {
        auto& peer = *item.second;
        if (peer.qp != nullptr) {
            if (ibv_destroy_qp(peer.qp) != 0 && result == Status::OK()) {
                UC_WARN("[Transport][IBVERBS] destroy qp failed: peer={}", item.first);
                result = Status::Error();
            }
            peer.qp = nullptr;
        }
    }
    peers_.clear();

    for (auto& memory : memories_) {
        if (memory.second->mr != nullptr) {
            if (ibv_dereg_mr(memory.second->mr) != 0 && result == Status::OK()) {
                result = Status::Error();
            }
            memory.second->mr = nullptr;
        }
    }
    memories_.clear();
    pending_transfers_.clear();
    completions_.clear();
    next_transfer_handle_ = 1;

    if (cq_ != nullptr) {
        if (ibv_destroy_cq(cq_) != 0 && result == Status::OK()) { result = Status::Error(); }
        cq_ = nullptr;
    }
    if (pd_ != nullptr) {
        if (ibv_dealloc_pd(pd_) != 0 && result == Status::OK()) { result = Status::Error(); }
        pd_ = nullptr;
    }
    if (context_ != nullptr) {
        if (ibv_close_device(context_) != 0 && result == Status::OK()) { result = Status::Error(); }
        context_ = nullptr;
    }
    return result;
}

Status IbverbsTransport::RegisterMemory(const MemoryRegion& memory, MemoryHandle& handle)
{
    std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
    handle = kInvalidMemoryHandle;
    if (pd_ == nullptr) { return Status::Error(); }

    const int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                       IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;
    ibv_mr* mr = ibv_reg_mr(pd_, memory.addr, memory.length, access);
    if (mr == nullptr) {
        UC_ERROR("[Transport][IBVERBS] ibv_reg_mr failed addr=0x{:x} length={}",
                 detail::PtrToU64(memory.addr), memory.length);
        return Status::Error();
    }

    auto record = std::make_unique<LocalMemoryRecord>();
    record->mr = mr;
    record->region = memory;

    std::unique_lock<std::shared_mutex> memory_lock(memories_mutex_);
    handle = reinterpret_cast<MemoryHandle>(record.get());
    memories_.emplace(handle, std::move(record));
    UC_DEBUG("[Transport][IBVERBS] register memory addr=0x{:x} length={} lkey={} rkey={}",
             detail::PtrToU64(memory.addr), memory.length, mr->lkey, mr->rkey);
    return Status::OK();
}

Status IbverbsTransport::UnregisterMemory(MemoryHandle handle)
{
    std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
    if (handle == kInvalidMemoryHandle) { return Status::InvalidParam(); }

    std::unique_lock<std::shared_mutex> memory_lock(memories_mutex_);
    const auto it = memories_.find(handle);
    if (it == memories_.end()) { return Status::Error(); }
    if (it->second->mr != nullptr) {
        if (ibv_dereg_mr(it->second->mr) != 0) { return Status::Error(); }
        it->second->mr = nullptr;
    }
    memories_.erase(it);
    return Status::OK();
}

Status IbverbsTransport::EnsurePeerQpLocked(Peer& peer)
{
    if (peer.qp != nullptr) { return Status::OK(); }

    ibv_qp_init_attr init_attr{};
    init_attr.qp_context = this;
    init_attr.send_cq = cq_;
    init_attr.recv_cq = cq_;
    init_attr.cap.max_send_wr = 256;
    init_attr.cap.max_recv_wr = 64;
    init_attr.cap.max_send_sge = 4;
    init_attr.cap.max_recv_sge = 4;
    init_attr.cap.max_inline_data = 0;
    init_attr.qp_type = IBV_QPT_RC;

    ibv_qp* qp = ibv_create_qp(pd_, &init_attr);
    if (qp == nullptr) {
        UC_ERROR("[Transport][IBVERBS] ibv_create_qp failed");
        return Status::Error();
    }
    peer.qp = qp;
    peer.qpn = qp->qp_num;
    peer.psn = (peer.qpn & 0xffffff);
    peer.connected = false;

    ibv_gid gid{};
    if (ibv_query_gid(context_, port_, gid_index_, &gid) != 0) {
        UC_ERROR("[Transport][IBVERBS] ibv_query_gid failed index={}", gid_index_);
        ibv_destroy_qp(qp);
        peer.qp = nullptr;
        return Status::Error();
    }
    CopyGid(peer.gid, gid);
    peer.gid_index = gid_index_;
    UC_DEBUG("[Transport][IBVERBS] created qp qpn={} psn={} gid_index={}", peer.qpn, peer.psn,
             peer.gid_index);
    return Status::OK();
}

Status IbverbsTransport::TransitionQpToConnected(Peer& peer)
{
    if (peer.qp == nullptr) { return Status::Error(); }

    // RESET -> INIT
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = port_;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                           IBV_ACCESS_REMOTE_READ;
    int mask = IBV_QP_STATE | IBV_QP_PORT | IBV_QP_PKEY_INDEX | IBV_QP_ACCESS_FLAGS;
    int rc = ibv_modify_qp(peer.qp, &attr, mask);
    if (rc != 0) {
        UC_ERROR("[Transport][IBVERBS] modify qp RESET->INIT failed errno={} err={}", rc, strerror(rc));
        return Status::Error();
    }

    // INIT -> RTR. Mask set mirrors ibv_rc_pingpong (proven on Soft-RoCE/rxe).
    ibv_port_attr port_attr{};
    if (ibv_query_port(context_, port_, &port_attr) != 0) {
        UC_ERROR("[Transport][IBVERBS] ibv_query_port failed during RTR");
        return Status::Error();
    }
    std::memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = port_attr.active_mtu;
    attr.dest_qp_num = peer.remote_qpn;
    attr.rq_psn = peer.remote_psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;

    ibv_ah_attr& ah = attr.ah_attr;
    std::memset(&ah, 0, sizeof(ah));
    ah.is_global = 1;
    ah.port_num = port_;
    ah.sl = 0;
    ah.src_path_bits = 0;
    ah.dlid = 0;
    ah.grh.sgid_index = static_cast<uint8_t>(peer.gid_index);
    ah.grh.hop_limit = 1;
    ah.grh.traffic_class = 0;
    ah.grh.flow_label = 0;
    CopyGid(ah.grh.dgid, peer.remote_gid);

    mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
           IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(peer.qp, &attr, mask);
    if (rc != 0) {
        UC_ERROR("[Transport][IBVERBS] modify qp INIT->RTR failed errno={} err={}", rc, strerror(rc));
        return Status::Error();
    }

    // RTR -> RTS. Mask set mirrors ibv_rc_pingpong.
    std::memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = peer.psn;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.max_rd_atomic = 1;
    mask = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
           IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
    rc = ibv_modify_qp(peer.qp, &attr, mask);
    if (rc != 0) {
        UC_ERROR("[Transport][IBVERBS] modify qp RTR->RTS failed errno={} err={}", rc, strerror(rc));
        return Status::Error();
    }

    peer.connected = true;
    return Status::OK();
}

Status IbverbsTransport::ExportMetadata(const ManagerID& manager_id, Metadata& out)
{
    std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
    if (context_ == nullptr) { return Status::Error(); }

    Peer* peer = nullptr;
    {
        std::unique_lock<std::shared_mutex> peer_lock(peers_mutex_);
        auto& slot = peers_[manager_id];
        if (slot == nullptr) { slot = std::make_unique<Peer>(); }
        peer = slot.get();
    }
    // QP creation may allocate verbs objects but is safe to hold outside the
    // peers lock because ExportMetadata runs on the control path, not the data
    // path, and the lifecycle lock above guards against teardown races.
    auto status = EnsurePeerQpLocked(*peer);
    if (status != Status::OK()) { return status; }

    out.clear();
    if (!detail::AppendU8(out, kMetadataVersion)) { return Status::Error(); }
    if (EncodeGid(out, peer->gid) != Status::OK()) { return Status::Error(); }
    if (!detail::AppendU32(out, static_cast<uint32_t>(peer->gid_index)) ||
        !detail::AppendU32(out, peer->qpn) ||
        !detail::AppendU32(out, peer->psn)) {
        return Status::Error();
    }

    std::shared_lock<std::shared_mutex> memory_lock(memories_mutex_);
    if (memories_.size() > std::numeric_limits<uint32_t>::max()) { return Status::Error(); }
    UC_DEBUG("[Transport][IBVERBS] export peer={} local_mems={}", manager_id, memories_.size());
    if (!detail::AppendU32(out, static_cast<uint32_t>(memories_.size()))) { return Status::Error(); }
    for (const auto& memory : memories_) {
        const auto& record = *memory.second;
        if (!detail::AppendU64(out, detail::PtrToU64(record.region.addr)) ||
            !detail::AppendU64(out, record.region.length) ||
            !detail::AppendU32(out, record.mr->rkey)) {
            return Status::Error();
        }
    }
    return Status::OK();
}

Status IbverbsTransport::ImportMetadata(const ManagerID& manager_id, const Metadata& metadata)
{
    std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
    if (context_ == nullptr) { return Status::Error(); }

    size_t offset = 0;
    uint8_t version = 0;
    if (!detail::ReadU8(metadata, offset, version) || version != kMetadataVersion) {
        return Status::InvalidParam();
    }

    Peer imported;
    if (DecodeGid(metadata, offset, imported.remote_gid) != Status::OK()) {
        return Status::InvalidParam();
    }
    uint32_t gid_index = 0;
    uint32_t qpn = 0;
    uint32_t psn = 0;
    if (!detail::ReadU32(metadata, offset, gid_index) ||
        !detail::ReadU32(metadata, offset, qpn) ||
        !detail::ReadU32(metadata, offset, psn)) {
        return Status::InvalidParam();
    }
    imported.remote_gid_index = static_cast<int32_t>(gid_index);
    imported.remote_qpn = qpn;
    imported.remote_psn = psn;

    uint32_t mem_count = 0;
    if (!detail::ReadU32(metadata, offset, mem_count)) { return Status::InvalidParam(); }
    UC_DEBUG("[Transport][IBVERBS] import peer={} blob_size={} mem_count={}", manager_id,
             metadata.size(), mem_count);
    imported.remote_mems.clear();
    imported.remote_mems.reserve(mem_count);
    for (uint32_t i = 0; i < mem_count; ++i) {
        RemoteMemory mem;
        uint64_t addr = 0;
        uint64_t length = 0;
        uint32_t rkey = 0;
        if (!detail::ReadU64(metadata, offset, addr) ||
            !detail::ReadU64(metadata, offset, length) ||
            !detail::ReadU32(metadata, offset, rkey)) {
            return Status::InvalidParam();
        }
        mem.addr = addr;
        mem.length = length;
        mem.rkey = rkey;
        imported.remote_mems.push_back(mem);
    }
    if (offset != metadata.size()) { return Status::InvalidParam(); }

    {
        std::unique_lock<std::shared_mutex> peer_lock(peers_mutex_);
        auto& slot = peers_[manager_id];
        if (slot == nullptr) { slot = std::make_unique<Peer>(); }
        Peer& peer = *slot;
        // Preserve the local QP created during ExportMetadata, only refresh the
        // remote view. Do not reset `connected`: a metadata refresh (e.g. KV
        // caches registered after the initial connect) must not tear down an
        // already-established QP. Reconnects are driven by Disconnect (which
        // destroys the QP + clears connected) before the next ExchangeMetadata.
        peer.remote_gid_index = imported.remote_gid_index;
        peer.remote_qpn = imported.remote_qpn;
        peer.remote_psn = imported.remote_psn;
        std::memcpy(peer.remote_gid, imported.remote_gid, kGidSize);
        peer.remote_mems = std::move(imported.remote_mems);
    }
    UC_DEBUG("[Transport][IBVERBS] import peer={} remote_qpn={} remote_mems={}", manager_id,
             imported.remote_qpn, imported.remote_mems.size());
    return Status::OK();
}

Status IbverbsTransport::Connect(const ManagerID& manager_id)
{
    std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
    std::unique_lock<std::shared_mutex> peer_lock(peers_mutex_);
    const auto it = peers_.find(manager_id);
    if (it == peers_.end() || it->second == nullptr || it->second->qp == nullptr) {
        return Status::Error();
    }
    Peer& peer = *it->second;
    if (peer.connected) { return Status::OK(); }
    if (peer.remote_qpn == 0) { return Status::Error(); }

    auto status = TransitionQpToConnected(peer);
    if (status != Status::OK()) { return status; }
    UC_INFO("[Transport][IBVERBS] connected peer={} local_qpn={} remote_qpn={}", manager_id,
            peer.qpn, peer.remote_qpn);
    return Status::OK();
}

Status IbverbsTransport::Disconnect(const ManagerID& manager_id)
{
    std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
    std::unique_lock<std::shared_mutex> peer_lock(peers_mutex_);
    const auto it = peers_.find(manager_id);
    if (it == peers_.end() || it->second == nullptr) { return Status::Error(); }
    Peer& peer = *it->second;
    if (!peer.connected) { return Status::OK(); }
    if (peer.qp == nullptr) { return Status::Error(); }

    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_ERR;
    if (ibv_modify_qp(peer.qp, &attr, IBV_QP_STATE) != 0) {
        UC_WARN("[Transport][IBVERBS] move qp to ERR failed: peer={}", manager_id);
        return Status::Error();
    }
    // Drain in-flight completions so the QP can be torn down cleanly.
    ibv_wc wc{};
    while (ibv_poll_cq(cq_, 1, &wc) > 0) {}
    peer.connected = false;
    // Destroy the QP so a subsequent Connect recreates a fresh one (an ERR-state
    // QP cannot be transitioned back to RESET->INIT on rxe).
    if (ibv_destroy_qp(peer.qp) != 0) {
        UC_WARN("[Transport][IBVERBS] ibv_destroy_qp failed after disconnect: peer={}",
                 manager_id);
    }
    peer.qp = nullptr;
    peer.qpn = 0;
    peer.psn = 0;
    return Status::OK();
}

const IbverbsTransport::LocalMemoryRecord* IbverbsTransport::FindLocalMemory(uint64_t addr,
                                                                             uint64_t length) const
{
    for (const auto& memory : memories_) {
        const auto& record = *memory.second;
        const auto begin = detail::PtrToU64(record.region.addr);
        if (addr < begin || addr + length > begin + record.region.length) { continue; }
        return &record;
    }
    return nullptr;
}

const IbverbsTransport::RemoteMemory* IbverbsTransport::FindRemoteMemory(
    const Peer& peer, uint64_t addr, uint64_t length) const
{
    for (const auto& mem : peer.remote_mems) {
        if (addr >= mem.addr && addr + length <= mem.addr + mem.length) { return &mem; }
    }
    return nullptr;
}

Status IbverbsTransport::ValidateAndResolveLocked(const Operation& request, const Peer& peer,
                                                  size_t) const
{
    if (request.target_manager.empty() || request.ops.empty()) { return Status::InvalidParam(); }
    for (const auto& segment : request.ops) {
        if (segment.local_addr == nullptr || segment.length == 0 || segment.remote_addr == 0) {
            return Status::InvalidParam();
        }
        const auto local_addr = detail::PtrToU64(segment.local_addr);
        if (FindLocalMemory(local_addr, segment.length) == nullptr) {
            UC_ERROR("[Transport][IBVERBS] local addr 0x{:x} len {} not registered",
                     local_addr, segment.length);
            return Status::InvalidParam();
        }
        if (FindRemoteMemory(peer, segment.remote_addr, segment.length) == nullptr) {
            UC_ERROR("[Transport][IBVERBS] remote addr 0x{:x} len {} not in peer memory map",
                     segment.remote_addr, segment.length);
            return Status::InvalidParam();
        }
    }
    return Status::OK();
}

Status IbverbsTransport::PostTransfer(Peer& peer, Opcode opcode,
                                     const std::vector<Segment>& segments, uint64_t wr_id)
{
    std::vector<ibv_sge> sges;
    sges.reserve(segments.size());
    std::vector<ibv_send_wr> wrs(segments.size());

    for (size_t i = 0; i < segments.size(); ++i) {
        const auto local_addr = detail::PtrToU64(segments[i].local_addr);
        const auto* local_mem = FindLocalMemory(local_addr, segments[i].length);
        if (local_mem == nullptr || local_mem->mr == nullptr) { return Status::InvalidParam(); }
        const auto* remote_mem = FindRemoteMemory(peer, segments[i].remote_addr, segments[i].length);
        if (remote_mem == nullptr) { return Status::InvalidParam(); }

        sges.push_back(ibv_sge{});
        sges.back().addr = local_addr;
        sges.back().length = static_cast<uint32_t>(segments[i].length);
        sges.back().lkey = local_mem->mr->lkey;

        wrs[i] = ibv_send_wr{};
        wrs[i].wr_id = wr_id;
        wrs[i].sg_list = &sges[i];
        wrs[i].num_sge = 1;
        wrs[i].opcode = opcode == Opcode::Write ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ;
        wrs[i].wr.rdma.remote_addr = segments[i].remote_addr;
        wrs[i].wr.rdma.rkey = remote_mem->rkey;
        wrs[i].send_flags = IBV_SEND_SIGNALED;
        if (i + 1 < segments.size()) { wrs[i].next = &wrs[i + 1]; }
    }

    ibv_send_wr* bad_wr = nullptr;
    if (ibv_post_send(peer.qp, &wrs.front(), &bad_wr) != 0) {
        UC_ERROR("[Transport][IBVERBS] ibv_post_send {} failed segments={}",
                 VerbsOpName(wrs.front().opcode), segments.size());
        return Status::Error();
    }
    return Status::OK();
}

Status IbverbsTransport::ExecuteSync(const Operation& request)
{
    std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
    // Hold peers_mutex_ across validation + PostTransfer so a concurrent
    // ImportMetadata/Connect cannot mutate peer.remote_mems / peer.remote_qpn
    // while this thread dereferences them. memories_mutex_ is nested for
    // FindLocalMemory inside PostTransfer. Lock order (peers -> memories ->
    // send) is not taken in reverse anywhere else.
    std::shared_lock<std::shared_mutex> peer_lock(peers_mutex_);
    const auto it = peers_.find(request.target_manager);
    if (it == peers_.end() || it->second == nullptr) { return Status::Error(); }
    Peer& peer = *it->second;
    if (!peer.connected || peer.qp == nullptr) { return Status::Error(); }

    std::shared_lock<std::shared_mutex> memory_lock(memories_mutex_);
    if (ValidateAndResolveLocked(request, peer, 0) != Status::OK()) {
        return Status::InvalidParam();
    }

    const uint64_t wr_id = next_wr_id_.fetch_add(1);
    {
        std::lock_guard<std::mutex> completion_lock(completion_mutex_);
        completions_[wr_id] = TransferStatus::Waiting;
    }

    {
        std::lock_guard<std::mutex> send_lock(peer.send_mutex);
        auto status = PostTransfer(peer, request.opcode, request.ops, wr_id);
        if (status != Status::OK()) {
            std::lock_guard<std::mutex> ck(completion_mutex_);
            completions_.erase(wr_id);
            return status;
        }
    }

    std::unique_lock<std::mutex> completion_lock(completion_mutex_);
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(transfer_timeout_ms_);
    if (!completion_cv_.wait_until(completion_lock, deadline, [&] {
            const auto cit = completions_.find(wr_id);
            return cit == completions_.end() || cit->second != TransferStatus::Waiting;
        })) {
        completions_.erase(wr_id);
        UC_ERROR("[Transport][IBVERBS] sync transfer timeout wr_id={}", wr_id);
        return Status::Error();
    }
    const auto cit = completions_.find(wr_id);
    const auto status = cit == completions_.end() ? TransferStatus::Failed : cit->second;
    if (cit != completions_.end()) { completions_.erase(cit); }
    return status == TransferStatus::Completed ? Status::OK() : Status::Error();
}

Status IbverbsTransport::ExecuteAsync(const Operation& request, TransferHandle& handle)
{
    std::shared_lock<std::shared_mutex> lifecycle_lock(lifecycle_mutex_);
    handle = kInvalidTransferHandle;
    // Hold peers_mutex_ across validation + PostTransfer so a concurrent
    // ImportMetadata/Connect cannot mutate peer.remote_mems / peer.remote_qpn
    // while this thread dereferences them. memories_mutex_ is nested for
    // FindLocalMemory inside PostTransfer. Lock order (peers -> memories ->
    // send) is not taken in reverse anywhere else.
    std::shared_lock<std::shared_mutex> peer_lock(peers_mutex_);
    const auto it = peers_.find(request.target_manager);
    if (it == peers_.end() || it->second == nullptr) { return Status::Error(); }
    Peer& peer = *it->second;
    if (!peer.connected || peer.qp == nullptr) { return Status::Error(); }

    std::shared_lock<std::shared_mutex> memory_lock(memories_mutex_);
    if (ValidateAndResolveLocked(request, peer, 0) != Status::OK()) {
        return Status::InvalidParam();
    }

    const uint64_t wr_id = next_wr_id_.fetch_add(1);
    {
        std::lock_guard<std::mutex> completion_lock(completion_mutex_);
        completions_[wr_id] = TransferStatus::Waiting;
    }

    {
        std::lock_guard<std::mutex> send_lock(peer.send_mutex);
        auto status = PostTransfer(peer, request.opcode, request.ops, wr_id);
        if (status != Status::OK()) {
            std::lock_guard<std::mutex> ck(completion_mutex_);
            completions_.erase(wr_id);
            return status;
        }
    }

    std::lock_guard<std::mutex> pending_lock(pending_mutex_);
    handle = next_transfer_handle_++;
    if (handle == kInvalidTransferHandle) { handle = next_transfer_handle_++; }
    pending_transfers_.emplace(handle, PendingTransfer{wr_id});
    return Status::OK();
}

Status IbverbsTransport::GetStatus(TransferHandle handle, TransferStatus& status)
{
    status = TransferStatus::Failed;
    if (handle == kInvalidTransferHandle) { return Status::InvalidParam(); }

    PendingTransfer pending;
    {
        std::lock_guard<std::mutex> pending_lock(pending_mutex_);
        const auto it = pending_transfers_.find(handle);
        if (it == pending_transfers_.end()) { return Status::Error(); }
        pending = it->second;
    }

    {
        std::lock_guard<std::mutex> completion_lock(completion_mutex_);
        const auto it = completions_.find(pending.wr_id);
        if (it == completions_.end()) {
            status = TransferStatus::Failed;
        } else {
            status = it->second;
        }
    }
    if (status != TransferStatus::Waiting) {
        std::lock_guard<std::mutex> pending_lock(pending_mutex_);
        pending_transfers_.erase(handle);
        std::lock_guard<std::mutex> completion_lock(completion_mutex_);
        completions_.erase(pending.wr_id);
    }
    return Status::OK();
}

void IbverbsTransport::RecordCompletion(uint64_t wr_id, TransferStatus status)
{
    {
        std::lock_guard<std::mutex> lock(completion_mutex_);
        completions_[wr_id] = status;
    }
    completion_cv_.notify_all();
}

void IbverbsTransport::PollerMain()
{
    constexpr int kBatch = 16;
    std::vector<ibv_wc> wcs(kBatch);
    while (!stopping_.load()) {
        const int n = ibv_poll_cq(cq_, kBatch, wcs.data());
        if (n <= 0) {
            if (n < 0) {
                UC_WARN("[Transport][IBVERBS] ibv_poll_cq returned {}", n);
            }
            std::this_thread::sleep_for(std::chrono::microseconds(poll_interval_us_));
            continue;
        }
        for (int i = 0; i < n; ++i) {
            const auto& wc = wcs[i];
            const auto status = wc.status == IBV_WC_SUCCESS ? TransferStatus::Completed
                                                            : TransferStatus::Failed;
            if (wc.status != IBV_WC_SUCCESS) {
                UC_ERROR("[Transport][IBVERBS] completion failed wr_id={} status={} opcode={}",
                         wc.wr_id, static_cast<int>(wc.status), static_cast<int>(wc.opcode));
            }
            RecordCompletion(wc.wr_id, status);
        }
    }
}

}  // namespace transport
