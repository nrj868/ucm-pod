#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>
#include "core/transport.h"

namespace transport {

enum class HixlRole : uint8_t {
    Server = 0,
    Client = 1,
    Bidirectional = 2,
};

struct HixlInitAttrs : public InitAttrs {
    struct Instance {
        int32_t port = 0;
        int32_t device_id = -1;
        std::map<std::string, std::string> options;
    };

    std::string ip = "127.0.0.1";
    std::vector<Instance> instances;
    HixlRole role = HixlRole::Bidirectional;
    int32_t connect_timeout_ms = 1000;
    int32_t transfer_timeout_ms = 1000;
};

// Pure libibverbs backend configuration. Targets the RC (reliable connection)
// transport over a single device/port (e.g. Soft-RoCE/rxe). No ACL/HIXL runtime
// is required, so this backend builds on plain x86 hosts with libibverbs.
struct IbverbsInitAttrs : public InitAttrs {
    // Device name to open (e.g. "rxe0"). Empty selects the first available device.
    std::string device_name;
    // Device port number (1-based). 0 defaults to 1.
    uint8_t port = 1;
    // GID index to use for the address handle. -1 auto-selects a RoCEv2 IPv4 GID.
    int32_t gid_index = -1;
    int32_t send_wr_depth = 256;
    int32_t recv_wr_depth = 64;
    int32_t sge_depth = 4;
    int32_t cq_depth = 1024;
    // Poller thread sleep between empty CQ polls, microseconds.
    int32_t poll_interval_us = 50;
    int32_t connect_timeout_ms = 30000;
    int32_t transfer_timeout_ms = 30000;
};

}  // namespace transport
