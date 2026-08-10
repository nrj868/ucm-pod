#!/usr/bin/env bash
# Run the DramStore ibverbs (soft-RoCE/rxe) transport end-to-end across two
# nodes, over several scenarios (functional / large / concurrency).
#
# The CLIENT side uses UC::Dram::CreateTransportManagerBackend (the exact
# factory the DramStore store plugin uses) with the ibverbs protocol. The
# SERVER side is a minimal transport::TransportManager peer that RDMA-writes a
# pattern into the client's registered buffer, proving the client's
# RegisterMemory + Connect path carries real RDMA traffic.
#
# Usage:
#   ./run_dramstore_ibverbs_e2e.sh
#   SERVER=node0 CLIENT=node1 SERVER_IP=192.168.100.11 CLIENT_IP=192.168.100.12 \
#     DEVICE=rxe0 MODES="functional large concurrency" ./run_dramstore_ibverbs_e2e.sh
#
# The binary is built locally then rsynced to both nodes with its shared libs.
# libibverbs must be installed on the nodes (it is, since rxe is).
set -euo pipefail

# --- config (override via env) ---
SERVER="${SERVER:-node0}"
CLIENT="${CLIENT:-node1}"
SERVER_IP="${SERVER_IP:-192.168.100.11}"
CLIENT_IP="${CLIENT_IP:-192.168.100.12}"
DEVICE="${DEVICE:-rxe0}"
LOCAL_HOST_SERVER="${LOCAL_HOST_SERVER:-$SERVER_IP}"
LOCAL_HOST_CLIENT="${LOCAL_HOST_CLIENT:-$CLIENT_IP}"
WAIT="${WAIT:-25}"
MODES="${MODES:-functional large concurrency}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_NAME="dramstore_ibverbs_e2e"
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/build}"
BIN_PATH="$BUILD_DIR/ucm/store/dram/$BIN_NAME"

echo "[run] repo=$REPO_ROOT  server=$SERVER($SERVER_IP)  client=$CLIENT($CLIENT_IP)  device=$DEVICE  modes=[$MODES]"

# --- sanity ---
ssh -o ConnectTimeout=8 "$SERVER" true || { echo "[err] cannot ssh $SERVER"; exit 1; }
ssh -o ConnectTimeout=8 "$CLIENT" true || { echo "[err] cannot ssh $CLIENT"; exit 1; }
for N in "$SERVER" "$CLIENT"; do
  ssh -o ConnectTimeout=8 "$N" 'command -v ibv_devinfo >/dev/null' || { echo "[err] $N missing ibverbs-utils"; exit 1; }
  ssh -o ConnectTimeout=8 "$N" "ibv_devinfo 2>/dev/null | grep -q hca_id" || { echo "[err] no RDMA device on $N"; exit 1; }
done

# --- build locally ---
if [[ ! -x "$BIN_PATH" ]]; then
  echo "[run] building $BIN_NAME"
  cmake -B "$BUILD_DIR" -DRUNTIME_ENVIRONMENT=simu -DP2P_ENABLE_IBVERBS=ON -DBUILD_DRAM_IBVERBS_E2E=ON >/dev/null
  cmake --build "$BUILD_DIR" --target "$BIN_NAME" -j >/dev/null
fi
[[ -x "$BIN_PATH" ]] || { echo "[err] build did not produce $BIN_PATH"; exit 1; }

# --- rsync binary + shared libs to both nodes ---
DRAM_SO="$BUILD_DIR/ucm/store/dram/libdramstore.so"
P2P_SO="$BUILD_DIR/ucm/transport/p2p/libucm_p2p_transport.so"
for N in "$SERVER" "$CLIENT"; do
  echo "[run] syncing binary + libs to $N"
  rsync -az "$BIN_PATH" "$DRAM_SO" "$P2P_SO" "$N:/tmp/"
done

run_mode() {
  local mode="$1"
  local idx="$2"
  # Distinct ports per mode so sequential runs don't fight TIME_WAIT binds.
  local base=$((4500 + idx * 100))
  local mgr_a=$((base + 1))
  local mgr_b=$((base + 2))
  local ctl_a=$((base + 101))
  local ctl_b=$((base + 102))
  local be_ctl=$((base + 202))

  echo
  echo "================ MODE=$mode ================"
  ssh "$SERVER" 'rm -f /tmp/srv.log' 2>/dev/null || true
  ssh "$CLIENT" 'rm -f /tmp/cli.log' 2>/dev/null || true

  ssh "$SERVER" "nohup sh -c 'LD_LIBRARY_PATH=/tmp \
    IBV_TEST_LOCAL_HOST=$LOCAL_HOST_SERVER IBV_TEST_PEER_HOST=$CLIENT_IP IBV_TEST_DEVICE=$DEVICE \
    IBV_TEST_MODE=$mode \
    TRANSPORT_TEST_PORT_A=$mgr_a TRANSPORT_TEST_PORT_B=$mgr_b \
    TRANSPORT_CONTROL_PORT_A=$ctl_a TRANSPORT_CONTROL_PORT_B=$ctl_b \
    /tmp/$BIN_NAME server; echo EXIT=\$? >>/tmp/srv.log' >/tmp/srv.log 2>&1 & disown" >/dev/null
  sleep 1
  ssh "$CLIENT" "nohup sh -c 'LD_LIBRARY_PATH=/tmp \
    IBV_TEST_LOCAL_HOST=$LOCAL_HOST_CLIENT IBV_TEST_PEER_HOST=$SERVER_IP IBV_TEST_DEVICE=$DEVICE \
    IBV_TEST_MODE=$mode \
    TRANSPORT_TEST_PORT_A=$mgr_a TRANSPORT_TEST_PORT_B=$mgr_b \
    TRANSPORT_CONTROL_PORT_A=$ctl_a TRANSPORT_CONTROL_PORT_B=$ctl_b \
    DRAM_BACKEND_CONTROL_PORT=$be_ctl \
    /tmp/$BIN_NAME client; echo EXIT=\$? >>/tmp/cli.log' >/tmp/cli.log 2>&1 & disown" >/dev/null

  echo "[run] mode=$mode waiting ${WAIT}s..."
  sleep "$WAIT"

  echo "--- server ($SERVER) ---"
  ssh "$SERVER" 'grep -E "manager_id|READY|ADDR|RDMA write|DONE|EXIT=|failed|error" /tmp/srv.log' 2>/dev/null || echo "(no log)"
  echo "--- client ($CLIENT) ---"
  ssh "$CLIENT" 'grep -E "manager_id|backend Connect|VERIFY|EXIT=|failed|error" /tmp/cli.log' 2>/dev/null || echo "(no log)"

  local srv_exit cli_exit verify
  srv_exit=$(ssh "$SERVER" 'grep -oE "EXIT=[0-9]+" /tmp/srv.log | tail -1 | cut -d= -f2' 2>/dev/null || echo "")
  cli_exit=$(ssh "$CLIENT" 'grep -oE "EXIT=[0-9]+" /tmp/cli.log | tail -1 | cut -d= -f2' 2>/dev/null || echo "")
  verify=$(ssh "$CLIENT" 'grep -c "VERIFY.*PASS" /tmp/cli.log' 2>/dev/null || echo 0)

  ssh "$SERVER" "pkill -9 -x $BIN_NAME 2>/dev/null; true" >/dev/null 2>&1 || true
  ssh "$CLIENT" "pkill -9 -x $BIN_NAME 2>/dev/null; true" >/dev/null 2>&1 || true

  if [[ "$srv_exit" == "0" && "$cli_exit" == "0" && "$verify" == "1" ]]; then
    echo ">>> $mode: PASS (srv=$srv_exit cli=$cli_exit verify=$verify)"
    return 0
  else
    echo ">>> $mode: FAIL (srv=$srv_exit cli=$cli_exit verify=$verify)"
    return 1
  fi
}

# --- run all modes ---
overall=0
idx=0
declare -a results
for mode in $MODES; do
  if run_mode "$mode" "$idx"; then
    results+=("$mode|PASS")
  else
    results+=("$mode|FAIL")
    overall=1
  fi
  idx=$((idx + 1))
  sleep 1
done

echo
echo "================ SUMMARY ================"
printf '%-14s %s\n' "MODE" "RESULT"
for r in "${results[@]}"; do
  printf '%-14s %s\n' "${r%%|*}" "${r##*|}"
done

if [[ "$overall" == "0" ]]; then
  echo "ALL PASS"
else
  echo "SOME FAILED"
fi
exit $overall
