#!/usr/bin/env bash
# Run the DramStore bench over soft-RoCE (rxe) with TWO drampool nodes.
#
# node0 runs two drampool daemons (ibverbs/rxe0) on distinct ports; node1 runs
# the DramStore bench, which connects to both and the ring-hash router spreads
# blocks across the two pools. Verifies dump/load/lookup across a multi-node
# DramStore.
#
# Usage:
#   bash scripts/run_dramstore_bench_rxe_2node.sh
#   SERVER=node0 CLIENT=node1 SERVER_IP=192.168.100.11 CLIENT_IP=192.168.100.12 \
#     DEVICE=rxe0 BUILD_DIR=build/dram-test bash scripts/run_dramstore_bench_rxe_2node.sh
set -euo pipefail

SERVER="${SERVER:-node0}"
CLIENT="${CLIENT:-node1}"
SERVER_IP="${SERVER_IP:-192.168.100.11}"
CLIENT_IP="${CLIENT_IP:-192.168.100.12}"
DEVICE="${DEVICE:-rxe0}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/build/dram-test}"

DRAMPOOL="$BUILD_DIR/ucm/store/dram/drampool"
DRAMSTORE_SO="$BUILD_DIR/ucm/store/dram/libdramstore.so"
P2P_SO="$BUILD_DIR/ucm/transport/p2p/libucm_p2p_transport.so"
UCMPIPELINE_SO_DIR="$BUILD_DIR/ucm/store/pipeline"
BENCH="$REPO_ROOT/ucm/store/test/e2e/dramstore_bench_test.py"
YAML="$REPO_ROOT/examples/drampool_rxe_2node.yaml"

SHARD_SIZE="${SHARD_SIZE:-4096}"
POOL_MB="${POOL_MB:-16}"

echo "[run2] server=$SERVER($SERVER_IP) client=$CLIENT($CLIENT_IP) device=$DEVICE two pools"

for f in "$DRAMPOOL" "$DRAMSTORE_SO" "$P2P_SO" "$BENCH" "$YAML"; do
  [[ -e "$f" ]] || { echo "[err] missing $f (build first)"; exit 1; }
done

# --- sync runtime to both nodes ---
rsync -az "$DRAMSTORE_SO" "$P2P_SO" "$SERVER:/tmp/"
rsync -az "$DRAMSTORE_SO" "$P2P_SO" "$CLIENT:/tmp/"
rsync -az "$DRAMPOOL" "$YAML" "$SERVER:/tmp/"
rsync -az "$BENCH" "$CLIENT:/tmp/dramstore_bench_test.py"
rsync -az "$UCMPIPELINE_SO_DIR"/ucmpipelinestore*.so "$CLIENT:/tmp/"

start_pool() {  # $1 = label, $2 = control port, $3 = one_sided port
  local label="$1" ctl="$2" one="$3"
  ssh "$SERVER" "rm -f /tmp/dp${label}.log; nohup sh -c 'LD_LIBRARY_PATH=/tmp /tmp/drampool \
    --addr ${SERVER_IP}:${ctl} --nics ${DEVICE} --transport-protocol ibverbs \
    --pool-size-mb ${POOL_MB} --kvcache-block-sizes ${SHARD_SIZE} \
    --config /tmp/drampool_rxe_2node.yaml; echo EXIT=\$? >>/tmp/dp${label}.log' >/tmp/dp${label}.log 2>&1 & disown" >/dev/null
}

# --- start two drampools on SERVER (node0) ---
start_pool 0 9000 4501
start_pool 1 9001 4503

echo "[run2] waiting for drampools to be ready..."
for lbl in 0 1; do
  for i in $(seq 1 30); do
    if ssh "$SERVER" "grep -q 'DramPool service ready' /tmp/dp${lbl}.log" 2>/dev/null; then break; fi
    if ssh "$SERVER" "grep -q 'EXIT=' /tmp/dp${lbl}.log" 2>/dev/null; then
      echo "[err] drampool $lbl exited early"; ssh "$SERVER" "cat /tmp/dp${lbl}.log"; exit 1; fi
    sleep 1
  done
done
echo "[run2] both drampools ready"

# --- run bench on CLIENT (node1) against both pools ---
ssh "$CLIENT" "LD_LIBRARY_PATH=/tmp python3 /tmp/dramstore_bench_test.py \
  --so-dir /tmp --local-host $CLIENT_IP --ibverbs-device $DEVICE \
  --node-ids 1 2 \
  --node-control-endpoints $SERVER_IP:9000 $SERVER_IP:9001 \
  --node-transport-manager-ids $SERVER_IP:4501 $SERVER_IP:4503 \
  --batch-number 32 --tensor-size 4096 --layer-size 1 --chunk-size 1 --request-size 32" 2>&1 | tee /tmp/bench2_out.txt
bench_rc=${PIPESTATUS[0]}

# --- cleanup ---
for lbl in 0 1; do ssh "$SERVER" "pkill -9 -x drampool 2>/dev/null; true" >/dev/null 2>&1 || true; done

echo
if [[ "$bench_rc" == "0" ]]; then echo "RESULT: PASS"; else echo "RESULT: FAIL (rc=$bench_rc)"; fi
exit "$bench_rc"
