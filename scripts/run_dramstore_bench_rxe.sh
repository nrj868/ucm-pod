#!/usr/bin/env bash
# Run the DramStore bench over soft-RoCE (rxe) across two nodes.
#
# node0 runs the `drampool` daemon (ibverbs/rxe0); node1 runs the DramStore
# bench (ucmpipelinestore) which connects to drampool, registers host memory, and
# issues dump/load/lookup over real RDMA.
#
# Usage:
#   bash scripts/run_dramstore_bench_rxe.sh
#   SERVER=node0 CLIENT=node1 SERVER_IP=192.168.100.11 CLIENT_IP=192.168.100.12 \
#     DEVICE=rxe0 BUILD_DIR=build/dram-test bash scripts/run_dramstore_bench_rxe.sh
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
YAML="$REPO_ROOT/examples/drampool_rxe.yaml"

# bench defaults: tensor_size=4096 layer=4 chunk=1 -> shard=16384
SHARD_SIZE="${SHARD_SIZE:-4096}"  # per-shard size = bench tensor-size
POOL_MB="${POOL_MB:-64}"

echo "[run] server=$SERVER($SERVER_IP) client=$CLIENT($CLIENT_IP) device=$DEVICE shard=$SHARD_SIZE"

for f in "$DRAMPOOL" "$DRAMSTORE_SO" "$P2P_SO" "$BENCH" "$YAML"; do
  [[ -e "$f" ]] || { echo "[err] missing $f (build first)"; exit 1; }
done

# --- sync runtime to both nodes ---
for N in "$SERVER" "$CLIENT"; do
  rsync -az "$DRAMSTORE_SO" "$P2P_SO" "$N:/tmp/"
done
rsync -az "$DRAMPOOL" "$YAML" "$SERVER:/tmp/"
rsync -az "$BENCH" "$CLIENT:/tmp/dramstore_bench_test.py"
rsync -az "$UCMPIPELINE_SO_DIR"/ucmpipelinestore*.so "$CLIENT:/tmp/"

# --- start drampool on SERVER ---
ssh "$SERVER" 'rm -f /tmp/drampool.log; nohup sh -c "LD_LIBRARY_PATH=/tmp /tmp/drampool \
  --addr '"$SERVER_IP"':9000 --nics '"$DEVICE"' --transport-protocol ibverbs \
  --pool-size-mb '"$POOL_MB"' --kvcache-block-sizes '"$SHARD_SIZE"' \
  --config /tmp/drampool_rxe.yaml; echo EXIT=\$? >>/tmp/drampool.log" >/tmp/drampool.log 2>&1 & disown' >/dev/null

echo "[run] waiting for drampool to be ready..."
for i in $(seq 1 30); do
  if ssh "$SERVER" 'grep -q "DramPool service ready" /tmp/drampool.log' 2>/dev/null; then
    echo "[run] drampool ready"
    break
  fi
  if ssh "$SERVER" 'grep -q "EXIT=" /tmp/drampool.log' 2>/dev/null; then
    echo "[err] drampool exited early"; ssh "$SERVER" 'cat /tmp/drampool.log'; exit 1
  fi
  sleep 1
done

# --- run bench on CLIENT ---
ssh "$CLIENT" "LD_LIBRARY_PATH=/tmp python3 /tmp/dramstore_bench_test.py \
  --so-dir /tmp --local-host $CLIENT_IP --ibverbs-device $DEVICE \
  --node-control-endpoints $SERVER_IP:9000 \
  --node-transport-manager-ids $SERVER_IP:4501 \
  --node-ids 1 --batch-number 32 --tensor-size 4096 --layer-size 1 --chunk-size 1 --request-size 32" 2>&1 | tee /tmp/bench_out.txt
bench_rc=${PIPESTATUS[0]}

# --- cleanup ---
ssh "$SERVER" 'pkill -9 -x drampool 2>/dev/null; true' >/dev/null 2>&1 || true

echo
if [[ "$bench_rc" == "0" ]]; then echo "RESULT: PASS"; else echo "RESULT: FAIL (rc=$bench_rc)"; fi
echo "--- drampool tail ---"
ssh "$SERVER" 'tail -20 /tmp/drampool.log' 2>/dev/null || true
exit "$bench_rc"
