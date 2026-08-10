#!/usr/bin/env bash
# Run the DramStore bench over soft-RoCE (rxe) with a SPLIT two-node topology:
# node0 runs the DramStore client (worker + scheduler) AND drampool0; node1
# runs drampool1 alone. The ring-hash router spreads blocks across both pools,
# so each batch exercises a local hit (node0 pool) and a remote hit (node1 pool)
# over rxe in the same run. Verifies dump/load/lookup across the distributed pool.
#
# Usage:
#   bash scripts/run_dramstore_bench_rxe_split.sh
#   NODE0=node0 NODE1=node1 IP0=192.168.100.11 IP1=192.168.100.12 \
#     DEVICE=rxe0 BUILD_DIR=build/dram-test bash scripts/run_dramstore_bench_rxe_split.sh
set -euo pipefail

NODE0="${NODE0:-node0}"
NODE1="${NODE1:-node1}"
IP0="${IP0:-192.168.100.11}"
IP1="${IP1:-192.168.100.12}"
DEVICE="${DEVICE:-rxe0}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/build/dram-test}"

DRAMPOOL="$BUILD_DIR/ucm/store/dram/drampool"
DRAMSTORE_SO="$BUILD_DIR/ucm/store/dram/libdramstore.so"
P2P_SO="$BUILD_DIR/ucm/transport/p2p/libucm_p2p_transport.so"
UCMPIPELINE_SO_DIR="$BUILD_DIR/ucm/store/pipeline"
BENCH="$REPO_ROOT/ucm/store/test/e2e/dramstore_bench_test.py"
YAML="$REPO_ROOT/examples/drampool_rxe_split.yaml"

SHARD_SIZE="${SHARD_SIZE:-4096}"
POOL_MB="${POOL_MB:-16}"

echo "[split] node0=$NODE0($IP0) [dramstore+pool0]  node1=$NODE1($IP1) [pool1]  device=$DEVICE"

for f in "$DRAMPOOL" "$DRAMSTORE_SO" "$P2P_SO" "$BENCH" "$YAML"; do
  [[ -e "$f" ]] || { echo "[err] missing $f (build first)"; exit 1; }
done

# --- sync runtime to both nodes ---
# bench host = node0: needs ucmpipelinestore + libdramstore + libucm_p2p_transport
# pool hosts = node0 + node1: need drampool + YAML
rsync -az "$DRAMSTORE_SO" "$P2P_SO" "$NODE0:/tmp/"
rsync -az "$UCMPIPELINE_SO_DIR"/ucmpipelinestore*.so "$NODE0:/tmp/"
rsync -az "$DRAMPOOL" "$YAML" "$NODE0:/tmp/"
rsync -az "$DRAMSTORE_SO" "$P2P_SO" "$NODE1:/tmp/"
rsync -az "$DRAMPOOL" "$YAML" "$NODE1:/tmp/"
rsync -az "$BENCH" "$NODE0:/tmp/dramstore_bench_test.py"

start_pool() {  # $1 = host, $2 = label, $3 = control IP:port, $4 = log suffix
  local host="$1" label="$2" addr="$3" logsfx="$4"
  ssh "$host" "rm -f /tmp/dp${logsfx}.log; nohup sh -c 'LD_LIBRARY_PATH=/tmp /tmp/drampool \
    --addr ${addr} --nics ${DEVICE} --transport-protocol ibverbs \
    --pool-size-mb ${POOL_MB} --kvcache-block-sizes ${SHARD_SIZE} \
    --config /tmp/drampool_rxe_split.yaml; echo EXIT=\$? >>/tmp/dp${logsfx}.log' >/tmp/dp${logsfx}.log 2>&1 & disown" >/dev/null
}

# --- start drampool0 on node0, drampool1 on node1 ---
start_pool "$NODE0" 0 "${IP0}:9000" 0
start_pool "$NODE1" 1 "${IP1}:9000" 1

echo "[split] waiting for drampools to be ready..."
for sfx in 0 1; do
  host="$NODE0"; [[ "$sfx" == "1" ]] && host="$NODE1"
  for i in $(seq 1 30); do
    if ssh "$host" "grep -q 'DramPool service ready' /tmp/dp${sfx}.log" 2>/dev/null; then break; fi
    if ssh "$host" "grep -q 'EXIT=' /tmp/dp${sfx}.log" 2>/dev/null; then
      echo "[err] drampool $sfx exited early"; ssh "$host" "cat /tmp/dp${sfx}.log"; exit 1; fi
    sleep 1
  done
done
echo "[split] both drampools ready"

# --- run bench on node0 against both pools (local pool0 + remote pool1) ---
ssh "$NODE0" "LD_LIBRARY_PATH=/tmp python3 /tmp/dramstore_bench_test.py \
  --so-dir /tmp --local-host $IP0 --ibverbs-device $DEVICE \
  --node-ids 1 2 \
  --node-control-endpoints $IP0:9000 $IP1:9000 \
  --node-transport-manager-ids $IP0:4501 $IP1:4501 \
  --batch-number 32 --tensor-size 4096 --layer-size 1 --chunk-size 1 --request-size 32" 2>&1 | tee /tmp/bench_split_out.txt
bench_rc=${PIPESTATUS[0]}

# --- cleanup ---
ssh "$NODE0" "pkill -9 -x drampool 2>/dev/null; true" >/dev/null 2>&1 || true
ssh "$NODE1" "pkill -9 -x drampool 2>/dev/null; true" >/dev/null 2>&1 || true

echo
if [[ "$bench_rc" == "0" ]]; then echo "RESULT: PASS"; else echo "RESULT: FAIL (rc=$bench_rc)"; fi
exit "$bench_rc"
