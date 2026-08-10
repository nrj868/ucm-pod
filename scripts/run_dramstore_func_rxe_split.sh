#!/usr/bin/env bash
# Run the DramStore two-node functional + edge suite over soft-RoCE (rxe).
#
# Topology: node0 = DramStore client (worker+scheduler) + drampool0; node1 =
# drampool1. The ring-hash router spreads blocks across both pools.
#
# Cases run (see ucm/store/test/e2e/dramstore_2node_func_test.py for the
# canonical case list and expected behaviour):
#   A1-A6  functional correctness (cross-pool distribution, dump/load,
#          lookup phases, overwrite, load-missing, lookup-on-prefix)
#   B1-B7  size/shape boundaries (min batch, many small, large 1MiB, zero
#          length, multi-shard, asymmetric shard count, pool capacity)
#   C1-C4  key semantics (zero key, last-byte-differs, duplicate-in-batch,
#          non-16-byte key)
#   D1-D4  concurrency (disjoint, overlapping, dump+lookup, cyclic)
#   E1     resilience: pool1 restart + reconnect + dump/load
#   E2     resilience: kill pool1 mid-bench (bench must fail non-zero)
#   E3-E5  bad config (unreachable endpoint, bad device, bad router)
#   F1-F2  routing determinism (same key same pool, remove one pool)
#   G1     re-register KV caches
#   G3     endurance (100 batches sustained)
#
# Usage:
#   bash scripts/run_dramstore_func_rxe_split.sh
#   NODE0=node0 NODE1=node1 IP0=192.168.100.11 IP1=192.168.100.12 \
#     DEVICE=rxe0 BUILD_DIR=build/dram-test bash scripts/run_dramstore_func_rxe_split.sh
set -uo pipefail

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
TEST_PY="$REPO_ROOT/ucm/store/test/e2e/dramstore_2node_func_test.py"
BENCH_PY="$REPO_ROOT/ucm/store/test/e2e/dramstore_bench_test.py"
YAML="$REPO_ROOT/examples/drampool_rxe_split.yaml"

SHARD_SIZE="${SHARD_SIZE:-4096}"
POOL_MB="${POOL_MB:-16}"
ENDURANCE_BATCHES="${ENDURANCE_BATCHES:-100}"
DUMP_TIMEOUT_MS="${DUMP_TIMEOUT_MS:-2000}"
LOAD_TIMEOUT_MS="${LOAD_TIMEOUT_MS:-2000}"
LOOKUP_TIMEOUT_MS="${LOOKUP_TIMEOUT_MS:-500}"

echo "[func] node0=$NODE0($IP0) [dramstore+pool0]  node1=$NODE1($IP1) [pool1]  device=$DEVICE"

for f in "$DRAMPOOL" "$DRAMSTORE_SO" "$P2P_SO" "$TEST_PY" "$BENCH_PY" "$YAML"; do
  [[ -e "$f" ]] || { echo "[err] missing $f (build first)"; exit 1; }
done

# --- sync runtime to both nodes ---
rsync -az "$DRAMSTORE_SO" "$P2P_SO" "$NODE0:/tmp/"
rsync -az "$DRAMSTORE_SO" "$P2P_SO" "$NODE1:/tmp/"
rsync -az "$UCMPIPELINE_SO_DIR"/ucmpipelinestore*.so "$NODE0:/tmp/"
rsync -az "$DRAMPOOL" "$YAML" "$NODE0:/tmp/"
rsync -az "$DRAMPOOL" "$YAML" "$NODE1:/tmp/"
rsync -az "$TEST_PY" "$BENCH_PY" "$NODE0:/tmp/"

start_pool() {  # $1 = host, $2 = label, $3 = control IP:port, $4 = log suffix
  local host="$1" label="$2" addr="$3" logsfx="$4"
  ssh "$host" "rm -f /tmp/dp${logsfx}.log; nohup sh -c 'LD_LIBRARY_PATH=/tmp /tmp/drampool \
    --addr ${addr} --nics ${DEVICE} --transport-protocol ibverbs \
    --pool-size-mb ${POOL_MB} --kvcache-block-sizes 64 4096 65536 1048576 \
    --config /tmp/drampool_rxe_split.yaml; echo EXIT=\$? >>/tmp/dp${logsfx}.log' >/tmp/dp${logsfx}.log 2>&1 & disown" >/dev/null
}

wait_pool_ready() {  # $1 = host, $2 = log suffix
  local host="$1" sfx="$2"
  for i in $(seq 1 30); do
    if ssh "$host" "grep -q 'DramPool service ready' /tmp/dp${sfx}.log" 2>/dev/null; then return 0; fi
    if ssh "$host" "grep -q 'EXIT=' /tmp/dp${sfx}.log" 2>/dev/null; then
      echo "[err] drampool $sfx exited early"; ssh "$host" "cat /tmp/dp${sfx}.log"; return 1; fi
    sleep 1
  done
  echo "[err] drampool $sfx not ready in 30s"; ssh "$host" "tail -30 /tmp/dp${sfx}.log"; return 1
}

ensure_pool1_alive() {
  if ssh "$NODE1" "pgrep -x drampool >/dev/null" 2>/dev/null; then return 0; fi
  echo "[func] pool1 dead, relaunching"
  start_pool "$NODE1" 1 "${IP1}:9000" 1
  wait_pool_ready "$NODE1" 1
}

restart_pools() {
  cleanup_pools
  start_pool "$NODE0" 0 "${IP0}:9000" 0
  start_pool "$NODE1" 1 "${IP1}:9000" 1
  wait_pool_ready "$NODE0" 0 || return 1
  wait_pool_ready "$NODE1" 1 || return 1
}

cleanup_pools() {
  ssh "$NODE0" "pkill -9 -x drampool 2>/dev/null; true" >/dev/null 2>&1 || true
  ssh "$NODE1" "pkill -9 -x drampool 2>/dev/null; true" >/dev/null 2>&1 || true
}

# --- start pools ---
cleanup_pools
start_pool "$NODE0" 0 "${IP0}:9000" 0
start_pool "$NODE1" 1 "${IP1}:9000" 1
echo "[func] waiting for both drampools..."
wait_pool_ready "$NODE0" 0 || exit 1
wait_pool_ready "$NODE1" 1 || exit 1
echo "[func] both drampools ready"

run_case() {  # $1 = case name
  local c="$1"
  echo
  echo "----- case $c -----"
  # Fresh pool state per case so a polluting case (B7 fills the 4KiB class;
  # A1/F1/F2 kill pool1) cannot bleed into the next case.
  restart_pools || { echo "[FAIL] $c: pool restart failed"; record "$c" 1 "pool restart failed"; return; }
  ssh "$NODE0" "LD_LIBRARY_PATH=/tmp python3 /tmp/dramstore_2node_func_test.py \
    --so-dir /tmp --local-host $IP0 --ibverbs-device $DEVICE \
    --node-ids 1 2 \
    --node-control-endpoints $IP0:9000 $IP1:9000 \
    --node-transport-manager-ids $IP0:4501 $IP1:4501 \
    --tensor-size $SHARD_SIZE --pool-mb $POOL_MB --endurance-batches $ENDURANCE_BATCHES \
    --client-kill-host $IP1 --node1-ip $IP1 \
    --case $c" 2>&1 | tee "/tmp/func_${c}.out"
  local rc=${PIPESTATUS[0]}
  # Save drampool logs for this case before the next restart_pools overwrites them.
  ssh "$NODE0" "cp /tmp/dp0.log /tmp/dp0_${c}.log 2>/dev/null" 2>/dev/null
  ssh "$NODE1" "cp /tmp/dp1.log /tmp/dp1_${c}.log 2>/dev/null" 2>/dev/null
  local line
  line=$(grep -E "^\[(PASS|FAIL)\] $c:" "/tmp/func_${c}.out" 2>/dev/null | tail -1)
  [[ -z "$line" ]] && line=$(tail -1 "/tmp/func_${c}.out" 2>/dev/null)
  record "$c" "$rc" "$line"
}

# Per-case result accumulator.
RESULTS_FILE=/tmp/func_results.txt
: > "$RESULTS_FILE"
record() {  # $1 = case, $2 = rc (0=pass, else fail), $3 = summary
  printf "%s\t%s\t%s\n" "$1" "$2" "$3" >> "$RESULTS_FILE"
}

# --- main case list ---
CASES="A1 A2 A3 A4 A5 A6 B1 B2 B3 B4 B5 B6 B7 C1 C2 C3 C4 D1 D2 D3 D4 E1 F1 F2 G1 G3"
for c in $CASES; do
  run_case "$c"
done

# --- E2: kill pool1 mid-bench (functional mode), expect non-zero exit ---
echo
echo "----- case E2 (kill pool1 mid-bench) -----"
ensure_pool1_alive
ssh "$NODE0" "LD_LIBRARY_PATH=/tmp python3 /tmp/dramstore_2node_func_test.py \
  --so-dir /tmp --local-host $IP0 --ibverbs-device $DEVICE \
  --node-ids 1 2 \
  --node-control-endpoints $IP0:9000 $IP1:9000 \
  --node-transport-manager-ids $IP0:4501 $IP1:4501 \
  --tensor-size $SHARD_SIZE --pool-mb $POOL_MB \
  --client-kill-host $IP1 --node1-ip $IP1 \
  --mode functional" >/tmp/func_E2.out 2>&1 &
E2_PID=$!
echo "[E2] bench pid=$E2_PID, sleeping 4s before kill pool1"
sleep 4
ssh "$NODE1" "pkill -9 -x drampool" 2>/dev/null || true
wait $E2_PID
E2_RC=$?
echo "[E2] bench exit rc=$E2_RC (expected non-zero)"
if [[ "$E2_RC" != "0" ]]; then
  record "E2" 0 "kill pool1 mid-bench caused non-zero exit (rc=$E2_RC) as expected"
else
  record "E2" 1 "kill pool1 mid-bench did not cause bench failure (rc=0)"
fi

# --- E3-E5: bad config (no pools needed) ---
for c in E3 E4 E5; do
  echo
  echo "----- case $c (bad config) -----"
  ssh "$NODE0" "LD_LIBRARY_PATH=/tmp python3 /tmp/dramstore_2node_func_test.py \
    --so-dir /tmp --local-host $IP0 --ibverbs-device $DEVICE \
    --node-ids 1 2 \
    --node-control-endpoints $IP0:9000 $IP1:9000 \
    --node-transport-manager-ids $IP0:4501 $IP1:4501 \
    --tensor-size $SHARD_SIZE --pool-mb $POOL_MB \
    --client-kill-host $IP1 --node1-ip $IP1 \
    --case $c" 2>&1 | tee "/tmp/func_${c}.out"
  record "$c" "${PIPESTATUS[0]}" "$(tail -1 /tmp/func_${c}.out 2>/dev/null)"
done

# --- cleanup ---
cleanup_pools

# --- summary ---
echo
echo "================ 2-node functional suite summary ================"
total=0; passed=0; failed=0
while IFS=$'\t' read -r name rc summary; do
  total=$((total+1))
  if [[ "$rc" == "0" ]]; then passed=$((passed+1)); mark="PASS"; else failed=$((failed+1)); mark="FAIL"; fi
  printf "%-4s  %-4s  %s\n" "$name" "$mark" "$summary"
done < "$RESULTS_FILE"
echo "----------------------------------------------------------------"
echo "total=$total passed=$passed failed=$failed"
[[ "$failed" == "0" ]] && echo "RESULT: ALL PASS" || echo "RESULT: SOME FAIL"
exit $failed
