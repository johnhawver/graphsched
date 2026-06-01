#!/usr/bin/env bash
#
# run_matrix.sh — reproducible benchmark matrix for GraphSched.
#
# Runs: RUNS repetitions x {default, graphsched} x {chain, hub}
# Output: results/matrix_<scheduler>_<workload>_run<N>.json
#
# Prereqs: kind cluster up, kubeconfig valid, .venv created.
# Each benchmark starts/stops its own graphsched process, so make sure
# nothing else is holding port 9100 before running.

set -euo pipefail
cd "$(dirname "$0")/.."

if [ -d .venv ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

# Kill any stray scheduler so port 9100 is free for each run.
pkill -9 -f "scheduler/graphsched.py" 2>/dev/null || true
mkdir -p results

RUNS="${RUNS:-3}"
SCHEDULERS=(default graphsched)
WORKLOADS=(chain hub)
SEED="${SEED:-42}"

echo "Matrix: ${RUNS} runs x {${SCHEDULERS[*]}} x {${WORKLOADS[*]}} (seed=${SEED})"
echo

for run in $(seq 1 "$RUNS"); do
  for sched in "${SCHEDULERS[@]}"; do
    for workload in "${WORKLOADS[@]}"; do
      out="results/matrix_${sched}_${workload}_run${run}.json"
      echo "=== run ${run} | ${sched} | ${workload} -> ${out} ==="
      python3 benchmark/run_benchmark.py \
        --scheduler "$sched" \
        --workload "$workload" \
        --seed "$SEED" \
        --output "$out"
      sleep 2
    done
  done
done

echo
echo "Matrix complete. Files:"
ls -1 results/matrix_*.json
