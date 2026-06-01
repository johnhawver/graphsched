# Benchmarking

How to reproduce the numbers in [`../results/final_summary.md`](../results/final_summary.md).

## Prerequisites

- Docker, [kind](https://kind.sigs.k8s.io/), `kubectl`, Python 3.11+
- I run all of this in WSL.

```bash
kind create cluster --name graphsched --config kind-config.yaml
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## The workloads

Both have 5 pods and use `SCHEDULER_PLACEHOLDER` in the manifest, which the runner replaces with the scheduler under test.

- **chain** (`benchmark/workloads/chain.yaml`): a 3-tier app — frontend → backend → db. The frontend pods reference `bench-backend-svc`, the backend pods reference `bench-db-svc`, which gives 6 dependent pairs.
- **hub** (`benchmark/workloads/hub.yaml`): 1 hub + 4 spokes. Each spoke references `bench-hub-svc`, giving 4 dependent pairs.

Dependencies are inferred, not annotated: the runner reads Service selectors and pod env vars (e.g. `BACKEND_URL=http://bench-backend-svc`) to build the graph.

## Running one benchmark

```bash
python3 benchmark/run_benchmark.py --scheduler default    --workload chain
python3 benchmark/run_benchmark.py --scheduler graphsched --workload chain
```

Flags:

- `--scheduler {default, graphsched}` — which scheduler to test.
- `--workload {chain, hub}` — which topology.
- `--output PATH` — where to write the JSON (default `results/<scheduler>_<workload>.json`).
- `--seed N` — seed for reproducibility.
- `--external-graphsched` — do **not** start a graphsched subprocess; use one you already started in another terminal. Only valid with `--scheduler graphsched`.

By default, a `graphsched` run starts and stops its own scheduler process. A `default` run uses the kube-scheduler already in the cluster.

## Running the full matrix

```bash
./scripts/run_matrix.sh           # 3 runs x {default, graphsched} x {chain, hub}
python3 scripts/summarize_matrix.py
```

This writes `results/matrix_*.json` and regenerates `results/final_summary.md`. The summary is computed straight from the JSON files, so it always matches the committed results. Override defaults with env vars: `RUNS=5 SEED=7 ./scripts/run_matrix.sh`.

## Reading the output

Each run writes JSON like:

```json
{
  "workload": "chain",
  "scheduler": "graphsched",
  "scheduled": 5,
  "failed": 0,
  "p50_latency_ms": 222.8,
  "p99_latency_ms": 258.1,
  "co_location_rate": 1.0,
  "co_located_pairs": 6,
  "total_dependent_pairs": 6
}
```

- `co_location_rate` = `co_located_pairs / total_dependent_pairs`. A pair counts only once both pods are bound to a node.
- `p50/p99_latency_ms` is **end-to-end** (pod creation to bound), so it includes queue wait and apiserver propagation, not just the scheduling decision. The scheduler's own decision time is in the Grafana histogram (`graphsched_schedule_latency_seconds`).

## Monitoring (Prometheus + Grafana)

The scheduler exposes metrics on `:9100/metrics`. To view them in Grafana:

```bash
# Prometheus scrapes the scheduler on the host (see monitoring/prometheus.yml)
docker network create graphsched-net
docker run -d --name prometheus --network graphsched-net \
  --add-host=host.docker.internal:host-gateway -p 9090:9090 \
  -v "$PWD/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml" prom/prometheus

docker run -d --name grafana --network graphsched-net -p 3000:3000 grafana/grafana
```

In Grafana (`http://localhost:3000`, admin/admin), add a Prometheus data source at `http://prometheus:9090`, then import `monitoring/grafana-dashboard.json`. A sample dashboard is in [`grafana-screenshot.png`](grafana-screenshot.png).

## Gotchas

- **Port 9100 already in use:** a previous scheduler is still running. Clear it before a run:
  ```bash
  pkill -9 -f "scheduler/graphsched.py"
  ```
  Watch out for `Ctrl+Z` — it suspends the scheduler instead of stopping it, and it keeps the port.
- **Co-location reads 0 in Grafana when idle:** the gauge reflects the current cluster. After a benchmark cleans up its pods, there are no dependent pairs, so it drops to 0. Capture screenshots while pods are running.
- **Latency is noisy on kind:** expect wide run-to-run variance. That is why the summary reports a P99 range and averages over multiple runs.
