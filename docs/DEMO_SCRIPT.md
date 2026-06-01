# Demo script

A 5–10 minute walkthrough for showing GraphSched live (e.g. in an interview). Practice it once so the timing is tight.

## Before you start

```bash
cd ~/projects/graphsched
source .venv/bin/activate
pkill -9 -f "scheduler/graphsched.py" 2>/dev/null || true
kind get clusters | grep graphsched || kind create cluster --name graphsched --config kind-config.yaml
kubectl get nodes        # 1 control-plane + 4 workers, all Ready
```

## 1. The one-sentence pitch (30s)

> "The default Kubernetes scheduler places pods by resources but ignores which pods talk to each other. GraphSched builds a dependency graph from Services and env vars and co-locates dependent pods. In my benchmarks it co-locates 100% of dependent pairs versus about 17–25% for the default scheduler."

## 2. Tests pass (30s)

```bash
pytest -q        # 36 passing
```

Quick signal that it's real and covered.

## 3. Run the benchmark live (2–3 min)

```bash
python3 benchmark/run_benchmark.py --scheduler graphsched --workload chain
```

Point out, as it runs:
- The `PLACEMENT` log lines and the score climbing (30 → 53 → 100) as dependencies land on the same node.
- The final line: `scheduled 5/5, co_location_rate 1.0`.

Then contrast:

```bash
python3 benchmark/run_benchmark.py --scheduler default --workload chain
```

- Same workload, `co_location_rate` around 0.167 — the default scheduler spreads dependent pods across nodes.

## 4. The numbers (1–2 min)

Open [`../results/final_summary.md`](../results/final_summary.md). Walk through the table:
- Co-location: 1.000 vs 0.167 / 0.250 — the consistent win.
- Latency: be upfront that GraphSched adds overhead and the end-to-end P99 is noisy on kind; the scheduler's own decision is ~50–100ms (Grafana histogram). The goal is co-location, not raw speed.

## 5. The dashboard (1 min)

Show [`grafana-screenshot.png`](grafana-screenshot.png) (or the live dashboard if Prometheus/Grafana are up — see [BENCHMARKING.md](BENCHMARKING.md)). Four panels: P99 latency, pods scheduled, co-location rate (100%), errors (0).

## 6. How it works, briefly (1–2 min)

Pull up [ARCHITECTURE.md](ARCHITECTURE.md) and hit three points:
1. Watchers keep a live `networkx` dependency graph.
2. For each pending pod, score every node as `0.7 × co-location + 0.3 × resource balance`.
3. Pods are scheduled in dependency order (db → backend → frontend) so a pod's dependencies are already placed when it's scored.

## Likely questions (have answers ready)

- **"Isn't 100% co-location just stacking everything on one node?"** Yes, in these small workloads everything fits on one node, so co-location is easy to max out. A production version would weigh co-location against spreading and fault tolerance; the 0.3 resource term is the first step toward that.
- **"Why is it slower than the default scheduler?"** The end-to-end number includes queue wait and apiserver round-trips. The scheduler's own decision is ~50–100ms. The Python implementation and per-pod API calls add overhead; this is a learning prototype, not a production scheduler.
- **"How do you know two pods depend on each other?"** Heuristics: a pod's env var references a Service name, and that Service's selector matches another pod's labels. Real traffic data (eBPF / service mesh) would be more accurate.

## Teardown

```bash
kubectl delete deployment,svc -l benchmark=true
pkill -9 -f "scheduler/graphsched.py"
```
