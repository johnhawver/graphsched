# GraphSched v0.1.0 — Benchmark Summary

Matrix: 3 runs x {default, graphsched} x {chain, hub} on a 4-worker kind cluster (seed=42). Numbers are averaged directly from `results/matrix_*.json` via `scripts/summarize_matrix.py`.

| Scheduler | Workload | Runs | Avg P50 (ms) | Avg P99 (ms) | P99 range (ms) | Avg co-loc | Scheduled |
|-----------|----------|------|--------------|--------------|----------------|------------|-----------|
| default-scheduler | chain | 3 | 485.8 | 507.4 | 390.4–573.3 | 0.167 | 5/5 |
| default-scheduler | hub | 3 | 376.1 | 377.5 | 339.5–427.6 | 0.250 | 5/5 |
| graphsched | chain | 3 | 828.5 | 859.6 | 750.5–1031.9 | 1.000 | 5/5 |
| graphsched | hub | 3 | 407.3 | 437.7 | 404.7–475.9 | 1.000 | 5/5 |

## Co-location: GraphSched vs default

- **chain** (6 dependent pairs): graphsched **1.000** vs default 0.167
- **hub** (4 dependent pairs): graphsched **1.000** vs default 0.250

## Takeaway

GraphSched co-locates **all** dependent pod pairs (co-location 1.000 on both chain and hub), while the default scheduler leaves most dependent pairs split across nodes. That is the consistent result across every run.

Scheduling latency (end-to-end pend-to-bound):

- **chain**: graphsched P99 averaged 859.6ms vs 507.4ms for default (higher this run; ranges do not overlap).
- **hub**: graphsched P99 averaged 437.7ms vs 377.5ms for default (higher this run; ranges overlap).

Latency varies significantly run-to-run on kind (see the P99 range column), and GraphSched adds overhead from per-pod Python scoring, dependency-ordered batching, and extra apiserver calls. The project's goal is dependency-aware co-location, not beating the default scheduler on raw scheduling speed.
