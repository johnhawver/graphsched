# Architecture

GraphSched is a standalone scheduler process that runs against a kind cluster. It watches the cluster, keeps a live dependency graph of pods, and binds any pod whose `schedulerName` is `graphsched` to the node that best co-locates it with its dependencies.

## Components

| Package | Responsibility |
|---------|----------------|
| `watcher/` | Watch the cluster and maintain the dependency graph |
| `scheduler/` | The scheduler loop, the node scoring, and Prometheus metrics |
| `benchmark/` | Workloads, the benchmark runner, and the co-location metric |
| `monitoring/` | Prometheus scrape config and the Grafana dashboard |

### `watcher/`

- `graph_model.py` — `PodDependencyGraph`, a thread-safe wrapper around a `networkx.DiGraph`. Nodes are pods (keyed by UID), edges are dependencies with a type and weight. `get_snapshot()` returns a copy so readers never touch the live graph under the lock.
- `pod_watcher.py` — watches pods and adds/updates/removes nodes as pods come and go.
- `service_watcher.py` — watches Services and infers edges. It links a "caller" pod to a "target" pod when the caller's env vars reference the Service name and the target's labels match the Service selector. Holds the two helpers `labels_match_selector` and `pod_references_service`.
- `pvc_watcher.py` — watches PVCs (storage dependencies).
- `graph_watcher.py` — starts the three watchers and exposes `get_graph()`.

### `scheduler/`

- `graphsched.py` — `GraphSchedPlugin`. The main loop watches for pending pods with `schedulerName=graphsched`, then schedules them in batches (see below).
- `topology_scorer.py` — the scoring math:
  - `co_location_score` — fraction of a pod's graph neighbors (weighted by edge weight) already on a candidate node.
  - `resource_balance_score` — geometric mean of free CPU and free memory, so a node has to have room on both.
  - `compute_node_score` — combines them: `0.7 × topology + 0.3 × resource`, clamped to an integer 0–100.
- `metrics.py` — Prometheus metrics, served on `:9100`.

### `benchmark/`

- `workloads/chain.yaml` — db ← backend ← frontend (a 3-tier chain, 5 pods).
- `workloads/hub.yaml` — 1 hub + 4 spokes (5 pods).
- `run_benchmark.py` — applies a workload, watches how long each pod takes to schedule, and measures co-location.
- `colocation_metric.py` — given the dependency graph and the live pods, computes the fraction of dependent pairs placed on the same node.

## Scheduling flow

```
pending pod event (schedulerName=graphsched)
        │
        ▼
_drain_pending_pods()                 # one batch per event
        │
        ├── list all pending graphsched pods
        ├── order them by dependency (targets before callers: db → backend → frontend)
        │
        ▼  for each pod, in order
filter_and_score(pod)
        ├── infer network edges on a graph snapshot (Services + env vars)
        ├── build uid → node map from bound pods, the graph, and live pods
        ├── compute per-node CPU/memory usage from bound pods
        ├── score every node: 0.7 × co-location + 0.3 × resource balance
        └── return the highest-scoring node
        │
        ▼
bind(pod, node)                       # create a Binding via the API
        │
        ▼
(after the whole batch) update the co-location gauge once
```

### Why batch in dependency order

When a workload is applied, all of its pods are created at once. If they were scheduled in arrival order, the frontend might be placed before the backend it depends on exists, so there would be nothing to co-locate with. Scheduling targets before callers (db first, then backend, then frontend) means each pod's dependencies are already placed when it is scored, so co-location decisions are stable.

### Resource accounting

A pod is counted against a node as soon as GraphSched binds it, not only once it reaches `Running`. This is tracked in `_bound_pods` (uid → node) so that, within a batch, the second and later pods see the resource cost of the ones already placed.

## Reliability details

- **Retries:** each pod gets 3 scheduling attempts with exponential backoff. A 404 (pod deleted before bind) is skipped, not retried.
- **Stale-cache fallback:** the pod and service list calls are briefly cached; if a list call fails, the last good snapshot is reused instead of crashing.
- **Metrics server:** if port 9100 is already in use, the scheduler logs a warning and keeps running rather than dying.

## Metrics

Served on `:9100/metrics`:

| Metric | Type | Meaning |
|--------|------|---------|
| `graphsched_schedule_latency_seconds` | histogram | Time spent in `filter_and_score` per pod |
| `graphsched_pods_scheduled_total` | counter | Pods successfully bound |
| `graphsched_schedule_errors_total` | counter | Pods that failed after all retries |
| `graphsched_colocation_rate` | gauge | Fraction of dependent pairs on the same node |

See [BENCHMARKING.md](BENCHMARKING.md) for how to scrape and visualize these.
