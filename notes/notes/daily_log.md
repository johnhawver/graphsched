5/18/26
======================================================================
Scheduler                  Sched    P99ms    Co-loc Rate
======================================================================
default-scheduler              5    769.5          0.000
graphsched                     5    362.5          1.000
  {
    "scheduler": "default-scheduler",
    "scheduled": 5,
    "failed": 0,
    "p50_latency_ms": 764.1,
    "p99_latency_ms": 769.5,
    "co_location_rate": 0.0,
    "co_located_pairs": 0,
    "total_dependent_pairs": 0
  },
  {
    "scheduler": "graphsched",
    "scheduled": 5,
    "failed": 0,
    "p50_latency_ms": 339.5,
    "p99_latency_ms": 362.5,
    "co_location_rate": 1.0,
    "co_located_pairs": 6,
    "total_dependent_pairs": 6
  }


5/20/26 (Day 2 — separate --scheduler runs)
======================================================================
Scheduler                  Sched    P99ms    Co-loc Rate
======================================================================
default-scheduler              5    579.7          0.000
graphsched                     5   1299.3          1.000
  {
  "scheduler": "default-scheduler",
  "scheduled": 5,
  "failed": 0,
  "p50_latency_ms": 556.9,
  "p99_latency_ms": 579.7,
  "co_location_rate": 0.0,
  "co_located_pairs": 0,
  "total_dependent_pairs": 4
}
{
  "scheduler": "graphsched",
  "scheduled": 5,
  "failed": 0,
  "p50_latency_ms": 1244.9,
  "p99_latency_ms": 1299.3,
  "co_location_rate": 1.0,
  "co_located_pairs": 6,
  "total_dependent_pairs": 6
}

5/21/26
{
  "scheduler": "graphsched",
  "scheduled": 5,
  "failed": 0,
  "p50_latency_ms": 1142.6,
  "p99_latency_ms": 1216.5,
  "co_location_rate": 1.0,
  "co_located_pairs": 1,
  "total_dependent_pairs": 1
}
{
  "scheduler": "graphsched",
  "scheduled": 5,
  "failed": 0,
  "p50_latency_ms": 761.2,
  "p99_latency_ms": 808.0,
  "co_location_rate": 1.0,
  "co_located_pairs": 1,
  "total_dependent_pairs": 1
}