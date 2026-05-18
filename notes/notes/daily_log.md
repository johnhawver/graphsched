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