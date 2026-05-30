import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

log = logging.getLogger(__name__)

SCHEDULE_LATENCY = Histogram(
    "graphsched_schedule_latency_seconds",
    "Time spent in filter_and_score for one pod",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

PODS_SCHEDULED = Counter(
    "graphsched_pods_scheduled_total",
    "Pods successfully scheduled and bound",
)

SCHEDULE_ERRORS = Counter(
    "graphsched_schedule_errors_total",
    "Pods that failed scheduling after all retries",
)

COLOCATION_RATE = Gauge(
    "graphsched_colocation_rate",
    "Fraction of dependent pod pairs on the same node (0.0-1.0)",
)


def start_metrics_server(port: int = 9100) -> None:
    try:
        start_http_server(port)
        log.info("Prometheus metrics on http://localhost:%s/metrics", port)
    except OSError as e:
        if e.errno == 98:  # Address already in use
            log.warning(
                "Port %s already in use; metrics server not started "
                "(another graphsched instance may be running)",
                port,
            )
        else:
            raise