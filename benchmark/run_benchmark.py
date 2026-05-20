import argparse
import datetime
import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
from kubernetes import client, config, watch

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

log = logging.getLogger(__name__)

WORKLOAD_TEMPLATE = """
apiVersion: v1
kind: Service
metadata:
  name: bench-db-svc
  labels:
    benchmark: "true"
spec:
  selector:
    app: bench-db
  ports:
  - port: 5432
---
apiVersion: v1
kind: Service
metadata:
  name: bench-backend-svc
  labels:
    benchmark: "true"
spec:
  selector:
    app: bench-backend
  ports:
  - port: 8080
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bench-db
  labels:
    benchmark: "true"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: bench-db
  template:
    metadata:
      labels:
        app: bench-db
        benchmark: "true"
    spec:
      schedulerName: {SCHEDULER}
      containers:
      - name: db
        image: nginx
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bench-backend
  labels:
    benchmark: "true"
spec:
  replicas: 2
  selector:
    matchLabels:
      app: bench-backend
  template:
    metadata:
      labels:
        app: bench-backend
        benchmark: "true"
    spec:
      schedulerName: {SCHEDULER}
      containers:
      - name: backend
        image: nginx
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
        env:
        - name: DB_URL
          value: "http://bench-db-svc"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bench-frontend
  labels:
    benchmark: "true"
spec:
  replicas: 2
  selector:
    matchLabels:
      app: bench-frontend
  template:
    metadata:
      labels:
        app: bench-frontend
        benchmark: "true"
    spec:
      schedulerName: {SCHEDULER}
      containers:
      - name: frontend
        image: nginx
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
        env:
        - name: BACKEND_URL
          value: "http://bench-backend-svc"
"""


SCHEDULER_NAMES = {
    "default": "default-scheduler",
    "graphsched": "graphsched",
}


def start_graphsched_process() -> subprocess.Popen:
    log.info("Starting GraphSched background process...")
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{env.get('PYTHONPATH', '')}:."
    return subprocess.Popen(
        ["python3", "scheduler/graphsched.py"],
        env=env,
        preexec_fn=os.setsid,
    )


def stop_graphsched_process(proc: subprocess.Popen) -> None:
    log.info("Shutting down GraphSched...")
    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    proc.wait()


def wait_for_pods_scheduled(v1: client.CoreV1Api,
                            label_selector: str,
                            expected: int,
                            timeout: float = 90.0,
                            resource_version: str = "0") -> dict:
    latencies = []
    failed = 0
    scheduled_count = 0

    w = watch.Watch()
    print(f"Watching for {expected} pods to schedule...")

    try:
        for event in w.stream(v1.list_namespaced_pod,
                             namespace="default",
                             label_selector=label_selector,
                             timeout_seconds=int(timeout),
                             resource_version=resource_version):  # only new events
            pod = event['object']

            if pod.spec.node_name and event['type'] in ('ADDED', 'MODIFIED'):
                creation_ts = pod.metadata.creation_timestamp.timestamp()
                now = datetime.datetime.now(datetime.timezone.utc).timestamp()
                latency = (now - creation_ts) * 1000
                latencies.append(latency)
                scheduled_count += 1
                log.info(f"Pod {pod.metadata.name} scheduled on {pod.spec.node_name} ({latency:.2f}ms)")

                if scheduled_count >= expected:
                    w.stop()
                    break

    except Exception as e:
        log.error(f"Watch error: {e}")
    finally:
        w.stop()

    import statistics
    latencies.sort()
    return {
        "latencies_ms": latencies,
        "p50_ms": statistics.median(latencies) if latencies else -1,
        "p99_ms": latencies[max(0, int(len(latencies) * 0.99) - 1)] if latencies else -1,
        "failed": expected - scheduled_count,
        "scheduled": scheduled_count,
    }


def cleanup():
    subprocess.run(["kubectl", "delete", "all,svc", "-l", "benchmark=true", "--force", "--grace-period=0"], capture_output=True)
    time.sleep(5)


import threading

def run_for_scheduler(v1: client.CoreV1Api, scheduler_name: str) -> dict:
    log.info(f"\n{'='*50}\nBenchmarking: {scheduler_name}\n{'='*50}")

    manifest = WORKLOAD_TEMPLATE.replace("{SCHEDULER}", scheduler_name)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(manifest)
        fname = f.name

    # Start graph watcher early so graph is warm before pods arrive
    from watcher.graph_watcher import GraphWatcher
    from benchmark.colocation_metric import measure_colocation_rate
    watcher = GraphWatcher()
    watcher.start()
    time.sleep(3)  # let watch streams connect

    # Grab resource version BEFORE applying — watcher will only see events after this point
    pod_list = v1.list_namespaced_pod(namespace="default", label_selector="benchmark=true")
    rv = pod_list.metadata.resource_version

    latency_stats = {}

    def watch_task():
        nonlocal latency_stats
        latency_stats = wait_for_pods_scheduled(
            v1, "benchmark=true", expected=5, timeout=60.0, resource_version=rv)

    timer_thread = threading.Thread(target=watch_task)
    timer_thread.start()
    time.sleep(3)  # let the watch stream connect with the RV

    log.info(f"Applying workload for {scheduler_name}...")
    subprocess.run(["kubectl", "apply", "-f", fname], check=True, capture_output=True)
    os.unlink(fname)
    result = subprocess.run(["kubectl", "get", "svc", "-l", "benchmark=true"], capture_output=True, text=True)
    log.info(f"Services after apply:\n{result.stdout}")

    timer_thread.join(timeout=70)

    log.info("Analyzing topology co-location...")
    time.sleep(2)
    coloc = measure_colocation_rate(watcher.get_graph(), v1)

    result = {
        "scheduler": scheduler_name,
        "scheduled": latency_stats.get("scheduled", 0),
        "failed": latency_stats.get("failed", 6),
        "p50_latency_ms": round(latency_stats.get("p50_ms", -1), 1),
        "p99_latency_ms": round(latency_stats.get("p99_ms", -1), 1),
        "co_location_rate": round(coloc["co_location_rate"], 3),
        "co_located_pairs": coloc["co_located_pairs"],
        "total_dependent_pairs": coloc["total_dependent_pairs"],
    }

    log.info(f"Result: {result}")
    cleanup()
    return result


def print_summary(result: dict, outfile: str) -> None:
    print(f"\n{'='*70}")
    print(f"{'Scheduler':<25} {'Sched':>6} {'P99ms':>8} {'Co-loc Rate':>14}")
    print(f"{'='*70}")
    print(f"{result['scheduler']:<25} {result['scheduled']:>6} "
          f"{result['p99_latency_ms']:>8.1f} {result['co_location_rate']:>14.3f}")
    print(f"\nFull results saved to: {outfile}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run GraphSched benchmark workload")
    parser.add_argument(
        "--scheduler",
        required=True,
        choices=["default", "graphsched"],
        help="Scheduler to benchmark: default (kube-scheduler) or graphsched",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON path (default: results/<scheduler>.json)",
    )
    args = parser.parse_args()

    outfile = args.output or f"results/{args.scheduler}.json"
    scheduler_name = SCHEDULER_NAMES[args.scheduler]

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    config.load_kube_config()
    v1 = client.CoreV1Api()
    os.makedirs(os.path.dirname(outfile) or ".", exist_ok=True)

    sched_proc = None
    if args.scheduler == "graphsched":
        sched_proc = start_graphsched_process()
        time.sleep(10)

    try:
        result = run_for_scheduler(v1, scheduler_name)
    finally:
        if sched_proc is not None:
            stop_graphsched_process(sched_proc)

    with open(outfile, "w") as f:
        json.dump(result, f, indent=2)

    print_summary(result, outfile)


if __name__ == "__main__":
    main()