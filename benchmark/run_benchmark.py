import time
import json
import datetime
import subprocess
import tempfile
import os
import logging
from kubernetes import client, config, watch

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


from kubernetes import watch

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


import signal

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    config.load_kube_config()
    v1 = client.CoreV1Api()
    os.makedirs("results", exist_ok=True)

    results = []
    
    # Test Default Scheduler
    results.append(run_for_scheduler(v1, "default-scheduler"))

    # Test GraphSched with Auto-Start
    print("\nStarting GraphSched background process...")
    my_env = os.environ.copy()
    my_env["PYTHONPATH"] = f"{my_env.get('PYTHONPATH', '')}:."
    
    # Start the scheduler process
    sched_proc = subprocess.Popen(
        ["python3", "scheduler/graphsched.py"],
        env=my_env,
        preexec_fn=os.setsid 
    )
    
    try:
        # Give the scheduler time to connect to the cluster
        time.sleep(10) 
        results.append(run_for_scheduler(v1, "graphsched"))
    finally:
        # Kill the scheduler process group
        print("Shutting down GraphSched...")
        os.killpg(os.getpgid(sched_proc.pid), signal.SIGTERM)
        sched_proc.wait()

    # Results
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = f"results/benchmark_{ts}.json"
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n{'='*70}")
    print(f"{'Scheduler':<25} {'Sched':>6} {'P99ms':>8} {'Co-loc Rate':>14}")
    print(f"{'='*70}")
    for r in results:
        print(f"{r['scheduler']:<25} {r['scheduled']:>6} "
              f"{r['p99_latency_ms']:>8.1f} {r['co_location_rate']:>14.3f}")
    print(f"\nFull results saved to: {outfile}")