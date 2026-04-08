import time
import json
import datetime
import subprocess
import tempfile
import os
import logging
from kubernetes import client, config

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


def wait_for_pods_scheduled(v1: client.CoreV1Api,
                             label_selector: str,
                             expected: int,
                             timeout: float = 90.0) -> dict:
    """
    Wait for pods to be assigned nodes. Return latency stats.
    """
    deadline = time.time() + timeout

    while time.time() < deadline:
        pods = v1.list_namespaced_pod("default", label_selector=label_selector)
        scheduled = [p for p in pods.items if p.spec.node_name]
        if len(scheduled) >= expected:
            break
        time.sleep(0.5)

    pods = v1.list_namespaced_pod("default", label_selector=label_selector)
    latencies = []
    failed = 0

    for pod in pods.items:
        if not pod.spec.node_name:
            failed += 1
            continue
        created = pod.metadata.creation_timestamp.timestamp()
        if pod.status.conditions:
            for cond in pod.status.conditions:
                if cond.type == "PodScheduled" and cond.status == "True":
                    sched_time = cond.last_transition_time.timestamp()
                    latencies.append((sched_time - created) * 1000)
                    break

    import statistics
    latencies.sort()
    return {
        "latencies_ms": latencies,
        "p50_ms": statistics.median(latencies) if latencies else -1,
        "p99_ms": latencies[max(0, int(len(latencies) * 0.99) - 1)] if latencies else -1,
        "failed": failed,
        "scheduled": len(latencies),
    }


def cleanup():
    subprocess.run(["kubectl", "delete", "all,svc", "-l", "benchmark=true",
                    "--ignore-not-found"], capture_output=True)
    time.sleep(5)


def run_for_scheduler(v1: client.CoreV1Api, scheduler_name: str) -> dict:
    log.info(f"\n{'='*50}\nBenchmarking: {scheduler_name}\n{'='*50}")

    manifest = WORKLOAD_TEMPLATE.replace("{SCHEDULER}", scheduler_name)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(manifest)
        fname = f.name

    subprocess.run(["kubectl", "apply", "-f", fname], check=True, capture_output=True)
    os.unlink(fname)
    time.sleep(10)

    latency_stats = wait_for_pods_scheduled(
        v1, "benchmark=true", expected=5, timeout=90.0)

    # Measure co-location using live graph
    from watcher.graph_watcher import GraphWatcher
    from benchmark.colocation_metric import measure_colocation_rate
    watcher = GraphWatcher()
    watcher.start()
    time.sleep(4)
    coloc = measure_colocation_rate(watcher.get_graph(), v1)

    result = {
        "scheduler": scheduler_name,
        "scheduled": latency_stats["scheduled"],
        "failed": latency_stats["failed"],
        "p50_latency_ms": round(latency_stats["p50_ms"], 1),
        "p99_latency_ms": round(latency_stats["p99_ms"], 1),
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
        time.sleep(5) 
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