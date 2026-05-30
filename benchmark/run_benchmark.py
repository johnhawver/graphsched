import argparse
import datetime
import json
import logging
import os
import random
import signal
import subprocess
import sys
import tempfile
import time
from kubernetes import client, config, watch

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from benchmark.colocation_metric import measure_colocation_rate

log = logging.getLogger(__name__)

WORKLOADS_DIR = os.path.join(os.path.dirname(__file__), "workloads")
SCHEDULER_PLACEHOLDER = "SCHEDULER_PLACEHOLDER"

SCHEDULER_NAMES = {
    "default": "default-scheduler",
    "graphsched": "graphsched",
}

EXPECTED_PODS = {
    "chain": 5,
    "hub": 5,
}


def load_workload_manifest(workload: str, scheduler_name: str) -> str:
    path = os.path.join(WORKLOADS_DIR, f"{workload}.yaml")
    with open(path, encoding="utf-8") as f:
        return f.read().replace(SCHEDULER_PLACEHOLDER, scheduler_name)


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
    seen_uids: set[str] = set()
    latencies = []
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
                uid = pod.metadata.uid
                if uid in seen_uids:
                    continue
                seen_uids.add(uid)

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

    failed = expected - scheduled_count
    if scheduled_count + failed != expected:
        log.warning(
            "Expected %s pods total, scheduled=%s failed=%s",
            expected, scheduled_count, failed,
        )
    if len(latencies) > expected:
        log.warning(
            "Got %s latency samples (expected at most %s); duplicate counting?",
            len(latencies), expected,
        )

    import statistics
    latencies.sort()
    return {
        "latencies_ms": latencies,
        "p50_ms": statistics.median(latencies) if latencies else -1,
        "p99_ms": latencies[max(0, int(len(latencies) * 0.99) - 1)] if latencies else -1,
        "failed": failed,
        "scheduled": scheduled_count,
    }


def _benchmark_pods_remaining() -> bool:
    result = subprocess.run(
        ["kubectl", "get", "pods", "-l", "benchmark=true", "--no-headers"],
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def cleanup(timeout: float = 60.0) -> None:
    log.info("Cleaning up benchmark resources...")
    subprocess.run(
        ["kubectl", "delete", "all,svc", "-l", "benchmark=true", "--force", "--grace-period=0"],
        capture_output=True,
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _benchmark_pods_remaining():
            log.info("Benchmark pods cleared")
            return
        time.sleep(2)
    log.warning("Timed out after %.0fs waiting for benchmark pods to terminate", timeout)


import threading

def _measure_colocation_when_ready(watcher, v1: client.CoreV1Api,
                                   timeout: float = 15.0) -> dict:
    """Refresh graph edges and poll until dependent pairs are measurable."""
    deadline = time.monotonic() + timeout
    last = measure_colocation_rate(watcher.get_graph(), v1,
                                   label_selector="benchmark=true")
    while time.monotonic() < deadline:
        watcher.refresh_dependencies()
        last = measure_colocation_rate(watcher.get_graph(), v1,
                                       label_selector="benchmark=true")
        if last["total_dependent_pairs"] > 0:
            return last
        time.sleep(0.25)
    log.warning(
        "Co-location measurement found 0 dependent pairs after %.0fs",
        timeout,
    )
    return last


def run_for_scheduler(v1: client.CoreV1Api, scheduler_name: str, workload: str) -> dict:
    expected = EXPECTED_PODS[workload]
    log.info(f"\n{'='*50}\nBenchmarking: {scheduler_name} ({workload}, {expected} pods)\n{'='*50}")
    cleanup()

    manifest = load_workload_manifest(workload, scheduler_name)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(manifest)
        fname = f.name

    # Start graph watcher early so graph is warm before pods arrive
    from watcher.graph_watcher import GraphWatcher
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
            v1, "benchmark=true", expected=expected, timeout=60.0, resource_version=rv)

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
    coloc = _measure_colocation_when_ready(watcher, v1, timeout=15.0)

    result = {
        "workload": workload,
        "scheduler": scheduler_name,
        "scheduled": latency_stats.get("scheduled", 0),
        "failed": latency_stats.get("failed", expected),
        "p50_latency_ms": round(latency_stats.get("p50_ms", -1), 1),
        "p99_latency_ms": round(latency_stats.get("p99_ms", -1), 1),
        "co_location_rate": round(coloc["co_location_rate"], 3),
        "co_located_pairs": coloc["co_located_pairs"],
        "total_dependent_pairs": coloc["total_dependent_pairs"],
    }

    log.info(f"Result: {result}")
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
        "--workload",
        choices=["chain", "hub"],
        default="chain",
        help="Workload topology: chain (db/backend/frontend) or hub (1 hub + 4 spokes)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON path (default: results/<scheduler>_<workload>.json)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible runs (optional)",
    )
    parser.add_argument(
        "--external-graphsched",
        action="store_true",
        help="Do not start/stop GraphSched subprocess; run scheduler in another terminal first",
    )
    args = parser.parse_args()

    if args.external_graphsched and args.scheduler != "graphsched":
        parser.error("--external-graphsched only applies with --scheduler graphsched")

    outfile = args.output or f"results/{args.scheduler}_{args.workload}.json"
    scheduler_name = SCHEDULER_NAMES[args.scheduler]

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    if args.seed is not None:
        random.seed(args.seed)
        log.info("Random seed set to %s", args.seed)
    config.load_kube_config()
    v1 = client.CoreV1Api()
    os.makedirs(os.path.dirname(outfile) or ".", exist_ok=True)

    sched_proc = None
    if args.scheduler == "graphsched" and not args.external_graphsched:
        sched_proc = start_graphsched_process()
        time.sleep(6)

    try:
        result = run_for_scheduler(v1, scheduler_name, args.workload)
    finally:
        if sched_proc is not None:
            stop_graphsched_process(sched_proc)
        cleanup()

    with open(outfile, "w") as f:
        json.dump(result, f, indent=2)

    print_summary(result, outfile)


if __name__ == "__main__":
    main()