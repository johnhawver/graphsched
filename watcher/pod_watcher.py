import logging
import threading
from kubernetes import client, config, watch
from .graph_model import PodDependencyGraph, PodNode

log = logging.getLogger(__name__)


def parse_cpu_request(resources) -> int:
    """Convert K8s CPU request string to millicores."""
    if resources is None or resources.requests is None:
        return 100
    cpu_str = resources.requests.get("cpu", "100m")
    if cpu_str.endswith("m"):
        return int(cpu_str[:-1])
    else:
        return int(float(cpu_str) * 1000)


def parse_memory_request(resources) -> int:
    """Convert K8s memory request string to bytes."""
    if resources is None or resources.requests is None:
        return 134217728
    mem_str = resources.requests.get("memory", "128Mi")
    multipliers = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3,
                   "K": 1000, "M": 1000**2, "G": 1000**3}
    for suffix, mult in multipliers.items():
        if mem_str.endswith(suffix):
            return int(float(mem_str[:-len(suffix)]) * mult)
    return int(mem_str)


def pod_to_node(pod) -> PodNode:
    """Convert a K8s pod API object to our PodNode dataclass."""
    containers = pod.spec.containers or []
    total_cpu = sum(parse_cpu_request(c.resources) for c in containers)
    total_mem = sum(parse_memory_request(c.resources) for c in containers)

    return PodNode(
        uid=pod.metadata.uid,
        name=pod.metadata.name,
        namespace=pod.metadata.namespace,
        node_name=pod.spec.node_name,
        cpu_request_millicores=total_cpu,
        memory_request_bytes=total_mem,
        priority=pod.spec.priority or 0,
        phase=pod.status.phase or "Unknown",
        labels=pod.metadata.labels or {}
    )


class PodWatcher:
    """
    Runs a background thread that watches K8s pod events
    and keeps the PodDependencyGraph up-to-date.
    """

    def __init__(self, graph: PodDependencyGraph):
        self.graph = graph
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()
        log.info("PodWatcher started")

    def stop(self):
        self._stop_event.set()

    def _run(self):
        config.load_kube_config()
        v1 = client.CoreV1Api()
        w = watch.Watch()

        while not self._stop_event.is_set():
            try:
                for event in w.stream(v1.list_pod_for_all_namespaces,
                                      resource_version="",
                                      timeout_seconds=30):
                    if self._stop_event.is_set():
                        break

                    event_type = event['type']
                    pod = event['object']

                    if pod.metadata.namespace == "kube-system":
                        continue

                    uid = pod.metadata.uid

                    if event_type in ('ADDED', 'MODIFIED'):
                        node = pod_to_node(pod)
                        self.graph.add_or_update_pod(node)
                        log.debug(f"Updated pod {pod.metadata.namespace}/"
                                  f"{pod.metadata.name} (phase={node.phase})")

                    elif event_type == 'DELETED':
                        self.graph.remove_pod(uid)
                        log.info(f"Removed pod {pod.metadata.namespace}/"
                                 f"{pod.metadata.name}")

            except Exception as e:
                log.error(f"PodWatcher error (will retry): {e}")
                import time; time.sleep(2)