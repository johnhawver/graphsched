import logging
import threading
from kubernetes import client, config, watch
from .graph_model import PodDependencyGraph, DependencyEdge

log = logging.getLogger(__name__)


def labels_match_selector(pod_labels: dict, selector: dict) -> bool:
    """
    Return True if a pod's labels satisfy a service selector.
    A pod matches if it has ALL of the selector's key-value pairs.
    Empty selector = no match (a Service with no selector routes to no pods).
    """
    if not selector:
        return False
    for key, value in selector.items():
        if pod_labels.get(key) != value:
            return False
    return True


def pod_references_service(pod, service_name: str, service_namespace: str) -> bool:
    """
    Return True if this pod's env vars reference the given service.
    Same-namespace only for simplicity (cross-namespace requires FQDN).
    """
    if pod.metadata.namespace != service_namespace:
        return False
    for container in (pod.spec.containers or []):
        for env in (container.env or []):
            if env.value and service_name in env.value:
                return True
    return False


class ServiceWatcher:
    def __init__(self, graph: PodDependencyGraph, v1: client.CoreV1Api):
        self.graph = graph
        self.v1 = v1
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()
        log.info("ServiceWatcher started")

    def stop(self):
        self._stop_event.set()

    def _infer_dependencies_for_service(self, selector: dict,
                                         svc_name: str, svc_namespace: str):
        try:
            pod_list = self.v1.list_namespaced_pod(svc_namespace)
        except Exception as e:
            log.error(f"Failed to list pods: {e}")
            return

        target_uids = [
            pod.metadata.uid for pod in pod_list.items
            if labels_match_selector(pod.metadata.labels or {}, selector)
        ]
        caller_uids = [
            pod.metadata.uid for pod in pod_list.items
            if pod_references_service(pod, svc_name, svc_namespace)
        ]

        for caller_uid in caller_uids:
            for target_uid in target_uids:
                if caller_uid != target_uid:
                    self.graph.add_dependency(DependencyEdge(
                        source_uid=caller_uid,
                        target_uid=target_uid,
                        dep_type="network",
                        weight=1.0
                    ))
                    log.info(f"Network dependency: {caller_uid[:8]} -> {target_uid[:8]}")

    def _run(self):
        w = watch.Watch()
        while not self._stop_event.is_set():
            try:
                for event in w.stream(
                        self.v1.list_service_for_all_namespaces,
                        resource_version="",
                        timeout_seconds=30):
                    if self._stop_event.is_set():
                        break
                    event_type = event['type']
                    svc = event['object']
                    if svc.metadata.namespace == "kube-system":
                        continue
                    if event_type in ('ADDED', 'MODIFIED'):
                        self._infer_dependencies_for_service(
                            svc.spec.selector or {},
                            svc.metadata.name,
                            svc.metadata.namespace)
            except Exception as e:
                log.error(f"ServiceWatcher error (will retry): {e}")
                import time; time.sleep(2)