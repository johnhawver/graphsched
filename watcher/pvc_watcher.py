import logging
import threading
from kubernetes import client, config, watch
from .graph_model import PodDependencyGraph, DependencyEdge

log = logging.getLogger(__name__)


def get_pods_using_pvc(v1: client.CoreV1Api,
                        pvc_name: str, namespace: str) -> list[str]:
    """Return UIDs of all pods in this namespace that mount this PVC."""
    pod_uids = []
    try:
        pods = v1.list_namespaced_pod(namespace)
        for pod in pods.items:
            for volume in (pod.spec.volumes or []):
                if (volume.persistent_volume_claim and
                        volume.persistent_volume_claim.claim_name == pvc_name):
                    pod_uids.append(pod.metadata.uid)
                    break
    except Exception as e:
        log.error(f"Error listing pods for PVC {pvc_name}: {e}")
    return pod_uids


class PVCWatcher:
    def __init__(self, graph: PodDependencyGraph, v1: client.CoreV1Api):
        self.graph = graph
        self.v1 = v1
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()
        log.info("PVCWatcher started")

    def stop(self):
        self._stop_event.set()

    def _run(self):
        w = watch.Watch()
        while not self._stop_event.is_set():
            try:
                for event in w.stream(
                        self.v1.list_persistent_volume_claim_for_all_namespaces,
                        resource_version="",
                        timeout_seconds=30):
                    if self._stop_event.is_set():
                        break
                    event_type = event['type']
                    pvc = event['object']
                    if event_type in ('ADDED', 'MODIFIED'):
                        pvc_name = pvc.metadata.name
                        namespace = pvc.metadata.namespace
                        pod_uids = get_pods_using_pvc(self.v1, pvc_name, namespace)
                        for i, uid_a in enumerate(pod_uids):
                            for uid_b in pod_uids[i+1:]:
                                self.graph.add_dependency(DependencyEdge(
                                    source_uid=uid_a, target_uid=uid_b,
                                    dep_type="storage", weight=1.0))
                                self.graph.add_dependency(DependencyEdge(
                                    source_uid=uid_b, target_uid=uid_a,
                                    dep_type="storage", weight=1.0))
                                log.info(f"Storage dependency: "
                                         f"{uid_a[:8]} <-> {uid_b[:8]} "
                                         f"(shared PVC: {pvc_name})")
            except Exception as e:
                log.error(f"PVCWatcher error (will retry): {e}")
                import time; time.sleep(2)