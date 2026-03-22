import logging
import threading
from kubernetes import client, config, watch

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)


def watch_pods():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    w = watch.Watch()
    log.info("Starting pod watch stream...")

    for event in w.stream(v1.list_pod_for_all_namespaces,
                          resource_version="",
                          timeout_seconds=0):
        event_type = event['type']
        pod = event['object']
        namespace = pod.metadata.namespace
        name = pod.metadata.name
        phase = pod.status.phase
        node = pod.spec.node_name or "unscheduled"
        log.info(f"[{event_type}] {namespace}/{name} | phase={phase} | node={node}")


def watch_services():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    w = watch.Watch()
    log.info("Starting service watch stream...")

    for event in w.stream(v1.list_service_for_all_namespaces,
                          resource_version=""):
        event_type = event['type']
        svc = event['object']
        namespace = svc.metadata.namespace
        name = svc.metadata.name
        selector = svc.spec.selector or {}
        log.info(f"[{event_type}] Service {namespace}/{name} | "
                 f"selects pods with labels: {selector}")


if __name__ == "__main__":
    t1 = threading.Thread(target=watch_pods, daemon=True)
    t2 = threading.Thread(target=watch_services, daemon=True)
    t1.start()
    t2.start()
    log.info("Watching pods AND services. Create/delete resources and watch events appear.")
    log.info("Press Ctrl+C to stop.")
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        log.info("Stopped.")

