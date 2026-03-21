import time
import logging
from kubernetes import client, config, watch

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger(__name__)

SCHEDULER_NAME = "graphsched"


def get_nodes(v1: client.CoreV1Api) -> list[str]:
    """Return a list of node names that can run pods (not the control plane)."""
    nodes = v1.list_node()
    schedulable = []
    for node in nodes.items:
        taints = node.spec.taints or []
        is_control_plane = any(t.key == "node-role.kubernetes.io/control-plane"
                               for t in taints)
        if not is_control_plane:
            schedulable.append(node.metadata.name)
    log.info(f"Schedulable nodes: {schedulable}")
    return schedulable


def bind_pod(v1: client.CoreV1Api, pod_name: str,
             pod_namespace: str, node_name: str):
    """
    Tell Kubernetes: assign this pod to this node.
    This is the BIND step — the final step of scheduling.
    """
    binding = client.V1Binding(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=pod_namespace),
        target=client.V1ObjectReference(
            api_version="v1",
            kind="Node",
            name=node_name
        )
    )
    try:
        v1.create_namespaced_binding(
            namespace=pod_namespace,
            body=binding,
            _preload_content=False  # Avoids a deserialization bug
        )
        log.info(f"Bound {pod_namespace}/{pod_name} to node {node_name}")
    except Exception as e:
        log.error(f"Failed to bind {pod_name}: {e}")


def schedule_pod(v1: client.CoreV1Api, pod) -> str:
    """
    TODAY'S VERSION: Just pick the first available node.
    WEEK 3 VERSION: This will use the dependency graph + topology scorer.
    Returns the name of the node to schedule this pod on.
    """
    nodes = get_nodes(v1)
    if not nodes:
        raise RuntimeError("No schedulable nodes found!")
    chosen = nodes[0]
    log.info(f"[PLACEHOLDER] Chose node {chosen} for pod {pod.metadata.name}")
    return chosen


def run_scheduler():
    """Main loop: watch for Pending pods, schedule them."""
    config.load_kube_config()
    v1 = client.CoreV1Api()

    log.info(f"GraphSched started. Watching for pods with "
             f"schedulerName={SCHEDULER_NAME}")

    w = watch.Watch()

    for event in w.stream(v1.list_pod_for_all_namespaces):
        event_type = event['type']   # ADDED, MODIFIED, or DELETED
        pod = event['object']

        if (event_type == 'ADDED' and
                pod.status.phase == 'Pending' and
                pod.spec.scheduler_name == SCHEDULER_NAME and
                not pod.spec.node_name):

            log.info(f"New pod to schedule: {pod.metadata.namespace}/"
                     f"{pod.metadata.name}")

            try:
                node = schedule_pod(v1, pod)
                bind_pod(v1, pod.metadata.name,
                         pod.metadata.namespace, node)
            except Exception as e:
                log.error(f"Scheduling error: {e}")


if __name__ == "__main__":
    run_scheduler()
