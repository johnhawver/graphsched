import logging
import time
from kubernetes import client, config, watch
from watcher.graph_watcher import GraphWatcher
from scheduler.topology_scorer import compute_node_score

log = logging.getLogger(__name__)

SCHEDULER_NAME = "graphsched"


class GraphSchedPlugin:
    def __init__(self):
        config.load_kube_config()
        self.v1 = client.CoreV1Api()
        self.graph_watcher = GraphWatcher()
        self.graph_watcher.start()
        log.info("GraphSched started. Waiting 3s for graph to populate...")
        time.sleep(3)

    @staticmethod
    def _parse_cpu(s: str) -> int:
        if not s:
            return 0
        if s.endswith("m"):
            return int(s[:-1])
        return int(float(s) * 1000)

    @staticmethod
    def _parse_memory(s: str) -> int:
        if not s:
            return 0
        for suf, m in [("Ki", 1024), ("Mi", 1024**2), ("Gi", 1024**3),
                       ("K", 1000), ("M", 1000**2), ("G", 1000**3)]:
            if s.endswith(suf):
                return int(s[:-len(suf)]) * m
        return int(s) if s.isdigit() else 0

    def get_node_resource_usage(self, node_name: str) -> dict:
        """Get current CPU/memory used on a node by summing Running pod requests."""
        node = self.v1.read_node(node_name)
        allocatable = node.status.allocatable or {}
        cpu_total = self._parse_cpu(allocatable.get("cpu", "4"))
        mem_total = self._parse_memory(allocatable.get("memory", "8Gi"))

        running = self.v1.list_pod_for_all_namespaces(
            field_selector=f"spec.nodeName={node_name},status.phase=Running")
        cpu_used = mem_used = 0
        for pod in running.items:
            for c in (pod.spec.containers or []):
                reqs = (c.resources.requests or {}) if c.resources else {}
                cpu_used += self._parse_cpu(reqs.get("cpu", "0"))
                mem_used += self._parse_memory(reqs.get("memory", "0"))

        return dict(cpu_used=cpu_used, cpu_total=cpu_total,
                    mem_used=mem_used, mem_total=mem_total)

    def _get_pod_requests(self, pod) -> tuple[int, int]:
        """Return (cpu_millicores, memory_bytes) requested by this pod."""
        cpu = sum(
            self._parse_cpu((c.resources.requests or {}).get("cpu", "0"))
            if c.resources and c.resources.requests else 0
            for c in (pod.spec.containers or []))
        mem = sum(
            self._parse_memory((c.resources.requests or {}).get("memory", "0"))
            if c.resources and c.resources.requests else 0
            for c in (pod.spec.containers or []))
        return cpu, mem

    def filter_and_score(self, pod) -> str:
        """
        Run Filter (resource check) then Score (topology + resource balance).
        Returns the name of the best eligible node.
        Raises RuntimeError if no node can fit the pod.
        """
        pod_cpu, pod_mem = self._get_pod_requests(pod)

        # Get live graph snapshot + uid→node mapping
        graph = self.graph_watcher.get_graph()
        uid_to_node = {
            data.get("uid", uid): data.get("node_name")
            for uid, data in graph.nodes(data=True)
        }

        # Get all worker nodes (exclude control plane)
        all_nodes = [
            n for n in self.v1.list_node().items
            if not any(t.key == "node-role.kubernetes.io/control-plane"
                       for t in (n.spec.taints or []))
        ]

        best_node, best_score = None, -1

        for node in all_nodes:
            usage = self.get_node_resource_usage(node.metadata.name)

            # FILTER: skip nodes that cannot physically fit this pod
            if (usage["cpu_used"] + pod_cpu > usage["cpu_total"] or
                    usage["mem_used"] + pod_mem > usage["mem_total"]):
                log.debug(f"Filtered {node.metadata.name}: insufficient resources")
                continue

            # SCORE: topology co-location + resource balance
            score = compute_node_score(
                pod_uid=pod.metadata.uid,
                node_name=node.metadata.name,
                graph=graph,
                uid_to_node=uid_to_node,
                **usage
            )

            log.info(f"node_score({node.metadata.name}) = {score}")

            if score > best_score:
                best_score, best_node = score, node.metadata.name

        if best_node is None:
            raise RuntimeError(f"No eligible nodes for pod {pod.metadata.name}")

        return best_node

    def bind(self, pod_name: str, pod_ns: str, node_name: str):
        """Bind pod to node via K8s API."""
        self.v1.create_namespaced_binding(
            pod_ns,
            client.V1Binding(
                metadata=client.V1ObjectMeta(name=pod_name, namespace=pod_ns),
                target=client.V1ObjectReference(
                    api_version="v1", kind="Node", name=node_name)),
            _preload_content=False)
        log.info(f"Bound {pod_ns}/{pod_name} -> {node_name}")

    def run(self):
        """Main scheduling loop."""
        w = watch.Watch()
        log.info(f"Watching for pods with schedulerName={SCHEDULER_NAME}")

        for event in w.stream(self.v1.list_pod_for_all_namespaces):
            pod = event["object"]
            if (event["type"] != "ADDED" or
                    pod.status.phase != "Pending" or
                    pod.spec.scheduler_name != SCHEDULER_NAME or
                    pod.spec.node_name):
                continue

            try:
                node = self.filter_and_score(pod)
                self.bind(pod.metadata.name, pod.metadata.namespace, node)
            except Exception as e:
                log.error(f"Scheduling error for {pod.metadata.name}: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    GraphSchedPlugin().run()