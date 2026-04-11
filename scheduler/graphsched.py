import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        log.info("GraphSched started. Waiting 8s for graph to populate...")
        time.sleep(8)

        # Cache node capacity — allocatable resources don't change at runtime
        self._node_capacity: dict[str, dict] = {}
        self._cache_node_capacity()

        # Track pods we've already bound so uid_to_node is accurate
        # even before K8s transitions them to Running
        self._bound_pods: dict[str, str] = {}  # uid -> node_name

    def _cache_node_capacity(self):
        """Fetch and cache CPU/memory capacity for all worker nodes once."""
        all_nodes = self.v1.list_node().items
        for node in all_nodes:
            if any(t.key == "node-role.kubernetes.io/control-plane"
                   for t in (node.spec.taints or [])):
                continue
            allocatable = node.status.allocatable or {}
            self._node_capacity[node.metadata.name] = {
                "cpu_total": self._parse_cpu(allocatable.get("cpu", "4")),
                "mem_total": self._parse_memory(allocatable.get("memory", "8Gi")),
            }
        log.info(f"Cached capacity for {len(self._node_capacity)} nodes")

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
        """Get current CPU/memory used on a node by summing Running pod requests.
        
        Node capacity is served from cache — only pod usage requires a live API call.
        """
        capacity = self._node_capacity[node_name]

        running = self.v1.list_pod_for_all_namespaces(
            field_selector=f"spec.nodeName={node_name},status.phase=Running")
        cpu_used = mem_used = 0
        for pod in running.items:
            for c in (pod.spec.containers or []):
                reqs = (c.resources.requests or {}) if c.resources else {}
                cpu_used += self._parse_cpu(reqs.get("cpu", "0"))
                mem_used += self._parse_memory(reqs.get("memory", "0"))

        return dict(cpu_used=cpu_used, mem_used=mem_used, **capacity)

    def _get_pod_requests(self, pod) -> tuple[int, int]:
        cpu = sum(
            self._parse_cpu((c.resources.requests or {}).get("cpu", "0"))
            if c.resources and c.resources.requests else 0
            for c in (pod.spec.containers or []))
        mem = sum(
            self._parse_memory((c.resources.requests or {}).get("memory", "0"))
            if c.resources and c.resources.requests else 0
            for c in (pod.spec.containers or []))
        return cpu, mem

    def _score_node(self, node_name: str, pod_uid: str, pod_cpu: int, pod_mem: int,
                    graph, uid_to_node: dict) -> tuple[str, int] | None:
        """
        Score a single node. Returns (node_name, score) or None if filtered out.
        Designed to run in a thread — each call makes exactly 1 API request.
        """
        usage = self.get_node_resource_usage(node_name)

        # FILTER: skip nodes that cannot physically fit this pod
        if (usage["cpu_used"] + pod_cpu > usage["cpu_total"] or
                usage["mem_used"] + pod_mem > usage["mem_total"]):
            log.debug(f"Filtered {node_name}: insufficient resources")
            return None

        score = compute_node_score(
            pod_uid=pod_uid,
            node_name=node_name,
            graph=graph,
            uid_to_node=uid_to_node,
            **usage
        )
        log.info(f"node_score({node_name}) = {score}")
        return (node_name, score)

    def filter_and_score(self, pod) -> str:
        # Eagerly re-infer all service dependencies so the graph has edges
        # BEFORE scoring.  The background ServiceWatcher may not have
        # processed the Service events yet when pods arrive for scheduling.
        self.graph_watcher.refresh_dependencies()

        graph = self.graph_watcher.get_graph()
        log.info(f"Graph state: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges")
        pod_cpu, pod_mem = self._get_pod_requests(pod)

        # Build uid_to_node from three sources (each covers a different lag):
        #   1. Locally tracked bindings (covers just-bound, not yet Running)
        #   2. Graph node data       (covers pods the PodWatcher has seen)
        #   3. Live Running pod list  (authoritative for Running pods)
        uid_to_node: dict[str, str] = dict(self._bound_pods)

        for uid, data in graph.nodes(data=True):
            node = data.get("node_name")
            if node:
                uid_to_node[data.get("uid", uid)] = node

        running_pods = self.v1.list_namespaced_pod(
            namespace="default",
            field_selector="status.phase=Running"
        )
        for p in running_pods.items:
            if p.metadata.uid and p.spec.node_name:
                uid_to_node[p.metadata.uid] = p.spec.node_name

        node_names = list(self._node_capacity.keys())
        best_node, best_score = None, -1

        with ThreadPoolExecutor(max_workers=len(node_names)) as executor:
            futures = {
                executor.submit(
                    self._score_node,
                    name, pod.metadata.uid, pod_cpu, pod_mem, graph, uid_to_node
                ): name
                for name in node_names
            }
            for future in as_completed(futures):
                result = future.result()
                if result is None:
                    continue
                node_name, score = result
                if score > best_score:
                    best_score, best_node = score, node_name

        if best_node is None:
            raise RuntimeError(f"No eligible nodes for pod {pod.metadata.name}")

        return best_node

    def bind(self, pod_name: str, pod_ns: str, node_name: str):
        self.v1.create_namespaced_binding(
            pod_ns,
            client.V1Binding(
                metadata=client.V1ObjectMeta(name=pod_name, namespace=pod_ns),
                target=client.V1ObjectReference(
                    api_version="v1", kind="Node", name=node_name)),
            _preload_content=False)
        log.info(f"Bound {pod_ns}/{pod_name} -> {node_name}")

    def run(self):
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
                self._bound_pods[pod.metadata.uid] = node
            except Exception as e:
                log.error(f"Scheduling error for {pod.metadata.name}: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    GraphSchedPlugin().run()