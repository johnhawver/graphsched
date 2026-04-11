import logging
import time
from kubernetes import client, config, watch
from watcher.graph_watcher import GraphWatcher
from watcher.service_watcher import labels_match_selector, pod_references_service
from scheduler.topology_scorer import compute_node_score

log = logging.getLogger(__name__)

SCHEDULER_NAME = "graphsched"
_SERVICES_CACHE_TTL = 2.0
_PODS_CACHE_TTL = 0.15  # 150ms — safe because _bound_pods covers the staleness gap


class GraphSchedPlugin:
    def __init__(self):
        config.load_kube_config()
        self.v1 = client.CoreV1Api()
        self.graph_watcher = GraphWatcher()
        self.graph_watcher.start()
        log.info("GraphSched started. Waiting 8s for graph to populate...")
        time.sleep(8)

        self._node_capacity: dict[str, dict] = {}
        self._cache_node_capacity()

        self._bound_pods: dict[str, str] = {}  # uid -> node_name
        self._cached_services: list | None = None
        self._services_fetched_at: float = 0.0
        self._cached_pods: list | None = None
        self._pods_fetched_at: float = 0.0

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

    def _get_services(self) -> list:
        """Return non-system services, cached for _SERVICES_CACHE_TTL seconds."""
        now = time.monotonic()
        if self._cached_services is None or now - self._services_fetched_at > _SERVICES_CACHE_TTL:
            svc_list = self.v1.list_service_for_all_namespaces()
            self._cached_services = [
                s for s in svc_list.items
                if s.metadata.namespace != "kube-system"
            ]
            self._services_fetched_at = now
        return self._cached_services

    def _get_pods(self) -> list:
        """Return all pods cluster-wide, cached for _PODS_CACHE_TTL seconds.

        Safe to cache briefly because _bound_pods covers the gap for pods
        we just placed that the cached list doesn't yet reflect.
        """
        now = time.monotonic()
        if self._cached_pods is None or now - self._pods_fetched_at > _PODS_CACHE_TTL:
            self._cached_pods = self.v1.list_pod_for_all_namespaces().items
            self._pods_fetched_at = now
        return self._cached_pods

    def filter_and_score(self, pod) -> str:
        graph = self.graph_watcher.get_graph()

        all_pods = self._get_pods()

        # --- Inline dependency inference on the snapshot ---
        # Services are cached (rarely change mid-batch); pods are fresh.
        for svc in self._get_services():
            selector = svc.spec.selector or {}
            if not selector:
                continue
            svc_name = svc.metadata.name
            svc_ns = svc.metadata.namespace
            target_uids = [
                p.metadata.uid for p in all_pods
                if p.metadata.namespace == svc_ns
                and labels_match_selector(p.metadata.labels or {}, selector)
            ]
            caller_uids = [
                p.metadata.uid for p in all_pods
                if pod_references_service(p, svc_name, svc_ns)
            ]
            for caller_uid in caller_uids:
                for target_uid in target_uids:
                    if caller_uid != target_uid:
                        graph.add_edge(caller_uid, target_uid,
                                       dep_type="network", weight=1.0)

        log.info(f"Graph state: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges")
        pod_cpu, pod_mem = self._get_pod_requests(pod)

        # Build uid_to_node from locally tracked bindings, graph data, and live pods
        uid_to_node: dict[str, str] = dict(self._bound_pods)
        for uid, data in graph.nodes(data=True):
            nn = data.get("node_name")
            if nn:
                uid_to_node[data.get("uid", uid)] = nn
        for p in all_pods:
            if p.status.phase == "Running" and p.metadata.uid and p.spec.node_name:
                uid_to_node[p.metadata.uid] = p.spec.node_name

        # Pre-compute per-node resource usage from the same pod list
        node_usage: dict[str, dict] = {
            name: {"cpu_used": 0, "mem_used": 0, **cap}
            for name, cap in self._node_capacity.items()
        }
        for p in all_pods:
            if p.status.phase != "Running":
                continue
            nn = p.spec.node_name
            if nn not in node_usage:
                continue
            for c in (p.spec.containers or []):
                reqs = (c.resources.requests or {}) if c.resources else {}
                node_usage[nn]["cpu_used"] += self._parse_cpu(reqs.get("cpu", "0"))
                node_usage[nn]["mem_used"] += self._parse_memory(reqs.get("memory", "0"))

        # Score all nodes — pure computation, no API calls
        best_node, best_score = None, -1
        for name in self._node_capacity:
            usage = node_usage[name]
            if (usage["cpu_used"] + pod_cpu > usage["cpu_total"] or
                    usage["mem_used"] + pod_mem > usage["mem_total"]):
                continue
            score = compute_node_score(
                pod_uid=pod.metadata.uid, node_name=name,
                graph=graph, uid_to_node=uid_to_node, **usage)
            log.info(f"node_score({name}) = {score}")
            if score > best_score:
                best_score, best_node = score, name

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