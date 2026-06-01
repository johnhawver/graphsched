import logging
import os
import sys
import threading
import time
from typing import Any, cast

import networkx as nx

# Allow `from watcher...` when run as `python3 scheduler/graphsched.py`
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from kubernetes import client, config, watch
from kubernetes.client import V1Pod
from kubernetes.client.rest import ApiException
from watcher.graph_watcher import GraphWatcher
from watcher.service_watcher import labels_match_selector, pod_references_service
from scheduler.metrics import (
    COLOCATION_RATE,
    PODS_SCHEDULED,
    SCHEDULE_ERRORS,
    SCHEDULE_LATENCY,
    start_metrics_server,
)
from scheduler.topology_scorer import compute_node_score

log = logging.getLogger(__name__)

SCHEDULER_NAME = "graphsched"
_STARTUP_WAIT_S = 3.0
_SERVICES_CACHE_TTL = 2.0
_PODS_CACHE_TTL = 0.15  # 150ms — safe because _bound_pods covers the staleness gap


class GraphSchedPlugin:
    def __init__(self):
        config.load_kube_config()
        self.v1 = client.CoreV1Api()
        self.graph_watcher = GraphWatcher()
        self.graph_watcher.start()
        log.info("GraphSched started. Waiting %.0fs for graph to populate...", _STARTUP_WAIT_S)
        time.sleep(_STARTUP_WAIT_S)

        self._node_capacity: dict[str, dict] = {}
        self._cache_node_capacity()

        self._bound_pods: dict[str, str] = {}  # uid -> node_name
        self._scheduling_uids: set[str] = set()
        self._schedule_lock = threading.Lock()
        self._cached_services: list | None = None
        self._services_fetched_at: float = 0.0
        self._cached_pods: list | None = None
        self._pods_fetched_at: float = 0.0

        start_metrics_server(9100)

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
            try:
                svc_list = self.v1.list_service_for_all_namespaces()
                self._cached_services = [
                    s for s in svc_list.items
                    if s.metadata.namespace != "kube-system"
                ]
                self._services_fetched_at = now
            except Exception as e:
                log.warning("list services failed: %s", e)
                if self._cached_services is None:
                    raise
        if self._cached_services is None:
            return []
        return self._cached_services

    def _get_pods(self) -> list:
        """Return all pods cluster-wide, cached for _PODS_CACHE_TTL seconds.

        Safe to cache briefly because _bound_pods covers the gap for pods
        we just placed that the cached list doesn't yet reflect.
        """
        now = time.monotonic()
        if self._cached_pods is None or now - self._pods_fetched_at > _PODS_CACHE_TTL:
            try:
                self._cached_pods = self.v1.list_pod_for_all_namespaces().items
                self._pods_fetched_at = now
            except Exception as e:
                log.warning("list pods failed: %s", e)
                if self._cached_pods is None:
                    raise
        if self._cached_pods is None:
            return []
        return self._cached_pods

    def _invalidate_pod_cache(self) -> None:
        self._cached_pods = None
        self._pods_fetched_at = 0.0

    def _infer_network_edges(self, graph: nx.DiGraph, all_pods: list) -> None:
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
            for p in all_pods:
                if not pod_references_service(p, svc_name, svc_ns):
                    continue
                caller_uid = p.metadata.uid
                for target_uid in target_uids:
                    if caller_uid and target_uid and caller_uid != target_uid:
                        graph.add_edge(caller_uid, target_uid,
                                       dep_type="network", weight=1.0)

    def _order_pods_for_scheduling(self, pods: list) -> list:
        """Schedule dependency targets before callers (db → backend → frontend)."""
        if len(pods) <= 1:
            return pods

        batch_uids = {p.metadata.uid for p in pods if p.metadata.uid}
        dep_graph = nx.DiGraph()
        for uid in batch_uids:
            dep_graph.add_node(uid)

        all_pods = self._get_pods()
        scratch = self.graph_watcher.get_graph().copy()
        self._infer_network_edges(scratch, all_pods)
        for src, dst in scratch.edges():
            if src in batch_uids and dst in batch_uids:
                dep_graph.add_edge(src, dst)

        uid_to_pod = {p.metadata.uid: p for p in pods if p.metadata.uid}
        try:
            ordered_uids = list(reversed(list(nx.topological_sort(dep_graph))))
        except nx.NetworkXUnfeasible:
            ordered_uids = list(batch_uids)

        ordered = [uid_to_pod[uid] for uid in ordered_uids if uid in uid_to_pod]
        seen = {p.metadata.uid for p in ordered}
        for pod in pods:
            if pod.metadata.uid not in seen:
                ordered.append(pod)
        return ordered

    def _list_pending_pods(self) -> list:
        self._invalidate_pod_cache()
        pending = []
        for p in self._get_pods():
            if (p.spec is None or p.metadata is None or p.status is None or
                    p.spec.scheduler_name != SCHEDULER_NAME or
                    p.spec.node_name or
                    p.status.phase != "Pending"):
                continue
            uid = p.metadata.uid
            if not uid or uid in self._bound_pods or uid in self._scheduling_uids:
                continue
            pending.append(p)
        return pending

    def filter_and_score(self, pod) -> str:
        start = time.monotonic()
        try:
            graph = self.graph_watcher.get_graph()

            all_pods = self._get_pods()

            self._infer_network_edges(graph, all_pods)

            log.info(f"Graph state: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges")
            pod_cpu, pod_mem = self._get_pod_requests(pod)

            # Build uid_to_node from locally tracked bindings, graph data, and live pods
            uid_to_node: dict[str, str | None] = dict(self._bound_pods)
            for uid, data in graph.nodes(data=True):
                nn = data.get("node_name")
                if nn:
                    uid_to_node[data.get("uid", uid)] = nn
            for p in all_pods:
                if not p.metadata.uid:
                    continue
                if p.spec.node_name:
                    uid_to_node[p.metadata.uid] = p.spec.node_name
                elif p.metadata.uid in self._bound_pods:
                    uid_to_node[p.metadata.uid] = self._bound_pods[p.metadata.uid]

            # Pre-compute per-node resource usage (bound pods, not only Running)
            node_usage: dict[str, dict] = {
                name: {"cpu_used": 0, "mem_used": 0, **cap}
                for name, cap in self._node_capacity.items()
            }
            for p in all_pods:
                nn = p.spec.node_name or self._bound_pods.get(p.metadata.uid)
                if not nn or nn not in node_usage:
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

            log.info(
                "PLACEMENT %s/%s -> %s (score=%s)",
                pod.metadata.namespace,
                pod.metadata.name,
                best_node,
                best_score,
            )
            return best_node
        finally:
            SCHEDULE_LATENCY.observe(time.monotonic() - start)


    def bind(self, pod_name: str, pod_ns: str, node_name: str):
        self.v1.create_namespaced_binding(
            pod_ns,
            client.V1Binding(
                metadata=client.V1ObjectMeta(name=pod_name, namespace=pod_ns),
                target=client.V1ObjectReference(
                    api_version="v1", kind="Node", name=node_name)),
            _preload_content=False)
        log.info(f"Bound {pod_ns}/{pod_name} -> {node_name}")

    def _update_colocation_metric(self) -> None:
        from benchmark.colocation_metric import measure_colocation_rate

        # Infer dependency edges inline (same as filter_and_score) so the gauge
        # reflects the edges we actually scored on, not the bare watcher graph.
        graph = self.graph_watcher.get_graph()
        self._infer_network_edges(graph, self._get_pods())
        result = measure_colocation_rate(graph, self.v1)
        COLOCATION_RATE.set(result["co_location_rate"])

    def _schedule_one_pod(self, pod: V1Pod) -> None:
        if pod.metadata is None or pod.spec is None:
            return
        pod_name = pod.metadata.name
        pod_ns = pod.metadata.namespace
        pod_uid = pod.metadata.uid
        if not pod_name or not pod_ns or not pod_uid:
            return

        self._scheduling_uids.add(pod_uid)
        try:
            for attempt in range(3):
                try:
                    node = self.filter_and_score(pod)
                    self.bind(pod_name, pod_ns, node)
                    self._bound_pods[pod_uid] = node
                    self._invalidate_pod_cache()
                    PODS_SCHEDULED.inc()
                    return
                except ApiException as e:
                    if e.status == 404:
                        log.debug(
                            "Pod %s deleted before bind, skipping",
                            pod_name,
                        )
                        return
                    log.error(
                        "Scheduling error for %s (attempt %d/3): %s",
                        pod_name,
                        attempt + 1,
                        e,
                    )
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                    else:
                        SCHEDULE_ERRORS.inc()
                        log.error(
                            "Giving up on %s/%s after 3 attempts",
                            pod_ns,
                            pod_name,
                        )
                except Exception as e:
                    log.error(
                        "Scheduling error for %s (attempt %d/3): %s",
                        pod_name,
                        attempt + 1,
                        e,
                    )
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                    else:
                        SCHEDULE_ERRORS.inc()
                        log.error(
                            "Giving up on %s/%s after 3 attempts",
                            pod_ns,
                            pod_name,
                        )
        finally:
            self._scheduling_uids.discard(pod_uid)

    def _drain_pending_pods(self) -> None:
        """Schedule all pending graphsched pods in dependency order (one batch)."""
        with self._schedule_lock:
            pending = self._list_pending_pods()
            if not pending:
                return
            ordered = self._order_pods_for_scheduling(pending)
            log.info("Batch scheduling %d pending pod(s)", len(ordered))
            for pod in ordered:
                self._schedule_one_pod(pod)
            # Update the co-location gauge once per batch, off the per-pod
            # critical path (keeps API list calls out of the hot loop).
            self._update_colocation_metric()

    def run(self):
        w = watch.Watch()
        log.info(f"Watching for pods with schedulerName={SCHEDULER_NAME}")

        for raw in w.stream(self.v1.list_pod_for_all_namespaces):
            if not isinstance(raw, dict):
                continue
            event: dict[str, Any] = raw
            if event.get("type") not in ("ADDED", "MODIFIED"):
                continue

            pod = cast(V1Pod, event.get("object"))
            if (pod.status is None or pod.status.phase != "Pending" or
                    pod.spec is None or
                    pod.spec.scheduler_name != SCHEDULER_NAME or
                    pod.spec.node_name or
                    pod.metadata is None):
                continue

            self._drain_pending_pods()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    GraphSchedPlugin().run()