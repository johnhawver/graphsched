import networkx as nx
from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class PodNode:
    """
    Represents a single pod in the dependency graph.
    """
    uid: str
    name: str
    namespace: str
    node_name: Optional[str]        # Which K8s node it's on (None if Pending)
    cpu_request_millicores: int
    memory_request_bytes: int
    priority: int
    phase: str                      # Pending, Running, Succeeded, Failed, Unknown
    labels: dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


@dataclass
class DependencyEdge:
    """
    Represents a dependency between two pods.
    """
    source_uid: str      # The pod that depends on the target
    target_uid: str      # The pod being depended upon
    dep_type: str        # "network", "storage", or "config"
    weight: float        # 1.0 for network/storage, 0.3 for config


class PodDependencyGraph:
    """
    A live, mutable directed graph of pod dependencies.
    Thread-safe: uses a lock for all mutations.
    """

    def __init__(self):
        self.graph = nx.DiGraph()
        self._pods: dict[str, PodNode] = {}
        import threading
        self._lock = threading.RLock()

    def add_or_update_pod(self, pod: PodNode):
        with self._lock:
            self._pods[pod.uid] = pod
            self.graph.add_node(pod.uid,
                                uid=pod.uid,
                                name=pod.name,
                                namespace=pod.namespace,
                                cpu_request=pod.cpu_request_millicores,
                                memory_request=pod.memory_request_bytes,
                                priority=pod.priority,
                                phase=pod.phase,
                                node_name=pod.node_name)

    def remove_pod(self, uid: str):
        with self._lock:
            if uid in self._pods:
                del self._pods[uid]
            if self.graph.has_node(uid):
                self.graph.remove_node(uid)  # Also removes all connected edges

    def add_dependency(self, edge: DependencyEdge):
        with self._lock:
            if edge.source_uid != edge.target_uid:
                self.graph.add_edge(edge.source_uid, edge.target_uid,
                                    dep_type=edge.dep_type,
                                    weight=edge.weight)

    def get_pod(self, uid: str) -> Optional[PodNode]:
        with self._lock:
            return self._pods.get(uid)

    def get_snapshot(self) -> nx.DiGraph:
        """Return a copy of the current graph (safe to use outside the lock)."""
        with self._lock:
            return self.graph.copy()

    def stats(self) -> dict:
        with self._lock:
            return {
                "num_pods": self.graph.number_of_nodes(),
                "num_dependencies": self.graph.number_of_edges(),
            }