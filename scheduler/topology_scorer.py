import logging
from typing import Optional
import networkx as nx

log = logging.getLogger(__name__)

TOPOLOGY_WEIGHT = 0.7   # Tunable hyperparameter
RESOURCE_WEIGHT = 0.3   # Must sum to 1.0 with TOPOLOGY_WEIGHT


def co_location_score(pod_uid: str,
                       node_name: str,
                       graph: nx.DiGraph,
                       uid_to_node: dict[str, Optional[str]]) -> float:
    """
    Fraction of this pod's graph neighbors (weighted by edge weight)
    that are already running on node_name.

    Args:
        pod_uid:     UID of the pending pod we are scoring for
        node_name:   K8s worker node we are evaluating
        graph:       current dependency graph snapshot
        uid_to_node: {pod_uid: k8s_node_name} for all Running pods

    Returns:
        float in [0.0, 1.0].
        0.0 = no neighbors on this node (or pod not in graph).
        1.0 = all graph neighbors are on this node.
    """
    if not graph.has_node(pod_uid):
        return 0.0

    neighbors_with_weights: list[tuple[str, float]] = []

    # Outgoing: pods this pod depends on (e.g. frontend -> backend)
    for _, neighbor_uid, data in graph.out_edges(pod_uid, data=True):
        neighbors_with_weights.append((neighbor_uid, data.get("weight", 1.0)))

    # Incoming: pods that depend on this pod (e.g. backend <- frontend)
    for neighbor_uid, _, data in graph.in_edges(pod_uid, data=True):
        neighbors_with_weights.append((neighbor_uid, data.get("weight", 1.0)))

    if not neighbors_with_weights:
        return 0.0

    total_weight = sum(w for _, w in neighbors_with_weights)
    if total_weight == 0:
        return 0.0

    co_located_weight = sum(
        w for uid, w in neighbors_with_weights
        if uid_to_node.get(uid) == node_name
    )

    return co_located_weight / total_weight


def resource_balance_score(cpu_used: int, cpu_total: int,
                            mem_used: int, mem_total: int) -> float:
    """
    Score a node by how balanced its resource usage is.
    Uses geometric mean so a node must be good on BOTH CPU and memory.
    Prefers nodes that are neither overloaded nor completely idle.

    Returns float in [0.0, 1.0]. Higher = more available (better to schedule on).
    """
    if cpu_total == 0 or mem_total == 0:
        return 0.0

    cpu_score = 1.0 - (cpu_used / cpu_total)
    mem_score = 1.0 - (mem_used / mem_total)

    return (cpu_score * mem_score) ** 0.5


def compute_node_score(pod_uid: str,
                        node_name: str,
                        graph: nx.DiGraph,
                        uid_to_node: dict[str, Optional[str]],
                        cpu_used: int, cpu_total: int,
                        mem_used: int, mem_total: int) -> int:
    """
    Compute the final integer score (0-100) for placing pod_uid on node_name.
    """
    topo = co_location_score(pod_uid, node_name, graph, uid_to_node)
    resource = resource_balance_score(cpu_used, cpu_total, mem_used, mem_total)

    raw = TOPOLOGY_WEIGHT * topo + RESOURCE_WEIGHT * resource
    score = int(round(raw * 100))
    score = max(0, min(100, score))

    log.debug(f"score({pod_uid[:8]}, {node_name}) = {score} "
              f"[topo={topo:.3f}, resource={resource:.3f}]")
    return score