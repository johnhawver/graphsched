from kubernetes import client
import networkx as nx


def measure_colocation_rate(graph: nx.DiGraph, v1: client.CoreV1Api) -> dict:
    """
    For each edge (A, B) in the dependency graph where both A and B
    are Running, check if they are on the same K8s node.

    Returns:
        co_located:        number of dependent pairs on the same node
        total_edges:       edges where both pods are Running
        co_location_rate:  co_located / total_edges (0.0-1.0)
    """
    running_pods = v1.list_pod_for_all_namespaces(
        field_selector="status.phase=Running")
    uid_to_node = {
        p.metadata.uid: p.spec.node_name
        for p in running_pods.items
        if p.spec.node_name
    }

    co_located = 0
    total_edges = 0

    for src_uid, dst_uid in graph.edges():
        src_node = uid_to_node.get(src_uid)
        dst_node = uid_to_node.get(dst_uid)
        if src_node is None or dst_node is None:
            continue   # One pod not yet running — skip
        total_edges += 1
        if src_node == dst_node:
            co_located += 1

    rate = co_located / total_edges if total_edges > 0 else 0.0
    return {
        "co_located_pairs": co_located,
        "total_dependent_pairs": total_edges,
        "co_location_rate": rate,
    }