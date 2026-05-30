import networkx as nx
from unittest.mock import MagicMock, patch
from benchmark.colocation_metric import measure_colocation_rate


def make_running_pod(uid, node_name):
    pod = MagicMock()
    pod.metadata.uid = uid
    pod.spec.node_name = node_name
    pod.status.phase = "Running"
    return pod


def test_all_collocated():
    g = nx.DiGraph()
    g.add_node("uid-1"); g.add_node("uid-2")
    g.add_edge("uid-1", "uid-2")

    v1 = MagicMock()
    v1.list_pod_for_all_namespaces.return_value.items = [
        make_running_pod("uid-1", "worker1"),
        make_running_pod("uid-2", "worker1"),  # Both on worker1
    ]

    result = measure_colocation_rate(g, v1)
    assert result["co_location_rate"] == 1.0
    assert result["co_located_pairs"] == 1


def test_none_collocated():
    g = nx.DiGraph()
    g.add_node("uid-1"); g.add_node("uid-2")
    g.add_edge("uid-1", "uid-2")

    v1 = MagicMock()
    v1.list_pod_for_all_namespaces.return_value.items = [
        make_running_pod("uid-1", "worker1"),
        make_running_pod("uid-2", "worker2"),  # Different nodes
    ]

    result = measure_colocation_rate(g, v1)
    assert result["co_location_rate"] == 0.0


def test_empty_graph():
    g = nx.DiGraph()
    v1 = MagicMock()
    v1.list_pod_for_all_namespaces.return_value.items = []
    result = measure_colocation_rate(g, v1)
    assert result["co_location_rate"] == 0.0
    assert result["total_dependent_pairs"] == 0


def test_bound_pending_pods_count():
    """Bound pods count even before containers reach Running phase."""
    g = nx.DiGraph()
    g.add_node("uid-1")
    g.add_node("uid-2")
    g.add_edge("uid-1", "uid-2")

    bound_pending = MagicMock()
    bound_pending.metadata.uid = "uid-2"
    bound_pending.spec.node_name = "worker1"
    bound_pending.status.phase = "Pending"

    v1 = MagicMock()
    v1.list_pod_for_all_namespaces.return_value.items = [
        make_running_pod("uid-1", "worker1"),
        bound_pending,
    ]

    result = measure_colocation_rate(g, v1)
    assert result["co_location_rate"] == 1.0
    assert result["total_dependent_pairs"] == 1