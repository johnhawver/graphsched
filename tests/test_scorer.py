"""Unit tests for topology_scorer.py."""
import networkx as nx
import pytest
from scheduler.topology_scorer import (co_location_score, resource_balance_score,
                                        compute_node_score)


def make_graph_with_edge():
    g = nx.DiGraph()
    g.add_node("frontend-uid")
    g.add_node("backend-uid")
    g.add_edge("frontend-uid", "backend-uid", weight=1.0, dep_type="network")
    return g


class TestCoLocationScore:

    def test_co_located_dependency(self):
        g = make_graph_with_edge()
        uid_to_node = {"backend-uid": "worker1"}
        score = co_location_score("frontend-uid", "worker1", g, uid_to_node)
        assert score == 1.0

    def test_dependency_on_other_node(self):
        g = make_graph_with_edge()
        uid_to_node = {"backend-uid": "worker1"}
        score = co_location_score("frontend-uid", "worker2", g, uid_to_node)
        assert score == 0.0

    def test_isolated_pod_returns_zero(self):
        g = nx.DiGraph()
        g.add_node("isolated-uid")
        score = co_location_score("isolated-uid", "worker1", g, {})
        assert score == 0.0

    def test_pod_not_in_graph_returns_zero(self):
        g = nx.DiGraph()
        score = co_location_score("nonexistent-uid", "worker1", g, {})
        assert score == 0.0

    def test_partial_colocation(self):
        g = nx.DiGraph()
        g.add_node("pod-a"); g.add_node("pod-b"); g.add_node("pod-c")
        g.add_edge("pod-a", "pod-b", weight=1.0)
        g.add_edge("pod-a", "pod-c", weight=1.0)
        uid_to_node = {"pod-b": "worker1", "pod-c": "worker2"}
        score = co_location_score("pod-a", "worker1", g, uid_to_node)
        assert score == pytest.approx(0.5)


class TestResourceBalanceScore:

    def test_empty_node_scores_high(self):
        score = resource_balance_score(0, 4000, 0, 8589934592)
        assert score == pytest.approx(1.0)

    def test_full_node_scores_zero(self):
        score = resource_balance_score(4000, 4000, 8589934592, 8589934592)
        assert score == pytest.approx(0.0)

    def test_zero_capacity_returns_zero(self):
        assert resource_balance_score(0, 0, 0, 0) == 0.0


class TestComputeNodeScore:

    def test_co_located_beats_non_collocated(self):
        g = make_graph_with_edge()
        uid_to_node = {"backend-uid": "worker1"}
        score_w1 = compute_node_score("frontend-uid", "worker1", g, uid_to_node,
                                       cpu_used=1000, cpu_total=4000,
                                       mem_used=1073741824, mem_total=8589934592)
        score_w2 = compute_node_score("frontend-uid", "worker2", g, uid_to_node,
                                       cpu_used=500, cpu_total=4000,
                                       mem_used=536870912, mem_total=8589934592)
        assert score_w1 > score_w2

    def test_score_in_valid_range(self):
        g = make_graph_with_edge()
        score = compute_node_score("frontend-uid", "worker1", g, {},
                                    cpu_used=2000, cpu_total=4000,
                                    mem_used=4294967296, mem_total=8589934592)
        assert 0 <= score <= 100