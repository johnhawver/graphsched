import pytest
from watcher.graph_model import PodDependencyGraph, PodNode, DependencyEdge
from unittest.mock import MagicMock
from watcher.service_watcher import labels_match_selector, pod_references_service

def make_pod(uid: str, name: str, labels: dict = None) -> PodNode:
    return PodNode(uid=uid, name=name, namespace="default",
                   node_name=None, cpu_request_millicores=100,
                   memory_request_bytes=134217728, priority=0,
                   phase="Running", labels=labels or {})


class TestPodDependencyGraph:

    def test_add_pod(self):
        g = PodDependencyGraph()
        g.add_or_update_pod(make_pod("uid-1", "frontend"))
        assert g.stats()["num_pods"] == 1

    def test_add_multiple_pods(self):
        g = PodDependencyGraph()
        g.add_or_update_pod(make_pod("uid-1", "frontend"))
        g.add_or_update_pod(make_pod("uid-2", "backend"))
        assert g.stats()["num_pods"] == 2

    def test_remove_pod(self):
        g = PodDependencyGraph()
        g.add_or_update_pod(make_pod("uid-1", "frontend"))
        g.remove_pod("uid-1")
        assert g.stats()["num_pods"] == 0

    def test_add_dependency(self):
        g = PodDependencyGraph()
        g.add_or_update_pod(make_pod("uid-1", "frontend"))
        g.add_or_update_pod(make_pod("uid-2", "backend"))
        g.add_dependency(DependencyEdge("uid-1", "uid-2", "network", 1.0))
        assert g.stats()["num_dependencies"] == 1

    def test_dependency_requires_both_pods(self):
        """Edge should not be added if one pod doesn't exist."""
        g = PodDependencyGraph()
        g.add_or_update_pod(make_pod("uid-1", "frontend"))
        g.add_dependency(DependencyEdge("uid-1", "uid-99", "network", 1.0))
        assert g.stats()["num_dependencies"] == 0

    def test_remove_pod_removes_edges(self):
        g = PodDependencyGraph()
        g.add_or_update_pod(make_pod("uid-1", "frontend"))
        g.add_or_update_pod(make_pod("uid-2", "backend"))
        g.add_dependency(DependencyEdge("uid-1", "uid-2", "network", 1.0))
        g.remove_pod("uid-1")
        assert g.stats()["num_dependencies"] == 0

    def test_graph_snapshot_is_independent(self):
        g = PodDependencyGraph()
        g.add_or_update_pod(make_pod("uid-1", "frontend"))
        snap = g.get_snapshot()
        g.add_or_update_pod(make_pod("uid-2", "backend"))
        assert snap.number_of_nodes() == 1
        assert g.stats()["num_pods"] == 2


class TestLabelMatching:

    def test_exact_match(self):
        assert labels_match_selector({"app": "backend"}, {"app": "backend"})

    def test_superset_labels_match(self):
        assert labels_match_selector(
            {"app": "backend", "version": "v2"}, {"app": "backend"})

    def test_wrong_value_no_match(self):
        assert not labels_match_selector({"app": "frontend"}, {"app": "backend"})

    def test_empty_selector_no_match(self):
        """Empty selector should NOT match every pod."""
        assert not labels_match_selector({"app": "backend"}, {})

    def test_empty_labels_no_match(self):
        assert not labels_match_selector({}, {"app": "backend"})


class TestServiceReference:
    """Tests for env-var-based service reference detection."""
    def _make_pod(self, namespace="default", env_pairs=None):
        """env_pairs: list of (name, value) tuples."""
        pod = MagicMock()
        pod.metadata.namespace = namespace
        container = MagicMock()
        container.env = []
        for name, value in (env_pairs or []):
            env = MagicMock()
            env.name = name
            env.value = value
            container.env.append(env)
        pod.spec.containers = [container]
        return pod
    def test_env_reference_detected(self):
        pod = self._make_pod(env_pairs=[
            ("BACKEND_URL", "http://bench-backend-svc"),
        ])
        assert pod_references_service(pod, "bench-backend-svc", "default")
    def test_unrelated_env_not_detected(self):
        pod = self._make_pod(env_pairs=[
            ("FOO", "bar"),
        ])
        assert not pod_references_service(pod, "bench-backend-svc", "default")
    def test_wrong_namespace_not_detected(self):
        pod = self._make_pod(
            namespace="other-ns",
            env_pairs=[("URL", "http://bench-backend-svc")],
        )
        assert not pod_references_service(pod, "bench-backend-svc", "default")
    def test_empty_env_not_detected(self):
        pod = self._make_pod(env_pairs=[])
        assert not pod_references_service(pod, "bench-backend-svc", "default")