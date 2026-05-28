from unittest.mock import MagicMock, patch

import pytest

from scheduler.graphsched import GraphSchedPlugin


def _plugin_without_init() -> GraphSchedPlugin:
    return GraphSchedPlugin.__new__(GraphSchedPlugin)


class TestGraphSchedCacheFallback:
    def test_get_pods_returns_stale_cache_on_list_failure(self):
        plugin = _plugin_without_init()
        stale = [MagicMock(metadata=MagicMock(uid="old"))]
        plugin._cached_pods = stale
        plugin._pods_fetched_at = 0.0
        plugin.v1 = MagicMock()
        plugin.v1.list_pod_for_all_namespaces.side_effect = RuntimeError("apiserver down")

        assert plugin._get_pods() is stale

    def test_get_pods_raises_when_no_stale_cache(self):
        plugin = _plugin_without_init()
        plugin._cached_pods = None
        plugin._pods_fetched_at = 0.0
        plugin.v1 = MagicMock()
        plugin.v1.list_pod_for_all_namespaces.side_effect = RuntimeError("apiserver down")

        with pytest.raises(RuntimeError, match="apiserver down"):
            plugin._get_pods()

    def test_get_services_returns_stale_cache_on_list_failure(self):
        plugin = _plugin_without_init()
        stale = [MagicMock(metadata=MagicMock(namespace="app"))]
        plugin._cached_services = stale
        plugin._services_fetched_at = 0.0
        plugin.v1 = MagicMock()
        plugin.v1.list_service_for_all_namespaces.side_effect = RuntimeError("apiserver down")

        assert plugin._get_services() is stale

    def test_get_services_raises_when_no_stale_cache(self):
        plugin = _plugin_without_init()
        plugin._cached_services = None
        plugin._services_fetched_at = 0.0
        plugin.v1 = MagicMock()
        plugin.v1.list_service_for_all_namespaces.side_effect = RuntimeError("apiserver down")

        with pytest.raises(RuntimeError, match="apiserver down"):
            plugin._get_services()
