import logging
import time
from kubernetes import client, config

from .graph_model import PodDependencyGraph
from .pod_watcher import PodWatcher
from .service_watcher import ServiceWatcher
from .pvc_watcher import PVCWatcher

log = logging.getLogger(__name__)


class GraphWatcher:
    def __init__(self):
        self.graph = PodDependencyGraph()
        config.load_kube_config()
        self.v1 = client.CoreV1Api()
        self.pod_watcher = PodWatcher(self.graph)
        self.service_watcher = ServiceWatcher(self.graph, self.v1)
        self.pvc_watcher = PVCWatcher(self.graph, self.v1)

    def start(self):
        self.pod_watcher.service_watcher = self.service_watcher
        self.pod_watcher.start()
        # Give pod watcher 2 seconds to populate initial pods
        # before service/pvc watchers try to look them up
        time.sleep(2)
        self.service_watcher.start()
        self.pvc_watcher.start()
        log.info("GraphWatcher fully started")

    def refresh_dependencies(self):
        """Synchronously re-infer all service-based edges from current cluster state."""
        self.service_watcher.refresh_dependencies()

    def get_graph(self):
        return self.graph.get_snapshot()

    def stats(self):
        return self.graph.stats()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s')
    watcher = GraphWatcher()
    watcher.start()
    while True:
        log.info(f"Graph stats: {watcher.stats()}")
        time.sleep(5)