import logging
import grpc
from concurrent import futures
import sys
from proto import graph_service_pb2, graph_service_pb2_grpc
from .graph_watcher import GraphWatcher

log = logging.getLogger(__name__)


class GraphServicer(graph_service_pb2_grpc.GraphServiceServicer):
    def __init__(self, watcher: GraphWatcher):
        self.watcher = watcher

    def GetGraphStats(self, request, context):
        stats = self.watcher.stats()
        return graph_service_pb2.StatsResponse(
            num_pods=stats["num_pods"],
            num_dependencies=stats["num_dependencies"]
        )

    def GetGraphEdges(self, request, context):
        graph = self.watcher.get_graph()
        edges = []
        for src, dst, data in graph.edges(data=True):
            edges.append(graph_service_pb2.EdgeProto(
                source_uid=src,
                target_uid=dst,
                dep_type=data.get("dep_type", "network"),
                weight=data.get("weight", 1.0)
            ))
        return graph_service_pb2.EdgesResponse(edges=edges)


def serve(watcher: GraphWatcher, port: int = 50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = GraphServicer(watcher)
    graph_service_pb2_grpc.add_GraphServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log.info(f"gRPC server started on port {port}")
    return server, servicer