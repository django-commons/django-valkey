from typing import Any

from django.utils.module_loading import import_string
from valkey.cluster import ValkeyCluster
from valkey.connection import ConnectionPool, DefaultParser

from django_valkey.base_pool import BaseConnectionFactory


class ClusterConnectionFactory(BaseConnectionFactory[ValkeyCluster, ConnectionPool]):
    path_pool_cls = "valkey.connection.ConnectionPool"
    path_base_cls = "valkey.cluster.ValkeyCluster"

    def disconnect(self, connection: ValkeyCluster) -> None:
        connection.disconnect_connection_pools()

    def get_parser_cls(self):
        cls = self.options.get("PARSER_CLS", None)
        if cls is None:
            return DefaultParser
        return import_string(cls)

    def connect(self, url: str) -> ValkeyCluster:
        params = self.make_connection_params(url)
        return self.get_connection(params)

    def get_connection(self, params: dict) -> ValkeyCluster | Any:
        # ValkeyCluster builds a connection pool per node, so the pool class
        # and pool kwargs are handed to it along with the connection params.
        kwargs = {
            **params,
            "connection_pool_class": self.pool_cls,
            **self.pool_cls_kwargs,
            **self.base_client_cls_kwargs,
        }
        return self.base_client_cls(**kwargs)
