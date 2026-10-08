from valkey.connection import BlockingConnectionPool
from valkey.credentials import UsernamePasswordCredentialProvider

from django_valkey.cluster_cache.pool import ClusterConnectionFactory

URL = "valkey://127.0.0.1:7005"


def test_options_reach_valkey_cluster(mocker):
    factory = ClusterConnectionFactory(
        {
            "PASSWORD": "secret",
            "SOCKET_TIMEOUT": 3,
            "SOCKET_CONNECT_TIMEOUT": 2,
            "CONNECTION_POOL_CLASS": "valkey.connection.BlockingConnectionPool",
            "CONNECTION_POOL_KWARGS": {"max_connections": 7, "retry_on_timeout": True},
            "BASE_CLIENT_KWARGS": {"max_connections": 9, "read_from_replicas": True},
        }
    )
    factory.base_client_cls = mocker.Mock()
    factory.connect(URL)

    kwargs = factory.base_client_cls.call_args.kwargs
    assert kwargs["url"] == URL
    assert kwargs["password"] == "secret"
    assert kwargs["socket_timeout"] == 3
    assert kwargs["socket_connect_timeout"] == 2
    assert kwargs["connection_pool_class"] is BlockingConnectionPool
    assert kwargs["retry_on_timeout"] is True
    assert kwargs["read_from_replicas"] is True
    # BASE_CLIENT_KWARGS is the most specific, so it wins.
    assert kwargs["max_connections"] == 9


def test_options_reach_node_pools():
    # The test cluster has no password, and a passwordless user accepts any
    # password, so this exercises AUTH without needing a real secret.
    provider = UsernamePasswordCredentialProvider("default", "token")
    factory = ClusterConnectionFactory(
        {
            "SOCKET_TIMEOUT": 3,
            "CONNECTION_POOL_CLASS": "valkey.connection.BlockingConnectionPool",
            "CONNECTION_POOL_KWARGS": {"max_connections": 7},
            "CREDENTIAL_PROVIDER": provider,
        }
    )
    client = factory.connect(URL)
    try:
        assert client.ping()
        node_pool = client.get_default_node().valkey_connection.connection_pool
        assert isinstance(node_pool, BlockingConnectionPool)
        assert node_pool.max_connections == 7
        assert node_pool.connection_kwargs["socket_timeout"] == 3
        assert node_pool.connection_kwargs["credential_provider"] is provider
    finally:
        factory.disconnect(client)
