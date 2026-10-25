import pytest
from django.core.exceptions import ImproperlyConfigured
from valkey.credentials import CredentialProvider

from django_valkey import pool


def test_connection_factory_redefine_from_opts():
    cf = pool.get_connection_factory(
        path="django_valkey.pool.ConnectionFactory",
        options={
            "CONNECTION_FACTORY": "django_valkey.pool.SentinelConnectionFactory",
            "SENTINELS": [("127.0.0.1", "26739")],
        },
    )
    assert cf.__class__.__name__ == "SentinelConnectionFactory"


@pytest.mark.parametrize(
    "conn_factory,expected",
    [
        (
            "django_valkey.pool.SentinelConnectionFactory",
            pool.SentinelConnectionFactory,
        ),
        ("django_valkey.pool.ConnectionFactory", pool.ConnectionFactory),
    ],
)
def test_connection_factory_opts(conn_factory: str, expected):
    cf = pool.get_connection_factory(
        path=None,
        options={
            "CONNECTION_FACTORY": conn_factory,
            "SENTINELS": [("127.0.0.1", "26739")],
        },
    )
    assert isinstance(cf, expected)


@pytest.mark.parametrize(
    "conn_factory,expected",
    [
        (
            "django_valkey.pool.SentinelConnectionFactory",
            pool.SentinelConnectionFactory,
        ),
        ("django_valkey.pool.ConnectionFactory", pool.ConnectionFactory),
    ],
)
def test_connection_factory_path(conn_factory: str, expected):
    cf = pool.get_connection_factory(
        path=conn_factory,
        options={
            "SENTINELS": [("127.0.0.1", "26739")],
        },
    )
    assert isinstance(cf, expected)


def test_connection_factory_no_sentinels():
    with pytest.raises(ImproperlyConfigured):
        pool.get_connection_factory(
            path=None,
            options={
                "CONNECTION_FACTORY": "django_valkey.pool.SentinelConnectionFactory",
            },
        )


class StaticCredentialProvider(CredentialProvider):
    def __init__(self, username="default", password="secret"):
        self.username = username
        self.password = password

    def get_credentials(self):
        return self.username, self.password


static_provider = StaticCredentialProvider()

# Pools connect lazily, so nothing is sent to this server.
URL = "valkey://127.0.0.1:6379/0"


def test_credential_provider_instance():
    provider = StaticCredentialProvider()
    cf = pool.ConnectionFactory({"CREDENTIAL_PROVIDER": provider})
    assert cf.make_connection_params(URL)["credential_provider"] is provider


def test_credential_provider_class_path():
    cf = pool.ConnectionFactory(
        {
            "CREDENTIAL_PROVIDER": "tests.test_connection_factory.StaticCredentialProvider",
            "CREDENTIAL_PROVIDER_KWARGS": {"username": "iam-user", "password": "token"},
        }
    )
    provider = cf.make_connection_params(URL)["credential_provider"]
    assert isinstance(provider, StaticCredentialProvider)
    assert provider.get_credentials() == ("iam-user", "token")


def test_credential_provider_instance_path():
    cf = pool.ConnectionFactory(
        {"CREDENTIAL_PROVIDER": "tests.test_connection_factory.static_provider"}
    )
    assert cf.make_connection_params(URL)["credential_provider"] is static_provider


def test_credential_provider_reaches_connection_pool():
    provider = StaticCredentialProvider()
    client = pool.ConnectionFactory({"CREDENTIAL_PROVIDER": provider}).connect(URL)
    assert client.connection_pool.connection_kwargs["credential_provider"] is provider


def test_credential_provider_reaches_sentinel_connections():
    provider = StaticCredentialProvider()
    cf = pool.SentinelConnectionFactory(
        {"SENTINELS": [("127.0.0.1", "26739")], "CREDENTIAL_PROVIDER": provider}
    )
    assert cf._sentinel.connection_kwargs["credential_provider"] is provider


def test_credential_provider_with_password():
    with pytest.raises(ImproperlyConfigured):
        pool.ConnectionFactory(
            {"CREDENTIAL_PROVIDER": StaticCredentialProvider(), "PASSWORD": "secret"}
        )


def test_no_credential_provider():
    cf = pool.ConnectionFactory({})
    assert "credential_provider" not in cf.make_connection_params(URL)


def test_pool_shared_between_factories_with_equal_options():
    first = pool.ConnectionFactory({"CONNECTION_POOL_KWARGS": {"max_connections": 5}})
    second = pool.ConnectionFactory({"CONNECTION_POOL_KWARGS": {"max_connections": 5}})
    assert first.connect(URL).connection_pool is second.connect(URL).connection_pool


def test_pool_not_shared_between_factories_with_different_options():
    first = pool.ConnectionFactory({"CONNECTION_POOL_KWARGS": {"max_connections": 5}})
    second = pool.ConnectionFactory({"CONNECTION_POOL_KWARGS": {"max_connections": 6}})
    first_pool = first.connect(URL).connection_pool
    second_pool = second.connect(URL).connection_pool
    assert first_pool is not second_pool
    assert second_pool.max_connections == 6


def test_pool_key_with_unhashable_option():
    class UnhashableProvider(StaticCredentialProvider):
        __hash__ = None

    provider = UnhashableProvider()
    first = pool.ConnectionFactory({"CREDENTIAL_PROVIDER": provider})
    second = pool.ConnectionFactory({"CREDENTIAL_PROVIDER": provider})
    other = pool.ConnectionFactory({"CREDENTIAL_PROVIDER": UnhashableProvider()})
    first_pool = first.connect(URL).connection_pool
    assert first_pool is second.connect(URL).connection_pool
    assert first_pool is not other.connect(URL).connection_pool
