from urllib.parse import parse_qs, urlparse

import pytest

from django_valkey.async_cache.client.sentinel import AsyncSentinelClient
from django_valkey.client.sentinel import SentinelClient


@pytest.mark.parametrize(
    "client_class",
    [SentinelClient, AsyncSentinelClient],
)
def test_init_creates_primary_and_replica_urls(client_class):
    client = client_class(
        "valkey://service_name/1?socket_timeout=1",
        {"OPTIONS": {"SENTINELS": [("sentinel.example.com", 26379)]}},
        backend=None,
    )

    queries = [parse_qs(urlparse(url).query) for url in client._server]
    assert queries == [
        {"socket_timeout": ["1"], "is_master": ["1"]},
        {"socket_timeout": ["1"], "is_master": ["0"]},
    ]
