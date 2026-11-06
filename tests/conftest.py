import copy
from collections.abc import Iterable
from typing import NamedTuple, cast

import pytest
from asgiref.compatibility import iscoroutinefunction
from django.core.cache import cache as default_cache
from django.core.cache import caches
from pytest_django import Settings

from django_valkey.base import BaseValkeyCache
from django_valkey.cache import ValkeyCache

pytestmark = pytest.mark.anyio

if iscoroutinefunction(default_cache.clear):

    @pytest.fixture(scope="function")
    async def cache():
        yield default_cache
        await default_cache.aclear()

else:

    @pytest.fixture
    def cache() -> Iterable[BaseValkeyCache]:
        yield default_cache
        default_cache.clear()


class Expiry(NamedTuple):
    timeout: float
    wait: float


@pytest.fixture
def expiry() -> Expiry:
    """
    A short timeout for tests that wait for a key to expire, and how long to
    wait for it to pass.

    Timeouts reach the server in milliseconds, so expiry can be tested in well
    under a second. test_timeout_whole_seconds covers a whole-second timeout.
    """
    return Expiry(timeout=0.25, wait=0.5)


@pytest.fixture
def key_prefix_cache(cache: ValkeyCache, settings: Settings) -> Iterable[ValkeyCache]:
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"]["KEY_PREFIX"] = "*"
    settings.CACHES = caches_setting
    yield cache


@pytest.fixture
def with_prefix_cache() -> Iterable[ValkeyCache]:
    with_prefix = cast(ValkeyCache, caches["with_prefix"])
    yield with_prefix
    with_prefix.clear()
