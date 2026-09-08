from valkey.asyncio.client import Valkey as AValkey

from django_valkey.async_cache.client.default import AsyncDefaultClient
from django_valkey.base import (
    AsyncBackendCommands,
    BaseValkeyCache,
    decorate_all_methods,
    omit_exception_async,
)


@decorate_all_methods(omit_exception_async)
class DecoratedAsyncBackendCommands(AsyncBackendCommands):
    pass


class AsyncValkeyCache(
    BaseValkeyCache[AsyncDefaultClient, AValkey], DecoratedAsyncBackendCommands
):
    DEFAULT_CLIENT_CLASS = "django_valkey.async_cache.client.default.AsyncDefaultClient"
    is_async = True
