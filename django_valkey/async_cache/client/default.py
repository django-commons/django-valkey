from valkey.asyncio import Valkey as AValkey

from django_valkey.base_client import BaseClient, AsyncClientCommands
from django_valkey.commands.hash import AsyncHashCommands


class AsyncDefaultClient(BaseClient[AValkey], AsyncClientCommands[AValkey]):
    CONNECTION_FACTORY_PATH = "django_valkey.async_cache.pool.AsyncConnectionFactory"


class AsyncHashClient(
    BaseClient[AValkey], AsyncHashCommands, AsyncClientCommands[AValkey]
):
    CONNECTION_FACTORY_PATH = "django_valkey.async_cache.pool.AsyncConnectionFactory"
