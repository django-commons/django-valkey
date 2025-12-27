from valkey import Valkey

from django_valkey.base_client import BaseClient, ClientCommands
from django_valkey.commands.hash import HashCommands


class DefaultClient(BaseClient[Valkey], ClientCommands[Valkey]):
    CONNECTION_FACTORY_PATH = "django_valkey.pool.ConnectionFactory"


class HashClient(BaseClient[Valkey], HashCommands, ClientCommands[Valkey]):
    CONNECTION_FACTORY_PATH = "django_valkey.pool.ConnectionFactory"
