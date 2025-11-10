from valkey import Valkey

from django_valkey.base_client import BaseClient, ClientCommands
from django_valkey.client.mixins import SortedSetMixin


class DefaultClient(BaseClient[Valkey], ClientCommands[Valkey], SortedSetMixin):
    CONNECTION_FACTORY_PATH = "django_valkey.pool.ConnectionFactory"
