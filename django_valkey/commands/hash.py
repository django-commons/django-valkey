from typing import Any

from valkey.typing import EncodableT, KeyT

from django_valkey.base_client import BaseClient, _main_exceptions
from django_valkey.exceptions import ConnectionInterrupted


class HashCommands:
    def hset(
        self: BaseClient,
        name: str,
        key: KeyT | None = None,
        value: EncodableT | None = None,
        mapping: dict[KeyT, EncodableT] | None = None,
        items: list[tuple[KeyT, EncodableT]] | None = None,
        version: int | None = None,
        client=None,
    ) -> int:
        """
        Set the value of hash name at key to value.
        Returns the number of keys added to the hash.
        """
        client = self._get_client(write=True, client=client)
        nname = self.make_key(name, version=version)

        if key:
            key = self.make_key(key, version=version)
            value = self.encode(value)
        if mapping:
            mapping = {k: v for k, v in mapping.items()}
        if items:
            items = [(self.make_key(k), self.encode(v)) for k, v in items]

        try:
            return client.hset(
                name=nname, key=key, value=value, mapping=mapping, items=items
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hget(
        self: BaseClient, name: str, key: KeyT, version: int | None = None, client=None
    ) -> EncodableT:
        """Returns the value associated with key in the hash stored at name."""
        client = self._get_client(write=False, client=client)
        nname = self.make_key(name, version=version)
        nkey = self.make_key(key, version=version)

        try:
            return client.hget(nname, nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hdel(
        self: BaseClient,
        name: str,
        key: KeyT,
        version: int | None = None,
        client=None,
    ) -> int:
        """
        Remove key from hash name.
        Returns the number of keys deleted from the hash.
        """
        client = self._get_client(write=True, client=client)
        nname = self.make_key(name, version=version)
        nkey = self.make_key(key, version=version)
        try:
            return client.hdel(nname, nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hlen(
        self: BaseClient,
        name: str,
        version: int | None = None,
        client=None,
    ) -> int:
        """
        Return the number of items in hash name.
        """
        client = self._get_client(write=False, client=client)
        nname = self.make_key(name, version=version)
        try:
            return client.hlen(nname)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hkeys(
        self: BaseClient,
        name: str,
        version: int | None = None,
        client=None,
    ) -> list[Any]:
        """
        Return a list of fields in hash name.
        """
        client = self._get_client(write=False, client=client)
        nname = self.make_key(name, version=version)
        try:
            return [self.reverse_key(k.decode()) for k in client.hkeys(nname)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    def hexists(
        self: BaseClient,
        name: str,
        key: KeyT,
        version: int | None = None,
        client=None,
    ) -> bool:
        """
        Return True if key exists in hash name, else False.
        """
        client = self._get_client(write=False, client=client)
        nname = self.make_key(name, version=version)
        nkey = self.make_key(key, version=version)
        try:
            return client.hexists(nname, nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e


class AsyncHashCommands:
    async def hset(
        self: BaseClient,
        name: str,
        key: KeyT | None = None,
        value: EncodableT | None = None,
        mapping: dict[KeyT, EncodableT] | None = None,
        items: list[tuple[KeyT, EncodableT]] | None = None,
        version: int | None = None,
        client=None,
    ) -> int:
        """
        Set the value of hash name at key to value.
        Returns the number of keys added to the hash.
        """
        client = await self._get_client(write=True, client=client)
        nname = self.make_key(name, version=version)

        if key:
            key = self.make_key(key, version=version)
            value = self.encode(value)
        if mapping:
            mapping = {k: v for k, v in mapping.items()}
        if items:
            items = [(self.make_key(k), self.encode(v)) for k, v in items]

        try:
            return await client.hset(
                nname, key=key, value=value, mapping=mapping, items=items
            )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hget(
        self: BaseClient, name: str, key: KeyT, version: int | None = None, client=None
    ) -> EncodableT:
        """Returns the value associated with key in the hash stored at name."""
        client = await self._get_client(write=False, client=client)
        nname = self.make_key(name, version=version)
        nkey = self.make_key(key, version=version)

        try:
            return await client.hget(nname, nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hdel(
        self: BaseClient,
        name: str,
        key: KeyT,
        version: int | None = None,
        client=None,
    ) -> int:
        """
        Remove key from hash name.
        Returns the number of keys deleted from the hash.
        """
        client = await self._get_client(write=True, client=client)
        nname = self.make_key(name, version=version)
        nkey = self.make_key(key, version=version)
        try:
            return await client.hdel(nname, nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hlen(
        self: BaseClient,
        name: str,
        version: int | None = None,
        client=None,
    ) -> int:
        """
        Return the number of items in hash name.
        """
        client = await self._get_client(write=False, client=client)
        nname = self.make_key(name, version=version)
        try:
            return await client.hlen(nname)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hkeys(
        self: BaseClient,
        name: str,
        version: int | None = None,
        client=None,
    ) -> list[Any]:
        """
        Return a list of keys in hash name.
        """
        client = await self._get_client(write=False, client=client)
        nname = self.make_key(name, version=version)
        try:
            return [self.reverse_key(k.decode()) for k in await client.hkeys(nname)]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e

    async def hexists(
        self: BaseClient,
        name: str,
        key: KeyT,
        version: int | None = None,
        client=None,
    ) -> bool:
        """
        Return True if field exists in hash name, else False.
        """
        client = await self._get_client(write=False, client=client)
        nname = self.make_key(name, version=version)
        nkey = self.make_key(key, version=version)
        try:
            return await client.hexists(nname, nkey)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client) from e
