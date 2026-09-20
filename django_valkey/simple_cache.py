import pickle
import random
import re
import sys

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache

if sys.version_info >= (3, 14):
    from compression import zstd
else:
    import gzip as zstd


class ValkeySerializer:
    def __init__(self, protocol=None, compress=False):
        self.protocol = pickle.HIGHEST_PROTOCOL if protocol is None else protocol
        self.compress = compress

    def dumps(self, obj):
        # For better incr() and decr() atomicity, don't pickle integers.
        # Using type() rather than isinstance() matches only integers and not
        # subclasses like bool.
        if type(obj) is int:
            return obj
        data = pickle.dumps(obj, self.protocol)
        if self.compress:
            return zstd.compress(data)
        return data

    def loads(self, data):
        try:
            return int(data)
        except ValueError:
            if self.compress:
                data = zstd.decompress(data)
            return pickle.loads(data)


class SimpleCache(BaseCache):
    def __init__(self, server, params, **kwargs):
        import valkey

        super().__init__(params, **kwargs)
        if isinstance(server, str):
            self._servers = re.split("[;,]", server)
        else:
            self._servers = server

        self._options: dict = params.get("OPTIONS", {})

        self._lib = valkey
        self._pools = {}

        self._client = self._lib.Valkey

        self._pool_class = self._lib.ConnectionPool

        compress = self._options.get("COMPRESS", True)
        self._serializer = ValkeySerializer(compress=compress)

        parser_class = self._lib.connection.DefaultParser

        self._pool_options = {"parser_class": parser_class, **self._options}

    def get_backend_timeout(self, timeout=DEFAULT_TIMEOUT):
        if timeout == DEFAULT_TIMEOUT:
            timeout = self.default_timeout

        return None if timeout is None else max(0, int(timeout))

    def _get_connection_pool_index(self, write):
        # Write to the first server. Read from other servers if there are more,
        # otherwise read from the first server.
        if write or len(self._servers) == 1:
            return 0
        return random.randint(1, len(self._servers) - 1)

    def _get_connection_pool(self, write):
        index = self._get_connection_pool_index(write)
        if index not in self._pools:
            self._pools[index] = self._pool_class.from_url(
                self._servers[index],
                **self._pool_options,
            )
        return self._pools[index]

    def get_client(self, key=None, *, write=False):
        # key is used so that the method signature remains the same and custom
        # cache client can be implemented which might require the key to select
        # the server, e.g. sharding.
        pool = self._get_connection_pool(write)
        return self._client(connection_pool=pool)

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_and_validate_key(key, version=version)
        client = self.get_client(key, write=True)
        value = self._serializer.dumps(value)

        if timeout == 0:
            if ret := bool(client.set(key, value, nx=True)):
                client.delete(key)
            return ret
        else:
            return bool(client.set(key, value, ex=timeout, nx=True))

    def get(self, key, default=None, version=None):
        key = self.make_and_validate_key(key, version=version)
        client = self.get_client(key)
        value = client.get(key)
        return default if value is None else self._serializer.loads(value)

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_and_validate_key(key, version=version)
        client = self.get_client(key, write=True)
        value = self._serializer.dumps(value)
        if timeout == 0:
            client.delete(key)
        else:
            client.set(key, value, ex=timeout)

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_and_validate_key(key, version=version)
        client = self.get_client(key, write=True)
        if timeout is None:
            return bool(client.persist(key))
        else:
            return bool(client.expire(key, timeout))

    def delete(self, key, version=None):
        key = self.make_and_validate_key(key, version=version)
        client = self.get_client(key, write=True)
        return bool(client.delete(key))

    def get_many(self, keys, version=None):
        key_map = {
            self.make_and_validate_key(key, version=version): key for key in keys
        }
        client = self.get_client(None)
        ret = client.mget(key_map.keys())
        return {
            k: self._serializer.loads(v) for k, v in zip(keys, ret) if v is not None
        }

    def has_key(self, key, version=None):
        key = self.make_and_validate_key(key, version=version)
        client = self.get_client(key)
        return bool(client.exists(key))

    def incr(self, key, delta=1, version=None):
        key = self.make_and_validate_key(key, version=version)

        client = self.get_client(key, write=True)
        if not client.exists(key):
            raise ValueError(f"Key '{key}' not found.")
        return client.incr(key, delta)

    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
        if not data:
            return []
        safe_data = {}
        for key, value in data.items():
            key = self.make_and_validate_key(key, version=version)
            safe_data[key] = value

        timeout = self.get_backend_timeout(timeout)

        client = self.get_client(None, write=True)
        pipeline = client.pipeline()
        pipeline.mset({k: self._serializer.dumps(v) for k, v in safe_data.items()})

        if timeout is not None:
            # Setting timeout for each key as valkey does not support timeout
            # with mset().
            for key in data:
                pipeline.expire(key, timeout)
        pipeline.execute()
        return []

    def mset(self, data, version=None):
        if not data:
            return
        safe_data = {
            self.make_and_validate_key(k, version=version): self._serializer.dumps(v)
            for k, v in data.items()
        }
        client = self.get_client(None, write=True)
        client.mset(safe_data)

    def delete_many(self, keys, version=None):
        if not keys:
            return
        safe_keys = [self.make_and_validate_key(key, version=version) for key in keys]

        client = self.get_client(None, write=True)
        client.delete(*safe_keys)

    def clear(self):
        client = self.get_client(None, write=True)
        return bool(client.flushdb())
