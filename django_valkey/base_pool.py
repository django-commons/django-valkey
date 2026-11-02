from typing import Any, ClassVar, Generic, TypeVar

from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

Pool = TypeVar("Pool")
Base = TypeVar("Base")


class _ByIdentity:
    """
    Hashable stand-in for an unhashable object, equal only to itself.

    Holding the object keeps its id from being reused while the key is cached.
    """

    __slots__ = ("obj",)

    def __init__(self, obj: Any):
        self.obj = obj

    def __hash__(self) -> int:
        return id(self.obj)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _ByIdentity) and other.obj is self.obj


def freeze_options(value: Any) -> Any:
    """
    Turn OPTIONS into something hashable, for use in the connection pool key.

    Containers compare by content. Other objects, such as credential providers
    or retry policies, compare the way they normally do, which for most of
    them means by identity.
    """
    if isinstance(value, dict):
        return frozenset((k, freeze_options(v)) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return tuple(freeze_options(v) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(freeze_options(v) for v in value)
    try:
        hash(value)
    except TypeError:
        return _ByIdentity(value)
    return value


class BaseConnectionFactory(Generic[Base, Pool]):
    # Store connection pool by cache backend options.
    #
    # _pools is a process-global, as otherwise _pools is cleared every time
    # ConnectionFactory is instantiated, as Django creates new cache client
    # (DefaultClient) instance for every request.
    #
    # Pools are keyed by factory class, URL and OPTIONS, so caches that share a
    # LOCATION but differ in options (or are sync and async) get their own pool.

    _pools: ClassVar[dict[tuple, Pool | Any]] = {}

    def __init__(self, options: dict):
        pool_cls_path = options.get("CONNECTION_POOL_CLASS", self.path_pool_cls)
        self.pool_cls: type[Pool] | type = import_string(pool_cls_path)
        self.pool_cls_kwargs = options.get("CONNECTION_POOL_KWARGS", {})

        base_client_cls_path = options.get("BASE_CLIENT_CLASS", self.path_base_cls)
        self.base_client_cls: type[Base] | type = import_string(base_client_cls_path)
        self.base_client_cls_kwargs = options.get("BASE_CLIENT_KWARGS", {})

        self.credential_provider = self.get_credential_provider(options)

        self.options = options

    def get_credential_provider(self, options: dict) -> Any:
        """
        Return the valkey-py credential provider configured by
        CREDENTIAL_PROVIDER, or None.

        CREDENTIAL_PROVIDER is either a provider instance or a dotted path. A
        path to a class is instantiated with CREDENTIAL_PROVIDER_KWARGS; a path
        to anything else (for example a module-level instance) is used as is.
        """
        provider = options.get("CREDENTIAL_PROVIDER")
        if provider is None:
            return None

        if options.get("PASSWORD"):
            error_message = "PASSWORD and CREDENTIAL_PROVIDER cannot be used together"
            raise ImproperlyConfigured(error_message)

        if isinstance(provider, str):
            provider = import_string(provider)
            if isinstance(provider, type):
                provider = provider(**options.get("CREDENTIAL_PROVIDER_KWARGS", {}))
        return provider

    def make_connection_params(self, url: str | None) -> dict:
        """
        Given a main connection parameters, build a complete
        dict of connection parameters.
        """

        kwargs = {
            "url": url,
            "parser_class": self.get_parser_cls(),
        }

        socket_timeout = self.options.get("SOCKET_TIMEOUT", None)
        # TODO: do we need to check for existence?
        if socket_timeout:
            if not isinstance(socket_timeout, (int, float)):
                error_message = "Socket timeout should be float or integer"
                raise ImproperlyConfigured(error_message)
            kwargs["socket_timeout"] = socket_timeout

        socket_connect_timeout = self.options.get("SOCKET_CONNECT_TIMEOUT", None)
        if socket_connect_timeout:
            if not isinstance(socket_connect_timeout, (int, float)):
                error_message = "Socket connect timeout should be float or integer"
                raise ImproperlyConfigured(error_message)
            kwargs["socket_connect_timeout"] = socket_connect_timeout

        password = self.options.get("PASSWORD", None)
        if password:
            kwargs["password"] = password

        if self.credential_provider is not None:
            kwargs["credential_provider"] = self.credential_provider

        return kwargs

    def get_connection_pool(self, params: dict) -> Pool | Any:
        """
        Given a connection parameters, return a new
        connection pool for them.

        Overwrite this method if you want a custom
        behavior on creating connection pool.
        """
        cp_params = params
        cp_params.update(self.pool_cls_kwargs)
        pool = self.pool_cls.from_url(**cp_params)

        if pool.connection_kwargs.get("password", None) is None:
            pool.connection_kwargs["password"] = params.get("password", None)
            pool.reset()

        return pool

    def get_or_create_connection_pool(self, params: dict) -> Pool | Any:
        """
        Given a connection parameters and return a new
        or cached connection pool for them.

        Reimplement this method if you want distinct
        connection pool instance caching behavior.
        """
        key = (type(self), params["url"], freeze_options(self.options))
        if key not in self._pools:
            self._pools[key] = self.get_connection_pool(params)
        return self._pools[key]

    def get_connection(self, params: dict) -> Base | Any:
        """
        Given a now preformatted params, return a
        new connection.

        The default implementation uses a cached pools
        for create new connection.
        """
        raise NotImplementedError

    def connect(self, url: str) -> Base | Any:
        """
        Given a basic connection parameters,
        return a new connection.
        """
        raise NotImplementedError

    def disconnect(self, connection: type[Base]):
        raise NotImplementedError

    def get_parser_cls(self):
        raise NotImplementedError
