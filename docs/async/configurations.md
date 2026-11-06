# Configure The Async Client

**Warning**: as of django 5.2, async support for cache backends is flaky, if you decide to use the async backends do so with caution.

**Important**: the async client is not compatible with django's cache middlewares.
if you need the middlewares, consider using the sync client or implement a new middleware.

there are three async clients available, a normal client, sentinel client and a herd client (herd is no longer supported).

## Default client

to setup the async client you can configure your settings file to look like this:

```python
CACHES = {
    "default": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379",
        "OPTIONS": {...},
    },
}
```

take a look at [Configure the database URL](../configure/advanced_configurations.md#configure-the-database-url) to see other ways to write the URL.
And that's it, the backend defaults to use AsyncDefaultClient as client interface, AsyncConnectionFactory as connection factory and valkey-py's async client.

you can, of course configure it to use any other class, or pass in extras args and kwargs, the same way that was discussed at [Advanced Configurations](../configure/advanced_configurations.md).

## Sentinel client

In order to facilitate using [Valkey Sentinels](https://valkey.io/topics/sentinel), django-valkey comes with a built-in sentinel client and a connection factory

since this is a big topic, you can find detailed explanation in [Sentinel Configuration](sentinel_configurations.md)
but for a simple configuration this will work:

```python
SENTINELS = [
    ("127.0.0.1", 26379),  # a list of (host name, port) tuples.
]

CACHES = {
    "default": {
        "BACKEND": "django_valkey.async_cache.cache.AsyncValkeyCache",
        "LOCATION": "valkey://service_name/db",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncSentinelClient",
            "SENTINELS": SENTINELS,
            # optional
            "SENTINEL_KWARGS": {},
        },
    }
}
```

*note*: the sentinel client uses the sentinel connection factory by default.
you can change this behaviour by setting `DJANGO_VALKEY_CONNECTION_FACTORY` in your django settings or `CONNECTION_FACTORY` in your `CACHES` OPTIONS.

## Herd client

!!! warning

    Herd Client is not longer supported, it will remain in the codebase but will get no updates or support of any kind.

to set up herd client configure your settings like this:

```python
CACHES = {
    "default": {
        "BACKEND": "django_valkey.async_cache.caches.AsyncValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379",
        "OPTIONS": {
            "CLIENT_CLASS": "django_valkey.async_cache.client.AsyncHerdClient",
        },
    },
}
```

for a more specified guide look at [Advanced Async Configuration](advanced_configurations.md).
