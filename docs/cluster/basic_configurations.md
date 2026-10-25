# Basic cluster configuration

for installation, look at our [Installation guide](../installation.md)


## Configure as cache backend

to start using django-valkey's cluster backend, change your django cache setting to something like this:

```python
CACHES = {
    "default": {
        "BACKEND": "django_valkey.cluster_cache.cache.ClusterValkeyCache",
        "LOCATION": "valkey://127.0.0.1:6379",
        "OPTIONS": {...},
    }
}
```

you need to point to at least one of the cluster nodes in `LOCATION`, or pass a list of multiple nodes

valkey-py's `ValkeyCluster` creates a connection pool for each node it talks to. `CONNECTION_POOL_CLASS` and `CONNECTION_POOL_KWARGS` configure those per-node pools,
and `PASSWORD`, `CREDENTIAL_PROVIDER`, `SOCKET_TIMEOUT` and `SOCKET_CONNECT_TIMEOUT` apply to their connections, the same as with the default backend.
`BASE_CLIENT_KWARGS` is passed to `ValkeyCluster` itself, which is the place for cluster options such as `read_from_replicas`; if a key is set in both, `BASE_CLIENT_KWARGS` wins.

at the moment, only one client is available for cluster backend

most of the configurations you see in [basic configuration](../configure/basic_configurations.md) and [advanced configuration](../configure/advanced_configurations.md)
apply here as well, except the following:



### Memcached exception behavior
in [Memcached exception behavior](../configure/basic_configurations.md#memcached-exception-behavior) we discussed how to ignore and log exceptitions,
sadly, until we find a way around it, this is not accessible with cluster backend


## Multi-key Commands

please refer to [valkey-py docs](https://valkey-py.readthedocs.io/en/latest/clustering.html#multi-key-commands) on how to use multi-key commands, such as `mset`, `megt`, etc...

there are some other info in their documentations that might be of interest to you, we suggest you take a look


## Additional methods
in addition to what other `django-valkey` clients provide, cluster client supports the following methods:

* mset_nonatomic (same as `set_many`)
* msetnx
* mget_nonatomic (same as `get_many`)
* readonly
* readwrite
* keyslot
* flushall
* invalidate_key_from_cache
