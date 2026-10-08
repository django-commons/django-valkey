# IAM authentication

AWS ElastiCache and Google Cloud Memorystore can authenticate connections with short-lived IAM tokens instead of a fixed password.
django-valkey ships a credential provider for each, built on the `CREDENTIAL_PROVIDER` option described in [Advanced configurations](advanced_configurations.md#credential-providers).
valkey-py asks the provider for credentials every time it opens a connection, so new connections always authenticate with a current token, and connections that are already open are left alone.

the providers work with every backend, including the cluster backend (both services default to cluster mode) and the async backend.

## Google Cloud Memorystore

install the extra:

```shell
pip install django-valkey[gcp]
```

and point `CREDENTIAL_PROVIDER` at the provider:

```python
CACHES = {
    "default": {
        "BACKEND": "django_valkey.cluster_cache.cache.ClusterValkeyCache",
        "LOCATION": "valkeys://10.0.0.5:6379",
        "OPTIONS": {
            "CREDENTIAL_PROVIDER": "django_valkey.credentials.gcp.MemorystoreIAMCredentialProvider",
            "CONNECTION_POOL_KWARGS": {"ssl_ca_certs": "/path/to/server-ca.pem"},
        },
    }
}
```

for an instance with cluster mode disabled, use `django_valkey.cache.ValkeyCache` with the same `OPTIONS`.

the provider gets an access token from [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials), which on GKE, Cloud Run and Compute Engine is the workload's service account.
that principal needs the `roles/memorystore.dbConnectionUser` role on the instance or project.
the token is sent with the `default` user, which is the only user Memorystore accepts for IAM authentication.
it is cached and refreshed a few minutes before it expires, so the provider only goes to Google for a new token about once an hour.

to connect as a different service account than the one the workload runs as, set `target_principal` and the provider will impersonate it, which needs `roles/iam.serviceAccountTokenCreator` on that account.
`lifetime` sets how long the impersonated token lasts, in seconds; it defaults to an hour, and anything over an hour needs the `iam.allowServiceAccountCredentialLifetimeExtension` organization policy.
`scopes` defaults to the cloud-platform scope.

```python
CACHES = {
    "default": {
        # ...
        "OPTIONS": {
            "CREDENTIAL_PROVIDER": "django_valkey.credentials.gcp.MemorystoreIAMCredentialProvider",
            "CREDENTIAL_PROVIDER_KWARGS": {
                "target_principal": "cache-client@my-project.iam.gserviceaccount.com",
            },
        },
    }
}
```

Memorystore only checks the token when a connection authenticates, so an open connection keeps working after its token expires.
it also throttles how fast IAM-authenticated connections can be opened, so leave `CLOSE_CONNECTION` off and let the pool reuse connections.
without in-transit encryption the token is sent in plain text, so enable it on the instance and pass the server CA in `ssl_ca_certs` as shown above.

## AWS ElastiCache

install the extra:

```shell
pip install django-valkey[aws]
```

and point `CREDENTIAL_PROVIDER` at the provider, with the ElastiCache user and cache:

```python
CACHES = {
    "default": {
        "BACKEND": "django_valkey.cluster_cache.cache.ClusterValkeyCache",
        "LOCATION": "valkeys://my-cache-abc123.serverless.use1.cache.amazonaws.com:6379",
        "OPTIONS": {
            "CREDENTIAL_PROVIDER": "django_valkey.credentials.aws.ElastiCacheIAMCredentialProvider",
            "CREDENTIAL_PROVIDER_KWARGS": {
                "user_id": "iam-user-01",
                "serverless_cache_name": "my-cache",
                "region": "us-east-1",
            },
        },
    }
}
```

for a node-based cluster, pass `replication_group_id` instead of `serverless_cache_name`; the token is signed differently for each, so it has to be the right one.
`user_id` is the ElastiCache user ID, which for IAM-enabled users is also the user name the connection authenticates as.
both names are lowercased, since that is how ElastiCache stores them.

the token is a SigV4-signed request, signed with credentials from the default AWS credential chain: environment variables, the shared config and credentials files, SSO, or the container or instance role.
set `profile_name` to use a specific profile.
`region` falls back to `AWS_REGION`, `AWS_DEFAULT_REGION` and then the profile's region.
the principal needs `elasticache:Connect` on both the cache and the user.

signing happens locally, so the provider signs a new token for every connection rather than caching one; tokens are valid for 15 minutes.
ElastiCache requires in-transit encryption for IAM authentication, which is why the `LOCATION` above uses `valkeys://`.
it also disconnects IAM-authenticated connections after 12 hours; the pool replaces a dropped connection with a new one, which gets a fresh token.
IAM authentication needs ElastiCache for Valkey 7.2 or later, or Redis OSS 7.0 or later.

## Async backends

valkey-py calls `get_credentials()` synchronously, even from the async backend.
for Memorystore that means the token refresh, an HTTP request to Google about once an hour, briefly blocks the event loop.
ElastiCache signs locally, though refreshing the AWS credentials underneath, for example an assumed role, can make a network request.
