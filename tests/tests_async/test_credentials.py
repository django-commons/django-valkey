import copy

import pytest
from pytest_django import Settings

from django_valkey.async_cache.cache import AsyncValkeyCache

pytestmark = pytest.mark.anyio


def use_provider(settings: Settings, path: str, kwargs: dict | None = None) -> None:
    caches_setting = copy.deepcopy(settings.CACHES)
    options = caches_setting["default"].setdefault("OPTIONS", {})
    options["CREDENTIAL_PROVIDER"] = path
    if kwargs:
        options["CREDENTIAL_PROVIDER_KWARGS"] = kwargs
    settings.CACHES = caches_setting


# The test servers have no password, and a passwordless user accepts any
# password, so these send real tokens over AUTH without a managed service.


@pytest.mark.filterwarnings("ignore:coroutine 'AsyncBackendCommands.close'")
async def test_memorystore_provider(
    cache: AsyncValkeyCache, settings: Settings, mocker
):
    pytest.importorskip("google.auth")
    credentials = mocker.Mock(valid=False, token="token")
    mocker.patch("google.auth.default", return_value=(credentials, "project"))
    use_provider(
        settings, "django_valkey.credentials.gcp.MemorystoreIAMCredentialProvider"
    )

    await cache.aset("foo", "bar")
    assert await cache.aget("foo") == "bar"
    credentials.refresh.assert_called()


@pytest.mark.filterwarnings("ignore:coroutine 'AsyncBackendCommands.close'")
async def test_elasticache_provider(
    cache: AsyncValkeyCache, settings: Settings, monkeypatch, tmp_path
):
    pytest.importorskip("botocore")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-key-example")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    use_provider(
        settings,
        "django_valkey.credentials.aws.ElastiCacheIAMCredentialProvider",
        {
            "user_id": "default",
            "replication_group_id": "my-cluster",
            "region": "us-east-1",
        },
    )

    await cache.aset("foo", "bar")
    assert await cache.aget("foo") == "bar"
