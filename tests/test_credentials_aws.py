import copy
from urllib.parse import parse_qs, urlsplit

import pytest
from django.core.exceptions import ImproperlyConfigured
from pytest_django import Settings

pytest.importorskip("botocore")

from django_valkey.base import BaseValkeyCache
from django_valkey.credentials.aws import (
    ElastiCacheIAMCredentialProvider,
)


@pytest.fixture(autouse=True)
def aws_environment(monkeypatch, tmp_path):
    # Static credentials from the environment, and nothing from the machine
    # running the tests: no config files, profile, region or instance metadata.
    for name in ("AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-key-example")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "session-token-example")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


def parse_token(token: str) -> tuple[str, dict[str, str]]:
    url = urlsplit(f"https://{token}")
    query = {key: values[0] for key, values in parse_qs(url.query).items()}
    return url.netloc, query


def test_replication_group_token():
    provider = ElastiCacheIAMCredentialProvider(
        user_id="IAM-User-01", replication_group_id="My-Cluster", region="us-east-1"
    )
    user, token = provider.get_credentials()

    host, query = parse_token(token)
    assert user == "iam-user-01"
    assert token.startswith("my-cluster/?Action=connect&User=iam-user-01&X-Amz-")
    assert host == "my-cluster"
    assert "ResourceType" not in query
    assert query["X-Amz-Algorithm"] == "AWS4-HMAC-SHA256"
    assert query["X-Amz-Credential"].startswith("AKIDEXAMPLE/")
    assert query["X-Amz-Credential"].endswith("/us-east-1/elasticache/aws4_request")
    assert query["X-Amz-Expires"] == "900"
    assert query["X-Amz-Security-Token"] == "session-token-example"
    assert len(query["X-Amz-Signature"]) == 64


def test_serverless_token():
    provider = ElastiCacheIAMCredentialProvider(
        user_id="iam-user-01", serverless_cache_name="my-cache", region="us-east-1"
    )
    _, token = provider.get_credentials()

    host, query = parse_token(token)
    assert host == "my-cache"
    assert query["ResourceType"] == "ServerlessCache"


def test_region_from_environment(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    provider = ElastiCacheIAMCredentialProvider(
        user_id="iam-user-01", replication_group_id="my-cluster"
    )
    _, token = provider.get_credentials()

    _, query = parse_token(token)
    assert query["X-Amz-Credential"].endswith("/eu-west-2/elasticache/aws4_request")


def test_no_region():
    provider = ElastiCacheIAMCredentialProvider(
        user_id="iam-user-01", replication_group_id="my-cluster"
    )
    with pytest.raises(ImproperlyConfigured):
        provider.get_credentials()


@pytest.mark.parametrize(
    "names",
    [
        {},
        {"replication_group_id": "my-cluster", "serverless_cache_name": "my-cache"},
    ],
)
def test_exactly_one_cache_name(names):
    with pytest.raises(ImproperlyConfigured):
        ElastiCacheIAMCredentialProvider(user_id="iam-user-01", **names)


def test_session_created_on_first_use(mocker):
    session = mocker.patch("django_valkey.credentials.aws.Session")
    ElastiCacheIAMCredentialProvider(
        user_id="iam-user-01", replication_group_id="my-cluster", region="us-east-1"
    )
    session.assert_not_called()


def test_cache_with_elasticache_provider(cache: BaseValkeyCache, settings: Settings):
    # The test servers have no password, and a passwordless user accepts any
    # password, so this sends the token over AUTH without a real ElastiCache.
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"].setdefault("OPTIONS", {})
    caches_setting["default"]["OPTIONS"]["CREDENTIAL_PROVIDER"] = (
        "django_valkey.credentials.aws.ElastiCacheIAMCredentialProvider"
    )
    caches_setting["default"]["OPTIONS"]["CREDENTIAL_PROVIDER_KWARGS"] = {
        "user_id": "default",
        "replication_group_id": "my-cluster",
        "region": "us-east-1",
    }
    settings.CACHES = caches_setting

    cache.set("foo", "bar")
    assert cache.get("foo") == "bar"
