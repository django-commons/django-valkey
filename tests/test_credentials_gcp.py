import copy

import pytest
from pytest_django import Settings

pytest.importorskip("google.auth")

from django_valkey.base import BaseValkeyCache
from django_valkey.credentials.gcp import (
    CLOUD_PLATFORM_SCOPE,
    MemorystoreIAMCredentialProvider,
)


class FakeCredentials:
    """Stands in for google-auth credentials: a token that refresh() replaces."""

    def __init__(self):
        self.token = None
        self.valid = False
        self.refreshes = 0

    def refresh(self, request):
        self.refreshes += 1
        self.token = f"token-{self.refreshes}"
        self.valid = True


@pytest.fixture
def credentials(mocker):
    creds = FakeCredentials()
    mocker.patch("google.auth.default", return_value=(creds, "project"))
    return creds


def test_credentials_loaded_on_first_use(mocker):
    default = mocker.patch("google.auth.default")
    MemorystoreIAMCredentialProvider()
    default.assert_not_called()


def test_default_user_and_token(credentials: FakeCredentials, mocker):
    default = mocker.patch("google.auth.default", return_value=(credentials, "p"))
    provider = MemorystoreIAMCredentialProvider()

    assert provider.get_credentials() == ("default", "token-1")
    default.assert_called_once_with(scopes=[CLOUD_PLATFORM_SCOPE])


def test_token_reused_while_valid(credentials: FakeCredentials):
    provider = MemorystoreIAMCredentialProvider()
    provider.get_credentials()
    provider.get_credentials()
    assert credentials.refreshes == 1


def test_token_refreshed_when_no_longer_valid(credentials: FakeCredentials):
    provider = MemorystoreIAMCredentialProvider()
    provider.get_credentials()
    credentials.valid = False

    assert provider.get_credentials() == ("default", "token-2")


def test_impersonation(credentials: FakeCredentials, mocker):
    impersonated = FakeCredentials()
    impersonate = mocker.patch(
        "google.auth.impersonated_credentials.Credentials", return_value=impersonated
    )
    provider = MemorystoreIAMCredentialProvider(
        target_principal="cache@project.iam.gserviceaccount.com", lifetime=7200
    )

    assert provider.get_credentials() == ("default", "token-1")
    impersonate.assert_called_once_with(
        source_credentials=credentials,
        target_principal="cache@project.iam.gserviceaccount.com",
        target_scopes=[CLOUD_PLATFORM_SCOPE],
        lifetime=7200,
    )
    assert impersonated.refreshes == 1
    assert credentials.refreshes == 0


def test_cache_with_memorystore_provider(
    cache: BaseValkeyCache, settings: Settings, credentials: FakeCredentials
):
    # The test servers have no password, and a passwordless user accepts any
    # password, so this sends the token over AUTH without a real Memorystore.
    caches_setting = copy.deepcopy(settings.CACHES)
    caches_setting["default"].setdefault("OPTIONS", {})
    caches_setting["default"]["OPTIONS"]["CREDENTIAL_PROVIDER"] = (
        "django_valkey.credentials.gcp.MemorystoreIAMCredentialProvider"
    )
    settings.CACHES = caches_setting

    cache.set("foo", "bar")
    assert cache.get("foo") == "bar"
    assert credentials.refreshes >= 1
