import threading
from collections.abc import Iterable
from typing import Any

import google.auth
from google.auth import impersonated_credentials
from google.auth.transport.requests import Request
from valkey.credentials import CredentialProvider

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


class MemorystoreIAMCredentialProvider(CredentialProvider):
    """
    Authenticate to Google Cloud Memorystore with an IAM access token.

    The token comes from Application Default Credentials, or from a service
    account impersonated through them when ``target_principal`` is set. It is
    cached and refreshed shortly before it expires, so opening a connection
    only fetches a new token about once an hour.

    Memorystore checks the token only when a connection authenticates, so
    connections that are already open keep working after it expires.
    """

    # Memorystore only accepts the "default" user for IAM authentication.
    username = "default"

    def __init__(
        self,
        scopes: Iterable[str] = (CLOUD_PLATFORM_SCOPE,),
        target_principal: str | None = None,
        lifetime: int = 3600,
    ):
        self.scopes = list(scopes)
        self.target_principal = target_principal
        self.lifetime = lifetime
        # Credentials are loaded on first use, not here: django-valkey can
        # build a provider for every cache client, and most are never used.
        self._credentials: Any = None
        self._request: Request | None = None
        self._lock = threading.Lock()

    def _load_credentials(self):
        credentials, _ = google.auth.default(scopes=self.scopes)
        if self.target_principal:
            credentials = impersonated_credentials.Credentials(
                source_credentials=credentials,
                target_principal=self.target_principal,
                target_scopes=self.scopes,
                lifetime=self.lifetime,
            )
        return credentials

    def get_credentials(self) -> tuple[str, str]:
        with self._lock:
            if self._credentials is None:
                self._credentials = self._load_credentials()
                self._request = Request()
            # valid is False once the token is within google-auth's refresh
            # threshold of expiring, so a token is never sent about to lapse.
            if not self._credentials.valid:
                self._credentials.refresh(self._request)
            return self.username, self._credentials.token
