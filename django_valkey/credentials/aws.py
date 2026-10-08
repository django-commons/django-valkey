import os
import threading
from typing import Any
from urllib.parse import urlencode

from botocore.auth import SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.session import Session
from django.core.exceptions import ImproperlyConfigured
from valkey.credentials import CredentialProvider

# ElastiCache rejects tokens older than this.
TOKEN_LIFETIME = 900


class ElastiCacheIAMCredentialProvider(CredentialProvider):
    """
    Authenticate to AWS ElastiCache with an IAM authentication token.

    The token is a SigV4 presigned ``connect`` request for the cache, signed
    with credentials from the default AWS credential chain. Signing is local,
    so a fresh token is signed for every new connection.

    Pass exactly one of ``replication_group_id`` (node-based clusters) or
    ``serverless_cache_name`` (serverless caches). ``user_id`` is the
    ElastiCache user ID, which for IAM-enabled users is also its user name.
    """

    def __init__(
        self,
        user_id: str,
        replication_group_id: str | None = None,
        serverless_cache_name: str | None = None,
        region: str | None = None,
        profile_name: str | None = None,
    ):
        cache_name = replication_group_id or serverless_cache_name
        if not cache_name or (replication_group_id and serverless_cache_name):
            error_message = (
                "Pass exactly one of replication_group_id or serverless_cache_name"
            )
            raise ImproperlyConfigured(error_message)

        # ElastiCache stores both lowercase, and the token has to match.
        self.user_id = user_id.lower()
        self.cache_name = cache_name.lower()
        self.serverless = bool(serverless_cache_name)
        self.region = region
        self.profile_name = profile_name
        # The botocore session is created on first use, not here: django-valkey
        # can build a provider for every cache client, and most are never used.
        self._credentials: Any = None
        self._lock = threading.Lock()

    def _load_credentials(self):
        session = Session(profile=self.profile_name)
        # botocore resolves AWS_DEFAULT_REGION and the profile, not AWS_REGION.
        region = (
            self.region
            or os.environ.get("AWS_REGION")
            or session.get_config_variable("region")
        )
        if not region:
            error_message = (
                "No AWS region found: pass region, or set AWS_REGION, "
                "AWS_DEFAULT_REGION or a region in the AWS config profile"
            )
            raise ImproperlyConfigured(error_message)
        credentials = session.get_credentials()
        if credentials is None:
            error_message = "No AWS credentials found in the default credential chain"
            raise ImproperlyConfigured(error_message)
        self.region = region
        return credentials

    def get_token(self) -> str:
        with self._lock:
            if self._credentials is None:
                self._credentials = self._load_credentials()
        params = {"Action": "connect", "User": self.user_id}
        if self.serverless:
            params["ResourceType"] = "ServerlessCache"
        request = AWSRequest(
            method="GET", url=f"https://{self.cache_name}/?{urlencode(params)}"
        )
        # Refreshable credentials (assumed roles, instance profiles, SSO) renew
        # themselves here when they are close to expiring.
        frozen = self._credentials.get_frozen_credentials()
        SigV4QueryAuth(
            frozen, "elasticache", self.region, expires=TOKEN_LIFETIME
        ).add_auth(request)
        return request.url.removeprefix("https://")

    def get_credentials(self) -> tuple[str, str]:
        return self.user_id, self.get_token()
