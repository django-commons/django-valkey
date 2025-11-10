from typing import Any, Optional, Protocol, Union

from valkey import Valkey
from valkey.typing import KeyT


class ClientProtocol(Protocol):
    """
    Protocol for client methods required by mixins.

    Any class using django-valkey mixins must implement these methods.
    """

    def make_key(
        self,
        key: KeyT,
        version: Optional[int] = None,
        prefix: Optional[str] = None,
    ) -> KeyT:
        """Create a cache key with optional version and prefix."""
        ...

    def encode(self, value: Any) -> Union[bytes, int, float]:
        """Encode a value for storage in Valkey."""
        ...

    def decode(self, value: Union[bytes, int]) -> Any:
        """Decode a value retrieved from Valkey."""
        ...

    def get_client(self, write: bool = False) -> Valkey:
        """Get a Valkey client instance for read or write operations."""
        ...
