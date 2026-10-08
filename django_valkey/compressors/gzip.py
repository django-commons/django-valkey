import gzip

from django_valkey.compressors.base import BaseCompressor


class GzipCompressor(BaseCompressor):
    """
    GzipCompressor
    set with:
    CACHES = {
        "default": {
            # ...
            "OPTIONS": {
                "COMPRESSOR": "django_valkey.compressors.gzip.GzipCompressor",
                }
            }
        }

    compression parameters:
    to set `compresslevel` use `CACHE_COMPRESS_LEVEL` in your settings, defaults to 4.
    to set `minimum size` set `CACHE_COMPRESS_MIN_LENGTH` in your settings, defaults to 15.
    """

    def _compress(self, value: bytes) -> bytes:
        # mtime=0 keeps the output the same for the same input; by default gzip
        # writes the current time into the header, so set members compressed
        # in different seconds would never match.
        return gzip.compress(value, compresslevel=self.level or 9, mtime=0)

    def _decompress(self, value: bytes) -> bytes:
        return gzip.decompress(value)
