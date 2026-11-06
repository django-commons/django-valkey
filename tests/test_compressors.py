from unittest import mock

from django_valkey.compressors.gzip import GzipCompressor


class TestGzipCompressor:
    def test_compress_is_deterministic(self):
        compressor = GzipCompressor({})
        value = b"a value long enough to be compressed"
        with mock.patch("time.time", return_value=1_000_000_000):
            first = compressor.compress(value)
        with mock.patch("time.time", return_value=2_000_000_000):
            second = compressor.compress(value)
        assert first == second
        assert compressor.decompress(first) == value
