import pytest

from django_valkey.async_cache.cache import AsyncValkeyCache


pytestmark = pytest.mark.anyio


class TestAsyncSortedSetOperations:
    """Tests for async sorted set (ZSET) operations."""

    async def test_zadd_basic(self, cache: AsyncValkeyCache):
        """Test adding members to sorted set."""
        result = await cache.azadd("scores", {"player1": 100.0, "player2": 200.0})
        assert result == 2
        assert await cache.azcard("scores") == 2

    async def test_zadd_with_nx(self, cache: AsyncValkeyCache):
        """Test zadd with nx flag (only add new)."""
        await cache.azadd("scores", {"alice": 10.0})
        result = await cache.azadd("scores", {"alice": 20.0}, nx=True)
        assert result == 0
        assert await cache.azscore("scores", "alice") == 10.0

    async def test_zadd_with_xx(self, cache: AsyncValkeyCache):
        """Test zadd with xx flag (only update existing)."""
        await cache.azadd("scores", {"bob": 15.0})
        result = await cache.azadd("scores", {"bob": 25.0}, xx=True)
        assert result == 0  # No new members added
        assert await cache.azscore("scores", "bob") == 25.0
        result = await cache.azadd("scores", {"charlie": 30.0}, xx=True)
        assert result == 0
        assert await cache.azscore("scores", "charlie") is None

    async def test_zadd_with_ch(self, cache: AsyncValkeyCache):
        """Test zadd with ch flag (return changed count)."""
        await cache.azadd("scores", {"player1": 100.0})
        result = await cache.azadd(
            "scores", {"player1": 150.0, "player2": 200.0}, ch=True
        )
        assert result == 2  # 1 changed + 1 added

    async def test_zcard(self, cache: AsyncValkeyCache):
        """Test getting sorted set cardinality."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        assert await cache.azcard("scores") == 3
        assert await cache.azcard("nonexistent") == 0

    async def test_zcount(self, cache: AsyncValkeyCache):
        """Test counting members in score range."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        assert await cache.azcount("scores", 2.0, 4.0) == 3  # b, c, d
        assert await cache.azcount("scores", "-inf", "+inf") == 5
        assert await cache.azcount("scores", 10.0, 20.0) == 0

    async def test_zincrby(self, cache: AsyncValkeyCache):
        """Test incrementing member score."""
        await cache.azadd("scores", {"player1": 100.0})
        new_score = await cache.azincrby("scores", 50.0, "player1")
        assert new_score == 150.0
        assert await cache.azscore("scores", "player1") == 150.0
        new_score = await cache.azincrby("scores", 25.0, "player2")
        assert new_score == 25.0

    async def test_zpopmax(self, cache: AsyncValkeyCache):
        """Test popping highest scored members."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azpopmax("scores")
        assert result == ("c", 3.0)
        assert await cache.azcard("scores") == 2
        await cache.azadd("scores", {"d": 4.0, "e": 5.0})
        result = await cache.azpopmax("scores", count=2)
        assert len(result) == 2
        assert result[0][0] == "e" and result[0][1] == 5.0
        assert result[1][0] == "d" and result[1][1] == 4.0

    async def test_zpopmin(self, cache: AsyncValkeyCache):
        """Test popping lowest scored members."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azpopmin("scores")
        assert result == ("a", 1.0)
        assert await cache.azcard("scores") == 2
        await cache.azadd("scores", {"d": 0.5, "e": 0.1})
        result = await cache.azpopmin("scores", count=2)
        assert len(result) == 2
        assert result[0][0] == "e" and result[0][1] == 0.1
        assert result[1][0] == "d" and result[1][1] == 0.5

    async def test_zrange_basic(self, cache: AsyncValkeyCache):
        """Test getting range of members by index."""
        await cache.azadd("scores", {"alice": 10.0, "bob": 20.0, "charlie": 15.0})
        result = await cache.azrange("scores", 0, -1)
        assert result == ["alice", "charlie", "bob"]
        result = await cache.azrange("scores", 0, 1)
        assert result == ["alice", "charlie"]

    async def test_zrange_withscores(self, cache: AsyncValkeyCache):
        """Test zrange with scores."""
        await cache.azadd("scores", {"alice": 10.5, "bob": 20.0, "charlie": 15.5})
        result = await cache.azrange("scores", 0, -1, withscores=True)
        assert result == [("alice", 10.5), ("charlie", 15.5), ("bob", 20.0)]

    async def test_zrange_desc(self, cache: AsyncValkeyCache):
        """Test zrange in descending order."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azrange("scores", 0, -1, desc=True)
        assert result == ["c", "b", "a"]

    async def test_zrangebyscore(self, cache: AsyncValkeyCache):
        """Test getting members by score range."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache.azrangebyscore("scores", 2.0, 4.0)
        assert result == ["b", "c", "d"]
        result = await cache.azrangebyscore("scores", "-inf", 2.0)
        assert result == ["a", "b"]

    async def test_zrangebyscore_withscores(self, cache: AsyncValkeyCache):
        """Test zrangebyscore with scores."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azrangebyscore("scores", 1.0, 2.0, withscores=True)
        assert result == [("a", 1.0), ("b", 2.0)]

    async def test_zrangebyscore_pagination(self, cache: AsyncValkeyCache):
        """Test zrangebyscore with pagination."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache.azrangebyscore("scores", "-inf", "+inf", start=1, num=2)
        assert len(result) == 2
        assert result == ["b", "c"]

    async def test_zrank(self, cache: AsyncValkeyCache):
        """Test getting member rank."""
        await cache.azadd("scores", {"alice": 10.0, "bob": 20.0, "charlie": 15.0})
        assert await cache.azrank("scores", "alice") == 0  # Lowest score
        assert await cache.azrank("scores", "charlie") == 1
        assert await cache.azrank("scores", "bob") == 2
        assert await cache.azrank("scores", "nonexistent") is None

    async def test_zrem(self, cache: AsyncValkeyCache):
        """Test removing members from sorted set."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azrem("scores", "b")
        assert result == 1
        assert await cache.azcard("scores") == 2
        result = await cache.azrem("scores", "a", "c")
        assert result == 2
        assert await cache.azcard("scores") == 0

    async def test_zremrangebyscore(self, cache: AsyncValkeyCache):
        """Test removing members by score range."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache.azremrangebyscore("scores", 2.0, 4.0)
        assert result == 3  # b, c, d removed
        assert await cache.azcard("scores") == 2
        assert await cache.azrange("scores", 0, -1) == ["a", "e"]

    async def test_zrevrange(self, cache: AsyncValkeyCache):
        """Test getting reverse range (highest to lowest)."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azrevrange("scores", 0, -1)
        assert result == ["c", "b", "a"]

    async def test_zrevrange_withscores(self, cache: AsyncValkeyCache):
        """Test zrevrange with scores."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0})
        result = await cache.azrevrange("scores", 0, -1, withscores=True)
        assert result == [("c", 3.0), ("b", 2.0), ("a", 1.0)]

    async def test_zrevrangebyscore(self, cache: AsyncValkeyCache):
        """Test getting reverse range by score."""
        await cache.azadd("scores", {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0, "e": 5.0})
        result = await cache.azrevrangebyscore("scores", 4.0, 2.0)
        assert result == ["d", "c", "b"]

    async def test_zscore(self, cache: AsyncValkeyCache):
        """Test getting member score."""
        await cache.azadd("scores", {"alice": 42.5, "bob": 100.0})
        assert await cache.azscore("scores", "alice") == 42.5
        assert await cache.azscore("scores", "bob") == 100.0
        assert await cache.azscore("scores", "nonexistent") is None

    async def test_sorted_set_serialization(self, cache: AsyncValkeyCache):
        """Test that complex objects serialize correctly as members."""
        await cache.azadd("complex", {("tuple", "key"): 1.0, "string": 2.0})
        result = await cache.azrange("complex", 0, -1)
        assert ("tuple", "key") in result or ["tuple", "key"] in result
        assert "string" in result

    async def test_sorted_set_version_support(self, cache: AsyncValkeyCache):
        """Test version parameter works correctly."""
        await cache.azadd("data", {"v1": 1.0}, version=1)
        await cache.azadd("data", {"v2": 2.0}, version=2)

        assert await cache.azcard("data", version=1) == 1
        assert await cache.azcard("data", version=2) == 1
        assert await cache.azrange("data", 0, -1, version=1) == ["v1"]
        assert await cache.azrange("data", 0, -1, version=2) == ["v2"]

    async def test_sorted_set_float_scores(self, cache: AsyncValkeyCache):
        """Test that float scores work correctly."""
        await cache.azadd("precise", {"a": 1.1, "b": 1.2, "c": 1.15})
        result = await cache.azrange("precise", 0, -1, withscores=True)
        assert result[0] == ("a", 1.1)
        assert result[1] == ("c", 1.15)
        assert result[2] == ("b", 1.2)

    async def test_sorted_set_negative_scores(self, cache: AsyncValkeyCache):
        """Test that negative scores work correctly."""
        await cache.azadd("temps", {"freezing": -10.0, "cold": 0.0, "warm": 20.0})
        result = await cache.azrange("temps", 0, -1)
        assert result == ["freezing", "cold", "warm"]

    async def test_zpopmin_empty_set(self, cache: AsyncValkeyCache):
        """Test zpopmin on empty sorted set."""
        result = await cache.azpopmin("nonexistent")
        assert result is None
        result = await cache.azpopmin("nonexistent", count=5)
        assert result == []

    async def test_zpopmax_empty_set(self, cache: AsyncValkeyCache):
        """Test zpopmax on empty sorted set."""
        result = await cache.azpopmax("nonexistent")
        assert result is None
        result = await cache.azpopmax("nonexistent", count=5)
        assert result == []
