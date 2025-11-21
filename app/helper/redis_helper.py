"""
Universal Redis Helper for Caching Operations

This helper provides a reusable interface for managing Redis connections and
caching operations. It can be easily copied to other projects by just
configuring Redis settings in the config file.

Usage:
    from app.helper.redis_helper import RedisHelper

    # Initialize helper
    redis_helper = RedisHelper()
    await redis_helper.initialize()

    # Set a value
    await redis_helper.set("key", {"data": "value"}, ttl=300)

    # Get a value
    data = await redis_helper.get("key")

    # Delete a key
    await redis_helper.delete("key")

    # Invalidate by pattern
    await redis_helper.invalidate_pattern("myapp:*")

    # Close connection when done
    await redis_helper.close()
"""

from __future__ import annotations

import json
from typing import Any, Optional

import redis.asyncio as redis
from app.config.baseapp_config import get_base_config
from app.config.logger_config import logger


class RedisHelper:
    """
    Universal Redis helper for caching operations.

    This class provides a reusable interface for Redis operations that can
    be easily ported to other projects by just updating the config.
    """

    def __init__(self, redis_url: Optional[str] = None):
        """
        Initialize Redis helper.

        Args:
            redis_url: Optional Redis URL. If not provided, uses config.REDIS_URL

        Note:
            If IS_REDIS_CACHE_ENABLED=False for this service, the helper will be created
            but all operations will be disabled. Methods will return None/False gracefully.
        """
        self.config = get_base_config()
        self._enabled = self.config.IS_REDIS_CACHE_ENABLED

        if self._enabled:
            self.redis_url = redis_url or self.config.REDIS_URL
            self.default_ttl = self.config.REDIS_CACHE_TTL
        else:
            self.redis_url = None
            self.default_ttl = None
            logger.warning(
                "Redis helper initialized but Redis cache is disabled for this service "
                "(IS_REDIS_CACHE_ENABLED=False)"
            )

        self._redis: Optional[redis.Redis] = None

    async def initialize(self) -> bool:
        """
        Initialize Redis connection.

        Returns:
            True if connection was established successfully, False otherwise

        Raises:
            ConnectionError: If connection cannot be established and Redis is enabled
        """
        if not self._enabled:
            logger.debug("Redis cache is disabled, skipping initialization")
            return False

        if self._redis is None:
            try:
                self._redis = await redis.from_url(
                    self.redis_url, encoding="utf-8", decode_responses=True
                )
                await self._redis.ping()
                logger.info("✅ Redis connection established successfully")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to connect to Redis: {e}")
                self._redis = None
                raise ConnectionError(f"Failed to connect to Redis: {e}") from e

        return True

    async def close(self) -> None:
        """
        Close Redis connection.

        This should be called when the helper is no longer needed,
        typically during application shutdown.
        """
        if self._redis:
            await self._redis.close()
            self._redis = None
            logger.info("Redis connection closed")

    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from Redis cache.

        Args:
            key: Cache key

        Returns:
            Cached value (automatically JSON-decoded) or None if not found or Redis is disabled
        """
        if not self._enabled or not self._redis:
            return None

        try:
            value = await self._redis.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Redis GET error for key '{key}': {e}")
            return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set value in Redis cache.

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized automatically)
            ttl: Time to live in seconds. If None, uses default TTL from config

        Returns:
            True if successful, False otherwise or if Redis is disabled
        """
        if not self._enabled or not self._redis:
            return False

        try:
            serialized_value = json.dumps(value)
            cache_ttl = ttl if ttl is not None else self.default_ttl

            if cache_ttl:
                await self._redis.setex(key, cache_ttl, serialized_value)
            else:
                await self._redis.set(key, serialized_value)
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Redis SET error for key '{key}': {e}")
            return False

    async def delete(self, key: str) -> bool:
        """
        Delete a key from Redis cache.

        Args:
            key: Cache key to delete

        Returns:
            True if successful, False otherwise or if Redis is disabled
        """
        if not self._enabled or not self._redis:
            return False

        try:
            await self._redis.delete(key)
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Redis DELETE error for key '{key}': {e}")
            return False

    async def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate all keys matching a pattern.

        Args:
            pattern: Pattern to match (e.g., "myapp:user:123:*")

        Returns:
            Number of keys deleted, or 0 if Redis is disabled
        """
        if not self._enabled or not self._redis:
            return 0

        try:
            keys = []
            async for key in self._redis.scan_iter(match=pattern):
                keys.append(key)

            if keys:
                deleted = await self._redis.delete(*keys)
                logger.debug(
                    f"Invalidated {deleted} cache keys matching pattern: {pattern}"
                )
                return deleted
            return 0
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Redis INVALIDATE error for pattern '{pattern}': {e}")
            return 0

    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in Redis.

        Args:
            key: Cache key to check

        Returns:
            True if key exists, False otherwise or if Redis is disabled
        """
        if not self._enabled or not self._redis:
            return False

        try:
            result = await self._redis.exists(key)
            return bool(result)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Redis EXISTS error for key '{key}': {e}")
            return False

    async def expire(self, key: str, ttl: int) -> bool:
        """
        Set expiration time for a key.

        Args:
            key: Cache key
            ttl: Time to live in seconds

        Returns:
            True if successful, False otherwise or if Redis is disabled
        """
        if not self._enabled or not self._redis:
            return False

        try:
            await self._redis.expire(key, ttl)
            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Redis EXPIRE error for key '{key}': {e}")
            return False

    async def ttl(self, key: str) -> Optional[int]:
        """
        Get time to live for a key.

        Args:
            key: Cache key

        Returns:
            TTL in seconds, -1 if key has no expiration, -2 if key doesn't exist,
            or None if Redis is disabled
        """
        if not self._enabled or not self._redis:
            return None

        try:
            return await self._redis.ttl(key)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Redis TTL error for key '{key}': {e}")
            return None

    async def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """
        Increment a counter.

        Args:
            key: Cache key
            amount: Amount to increment by (default: 1)

        Returns:
            New value after increment, or None if Redis is disabled
        """
        if not self._enabled or not self._redis:
            return None

        try:
            return await self._redis.incrby(key, amount)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Redis INCREMENT error for key '{key}': {e}")
            return None

    async def decrement(self, key: str, amount: int = 1) -> Optional[int]:
        """
        Decrement a counter.

        Args:
            key: Cache key
            amount: Amount to decrement by (default: 1)

        Returns:
            New value after decrement, or None if Redis is disabled
        """
        if not self._enabled or not self._redis:
            return None

        try:
            return await self._redis.decrby(key, amount)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(f"Redis DECREMENT error for key '{key}': {e}")
            return None

    def is_connected(self) -> bool:
        """
        Check if Redis connection is active.

        Returns:
            True if connected, False otherwise
        """
        return self._redis is not None and self._enabled

    def is_enabled(self) -> bool:
        """
        Check if Redis cache is enabled for this service.

        Returns:
            True if enabled, False otherwise
        """
        return self._enabled

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


# Global Redis helper instance (singleton pattern)
_redis_helper_instance: Optional[RedisHelper] = None


def get_redis_helper() -> RedisHelper:
    """
    Get the global Redis helper instance (singleton).

    Returns:
        RedisHelper instance
    """
    global _redis_helper_instance  # pylint: disable=global-statement
    if _redis_helper_instance is None:
        _redis_helper_instance = RedisHelper()
    return _redis_helper_instance
