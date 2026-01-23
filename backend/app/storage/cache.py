"""Redis-backed cache helpers for low-latency API responses."""
import json
import logging
from dataclasses import dataclass
from typing import Any, Optional

from redis import Redis
from redis.exceptions import RedisError

from app.core.config import Settings

logger = logging.getLogger(__name__)


class BaseCache:
    """Minimal cache interface used by API endpoints."""

    def get_json(self, key: str) -> Optional[Any]:
        raise NotImplementedError

    def set_json(self, key: str, payload: Any, ttl_seconds: Optional[int] = None) -> bool:
        raise NotImplementedError


class NullCache(BaseCache):
    """No-op cache used when Redis is disabled or unavailable."""

    def get_json(self, key: str) -> Optional[Any]:
        return None

    def set_json(self, key: str, payload: Any, ttl_seconds: Optional[int] = None) -> bool:
        return False


@dataclass
class CacheStore(BaseCache):
    """Redis cache wrapper with JSON serialization."""

    client: Redis
    ttl_seconds: int
    prefix: str = "aml-ai:"

    def _key(self, key: str) -> str:
        return f"{self.prefix}{key}"

    def get_json(self, key: str) -> Optional[Any]:
        try:
            raw = self.client.get(self._key(key))
            if not raw:
                return None
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            return json.loads(raw)
        except (RedisError, json.JSONDecodeError) as exc:
            logger.warning("Cache read failed", extra={"key": key, "error": str(exc)})
            return None

    def set_json(self, key: str, payload: Any, ttl_seconds: Optional[int] = None) -> bool:
        ttl = ttl_seconds or self.ttl_seconds
        try:
            value = json.dumps(payload)
            return bool(self.client.setex(self._key(key), ttl, value))
        except (RedisError, TypeError) as exc:
            logger.warning("Cache write failed", extra={"key": key, "error": str(exc)})
            return False

    @classmethod
    def from_settings(cls, settings: Settings) -> BaseCache:
        if not settings.cache_enabled or not settings.redis_url:
            return NullCache()
        try:
            client = Redis.from_url(settings.redis_url, socket_timeout=2, socket_connect_timeout=2)
            client.ping()
        except RedisError as exc:
            logger.warning("Redis unavailable, caching disabled", extra={"error": str(exc)})
            return NullCache()
        return cls(client=client, ttl_seconds=settings.cache_ttl_seconds, prefix=settings.cache_prefix)
