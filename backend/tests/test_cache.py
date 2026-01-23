"""Cache layer tests."""
import json

from app.storage.cache import CacheStore, NullCache


class DummyRedis:
    def __init__(self) -> None:
        self.store = {}

    def get(self, key: str):
        return self.store.get(key)

    def setex(self, key: str, ttl: int, value: str):
        self.store[key] = value
        return True

    def ping(self) -> bool:
        return True


def test_cache_store_round_trip():
    cache = CacheStore(client=DummyRedis(), ttl_seconds=30, prefix="test:")
    payload = {"items": [1, 2, 3], "pagination": {"total": 3}}
    assert cache.set_json("cases:list:1:0", payload)
    assert cache.get_json("cases:list:1:0") == payload


def test_cache_store_handles_invalid_json():
    client = DummyRedis()
    client.store["test:bad"] = "not-json"
    cache = CacheStore(client=client, ttl_seconds=30, prefix="test:")
    assert cache.get_json("bad") is None


def test_null_cache_is_noop():
    cache = NullCache()
    assert cache.get_json("anything") is None
    assert cache.set_json("anything", {"a": 1}) is False
