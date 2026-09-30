"""Serialization tests for the store contract: JSON default, pickle opt-in, legacy reads."""

import pickle

import pytest

from diffsync.exceptions import StoreSerializationError
from diffsync.store.redis import RedisStore
from tests.store.conftest import FakeRedis


def test_json_default_prefix_and_content(make_redis_store):
    store = make_redis_store(namespace="ns")
    encoded = store.encode({"a": [1, 2, 3], "b": True, "c": None})
    assert encoded.startswith(b"diffsync-json:1:")
    assert store.decode(encoded) == {"a": [1, 2, 3], "b": True, "c": None}


def test_json_extended_types_roundtrip(make_redis_store):
    import datetime

    store = make_redis_store(namespace="ns")
    value = {"set": {1, 2, 2}, "bytes": b"abc", "when": datetime.datetime(2024, 1, 2, 3, 4, 5)}
    encoded = store.encode(value)
    restored = store.decode(encoded)
    assert restored["set"] == {1, 2}
    assert restored["bytes"] == b"abc"
    assert restored["when"] == datetime.datetime(2024, 1, 2, 3, 4, 5)


def test_pickle_requires_explicit_opt_in_for_redis(fake_redis):
    strict = RedisStore(client=fake_redis, store_id="ser", monitor=False, namespace="ns", allow_pickle=False)
    pickled_value = {"x": {1, 2}}
    with pytest.raises(StoreSerializationError):
        strict.set("p", lambda: 1)

    enabled = RedisStore(client=FakeRedis(), store_id="ser2", monitor=False, namespace="ns", allow_pickle=True)
    encoded = enabled.encode(pickled_value)
    assert encoded.startswith(b"diffsync-pickle:1:")
    assert enabled.decode(encoded) == pickled_value


def test_roundtrip_verified_when_pickle_enabled():
    # allow_pickle=True triggers a roundtrip self-check at construction; ensure it accepts data.
    store = RedisStore(client=FakeRedis(), store_id="rt", monitor=False, namespace="ns", allow_pickle=True)
    store.verify_roundtrip({"ok": (1, "two", None)})


def test_legacy_prefixless_data_readable_when_pickle_enabled(make_site):
    client = FakeRedis()
    store = RedisStore(client=client, store_id="legacy", monitor=False, namespace="ns", allow_pickle=True)
    site = make_site(name="old")
    raw_legacy = pickle.dumps({"site": site.name})
    assert store.decode(raw_legacy) == {"site": "old"}


def test_legacy_prefixless_data_refused_without_opt_in():
    client = FakeRedis()
    store = RedisStore(client=client, store_id="strict", monitor=False, namespace="ns")
    with pytest.raises(StoreSerializationError):
        store.decode(pickle.dumps({"a": 1}))


def test_prefixed_pickle_refused_without_opt_in():
    store = RedisStore(client=FakeRedis(), store_id="p", monitor=False, namespace="ns")
    with pytest.raises(StoreSerializationError):
        store.decode(b"diffsync-pickle:1:garbage")


def test_corrupt_payload_raises_serialization_error():
    store = RedisStore(client=FakeRedis(), store_id="c", monitor=False, namespace="ns")
    with pytest.raises(StoreSerializationError):
        store.decode(b"diffsync-json:1:{not-json")
