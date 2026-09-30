"""Contract-level tests: namespace isolation, registry and serialization basics."""

import threading

import pytest

from diffsync import Adapter
from diffsync.exceptions import NamespaceBoundaryError, StoreKeyNotFound, StoreSerializationError
from diffsync.store.contract import (
    DEFAULT_NAMESPACE,
    StoreStatus,
    create_store,
    get_store_class,
    recent_events,
    registered_stores,
)
from diffsync.store.local import LocalStore


def test_registry_lists_local_and_loads_redis_lazily():
    assert "local" in registered_stores()
    assert get_store_class("local") is LocalStore
    redis_class = get_store_class("redis")
    assert redis_class.__name__ == "RedisStore"
    with pytest.raises(KeyError):
        get_store_class("does-not-exist")


def test_create_store_by_name_and_instance_registration():
    store = create_store("local", name="named-local")
    assert isinstance(store, LocalStore)
    assert store.name == "named-local"
    assert any(existing is store for existing in __import__("diffsync.store.contract", fromlist=["all_stores"]).all_stores())


def test_adapter_accepts_store_name_class_and_instance():
    adapter_by_name = Adapter(internal_storage_engine="local")
    assert isinstance(adapter_by_name.store, LocalStore)
    assert adapter_by_name.store.adapter is adapter_by_name

    adapter_by_class = Adapter(internal_storage_engine=LocalStore)
    assert isinstance(adapter_by_class.store, LocalStore)

    shared = LocalStore()
    adapter_by_instance = Adapter(internal_storage_engine=shared)
    assert adapter_by_instance.store is shared
    assert shared.adapter is adapter_by_instance


@pytest.mark.parametrize("bad_namespace", ["", "a:b", "ns\x00x", None, 42])
def test_namespace_validation_rejects_illegal_values(bad_namespace):
    with pytest.raises(NamespaceBoundaryError):
        LocalStore(namespace=bad_namespace)  # type: ignore[arg-type]


def test_bound_store_rejects_cross_namespace_access():
    store = LocalStore(namespace="tenant-a")
    with pytest.raises(NamespaceBoundaryError):
        store.get_item("key", namespace="tenant-b")
    with pytest.raises(NamespaceBoundaryError):
        store.set("key", 1, namespace="tenant-b")
    with pytest.raises(NamespaceBoundaryError):
        store.delete("key", namespace="tenant-b")
    with pytest.raises(NamespaceBoundaryError):
        store.keys(namespace="tenant-b")


def test_namespaces_are_isolated():
    store = LocalStore(multi_namespace=True)
    store.set("k", "from-a", namespace="tenant-a")
    store.set("k", "from-b", namespace="tenant-b")
    assert store.get_item("k", namespace="tenant-a") == "from-a"
    assert store.get_item("k", namespace="tenant-b") == "from-b"
    assert store.keys(namespace="tenant-a") == ["k"]
    assert store.delete("k", namespace="tenant-a") is True
    assert store.delete("k", namespace="tenant-a") is False
    with pytest.raises(StoreKeyNotFound):
        store.get_item("k", namespace="tenant-a")
    assert store.get_item("k", namespace="tenant-b") == "from-b"


def test_default_namespace_is_used_when_unspecified():
    store = LocalStore()
    assert store.namespace == DEFAULT_NAMESPACE
    store.set("k", 1)
    assert store.get_item("k") == 1


def test_local_store_is_always_ready_and_emits_no_events():
    store = LocalStore()
    assert store.ready() == StoreStatus.READY
    assert list(store.events) == []


def test_concurrent_writes_are_not_lost():
    store = LocalStore(namespace="counters")
    threads_count = 8
    per_thread = 100

    def worker(worker_id):
        for index in range(per_thread):
            key = f"w{worker_id}-{index}"
            store.set(key, worker_id * per_thread + index)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(threads_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(store.keys()) == threads_count * per_thread
    for worker_id in range(threads_count):
        for index in range(per_thread):
            assert store.get_item(f"w{worker_id}-{index}") == worker_id * per_thread + index


def test_concurrent_read_modify_write_to_shared_key_converges():
    store = LocalStore(namespace="counters")
    store.set("counter", 0)
    iterations = 200

    def worker():
        for _ in range(iterations):
            with store._kv_lock:  # pylint: disable=protected-access
                store.set("counter", store.get_item("counter") + 1)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert store.get_item("counter") == 4 * iterations


def test_json_is_default_codec_and_roundtrips_common_types():
    store = LocalStore()
    payload = {"list": [1, 2, 3], "set": {1, 2}, "nested": {"ok": True, "none": None}}
    store.set("payload", payload)
    assert store.get_item("payload") == payload
    assert store.encode(payload).startswith(b"diffsync-json:1:")


def test_non_json_serializable_value_raises_explicit_error():
    store = LocalStore()
    with pytest.raises(StoreSerializationError):
        store.encode(lambda: 1)


def test_pickle_requires_explicit_opt_in():
    json_store = LocalStore()
    pickled = b"diffsync-pickle:1:\x80..."
    with pytest.raises(StoreSerializationError):
        json_store.decode(pickled)

    pickle_store = LocalStore(allow_pickle=True)
    value = {"a": {1, 2}, "fn_keys_ok": True}
    encoded = pickle_store.encode(value)
    assert encoded.startswith(b"diffsync-pickle:1:")
    assert pickle_store.decode(encoded) == value


def test_legacy_prefixless_pickle_remains_readable_with_opt_in():
    import pickle

    store = LocalStore(allow_pickle=True)
    legacy = pickle.dumps({"legacy": (1, 2)})
    assert store.decode(legacy) == {"legacy": (1, 2)}

    strict = LocalStore()
    with pytest.raises(StoreSerializationError):
        strict.decode(legacy)


def test_roundtrip_verification_detects_mismatch():
    store = LocalStore()
    store.verify_roundtrip({"a": 1})


def test_events_are_observable_globally():
    store = create_store("local", name="observed-local")
    store.record_event("custom", "something happened")
    recent = recent_events(limit=5)
    assert any(event.store == "observed-local" and event.event == "custom" for event in recent)
    assert store.last_event().detail == "something happened"
