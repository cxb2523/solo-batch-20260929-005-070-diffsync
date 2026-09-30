"""Contract tests: namespace isolation, registry, basic CRUD semantics."""

from __future__ import annotations

import threading

import pytest

from diffsync.exceptions import BackendNotFound, NamespaceViolation
from diffsync.store import adapter as adapter_module
from diffsync.store.contract import (
    StoreState,
    create_store,
    get_store_class,
    is_within_namespace,
    register_store,
    registered_stores,
    validate_namespace,
)
from diffsync.store.kv import LocalStore


def test_backend_registry_lists_local_and_redis():
    assert "local" in registered_stores()
    assert "redis" in registered_stores()
    assert get_store_class("local") is LocalStore


def test_create_store_by_name_and_instance():
    store = create_store("local", namespace="a/b")
    assert isinstance(store, LocalStore)
    assert store.namespace == "a/b"
    assert create_store(store) is store


def test_create_unknown_store_raises():
    with pytest.raises(BackendNotFound):
        create_store("does-not-exist")


def test_duplicate_registration_requires_override():
    class Dummy(LocalStore):
        pass

    with pytest.raises(ValueError):
        register_store("local", Dummy)
    register_store("dummy", Dummy)


@pytest.mark.parametrize(
    "namespace",
    ["", "a//b", "/a", "a/", ".", "..", "a/../b", "a/b/..", "a b", "a$b", "a\x00b"],
)
def test_invalid_namespaces_rejected(namespace):
    with pytest.raises(NamespaceViolation):
        validate_namespace(namespace)


def test_namespace_boundary_rejects_cross_root(local_store):
    store = LocalStore(namespace="tenant-a/site", namespace_root="tenant-a")
    store.set("k1", 1)  # within root
    assert store.get("k1") == 1
    with pytest.raises(NamespaceViolation):
        store.set("k", 1, namespace="tenant-b/site")
    with pytest.raises(NamespaceViolation):
        store.set("k", 1, namespace="tenant-ab")
    with pytest.raises(NamespaceViolation):
        LocalStore(namespace="tenant-b", namespace_root="tenant-a")


def test_is_within_namespace_helper():
    assert is_within_namespace("a", "a")
    assert is_within_namespace("a", "a/b")
    assert not is_within_namespace("a", "ab")


def test_namespaces_are_isolated(local_store):
    local_store.set("shared", 1, namespace="tenant-a")
    local_store.set("shared", 2, namespace="tenant-b")
    assert local_store.get("shared", namespace="tenant-a") == 1
    assert local_store.get("shared", namespace="tenant-b") == 2
    assert local_store.keys(namespace="tenant-a") == ["shared"]
    assert local_store.count(namespace="tenant-b") == 1
    local_store.delete("shared", namespace="tenant-a")
    assert local_store.get("shared", default=None, namespace="tenant-a") is None
    assert local_store.get("shared", namespace="tenant-b") == 2


@pytest.mark.parametrize("bad_key", ["", "a/b", "a/../b", ".", "..", "a\x00b"])
def test_keys_cannot_cross_namespace(local_store, bad_key):
    with pytest.raises(NamespaceViolation):
        local_store.set(bad_key, 1)


def test_ready_is_true_for_local_store(local_store):
    assert local_store.ready() is True
    assert local_store.state is StoreState.READY
    status = local_store.status()
    assert status["state"] == "ready"
    assert status["ready"] is True


def test_local_concurrent_writes_do_not_lose_data():
    store = create_store("local", namespace="concurrent", name="concurrent-local")
    thread_count = 8
    per_thread = 200
    barrier = threading.Barrier(thread_count)

    def worker(index):
        barrier.wait()
        for i in range(per_thread):
            store.set(f"t{index}-{i}", index * per_thread + i, namespace=f"ns{index % 4}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    total = sum(store.count(namespace=f"ns{n}") for n in range(4))
    assert total == thread_count * per_thread
    assert store.get("t0-0", namespace="ns0") == 0
    assert (
        store.get(f"t{thread_count - 1}-{per_thread - 1}", namespace=f"ns{(thread_count - 1) % 4}")
        == (thread_count - 1) * per_thread + per_thread - 1
    )


def test_adapter_registration_uses_contract_store(monkeypatch):
    # The model-oriented bridge must wrap the contract Store, not bypass it.
    sentinel = LocalStore(namespace="rooted")
    created = {}
    original = adapter_module.create_store

    def spy(backend, **kwargs):
        result = original(backend, **kwargs)
        created["result"] = result
        return result

    monkeypatch.setattr(adapter_module, "create_store", spy)
    bridge = adapter_module.ModelStoreAdapter("local", namespace_root="rooted")
    assert bridge.kv is created["result"]
    assert bridge.kv.namespace_root == "rooted"
    assert sentinel is not None  # keep fixture usage obvious
