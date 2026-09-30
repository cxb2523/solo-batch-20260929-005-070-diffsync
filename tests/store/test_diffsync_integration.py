"""Integration of the unified store contract with the DiffSync adapter."""

from __future__ import annotations

from typing import ClassVar

import pytest
from fakes import FakeRedis

from diffsync import Adapter, DiffSyncModel
from diffsync.store.adapter import ModelStoreAdapter
from diffsync.store.contract import create_store, live_stores
from diffsync.store.kv import LocalStore as KVLocalStore
from diffsync.store.redis_kv import RedisStore


class Site(DiffSyncModel):
    """Minimal site model."""

    _modelname = "site"
    _identifiers = ("name",)
    _attributes: ClassVar = ("role",)
    name: str
    role: str = "default"


class Device(DiffSyncModel):
    """Minimal device model nested under a site namespace."""

    _modelname = "device"
    _identifiers = ("name",)
    _attributes: ClassVar = ("site_name",)
    name: str
    site_name: str = "site1"


class DemoAdapter(Adapter):
    """Concrete adapter for contract integration tests."""

    site = Site
    device = Device
    top_level: ClassVar = ["site"]


def test_adapter_accepts_store_name():
    adapter = DemoAdapter(internal_storage_engine="local")
    assert isinstance(adapter.store, ModelStoreAdapter)
    assert isinstance(adapter.store.kv, KVLocalStore)
    assert adapter.store.ready() is True


def test_adapter_accepts_store_instance():
    kv = create_store("local", namespace="tenant", name="tenant-kv")
    adapter = DemoAdapter(internal_storage_engine=kv)
    assert adapter.store.kv is kv
    assert adapter.store.kv.adapter is adapter


def test_adapter_model_data_roundtrips_through_contract_store():
    adapter = DemoAdapter(internal_storage_engine="local")
    adapter.add(Site(name="paris", role="dc"))
    adapter.add(Site(name="rome", role="edge"))
    adapter.add(Device(name="rtr-1", site_name="paris"))

    assert len(adapter) == 3
    assert adapter.get(Site, "paris").role == "dc"
    assert {site.name for site in adapter.get_all(Site)} == {"paris", "rome"}
    assert adapter.store.count(model="site") == 2
    assert adapter.store.get_all_model_names() == {"site", "device"}

    # Storage really lives under the per-model namespaces of the contract store.
    kv = adapter.store.kv
    assert kv.keys(namespace="site") == ["paris", "rome"]
    assert kv.get("paris", namespace="site")["role"] == "dc"

    adapter.remove(adapter.get(Device, "rtr-1"))
    assert adapter.store.count(model="device") == 0


def test_adapter_add_twice_with_different_object_rejected():
    adapter = DemoAdapter(internal_storage_engine="local")
    adapter.add(Site(name="paris", role="a"))
    from diffsync.exceptions import ObjectAlreadyExists

    with pytest.raises(ObjectAlreadyExists):
        adapter.add(Site(name="paris", role="b"))


def test_redis_backend_powers_adapter_through_degradation():
    client = FakeRedis()
    store = RedisStore(client=client, namespace="demo", start_monitor=False, name="demo-redis")
    adapter = DemoAdapter(internal_storage_engine=store)
    adapter.add(Site(name="paris", role="dc"))

    client.disconnect()
    # New writes while degraded still work through the memory fallback.
    adapter.add(Site(name="rome", role="edge"))
    assert adapter.store.ready() is False
    assert adapter.get(Site, "rome").role == "edge"

    client.reconnect()
    assert store.attempt_recovery() is True
    assert adapter.store.ready() is True
    assert adapter.get(Site, "paris").role == "dc"
    assert adapter.get(Site, "rome").role == "edge"
    assert store.count(namespace="site") == 2
    store.close()


def test_live_stores_registry_tracks_instances():
    store = create_store("local", namespace="tracked", name="tracked-store")
    assert store in live_stores()
    store.close()
    assert store not in live_stores()
