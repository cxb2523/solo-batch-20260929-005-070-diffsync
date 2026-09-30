"""Model-level RedisStore API tests (add/get/get_all/remove/count) using the double."""

import pickle

import pytest

from diffsync import Adapter
from diffsync.exceptions import ObjectAlreadyExists, ObjectNotFound
from diffsync.store.contract import StoreStatus
from diffsync.store.redis import MODEL_NAMESPACE, RedisStore


class SiteAdapter(Adapter):
    """Adapter exposing the Site model."""

    from tests.store.conftest import Site as site  # noqa: E402

    top_level = ["site"]


@pytest.fixture
def adapter_with_store(fake_redis):
    adapter = SiteAdapter(
        internal_storage_engine=RedisStore(client=fake_redis, store_id="models", monitor=False)
    )
    return adapter


def test_add_get_model_healthy(adapter_with_store, make_site):
    adapter = adapter_with_store
    site = make_site(name="paris", description="city")
    adapter.add(site)
    fetched = adapter.get(adapter.site, "paris")
    assert fetched == site
    assert adapter.store.count() == 1
    assert "site" in adapter.store.get_all_model_names()
    assert adapter.store.get_all(model="site")[0] == site


def test_add_twice_is_idempotent_for_same_object(adapter_with_store, make_site):
    adapter = adapter_with_store
    site = make_site(name="paris")
    adapter.add(site)
    adapter.add(site)
    assert adapter.store.count() == 1


def test_add_conflicting_object_raises(adapter_with_store, make_site):
    adapter = adapter_with_store
    adapter.add(make_site(name="paris", description="a"))
    with pytest.raises(ObjectAlreadyExists):
        adapter.add(make_site(name="paris", description="b"))


def test_remove_model(adapter_with_store, make_site):
    adapter = adapter_with_store
    adapter.add(make_site(name="paris"))
    adapter.remove(adapter.get(adapter.site, "paris"))
    assert adapter.store.count() == 0
    with pytest.raises(ObjectNotFound):
        adapter.get(adapter.site, "paris")


def test_model_operations_during_degradation(fake_redis, make_site, make_device):
    adapter = SiteAdapter(
        internal_storage_engine=RedisStore(client=fake_redis, store_id="deg", monitor=False)
    )
    adapter.add(make_site(name="up"))

    fake_redis.fail()

    site_down = make_site(name="down", description="written degraded")
    adapter.add(site_down)
    assert adapter.store.ready() == StoreStatus.DEGRADED
    assert adapter.get(adapter.site, "down") == site_down
    assert adapter.store.count(model="site") == 2

    # Writes during degradation are queued in the model replay namespace.
    pending = adapter.store._pending.get(MODEL_NAMESPACE)  # pylint: disable=protected-access
    assert pending is not None and len(pending) >= 1

    fake_redis.heal()
    assert adapter.store.recover() is True
    assert adapter.store.ready() == StoreStatus.READY
    assert adapter.get(adapter.site, "down") == site_down
    assert adapter.store.count(model="site") == 2


def test_legacy_prefixless_model_data_is_readable(fake_redis, make_site):
    store = RedisStore(client=fake_redis, store_id="oldmodels", monitor=False)
    adapter = SiteAdapter()
    adapter.store = store
    store.adapter = adapter
    site = make_site(name="ancient")
    site.adapter = None
    key = f"diffsync:oldmodels:site:{site.get_unique_id()}"
    fake_redis.data[key.encode()] = pickle.dumps(site)

    fetched = adapter.get(adapter.site, "ancient")
    assert fetched.get_unique_id() == site.get_unique_id()
    assert fetched.name == site.name
