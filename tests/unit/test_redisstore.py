"""Testing of RedisStore.

These tests use an in-process redis-compatible double (see tests/store/conftest.py) so that no
external redis-server is required.
"""

import pickle

import pytest

from diffsync.exceptions import StoreBackendUnavailable
from diffsync.store.contract import StoreStatus
from diffsync.store.redis import RedisStore
from tests.store.conftest import FakeRedis


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def make_site():
    from tests.store.conftest import Site

    def _site(name="site1", **kwargs):
        return Site(name=name, **kwargs)

    return _site


@pytest.fixture
def make_device():
    from tests.store.conftest import Device

    def _device(name="device1", role="leaf", **kwargs):
        return Device(name=name, role=role, **kwargs)

    return _device


def _double(**kwargs):
    return FakeRedis(**kwargs)


def test_redisstore_init():
    store = RedisStore(client=_double(), name="mystore", store_id="123", monitor=False)
    assert str(store) == "mystore (123)"
    assert store.ready() == StoreStatus.READY


def test_redisstore_init_wrong_defaults_to_degraded():
    # No reachable backend: the store degrades explicitly instead of raising.
    store = RedisStore(client=_double(fail_ping=True, fail_ops=True), name="mystore", store_id="123", monitor=False)
    assert store.ready() == StoreStatus.DEGRADED


def test_redisstore_init_wrong_strict_raises():
    with pytest.raises(StoreBackendUnavailable):
        RedisStore(client=_double(fail_ping=True, fail_ops=True), store_id="123", auto_degrade=False, monitor=False)


def test_redisstore_init_url_and_host_conflict():
    with pytest.raises(ValueError):
        RedisStore(url="redis://x", host="y")


def test_redisstore_add_obj(fake_redis, make_site):
    store = RedisStore(client=fake_redis, name="mystore", store_id="123", monitor=False)
    site = make_site()
    store.add(obj=site)
    assert store.count() == 1


def test_redisstore_add_obj_twice(fake_redis, make_site):
    store = RedisStore(client=fake_redis, name="mystore", store_id="123", monitor=False)
    site = make_site()
    store.add(obj=site)
    store.add(obj=site)
    assert store.count() == 1


def test_redisstore_get_all_obj(fake_redis, make_site):
    store = RedisStore(client=fake_redis, name="mystore", store_id="123", monitor=False)
    site = make_site()
    store.add(obj=site)
    assert store.get_all(model=site.__class__)[0] == site


def test_redisstore_get_obj(fake_redis, make_site):
    store = RedisStore(client=fake_redis, name="mystore", store_id="123", monitor=False)
    site = make_site()
    store.add(obj=site)
    assert store.get(model=site.__class__, identifier=site.name) == site


def test_redisstore_remove_obj(fake_redis, make_site):
    store = RedisStore(client=fake_redis, name="mystore", store_id="123", monitor=False)
    site = make_site()
    store.add(obj=site)
    assert store.count(model=site.__class__) == store.count() == 1
    store.remove(obj=site)
    assert store.count(model=site.__class__) == store.count() == 0


def test_redisstore_get_all_model_names(fake_redis, make_site, make_device):
    store = RedisStore(client=fake_redis, name="mystore", store_id="123", monitor=False)
    site = make_site()
    store.add(obj=site)
    device = make_device()
    store.add(obj=device)
    assert site.get_type() in store.get_all_model_names()
    assert device.get_type() in store.get_all_model_names()


def test_redisstore_reads_legacy_pickled_objects(fake_redis, make_site):
    store = RedisStore(client=fake_redis, name="mystore", store_id="123", monitor=False)
    site = make_site()
    site.adapter = None
    # Simulate data written by an older DiffSync release (no codec prefix).
    fake_redis.data[b"diffsync:123:site:site1"] = pickle.dumps(site)
    fetched = store.get(model=site.__class__, identifier="site1")
    assert fetched.name == site.name
