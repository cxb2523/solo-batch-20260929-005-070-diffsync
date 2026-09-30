"""RedisStore resilience tests using an in-process Redis double (no external dependency)."""

import threading
import time

import pytest

from diffsync.exceptions import NamespaceBoundaryError, StoreBackendUnavailable, StoreKeyNotFound
from diffsync.store.contract import StoreStatus
from diffsync.store.redis import RedisStore
from tests.store.conftest import FakeRedis


def test_unavailable_redis_degrades_explicitly(make_redis_store):
    client = FakeRedis(fail_ping=True, fail_ops=True)
    store = make_redis_store(client=client, namespace="ns")
    assert store.ready() == StoreStatus.DEGRADED
    events = [event.event for event in store.events]
    assert "degraded" in events
    last = store.events[-1]
    assert "memory" in last.detail or "Redis" in last.detail


def test_unavailable_redis_raises_when_auto_degrade_disabled():
    client = FakeRedis(fail_ping=True, fail_ops=True)
    with pytest.raises(StoreBackendUnavailable):
        RedisStore(client=client, auto_degrade=False, monitor=False, namespace="ns")


def test_healthy_flow_roundtrips_json(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="ns")
    store.set("a", {"v": 1})
    assert store.get("a") == {"v": 1}
    assert store.keys() == ["a"]
    stored = fake_redis.data[b"diffsync:test-store:kv:ns:a"]
    assert stored.startswith(b"diffsync-json:1:")


def test_timeout_on_write_degrades_and_serves_from_memory(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="ns")
    fake_redis.configure(fail_ops=True)
    store.set("queued", {"v": 2})
    assert store.ready() == StoreStatus.DEGRADED
    assert store.get("queued") == {"v": 2}
    actions = [event.event for event in store.events]
    assert "degraded" in actions


def test_get_failure_falls_back_to_mirror(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="ns")
    store.set("mirrored", {"v": 3})
    fake_redis.fail()
    assert store.get("mirrored") == {"v": 3}
    assert store.ready() == StoreStatus.DEGRADED


def test_get_missing_key_raises(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="ns")
    with pytest.raises(StoreKeyNotFound):
        store.get("missing")


def test_recovery_drains_pending_writes_in_order(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="ns")
    fake_redis.fail()
    # First write enters degradation; subsequent ones queue directly.
    store.set("seq", 0)
    for index in range(1, 6):
        store.set("seq", index)
    store.delete("gone")
    store.set("other", "final")

    assert store.ready() == StoreStatus.DEGRADED
    assert store.get("seq") == 5

    fake_redis.heal()
    assert store.recover() is True
    assert store.ready() == StoreStatus.READY

    # The final state in Redis reflects the last queued write for each key.
    assert store.decode(fake_redis.data[b"diffsync:test-store:kv:ns:seq"]) == 5
    assert store.decode(fake_redis.data[b"diffsync:test-store:kv:ns:other"]) == "final"
    assert b"diffsync:test-store:kv:ns:gone" not in fake_redis.data
    assert store.get("seq") == 5


def test_pending_writes_converge_in_enqueue_order(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="ns")
    fake_redis.fail()
    for index in range(50):
        store.set(f"key-{index}", index)

    fake_redis.heal()
    assert store.recover() is True

    for index in range(50):
        raw = fake_redis.data[f"diffsync:test-store:kv:ns:key-{index}".encode()]
        assert store.decode(raw) == index


def test_recovery_enters_recovering_state_observably(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="ns")
    fake_redis.fail()
    store.set("k", 1)

    seen = []
    real_exec = store._exec_write  # pylint: disable=protected-access

    def spying_exec(op, payload):  # pylint: disable=protected-access
        seen.append(store.ready())
        return real_exec(op, payload)

    fake_redis.heal()
    store._exec_write = spying_exec  # pylint: disable=protected-access
    assert store.recover() is True
    assert StoreStatus.RECOVERING in seen
    event_kinds = [event.event for event in store.events]
    assert "recovering" in event_kinds and "recovered" in event_kinds


def test_recovery_failure_returns_to_degraded_and_retries(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="ns")
    fake_redis.fail()
    store.set("k", 1)

    # Ping succeeds but commands still fail: recovery must abort back to degraded and re-queue.
    fake_redis.configure(fail_ping=False, fail_ops=True)
    assert store.recover() is False
    assert store.ready() == StoreStatus.DEGRADED
    assert pending(store) >= 1

    fake_redis.heal()
    assert store.recover() is True
    assert store.get("k") == 1


def pending(store):
    return sum(len(ops) for ops in store._pending.values())  # pylint: disable=protected-access


def test_background_monitor_auto_recovers(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="ns", monitor=True, health_interval=0.05)
    fake_redis.fail()
    store.set("auto", "queued")
    assert store.ready() == StoreStatus.DEGRADED

    fake_redis.heal()
    deadline = time.time() + 3
    while time.time() < deadline and store.ready() != StoreStatus.READY:
        time.sleep(0.02)
    assert store.ready() == StoreStatus.READY
    assert store.get("auto") == "queued"


def test_concurrent_degraded_writes_converge_after_recovery(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="ns")
    fake_redis.fail()

    threads_count = 8
    per_thread = 50
    barrier = threading.Barrier(threads_count)

    def worker(worker_id):
        barrier.wait()
        for index in range(per_thread):
            store.set(f"w{worker_id}-{index}", worker_id * per_thread + index)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(threads_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(store.keys()) == threads_count * per_thread

    fake_redis.heal()
    assert store.recover() is True
    assert store.ready() == StoreStatus.READY

    for worker_id in range(threads_count):
        for index in range(per_thread):
            assert store.get(f"w{worker_id}-{index}") == worker_id * per_thread + index


def test_namespace_boundary_enforced_on_redis_store(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="tenant-a")
    with pytest.raises(NamespaceBoundaryError):
        store.get("k", namespace="tenant-b")
    with pytest.raises(NamespaceBoundaryError):
        store.set("k", 1, namespace="tenant-b")
    with pytest.raises(NamespaceBoundaryError):
        store.delete("k", namespace="tenant-b")
    with pytest.raises(NamespaceBoundaryError):
        store.keys(namespace="tenant-b")


def test_same_key_in_different_stores_is_isolated(make_redis_store, fake_redis):
    store_a = make_redis_store(client=fake_redis, namespace="a", store_id="iso")
    store_b = make_redis_store(client=fake_redis, namespace="b", store_id="iso")
    store_a.set("k", "A")
    store_b.set("k", "B")
    assert store_a.get("k") == "A"
    assert store_b.get("k") == "B"


def test_failure_is_not_swallowed(make_redis_store, fake_redis, caplog):
    store = make_redis_store(client=fake_redis, namespace="ns")
    fake_redis.fail()
    store.set("k", 1)
    details = " ".join(event.detail for event in store.events if event.event == "degraded")
    assert "timeout" in details or "failure" in details
    assert store.last_event() is not None
