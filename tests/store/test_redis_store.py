"""RedisStore resilience tests using a fake client (no external Redis)."""

from __future__ import annotations

import threading

import pytest
from fakes import ConnectionError as FakeConnectionError
from fakes import FakeRedis
from fakes import TimeoutError as FakeTimeoutError

from diffsync.exceptions import StoreOperationError
from diffsync.store.contract import StoreState
from diffsync.store.redis_kv import RedisStore


def make_store(client, **kwargs):
    """Build a non-monitored RedisStore over a fake client."""
    defaults = {"namespace": "ns", "start_monitor": False, "name": "r"}
    defaults.update(kwargs)
    return RedisStore(client=client, **defaults)


def test_healthy_store_is_ready(redis_store, fake_redis):
    assert redis_store.ready() is True
    redis_store.set("k", {"v": 1})
    assert fake_redis.get("diffsync-kv:ns-default:k") is not None
    assert redis_store.get("k") == {"v": 1}
    assert redis_store.delete("k") is True
    assert redis_store.get("k") is None


def test_init_failure_starts_degraded_not_raising():
    client = FakeRedis()
    client.disconnect()
    store = make_store(client)
    assert store.ready() is False
    assert store.state is StoreState.DEGRADED
    assert store.status()["last_event"]["reason"] == "initial-connection-failed"
    store.close()


def test_timeout_on_write_degrades_and_keeps_serving_from_memory(fake_redis):
    store = make_store(fake_redis)
    fake_redis._check = lambda action: (_ for _ in ()).throw(FakeTimeoutError("boom"))  # noqa: SLF001
    store.set("k", 1)  # must not raise
    assert store.state is StoreState.DEGRADED
    assert store.get("k") == 1  # served from memory mirror
    assert store.status()["pending_writes"] == 1
    reason = store.status()["last_event"]["reason"]
    assert reason == "redis-set-failed"
    store.close()


def test_errors_are_observable_not_swallowed(fake_redis, caplog):
    store = make_store(fake_redis)
    failures = []

    class TrackingLogger:
        def error(self, event, **kwargs):
            failures.append((event, kwargs))

        def warning(self, *a, **k):  # noqa: ANN002
            pass

        def info(self, *a, **k):  # noqa: ANN002
            pass

        def debug(self, *a, **k):  # noqa: ANN002
            pass

        def bind(self, **k):  # noqa: ANN002
            return self

    store._log = TrackingLogger()  # noqa: SLF001
    fake_redis.disconnect()
    store.set("k", 1)
    assert failures and failures[0][0] == "redis-operation-failed"
    assert "set" in failures[0][1]["action"]
    store.close()


def test_ready_distinguishes_three_states(fake_redis):
    store = make_store(fake_redis)
    seen = []
    fake_redis.disconnect()
    store.set("a", 1)
    seen.append(store.state)

    def spy_pipeline():  # during replay the state must be RECOVERING
        seen.append(store.state)
        return FakeRedis.pipeline(fake_redis)

    fake_redis.pipeline = spy_pipeline  # type: ignore[method-assign]
    fake_redis.reconnect()
    assert store.attempt_recovery() is True
    seen.append(store.state)
    assert seen == [StoreState.DEGRADED, StoreState.RECOVERING, StoreState.READY]
    store.close()


def test_degraded_writes_converge_in_order_on_recovery(fake_redis):
    store = make_store(fake_redis)
    fake_redis.disconnect()

    # Interleave sets and deletes on the same key and monotonic counters per key.
    expected = {}
    for i in range(50):
        store.set("counter", i)
        expected["counter"] = i
        store.set(f"key-{i % 5}", i)
        expected[f"key-{i % 5}"] = i
    store.set("gone", 1)
    store.delete("gone")
    store.set("gone", 2)
    expected["gone"] = 2

    assert store.state is StoreState.DEGRADED
    assert store.status()["pending_writes"] == 103

    # Verify per-namespace ordering applied to Redis exactly as issued.
    fake_redis.reconnect()
    assert store.attempt_recovery() is True
    assert store.ready() is True
    for key, value in expected.items():
        assert store.get(key) == value
    assert store.get("gone") == 2
    assert store.status()["pending_writes"] == 0

    # Pipeline command order reflects WAL order for the shared key.
    counter_ops = [
        (command, args)
        for batch in fake_redis.pipeline_history
        for command, args in batch
        if args and args[0] == "diffsync-kv:ns:counter"
    ]
    assert [command for command, _ in counter_ops] == ["set"] * 50
    assert store.get("counter") == 49
    store.close()


def test_namespace_ordering_preserved_per_namespace(fake_redis):
    store = make_store(fake_redis)
    fake_redis.disconnect()
    ops_a = []
    for i in range(20):
        store.set("seq", i, namespace="a")
        ops_a.append(i)
    for i in range(20):
        store.set("seq", i, namespace="b")
    fake_redis.reconnect()
    store.attempt_recovery()

    seq_a = []
    for batch in fake_redis.pipeline_history:
        for command, args in batch:
            if args[0] == "diffsync-kv:a:seq":
                seq_a.append(command)
    # All writes for namespace "a" were SET and strictly earlier than any "b" writes.
    assert seq_a == ["set"] * 20
    commands_in_order = [(args[0], command) for batch in fake_redis.pipeline_history for command, args in batch]
    last_a = max(i for i, (key, _) in enumerate(commands_in_order) if key == "diffsync-kv:a:seq")
    first_b = min(i for i, (key, _) in enumerate(commands_in_order) if key == "diffsync-kv:b:seq")
    assert last_a < first_b
    assert store.get("seq", namespace="a") == 19
    store.close()


def test_concurrent_writes_during_degradation_do_not_lose_data(fake_redis):
    store = make_store(fake_redis)
    fake_redis.disconnect()
    thread_count = 8
    per_thread = 100
    barrier = threading.Barrier(thread_count)

    def worker(index):
        barrier.wait()
        for i in range(per_thread):
            store.set(f"t{index}-{i}", index * per_thread + i, namespace="orders")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert store.count(namespace="orders") == thread_count * per_thread
    fake_redis.reconnect()
    assert store.attempt_recovery() is True
    assert store.count(namespace="orders") == thread_count * per_thread
    assert store.get("t0-0", namespace="orders") == 0
    store.close()


def test_failed_recovery_stays_degraded_and_keeps_wal(fake_redis):
    store = make_store(fake_redis)
    fake_redis.disconnect()
    store.set("k", 1)
    assert store.attempt_recovery() is False
    assert store.state is StoreState.DEGRADED
    assert store.status()["pending_writes"] == 1
    fake_redis.reconnect()
    assert store.attempt_recovery() is True
    assert store.get("k") == 1
    store.close()


def test_background_monitor_auto_failback(fake_redis):
    store = RedisStore(client=fake_redis, namespace="ns", start_monitor=True, monitor_interval=0.01, name="mon")
    fake_redis.disconnect()
    store.set("k", "v")
    assert store.state is StoreState.DEGRADED
    fake_redis.reconnect()
    deadline = threading.Event()
    deadline.wait(1.0)
    for _ in range(100):
        if store.ready():
            break
        deadline.wait(0.02)
    assert store.ready() is True
    assert store.get("k") == "v"
    store.close()


def test_requires_url_host_or_client():
    with pytest.raises(StoreOperationError):
        RedisStore(start_monitor=False)


def test_recoverable_error_types_include_connection_and_timeout():
    from diffsync.store.redis_kv import _redis_error_types

    types = _redis_error_types()
    assert issubclass(FakeConnectionError, types)
    assert issubclass(FakeTimeoutError, types)
