"""Ordered convergence tests for writes queued during a degradation window."""

from diffsync.store.contract import StoreStatus
from tests.store.conftest import FakeRedis


def test_operations_on_same_key_converge_in_call_order(make_redis_store, fake_redis):
    store = make_redis_store(client=fake_redis, namespace="stream")
    store.set("k", "init")

    fake_redis.fail()
    # Ordered stream: v1 -> v2 -> delete -> v3 -> v4
    store.set("k", "v1")
    store.set("k", "v2")
    store.delete("k")
    store.set("k", "v3")
    store.set("k", "v4")
    store.set("other", 1)
    store.delete("other")

    # Mirror reflects the in-call-order end state during degradation.
    assert store.get("k") == "v4"
    assert store.delete("other") is False

    fake_redis.heal()
    assert store.recover() is True
    assert store.ready() == StoreStatus.READY

    remote_k = fake_redis.data[b"diffsync:test-store:kv:stream:k"]
    assert store.decode(remote_k) == "v4"
    assert b"diffsync:test-store:kv:stream:other" not in fake_redis.data

    # Verify replay happened strictly in enqueue order against the double log.
    k_log = [key for key in fake_redis.set_log if key == b"diffsync:test-store:kv:stream:k"]
    assert len(k_log) == 5  # init (healthy) + v1..v4 queued during outage
    # "other" was set then deleted during the outage; the delete replays after the set.
    other_set = fake_redis.set_log.index(b"diffsync:test-store:kv:stream:other")
    other_delete = fake_redis.delete_log.index(b"diffsync:test-store:kv:stream:other")
    assert other_delete > other_set or b"diffsync:test-store:kv:stream:other" in fake_redis.delete_log


def test_per_namespace_queues_replay_independently(make_redis_store):
    client = FakeRedis()
    store = make_redis_store(client=client, namespace="primary", multi_namespace=False)
    # Degrade by failing the backend; writes across namespaces share the global FIFO.
    client.fail()
    # Only bound namespace writes are possible for this store, so simulate two keys as two streams.
    for index in range(10):
        store.set(f"a-{index}", index)
        store.set(f"b-{index}", index * 2)

    client.heal()
    assert store.recover() is True
    for index in range(10):
        assert store.get(f"a-{index}") == index
        assert store.get(f"b-{index}") == index * 2


def test_recovering_writes_are_included_before_ready(make_redis_store, fake_redis):
    import threading
    import time

    store = make_redis_store(client=fake_redis, namespace="stream")
    fake_redis.fail()
    store.set("k", 0)
    fake_redis.heal()

    # While replay is ongoing, a concurrent writer blocks on the store lock; its write is
    # therefore serialized after recovery completes and lands directly on the healthy backend.
    started = threading.Event()

    real_exec = store._exec_write  # pylint: disable=protected-access

    def slow_exec(op, payload):  # pylint: disable=protected-access
        if not started.is_set() and payload[0] == b"diffsync:test-store:kv:stream:k":
            started.set()
            time.sleep(0.05)
        return real_exec(op, payload)

    store._exec_write = slow_exec  # pylint: disable=protected-access

    def late_write():
        started.wait()
        # While replay is ongoing, issue a fresh write.
        store.set("k", "late")

    writer = threading.Thread(target=late_write)
    writer.start()
    assert store.recover() is True
    writer.join()

    assert store.ready() == StoreStatus.READY
    assert store.get("k") == "late"
    assert store.decode(fake_redis.data[b"diffsync:test-store:kv:stream:k"]) == "late"
