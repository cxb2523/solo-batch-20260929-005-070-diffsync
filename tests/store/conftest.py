"""Fixtures for the store contract tests: a scripted Redis double and minimal models."""

import threading
import time
from typing import ClassVar, Dict, List, Optional, Tuple

import pytest

try:  # redis is an optional dependency; align our double's errors with it when available
    from redis.exceptions import (
        ConnectionError as RedisConnectionError,
    )
    from redis.exceptions import (
        TimeoutError as RedisTimeoutError,
    )
except ImportError:  # pragma: no cover

    class RedisConnectionError(Exception):
        """Stand-in used when redis is not installed."""

    class RedisTimeoutError(Exception):
        """Stand-in used when redis is not installed."""

from diffsync import DiffSyncModel


class FakeRedisError(Exception):
    """Base error for scripted failures raised by the double."""


class FakeRedis:
    """Minimal, thread-safe redis-py compatible double with scriptable failure modes.

    No network or external process is involved.
    """

    def __init__(self, *, fail_ping: bool = False, fail_ops: bool = False, latency: float = 0.0):
        self.data: Dict[bytes, bytes] = {}
        self.lock = threading.RLock()
        self.fail_ping = fail_ping
        self.fail_ops = fail_ops
        self.latency = latency
        self.set_log: List[bytes] = []
        self.delete_log: List[bytes] = []
        self.blocker: Optional[threading.Event] = None
        self.block_on_set: Optional[bytes] = None
        self.ping_count = 0

    def configure(self, *, fail_ping=None, fail_ops=None, latency=None):
        with self.lock:
            if fail_ping is not None:
                self.fail_ping = fail_ping
            if fail_ops is not None:
                self.fail_ops = fail_ops
            if latency is not None:
                self.latency = latency

    def fail(self):
        self.configure(fail_ping=True, fail_ops=True)

    def heal(self):
        self.configure(fail_ping=False, fail_ops=False)

    def ping(self):
        self.ping_count += 1
        if self.fail_ping:
            raise RedisConnectionError("simulated connection failure")
        return True

    def _maybe_sleep(self):
        if self.latency:
            time.sleep(self.latency)

    def set(self, key, value):
        with self.lock:
            if self.blocker is not None and key == self.block_on_set:
                target = self.blocker
                self.lock.release()
                try:
                    target.wait(timeout=5)
                finally:
                    self.lock.acquire()
            self._maybe_sleep()
            if self.fail_ops:
                raise RedisTimeoutError("simulated command timeout")
            self.data[key] = value
            self.set_log.append(key)

    def get(self, key):
        with self.lock:
            self._maybe_sleep()
            if self.fail_ops:
                raise RedisTimeoutError("simulated command timeout")
            return self.data.get(key)

    def delete(self, key):
        with self.lock:
            self._maybe_sleep()
            if self.fail_ops:
                raise RedisConnectionError("simulated connection failure")
            self.delete_log.append(key)
            return self.data.pop(key, None)

    def exists(self, key):
        with self.lock:
            if self.fail_ops:
                raise RedisConnectionError("simulated connection failure")
            return 1 if key in self.data else 0

    def scan_iter(self, pattern):
        import fnmatch

        with self.lock:
            if self.fail_ops:
                raise RedisConnectionError("simulated connection failure")
            pat = pattern.decode() if isinstance(pattern, bytes) else pattern
            return [key for key in list(self.data.keys()) if fnmatch.fnmatch(key.decode(), pat)]


@pytest.fixture
def fake_redis():
    """Return a healthy redis double."""
    return FakeRedis()


@pytest.fixture
def make_redis_store():
    """Factory building a monitor-free RedisStore backed by a fresh double."""
    from diffsync.store.redis import RedisStore

    created = []

    def _make(client=None, **kwargs):
        kwargs.setdefault("store_id", "test-store")
        kwargs.setdefault("monitor", False)
        store = RedisStore(client=client if client is not None else FakeRedis(), **kwargs)
        created.append(store)
        return store

    yield _make

    for store in created:
        store.stop()


class Site(DiffSyncModel):
    """Minimal site model for model-level store tests."""

    _modelname = "site"
    _identifiers = ("name",)

    name: str
    description: Optional[str] = None


class Device(DiffSyncModel):
    """Minimal device model for model-level store tests."""

    _modelname = "device"
    _identifiers = ("name",)
    _attributes: ClassVar[Tuple[str, ...]] = ("role",)

    name: str
    role: str = "default"


@pytest.fixture
def make_site():
    """Factory for Site instances."""

    def _site(name="site1", **kwargs):
        return Site(name=name, **kwargs)

    return _site


@pytest.fixture
def make_device():
    """Factory for Device instances."""

    def _device(name="device1", role="leaf", **kwargs):
        return Device(name=name, role=role, **kwargs)

    return _device
