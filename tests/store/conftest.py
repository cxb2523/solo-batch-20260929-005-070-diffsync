"""Shared fixtures for store contract tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make the fake Redis double importable without packaging it.
sys.path.insert(0, str(Path(__file__).parent))

from fakes import FakeRedis  # noqa: E402

from diffsync.store.kv import LocalStore  # noqa: E402
from diffsync.store.redis_kv import RedisStore  # noqa: E402


@pytest.fixture
def local_store():
    """Provide a fresh local store."""
    return LocalStore(namespace="ns-default", name="local-under-test")


@pytest.fixture
def fake_redis():
    """Provide a fresh fake Redis client."""
    return FakeRedis(url="redis://fake")


@pytest.fixture
def redis_store(fake_redis):
    """Provide a RedisStore backed by the fake client with no monitor thread."""
    yield RedisStore(
        client=fake_redis,
        namespace="ns-default",
        name="redis-under-test",
        start_monitor=False,
    )
