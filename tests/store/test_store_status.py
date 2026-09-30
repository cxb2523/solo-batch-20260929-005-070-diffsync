"""Tests for the store_status monitoring command."""

from __future__ import annotations

import io

from fakes import FakeRedis

from diffsync.store import status as status_module
from diffsync.store.kv import LocalStore
from diffsync.store.redis_kv import RedisStore


def test_format_statuses_lists_backends_and_events():
    local = LocalStore(namespace="a", name="status-local")
    client = FakeRedis()
    client.disconnect()
    redis_store = RedisStore(client=client, namespace="b", start_monitor=False, name="status-redis")
    try:
        rendered = status_module.format_statuses([local, redis_store])
        assert "local:status-local" in rendered
        assert "[DEGRADED]" in rendered
        assert "status-redis" in rendered
        assert "initial-connection-failed" in rendered
        assert local.status()["state"] == "ready"
        assert "[OK]" in rendered
    finally:
        redis_store.close()
        local.close()


def test_watch_renders_n_iterations_without_clearing():
    local = LocalStore(namespace="w", name="watch-local")
    stream = io.StringIO()
    status_module.watch(interval=0, iterations=3, stream=stream, stores=[local], clear_screen=False)
    output = stream.getvalue()
    assert output.count("watch-local") == 3
    local.close()


def test_format_statuses_empty_registry():
    rendered = status_module.format_statuses(stores=[])
    assert "No stores registered" in rendered


def test_main_once(capsys):
    store = LocalStore(namespace="m", name="main-once")
    try:
        exit_code = status_module.main(["--once"])
        captured = capsys.readouterr()
        assert exit_code == 0
        assert "main-once" in captured.out
    finally:
        store.close()
