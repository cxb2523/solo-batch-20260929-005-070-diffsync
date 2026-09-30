"""Tests for the ``store_status`` monitoring command."""



from diffsync.store import status as status_module
from diffsync.store.contract import StoreStatus
from diffsync.store.local import LocalStore
from tests.store.conftest import FakeRedis


def test_render_snapshot_lists_store_and_state():
    local = LocalStore(name="status-local")
    local.set("k", 1)
    output = status_module.render_snapshot()
    assert "status-local" in output
    assert "LocalStore" in output
    assert "READY" in output
    assert "default" in output


def test_render_snapshot_shows_degraded_and_events(make_redis_store):
    client = FakeRedis(fail_ping=True, fail_ops=True)
    store = make_redis_store(client=client, name="degraded-demo", namespace="ns")
    assert store.ready() == StoreStatus.DEGRADED
    output = status_module.render_snapshot()
    assert "degraded-demo" in output
    assert "DEGRADED" in output
    assert "degraded" in output
    assert "Recent switch events" in output


def test_main_once_prints_snapshot(monkeypatch, capsys):
    LocalStore(name="once-local")
    monkeypatch.setattr(sys_argv(), "argv", ["store_status", "--once"])
    rc = status_module.main(["--once"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "DiffSync store status" in captured.out
    assert "once-local" in captured.out


def sys_argv():
    import sys

    return sys


def test_main_demo_once_runs_without_external_dependency(capsys):
    rc = status_module.main(["--demo", "--once"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "redis-demo" in captured.out
    assert "DEGRADED" in captured.out


def test_pending_count_reports_queued_writes(make_redis_store):
    client = FakeRedis()
    store = make_redis_store(client=client, name="pending-demo", namespace="ns")
    assert status_module.pending_count(store) == 0
    client.fail()
    store.set("a", 1)
    store.set("b", 2)
    assert status_module.pending_count(store) == 2


def test_events_render_with_timestamp():
    local = LocalStore(name="rendered")
    event = local.record_event("custom", "hello")
    rendered = event.render()
    assert "rendered" in rendered
    assert "custom" in rendered
    assert "hello" in rendered
