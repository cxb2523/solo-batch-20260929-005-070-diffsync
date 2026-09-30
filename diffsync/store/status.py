"""``store_status`` command: continuously report store backend status and recent switch events.

Designed for live observation and screen recording. It prints a refreshed table of every live
store instance (name, backend type, ready state, bound namespace, pending writes) followed by the
most recent status transition events.

Examples:
    python -m diffsync.store.status              # watch live stores, refresh every second
    python -m diffsync.store.status --interval 2 # refresh every 2 seconds
    python -m diffsync.store.status --once       # print a single snapshot and exit
    python -m diffsync.store.status --demo       # self-contained degraded/recovering/ready demo
"""

from __future__ import annotations

import argparse
import fnmatch
import sys
import threading
import time
from typing import Any, Dict, List, Optional

from diffsync.store.contract import (
    DEFAULT_NAMESPACE,
    StoreStatus,
    all_stores,
    recent_events,
)


def _clear_screen() -> None:
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()


def pending_count(store: Any) -> int:
    """Return the number of writes queued for replay on ``store`` (0 for non-resilient backends)."""
    pending = getattr(store, "_pending", None)
    if not pending:
        return 0
    return sum(len(ops) for ops in pending.values())


def render_snapshot(limit: int = 10) -> str:
    """Render a single status snapshot as a string."""
    lines: List[str] = []
    lines.append("DiffSync store status")
    lines.append("=" * 78)
    stores = all_stores()
    if not stores:
        lines.append("(no live store instances in this process)")
    else:
        header = f"{'Backend':<28}{'Type':<16}{'State':<12}{'Namespace':<14}{'Pending':>8}"
        lines.append(header)
        lines.append("-" * 78)
        for store in stores:
            state = store.ready()
            marker = {
                StoreStatus.READY: "READY",
                StoreStatus.DEGRADED: "DEGRADED",
                StoreStatus.RECOVERING: "RECOVERING",
            }.get(state, str(state))
            lines.append(
                f"{str(store):<28.28}{store.__class__.__name__:<16}{marker:<12}"
                f"{getattr(store, 'namespace', DEFAULT_NAMESPACE):<14}{pending_count(store):>8}"
            )
    lines.append("")
    lines.append(f"Recent switch events (last {limit}):")
    lines.append("-" * 78)
    events = recent_events(limit=limit)
    if not events:
        lines.append("(none)")
    else:
        for event in events:
            lines.append(event.render())
    lines.append("")
    return "\n".join(lines)


# -- self-contained demo ------------------------------------------------------------------------


class _DemoRedis:
    """Minimal redis-compatible double driving the --demo scenario, no external dependency."""

    def __init__(self) -> None:
        self.data: Dict[bytes, bytes] = {}
        self.lock = threading.Lock()
        self.available = True
        self._failing = False
        self._recovered_once = False

    def ping(self) -> bool:
        with self.lock:
            if self._failing:
                raise ConnectionError("simulated outage")
            return True

    def set(self, key: bytes, value: bytes) -> None:
        with self.lock:
            if self._failing:
                raise ConnectionError("simulated outage")
            self.data[key] = value

    def get(self, key: bytes) -> Optional[bytes]:
        with self.lock:
            if self._failing:
                raise ConnectionError("simulated outage")
            return self.data.get(key)

    def delete(self, key: bytes) -> Any:
        with self.lock:
            if self._failing:
                raise ConnectionError("simulated outage")
            return self.data.pop(key, None)

    def exists(self, key: bytes) -> int:
        with self.lock:
            if self._failing:
                raise ConnectionError("simulated outage")
            return 1 if key in self.data else 0

    def scan_iter(self, pattern: bytes) -> List[bytes]:
        with self.lock:
            if self._failing:
                raise ConnectionError("simulated outage")
            pat = pattern.decode()
            return [key for key in self.data if fnmatch.fnmatch(key.decode(), pat)]

    def fail(self) -> None:
        with self.lock:
            self._failing = True

    def heal(self) -> None:
        with self.lock:
            self._failing = False


def run_demo(once: bool = False, interval: float = 1.0) -> int:
    """Run a local + resilient redis (doubled) backend through a degrade/recover/recut cycle."""
    from diffsync.store.local import LocalStore  # pylint: disable=import-outside-toplevel
    from diffsync.store.redis import RedisStore  # pylint: disable=import-outside-toplevel

    local = LocalStore(name="local-default")
    local.set("greeting", {"hello": "world"})

    fake = _DemoRedis()
    redis_store = RedisStore(
        client=fake,
        store_id="demo",
        name="redis-demo",
        namespace="demo",
        monitor=False,
        health_interval=interval,
    )
    redis_store.set("counter", 0)

    if once:
        fake.fail()
        redis_store.set("counter", 1)
        try:
            redis_store.health_check()
        except Exception as exc:  # pylint: disable=broad-except
            # Expected: Redis is still failing in the scripted --once demo.
            sys.stderr.write(f"health check still failing in demo snapshot: {exc}\n")
        sys.stdout.write(render_snapshot())
        return 0

    sequence = [
        (1.0, "steady"),
        (1.0, "outage"),
        (3.0, "degraded_writes"),
        (1.0, "recover"),
        (2.0, "steady"),
        (1.0, "outage2"),
        (2.0, "degraded_writes"),
        (1.0, "recover"),
        (2.0, "steady"),
    ]
    counter = 0
    for _, phase in sequence:
        if phase in ("outage", "outage2"):
            fake.fail()
        if phase == "degraded_writes":
            for _ in range(3):
                counter += 1
                redis_store.set("counter", counter)
                _clear_screen()
                sys.stdout.write(render_snapshot())
                sys.stdout.write(f"\nphase: {phase} -- queued writes during outage\n")
                time.sleep(interval)
            continue
        if phase == "recover":
            fake.heal()
            redis_store.health_check()
        elif fake._failing:  # pylint: disable=protected-access
            counter += 1
            redis_store.set("counter", counter)
        _clear_screen()
        sys.stdout.write(render_snapshot())
        sys.stdout.write(f"\nphase: {phase}\n")
        time.sleep(interval)
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Show DiffSync store backend status.")
    parser.add_argument("--interval", type=float, default=1.0, help="refresh interval in seconds")
    parser.add_argument("--once", action="store_true", help="print a single snapshot and exit")
    parser.add_argument("--events", type=int, default=10, help="number of recent events to show")
    parser.add_argument("--demo", action="store_true", help="run a self-contained degrade/recover demo")
    args = parser.parse_args(argv)

    if args.demo:
        return run_demo(once=args.once, interval=args.interval)

    if args.once:
        sys.stdout.write(render_snapshot(limit=args.events))
        return 0

    try:
        while True:
            _clear_screen()
            sys.stdout.write(render_snapshot(limit=args.events))
            sys.stdout.write("\npress Ctrl+C to exit\n")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
