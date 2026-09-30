"""``store_status`` command: continuously report store backend health.

Renders one block per live (or explicitly supplied) store with its backend,
namespace, three-state status (``ready`` / ``degraded`` / ``recovering``),
number of buffered writes and the most recent state-transition events - handy
to leave running on screen while recording a degradation/failover demo.

Run as a module::

    python -m diffsync.store.status --interval 1

Or from the repository::

    invoke diffsync.store-status
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from typing import Iterable, List, Optional, TextIO

from diffsync.store.contract import Store, live_stores

_STATE_ICONS = {"ready": "[OK]", "degraded": "[DEGRADED]", "recovering": "[RECOVERING]"}


def _format_ts(timestamp: float) -> str:
    """Render an epoch timestamp as a human-readable local-ish UTC string."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%H:%M:%S")


def format_store_status(store: Store) -> str:
    """Render a single store status block."""
    status = store.status()
    icon = _STATE_ICONS.get(status["state"], f"[{status['state']}]")
    lines = [
        f"{icon} {status['backend']}:{status['name']} "
        f"ns={status['namespace']} root={status['namespace_root'] or '-'} "
        f"pending_writes={status['pending_writes']}",
    ]
    events = status.get("recent_events") or []
    if not events:
        lines.append("    (no state transitions yet)")
    for event in events:
        origin = event["from"] or "-"
        detail = f" {event['detail']}" if event.get("detail") else ""
        lines.append(f"    {_format_ts(event['timestamp'])} {origin} -> {event['to']} via {event['reason']}{detail}")
    return "\n".join(lines)


def format_statuses(stores: Optional[Iterable[Store]] = None) -> str:
    """Render status blocks for all stores (live registry by default)."""
    selected: List[Store] = list(stores) if stores is not None else live_stores()
    if not selected:
        header = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        return f"{header}\nNo stores registered yet."
    header = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return header + "\n" + "\n".join(format_store_status(store) for store in selected)


def watch(
    interval: float = 1.0,
    iterations: Optional[int] = None,
    stream: TextIO = sys.stdout,
    stores: Optional[Iterable[Store]] = None,
    clear_screen: bool = True,
) -> None:
    """Continuously print store statuses.

    Args:
        interval: Seconds between refreshes.
        iterations: Stop after this many refreshes (``None`` runs forever).
        stream: Output stream.
        stores: Explicit stores to render; defaults to the live-store registry.
        clear_screen: Clear the terminal between refreshes for a stable view.
    """
    count = 0
    try:
        while iterations is None or count < iterations:
            rendered = format_statuses(stores)
            if clear_screen and stream.isatty():
                stream.write("\x1b[2J\x1b[H")
            stream.write(rendered + "\n")
            stream.flush()
            count += 1
            if iterations is not None and count >= iterations:
                break
            time.sleep(interval)
    except KeyboardInterrupt:  # pragma: no cover - interactive use
        stream.write("\nStopped store_status watch.\n")


def build_parser() -> argparse.ArgumentParser:
    """Build the command line argument parser."""
    parser = argparse.ArgumentParser(prog="store_status", description="Continuously report store backend status.")
    parser.add_argument("--interval", type=float, default=1.0, help="Refresh interval in seconds (default: 1.0).")
    parser.add_argument("--once", action="store_true", help="Print the status once and exit.")
    parser.add_argument("--iterations", type=int, default=None, help="Number of refreshes before exiting.")
    parser.add_argument("--no-clear", action="store_true", help="Do not clear the screen between refreshes.")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Entry point for ``python -m diffsync.store.status``."""
    args = build_parser().parse_args(argv)
    if args.once:
        print(format_statuses())
        return 0
    watch(interval=args.interval, iterations=args.iterations, clear_screen=not args.no_clear)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
