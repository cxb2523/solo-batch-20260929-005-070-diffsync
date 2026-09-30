"""Redis implementation of the unified store contract.

The backend is resilient by design:

* Every Redis operation is bounded by a socket timeout and wrapped to convert
  Redis connectivity errors into an explicit, observable state transition
  instead of an unhandled exception.
* When Redis becomes unreachable the store enters
  :attr:`~diffsync.store.contract.StoreState.DEGRADED`: reads and writes keep
  working against an in-memory mirror while every write is appended, in order,
  to a write-ahead log (WAL).
* A background monitor pings Redis; once it answers again the store enters
  :attr:`~diffsync.store.contract.StoreState.RECOVERING`, replays the WAL per
  namespace in the exact order writes were observed and only then returns to
  :attr:`~diffsync.store.contract.StoreState.READY`, dropping the tombstones.
* Nothing is silently swallowed: every transition and every backend error is
  logged through ``structlog`` and exposed via :meth:`Store.status`.

Values are serialized with :class:`~diffsync.store.serializer.Serializer`
(JSON by default; pickle only when explicitly enabled).
"""

from __future__ import annotations

import threading
from typing import Any, Deque, Dict, List, Optional, Tuple

from diffsync.exceptions import StoreOperationError
from diffsync.store.contract import (
    Operation,
    Store,
    StoreState,
    register_store,
)
from diffsync.store.serializer import Serializer

_TOMBSTONE = object()
REDIS_ROOT_LABEL = "diffsync-kv"


def _redis_error_types() -> Tuple[type, ...]:
    """Return the redis-py exception types considered recoverable.

    Importing redis is optional (the ``redis`` extra); tests inject a fake
    client, so a missing redis package simply yields an empty tuple.
    """
    try:
        from redis import exceptions as redis_exceptions  # type: ignore
    except ImportError:
        return ()
    return (
        redis_exceptions.ConnectionError,
        redis_exceptions.TimeoutError,
        redis_exceptions.BusyLoadingError,
        redis_exceptions.AuthenticationError,
        OSError,
    )


class RedisStore(Store):
    """Redis-backed store with automatic degradation and ordered recovery."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        namespace: str = "default",
        namespace_root: Optional[str] = None,
        name: str = "",
        url: Optional[str] = None,
        host: Optional[str] = None,
        port: int = 6379,
        db: int = 0,
        socket_timeout: float = 2.0,
        socket_connect_timeout: float = 2.0,
        client: Any = None,
        allow_pickle: bool = False,
        verify_roundtrip: bool = True,
        monitor_interval: float = 1.0,
        start_monitor: bool = True,
        key_prefix: str = REDIS_ROOT_LABEL,
        **redis_kwargs: Any,
    ) -> None:
        """Initialize the Redis store.

        A connection failure at construction does not raise: the store starts
        in ``DEGRADED`` mode and reconnects automatically.  Provide ``client``
        to inject a fake/alternate Redis client in tests.
        """
        self._serializer = Serializer(allow_pickle=allow_pickle, verify_roundtrip=verify_roundtrip)
        self._key_prefix = key_prefix
        self._monitor_interval = monitor_interval
        self._recoverable = _redis_error_types()
        self._mem: Dict[str, Dict[str, Any]] = {}
        self._wal: Deque[Operation] = Deque()
        self._wal_by_namespace: Dict[str, Deque[Operation]] = {}
        self._op_lock = threading.RLock()
        self._monitor: Optional[threading.Thread] = None
        self._monitor_stop = threading.Event()
        self._client = self._make_client(
            client, url, host, port, db, socket_timeout, socket_connect_timeout, **redis_kwargs
        )

        super().__init__(namespace=namespace, namespace_root=namespace_root, name=name)

        if self._ping_client():
            self._set_state(StoreState.READY, "initial-connection-ok")
        else:
            self._enter_degraded("initial-connection-failed", started=True)
        if start_monitor:
            self._start_monitor()

    # -- client setup ------------------------------------------------------

    @staticmethod
    def _make_client(client, url, host, port, db, socket_timeout, socket_connect_timeout, **redis_kwargs) -> Any:
        if client is not None:
            return client
        try:
            from redis import Redis  # type: ignore
        except ImportError as exc:
            raise StoreOperationError(
                "Redis backend requires the 'redis' extra (`pip install diffsync[redis]`) or an injected client"
            ) from exc
        if url:
            return Redis.from_url(
                url, db=db, socket_timeout=socket_timeout, socket_connect_timeout=socket_connect_timeout, **redis_kwargs
            )
        if host:
            return Redis(
                host=host,
                port=port,
                db=db,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                **redis_kwargs,
            )
        raise StoreOperationError("RedisStore requires either an url, a host or an injected client")

    def _ping_client(self) -> bool:
        try:
            return bool(self._client.ping())
        except Exception as exc:  # noqa: BLE001 - any client failure means "not reachable"
            self._log.debug("redis-ping-failed", error=repr(exc))
            return False

    # -- key layout --------------------------------------------------------

    def _redis_key(self, namespace: str, key: str) -> str:
        return f"{self._key_prefix}:{namespace}:{key}"

    def _redis_namespace_pattern(self, namespace: str) -> str:
        return f"{self._key_prefix}:{namespace}:*"

    @staticmethod
    def _to_text(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    # -- CRUD --------------------------------------------------------------

    def get(self, key: str, namespace: Optional[str] = None, default: Any = None) -> Any:
        """Return a value; served from the memory mirror while degraded."""
        nspace = self._resolve_namespace(namespace)
        self.namespace_key(namespace, key)
        if self._state is StoreState.READY:
            try:
                raw = self._client.get(self._redis_key(nspace, key))
                return self._serializer.decode(raw) if raw is not None else default
            except self._recoverable as exc:
                self._on_redis_error("get", exc)
        return self._mem.get(nspace, {}).get(key, default)

    def set(self, key: str, value: Any, namespace: Optional[str] = None) -> None:
        """Write to Redis when healthy; buffer an ordered op otherwise."""
        nspace = self._resolve_namespace(namespace)
        self.namespace_key(namespace, key)
        with self._op_lock:
            if self._state is StoreState.READY:
                try:
                    self._client.set(self._redis_key(nspace, key), self._serializer.encode(value))
                    return
                except self._recoverable as exc:
                    self._on_redis_error("set", exc)
            operation = self._buffer_operation(nspace, key, "set", value)
            self._mem.setdefault(nspace, {})[key] = value
            self._log.debug("write-buffered", seq=operation.seq, namespace=nspace, key=key)

    def delete(self, key: str, namespace: Optional[str] = None) -> bool:
        """Delete from Redis when healthy; buffer a tombstone otherwise."""
        nspace = self._resolve_namespace(namespace)
        self.namespace_key(namespace, key)
        with self._op_lock:
            if self._state is StoreState.READY:
                try:
                    return bool(self._client.delete(self._redis_key(nspace, key)))
                except self._recoverable as exc:
                    self._on_redis_error("delete", exc)
            self._buffer_operation(nspace, key, "delete")
            existed = self._mem.get(nspace, {}).pop(key, _TOMBSTONE) is not _TOMBSTONE
            return existed

    def keys(self, namespace: Optional[str] = None) -> List[str]:
        """List live keys in the namespace."""
        nspace = self._resolve_namespace(namespace)
        if self._state is StoreState.READY:
            try:
                prefix = f"{self._key_prefix}:{nspace}:"
                return [self._to_text(raw)[len(prefix) :] for raw in self._client.scan_iter(f"{prefix}*")]
            except self._recoverable as exc:
                self._on_redis_error("keys", exc)
        return [key for key in self._mem.get(nspace, {}) if self._mem[nspace][key] is not _TOMBSTONE]

    def namespaces(self) -> List[str]:
        """List namespaces holding data (scans Redis when ready, memory otherwise)."""
        if self._state is StoreState.READY:
            try:
                prefix = f"{self._key_prefix}:"
                found = {
                    self._to_text(raw_key)[len(prefix) :].rpartition(":")[0]
                    for raw_key in self._client.scan_iter(f"{prefix}*")
                }
                return sorted(name for name in found if name)
            except self._recoverable as exc:
                self._on_redis_error("namespaces", exc)
        return sorted(nspace for nspace, bucket in self._mem.items() if bucket)

    def clear(self, namespace: Optional[str] = None) -> None:
        """Remove every key of the namespace."""
        nspace = self._resolve_namespace(namespace)
        with self._op_lock:
            if self._state is StoreState.READY:
                try:
                    keys = list(self._client.scan_iter(self._redis_namespace_pattern(nspace)))
                    if keys:
                        self._client.delete(*keys)
                    return
                except self._recoverable as exc:
                    self._on_redis_error("clear", exc)
            for key in list(self._mem.get(nspace, {})):
                self._buffer_operation(nspace, key, "delete")
            self._mem[nspace] = {}

    # -- degradation / recovery -------------------------------------------

    def _buffer_operation(self, namespace: str, key: str, op: str, value: Any = None) -> Operation:
        operation = Operation(seq=next(self._seq_counter), namespace=namespace, key=key, op=op, value=value)
        self._wal.append(operation)
        self._wal_by_namespace.setdefault(namespace, Deque()).append(operation)
        return operation

    def _on_redis_error(self, action: str, exc: Exception) -> None:
        """Record a backend error and degrade (never swallow silently)."""
        self._log.error("redis-operation-failed", action=action, error=repr(exc))
        if self._state is StoreState.READY:
            self._enter_degraded(f"redis-{action}-failed", error=repr(exc))

    def _enter_degraded(self, reason: str, *, started: bool = False, **detail: Any) -> None:
        self._set_state(StoreState.DEGRADED, reason, **detail)
        if started:
            self._log.warning("store-degraded", reason=reason)

    def attempt_recovery(self) -> bool:
        """Try to replay buffered writes back to Redis.

        Returns:
            ``True`` if the store returned to ``READY``, ``False`` if Redis is
            still unavailable (the store stays ``DEGRADED`` with its WAL).
        """
        with self._op_lock:
            if self._state is StoreState.RECOVERING:
                return False
            if self._state is StoreState.READY and not self._wal:
                return True
            if not self._ping_client():
                if self._state is StoreState.READY:
                    self._enter_degraded("recovery-ping-failed")
                return False

            self._set_state(StoreState.RECOVERING, "redis-reachable-replaying", pending_writes=len(self._wal))
            try:
                self._replay_wal()
            except self._recoverable as exc:
                self._log.error("redis-recovery-failed", error=repr(exc))
                self._enter_degraded("recovery-replay-failed", error=repr(exc))
                return False
            self._mem = {}
            self._set_state(StoreState.READY, "recovery-complete", replayed=self._replayed_count)
            return True

    # Alias kept for readability at call sites.
    recover = attempt_recovery

    def _replay_wal(self) -> None:
        """Apply every buffered operation in global (per-namespace ordered) sequence."""
        # A pipeline reduces round trips; clients/fakes only need to implement
        # pipeline().__enter__ ... set/delete ... execute().
        batch_size = 500
        operations = list(self._wal)
        replayed = 0
        for start in range(0, len(operations), batch_size):
            batch = operations[start : start + batch_size]
            pipeline = self._client.pipeline() if hasattr(self._client, "pipeline") else None
            for operation in batch:
                redis_key = self._redis_key(operation.namespace, operation.key)
                if operation.op == "set":
                    payload = self._serializer.encode(operation.value)
                    if pipeline is not None:
                        pipeline.set(redis_key, payload)
                    else:
                        self._client.set(redis_key, payload)
                elif pipeline is not None:
                    pipeline.delete(redis_key)
                else:
                    self._client.delete(redis_key)
            if pipeline is not None:
                pipeline.execute()
            replayed += len(batch)
        self._replayed_count = replayed
        self._wal.clear()
        self._wal_by_namespace.clear()

    # -- background monitor ------------------------------------------------

    def _start_monitor(self) -> None:
        if self._monitor is not None and self._monitor.is_alive():
            return
        self._monitor = threading.Thread(target=self._monitor_loop, name=f"{self.name}-redis-monitor", daemon=True)
        self._monitor.start()

    def _monitor_loop(self) -> None:
        while not self._monitor_stop.wait(self._monitor_interval):
            try:
                if self._state is StoreState.DEGRADED and self._ping_client():
                    self._log.info("redis-back-online")
                    self.attempt_recovery()
            except Exception as exc:  # noqa: BLE001 - monitor must never die silently
                self._log.error("redis-monitor-error", error=repr(exc))

    # -- status / lifecycle ------------------------------------------------

    def _pending_count(self) -> int:
        return len(self._wal)

    def status(self) -> Dict[str, Any]:
        """Include Redis-specific detail in the status snapshot."""
        snapshot = super().status()
        snapshot["backend_detail"] = {
            "pending_writes": len(self._wal),
            "namespaces_with_pending_writes": sorted(self._wal_by_namespace),
            "allow_pickle": self._serializer.allow_pickle,
        }
        return snapshot

    def close(self) -> None:
        """Stop the monitor thread and unregister the instance."""
        self._monitor_stop.set()
        if self._monitor is not None:
            self._monitor.join(timeout=self._monitor_interval + 1)
        close = getattr(self._client, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:  # noqa: BLE001
                self._log.error("redis-close-failed", error=repr(exc))
        super().close()


register_store("redis", RedisStore, override=True)
