"""RedisStore module.

Resilient Redis backend implementing the unified :class:`~diffsync.store.contract.StoreContract`.

Behavior on failure:

* Redis timeouts and connection errors are never swallowed silently: they are logged with the
  original exception attached and the store transitions to :attr:`StoreStatus.DEGRADED`.
* While degraded, all reads and writes are served from an in-memory mirror (a LocalStore) and
  writes are appended to a FIFO replay log.
* A background health monitor pings Redis; on success the store transitions to
  :attr:`StoreStatus.RECOVERING`, drains the replay log in order under the store lock, and only
  then returns to :attr:`StoreStatus.READY`. Writes arriving during recovery are queued behind
  in-flight ones, so writes within a namespace always converge in order.

Key/value serialization is JSON by default; pickle requires ``allow_pickle=True`` (a roundtrip is
verified at construction). Legacy, prefix-less pickle data written by older DiffSync releases
remains readable when pickle is enabled.
"""

from __future__ import annotations

import copy
import threading
import uuid
from collections import OrderedDict, deque
from pickle import dumps, loads  # nosec
from typing import TYPE_CHECKING, Any, Deque, Dict, List, Optional, Set, Tuple, Type, Union

try:
    from redis import Redis
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import RedisError
    from redis.exceptions import TimeoutError as RedisTimeoutError
except ImportError:  # pragma: no cover - redis is an optional dependency
    Redis = None  # type: ignore[assignment]

    class RedisError(Exception):  # type: ignore[no-redef]
        """Placeholder used when the redis package is not installed."""

    class RedisConnectionError(RedisError):  # type: ignore[no-redef]
        """Placeholder used when the redis package is not installed."""

    class RedisTimeoutError(RedisError):  # type: ignore[no-redef]
        """Placeholder used when the redis package is not installed."""

from diffsync.exceptions import ObjectAlreadyExists, ObjectNotFound, StoreBackendUnavailable
from diffsync.store import BaseStore
from diffsync.store.contract import (
    DEFAULT_NAMESPACE,
    PICKLE_PREFIX,
    StoreStatus,
    register_store,
)
from diffsync.store.local import LocalStore

if TYPE_CHECKING:
    from diffsync import DiffSyncModel

REDIS_DIFFSYNC_ROOT_LABEL = "diffsync"
MODEL_NAMESPACE = "__models__"
_REDIS_BACKEND_ERRORS = (RedisError, OSError, TimeoutError)


@register_store("redis")
class RedisStore(BaseStore):
    """Redis-backed store with transparent in-memory degradation and ordered recovery."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *args: Any,
        store_id: Optional[str] = None,
        host: Optional[str] = None,
        port: int = 6379,
        url: Optional[str] = None,
        db: int = 0,
        socket_timeout: float = 2.0,
        socket_connect_timeout: float = 2.0,
        client: Optional[Any] = None,
        health_interval: float = 1.0,
        monitor: bool = True,
        allow_pickle: bool = False,
        namespace: str = DEFAULT_NAMESPACE,
        **kwargs: Any,
    ) -> None:
        """Initialize the Redis store.

        Args:
            store_id: Stable identifier for this store within Redis; a UUID is generated if absent.
            host: Redis host.
            port: Redis port.
            url: Full Redis URL; mutually exclusive with ``host``.
            db: Redis database index.
            socket_timeout: Timeout (seconds) applied to each Redis command.
            socket_connect_timeout: Timeout (seconds) applied to connection establishment.
            client: Optional pre-built redis-compatible client (used for testing/doubles).
            health_interval: Seconds between background health checks while degraded.
            monitor: If False, no background thread is started; recovery is then manual.
            allow_pickle: Enable the pickle codec for key/value data (JSON is used otherwise).
            namespace: Namespace this store is bound to.
            *args: Additional positional arguments forwarded to BaseStore.
            **kwargs: Additional keyword arguments, including ``auto_degrade`` (default True) to
                control whether an unreachable Redis at construction degrades or raises.

        Raises:
            ValueError: if both ``url`` and ``host`` are provided.
            StoreBackendUnavailable: if Redis is unreachable and ``auto_degrade`` is False.
        """
        self.auto_degrade = bool(kwargs.pop("auto_degrade", True))
        super().__init__(*args, namespace=namespace, allow_pickle=allow_pickle, **kwargs)

        if url and host:
            raise ValueError("'url' and 'host' arguments can't be specified together.")

        self._store_id = store_id if store_id else str(uuid.uuid4())
        self._store_label = f"{REDIS_DIFFSYNC_ROOT_LABEL}:{self._store_id}"
        self._socket_timeout = socket_timeout
        self._socket_connect_timeout = socket_connect_timeout
        self._health_interval = health_interval

        self._fallback = LocalStore(adapter=None, namespace=DEFAULT_NAMESPACE, multi_namespace=True)
        self._pending: "OrderedDict[str, Deque[Tuple[str, bytes]]]" = OrderedDict()
        self._stop_monitor = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None

        self._store = client if client is not None else self._build_client(url=url, host=host, port=port, db=db)

        if allow_pickle:
            self.verify_roundtrip({"roundtrip": ("x", 1, None, True)})

        if self._ping():
            self._status = StoreStatus.READY
            self.record_event("store_init", f"connected to Redis as {self._store_label}")
        else:
            if not self.auto_degrade:
                raise StoreBackendUnavailable("Redis store is unavailable.")
            self._enter_degraded("initial connection to Redis failed")

        if monitor:
            self.start_monitor()

    def _build_client(self, *, url: Optional[str], host: Optional[str], port: int, db: int) -> Any:
        if Redis is None:  # pragma: no cover - import guard
            raise StoreBackendUnavailable(
                "Redis is not installed. Have you installed diffsync with the redis extra? "
                "`pip install diffsync[redis]`"
            )
        if url:
            return Redis.from_url(
                url, db=db, socket_timeout=self._socket_timeout,
                socket_connect_timeout=self._socket_connect_timeout,
            )
        if host:
            return Redis(
                host=host, port=port, db=db, socket_timeout=self._socket_timeout,
                socket_connect_timeout=self._socket_connect_timeout,
            )
        if self.auto_degrade:
            return None
        raise RedisConnectionError("Neither 'host' nor 'url' were specified.")

    def __str__(self) -> str:
        """Render store name."""
        return f"{self.name} ({self._store_id})"
    # -- health, degradation and recovery -------------------------------------------------

    def _ping(self) -> bool:
        if self._store is None:
            return False
        try:
            return bool(self._store.ping())
        except _REDIS_BACKEND_ERRORS as exc:
            self._log.warning("store.redis_ping_failed", error=repr(exc))
            return False

    def start_monitor(self) -> None:
        """Start the background health-check thread that triggers automatic recovery."""
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
        self._stop_monitor.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, name=f"RedisStoreMonitor-{self._store_id[:8]}", daemon=True
        )
        self._monitor_thread.start()

    def stop(self) -> None:
        """Stop the background health monitor, if running."""
        self._stop_monitor.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=self._health_interval + 1.0)
            self._monitor_thread = None

    def _monitor_loop(self) -> None:
        while not self._stop_monitor.wait(self._health_interval):
            try:
                if self._status == StoreStatus.DEGRADED and self._ping():
                    self.recover()
            except Exception as exc:  # pylint: disable=broad-except
                # The monitor must never die silently; surface the failure and keep probing.
                self._log.error("store.monitor_error", error=repr(exc))

    def health_check(self) -> StoreStatus:
        """Actively probe the backend; trigger recovery if degraded and Redis is back."""
        with self._lock:
            if self._status == StoreStatus.DEGRADED and self._ping():
                self.recover()
            return self._status

    def _enter_degraded(self, reason: str) -> None:
        with self._lock:
            if self._status != StoreStatus.DEGRADED:
                self.record_event(
                    "degraded",
                    f"Redis unavailable, serving reads/writes from in-memory fallback: {reason}",
                    status=StoreStatus.DEGRADED,
                )
            else:
                self.record_event("backend_error", reason)

    def _queue_write(self, namespace: str, op: str, payload: bytes) -> None:
        self._pending.setdefault(namespace, deque()).append((op, payload))

    def recover(self) -> bool:
        """Drain pending writes into Redis in per-namespace enqueue order.

        Returns:
            True if the store transitioned back to :attr:`StoreStatus.READY`, False if Redis is
            still unavailable (the store remains :attr:`StoreStatus.DEGRADED`).
        """
        with self._lock:
            if self._status != StoreStatus.DEGRADED:
                return self._status == StoreStatus.READY
            if not self._ping():
                return False
            self.record_event(
                "recovering", "Redis reachable; replaying queued writes in namespace order", status=StoreStatus.RECOVERING
            )
            try:
                while self._pending:
                    namespace, ops = next(iter(self._pending.items()))
                    while ops:
                        op, payload = ops.popleft()
                        try:
                            self._exec_write(op, payload)
                        except _REDIS_BACKEND_ERRORS as exc:
                            # Reinstate the operation so it is retried on the next recovery.
                            ops.appendleft((op, payload))
                            self.record_event(
                                "recovery_failed", f"replay interrupted: {exc}", status=StoreStatus.DEGRADED
                            )
                            return False
                    del self._pending[namespace]
            except Exception as exc:  # pylint: disable=broad-except
                self._log.error("store.recovery_error", error=repr(exc))
                self.record_event("recovery_failed", str(exc), status=StoreStatus.DEGRADED)
                return False
            self.record_event("recovered", "all queued writes replayed; backend is back to normal", status=StoreStatus.READY)
            return True

    def _handle_backend_failure(self, action: str, exc: Exception) -> None:
        self._log.error("store.redis_error", action=action, error=repr(exc))
        self._enter_degraded(f"{action} failed: {exc}")

    def _exec_write(self, op: str, payload: bytes) -> None:
        if op == "set":
            self._store.set(payload[0], payload[1])
        elif op == "delete":
            self._store.delete(payload)
        else:  # pragma: no cover - defensive
            raise ValueError(f"unknown queued operation {op!r}")

    def _write_remote(self, namespace: str, op: str, payload: bytes) -> None:
        if self._status == StoreStatus.DEGRADED or self._store is None:
            self._queue_write(namespace, op, payload)
            return
        try:
            self._exec_write(op, payload)
        except _REDIS_BACKEND_ERRORS as exc:
            self._queue_write(namespace, op, payload)
            self._handle_backend_failure(f"redis {op}", exc)

    # -- contract key/value API -----------------------------------------------------------

    def _kv_key(self, namespace: str, key: str) -> bytes:
        return f"{self._store_label}:kv:{namespace}:{key}".encode("utf-8")

    @staticmethod
    def _parse_kv_key(raw: bytes) -> Tuple[str, str]:
        # b'diffsync:<id>:kv:<namespace>:<key>' -> (namespace, key)
        parts = raw.split(b":", 4)
        return parts[3].decode("utf-8"), parts[4].decode("utf-8")

    def get_item(self, key: str, namespace: Optional[str] = None) -> Any:
        """Return the value at ``key``; falls back to the in-memory mirror when Redis is down."""
        ns = self._resolve_namespace(namespace)
        if self._status == StoreStatus.DEGRADED or self._store is None:
            return self._fallback.get_item(key, namespace=ns)
        try:
            raw = self._store.get(self._kv_key(ns, key))
        except _REDIS_BACKEND_ERRORS as exc:
            self._handle_backend_failure("redis get", exc)
            return self._fallback.get_item(key, namespace=ns)
        if raw is None:
            raise self.missing_key(ns, key, str(self))
        return self.decode(raw)

    def set(self, key: str, value: Any, namespace: Optional[str] = None) -> None:
        """Write through to Redis and mirror locally; degraded writes queue for ordered replay."""
        ns = self._resolve_namespace(namespace)
        encoded = self.encode(value)
        with self._lock:
            self._fallback.set(key, value, namespace=ns)
            self._write_remote(ns, "set", (self._kv_key(ns, key), encoded))

    def delete(self, key: str, namespace: Optional[str] = None) -> bool:
        """Delete from Redis and the local mirror; degraded deletes queue for ordered replay."""
        ns = self._resolve_namespace(namespace)
        with self._lock:
            existed = self._fallback.delete(key, namespace=ns)
            self._write_remote(ns, "delete", self._kv_key(ns, key))
            return existed

    def keys(self, namespace: Optional[str] = None) -> List[str]:
        """List keys in ``namespace``; falls back to the in-memory mirror when Redis is down."""
        ns = self._resolve_namespace(namespace)
        if self._status == StoreStatus.DEGRADED or self._store is None:
            return self._fallback.keys(namespace=ns)
        try:
            found: List[str] = []
            prefix = f"{self._store_label}:kv:{ns}:".encode("utf-8")
            for raw in self._store.scan_iter(prefix + b"*"):
                key_ns, key = self._parse_kv_key(raw)
                if key_ns == ns:
                    found.append(key)
            return found
        except _REDIS_BACKEND_ERRORS as exc:
            self._handle_backend_failure("redis keys", exc)
            return self._fallback.keys(namespace=ns)
    # -- model-level API -------------------------------------------------------------------

    def _model_key(self, modelname: str, uid: str) -> str:
        return f"{self._store_label}:{modelname}:{uid}"

    def _model_key_bytes(self, modelname: str, uid: str) -> bytes:
        return self._model_key(modelname, uid).encode("utf-8")

    def _decode_model(self, raw: bytes) -> "DiffSyncModel":
        if raw.startswith(PICKLE_PREFIX):
            obj_result = loads(raw[len(PICKLE_PREFIX) :])  # noqa: S301
        else:
            # Legacy, prefix-less pickle payloads written by older DiffSync releases.
            obj_result = loads(raw)  # noqa: S301
        obj_result.adapter = self.adapter
        return obj_result

    def _encode_model(self, obj: "DiffSyncModel") -> bytes:
        obj_copy = copy.copy(obj)
        obj_copy.adapter = None
        return PICKLE_PREFIX + dumps(obj_copy)

    def _get_object_from_redis_key(self, key: str) -> "DiffSyncModel":
        """Get the object from a Redis model key."""
        pickled_object = self._store.get(key)
        if pickled_object:
            return self._decode_model(pickled_object)
        raise ObjectNotFound(f"{key} not present in Cache")

    def _remote_model_get(self, key: str) -> Optional[bytes]:
        if self._status == StoreStatus.DEGRADED or self._store is None:
            return None
        try:
            return self._store.get(key.encode("utf-8") if isinstance(key, str) else key)
        except _REDIS_BACKEND_ERRORS as exc:
            self._handle_backend_failure("redis get model", exc)
            return None

    def _remote_model_write(self, op: str, key: str, payload: Optional[bytes] = None) -> None:
        with self._lock:
            if op == "set":
                raw_key = key.encode("utf-8") if isinstance(key, str) else key
                self._write_remote(MODEL_NAMESPACE, "set", (raw_key, payload))
            elif op == "delete":
                raw_key = key.encode("utf-8") if isinstance(key, str) else key
                self._write_remote(MODEL_NAMESPACE, "delete", raw_key)

    def get_all_model_names(self) -> Set[str]:
        """Get all the model names stored."""
        if self._status == StoreStatus.DEGRADED or self._store is None:
            return self._fallback.get_all_model_names()
        all_model_names = set()
        try:
            prefix = f"{self._store_label}:".encode("utf-8")
            for item in self._store.scan_iter(prefix + b"*"):
                segments = item.split(b":")
                # b'diffsync:<id>:<model>:<uid>' -> model is segment 2; skip b'kv' entries
                if len(segments) > 2 and segments[2] != b"kv":
                    all_model_names.add(segments[2].decode())
        except _REDIS_BACKEND_ERRORS as exc:
            self._handle_backend_failure("redis scan model names", exc)
            return self._fallback.get_all_model_names()
        return all_model_names

    def get_model(
        self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]], identifier: Union[str, Dict]
    ) -> "DiffSyncModel":
        """Get one object from the data store based on its unique id."""
        object_class, modelname = self._get_object_class_and_model(model)
        uid = self._get_uid(model, object_class, identifier)
        key = self._model_key(modelname, uid)

        raw = self._remote_model_get(key)
        if raw is not None:
            obj = self._decode_model(raw)
            # Keep the in-memory mirror warm so reads survive a subsequent degradation window.
            try:
                self._fallback.update(obj=obj)
            except ObjectNotFound:
                self._fallback.add(obj=obj)
            return obj
        # Degraded (or key not in Redis): consult the in-memory mirror.
        return self._fallback.get_model(model=model, identifier=identifier)

    def get_all(self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]]) -> List["DiffSyncModel"]:
        """Get all objects of a given type."""
        if isinstance(model, str):
            modelname = model
        else:
            modelname = model.get_type()

        if self._status == StoreStatus.DEGRADED or self._store is None:
            return self._fallback.get_all(model=model)

        results: List["DiffSyncModel"] = []
        try:
            for raw_key in self._store.scan_iter(f"{self._store_label}:{modelname}:*"):
                results.append(self._decode_model(self._store.get(raw_key)))
        except _REDIS_BACKEND_ERRORS as exc:
            self._handle_backend_failure("redis get_all models", exc)
            return self._fallback.get_all(model=model)
        return results

    def get_by_uids(
        self, *, uids: List[str], model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]]
    ) -> List["DiffSyncModel"]:
        """Get multiple objects from the store by their unique IDs/Keys and type."""
        results: List["DiffSyncModel"] = []
        for uid in uids:
            results.append(self.get_model(model=model, identifier=uid))
        return results

    def add(self, *, obj: "DiffSyncModel") -> None:
        """Add a DiffSyncModel object to the store.

        Raises:
            ObjectAlreadyExists: if a different object with the same uid is already present.
        """
        modelname = obj.get_type()
        uid = obj.get_unique_id()
        key = self._model_key(modelname, uid)

        existing_obj_binary = self._remote_model_get(key)
        if existing_obj_binary:
            existing_obj = self._decode_model(existing_obj_binary)
            if existing_obj.dict() != obj.dict():
                raise ObjectAlreadyExists(f"Object {uid} already present", obj)
            return

        # Not in Redis. Keep the in-memory mirror authoritative for existence checks whenever the
        # backend is unreachable, so ObjectAlreadyExists semantics survive a degradation window.
        if self._status == StoreStatus.DEGRADED or self._store is None:
            self._fallback.add(obj=obj)
            self._remote_model_write("set", key, self._encode_model(obj))
            return
        try:
            self._fallback.add(obj=obj)
            self._remote_model_write("set", key, self._encode_model(obj))
        except ObjectAlreadyExists:
            self._fallback.remove_item(modelname, uid)
            raise

    def update(self, *, obj: "DiffSyncModel") -> None:
        """Update a DiffSyncModel object to the store."""
        modelname = obj.get_type()
        uid = obj.get_unique_id()
        key = self._model_key(modelname, uid)
        self._fallback.update(obj=obj)
        self._remote_model_write("set", key, self._encode_model(obj))

    def remove_item(self, modelname: str, uid: str) -> None:
        """Remove one item from store."""
        key = self._model_key(modelname, uid)
        if self._status != StoreStatus.DEGRADED and self._store is not None:
            try:
                exists = self._store.exists(key.encode("utf-8"))
            except _REDIS_BACKEND_ERRORS as exc:
                self._handle_backend_failure("redis exists on remove", exc)
                exists = 0
            if not exists and not self._fallback.count(model=modelname):
                raise ObjectNotFound(f"{modelname} {uid} not present in Cache")
        try:
            self._fallback.remove_item(modelname, uid)
        except ObjectNotFound:
            pass
        self._remote_model_write("delete", key)

    def count(self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"], None] = None) -> int:
        """Return the number of elements of a specific model, or all elements in the store."""
        if self._status == StoreStatus.DEGRADED or self._store is None:
            return self._fallback.count(model=model)
        search_pattern = f"{self._store_label}:*"
        if model is not None:
            modelname = model if isinstance(model, str) else model.get_type()
            search_pattern = f"{self._store_label}:{modelname}:*"
        try:
            count = 0
            for item in self._store.scan_iter(search_pattern):
                if b":kv:" not in item:
                    count += 1
            return count
        except _REDIS_BACKEND_ERRORS as exc:
            self._handle_backend_failure("redis count", exc)
            return self._fallback.count(model=model)
