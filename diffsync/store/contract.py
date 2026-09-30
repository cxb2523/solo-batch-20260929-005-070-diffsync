"""Unified store contract shared by every DiffSync storage backend.

The contract defines a small, namespace-isolated key/value surface (``get``/``set``/``delete``)
plus a ``ready()`` status probe. Backends are registered by name and can be loaded through
:func:`get_store_class`; the :class:`~diffsync.Adapter` constructor accepts either a registered
store name, a store class or a store instance.

Ready states:

* :attr:`StoreStatus.READY` -- the backend is operating normally.
* :attr:`StoreStatus.DEGRADED` -- the primary backend is unavailable; reads and writes are served
  from the in-memory fallback. The state is explicit, logged and observable.
* :attr:`StoreStatus.RECOVERING` -- the primary backend is reachable again; pending writes are being
  replayed in per-namespace order before returning to :attr:`StoreStatus.READY`.

All state transitions are recorded as observable events on the store (``events``) and mirrored in a
process-wide event buffer (:func:`recent_events`). Failures are never swallowed silently: they are
logged with the original exception attached and reflected in the store status.
"""

from __future__ import annotations

import datetime as _datetime
import enum
import json
import pickle  # nosec
import threading
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional

import structlog  # type: ignore

from diffsync.exceptions import (
    NamespaceBoundaryError,
    StoreSerializationError,
)

DEFAULT_NAMESPACE = "default"
JSON_PREFIX = b"diffsync-json:1:"
PICKLE_PREFIX = b"diffsync-pickle:1:"
_MAX_EVENTS = 100

_LOGGER = structlog.get_logger(__name__)


class StoreStatus(str, enum.Enum):
    """Operational status of a store backend as exposed by :meth:`StoreContract.ready`."""

    READY = "ready"
    DEGRADED = "degraded"
    RECOVERING = "recovering"


class SwitchEvent(dict):
    """A single store status transition or other notable backend event."""

    @property
    def timestamp(self) -> float:
        """Unix timestamp of the event."""
        return self["timestamp"]  # type: ignore[return-value]

    @property
    def store(self) -> str:
        """Name of the store that produced the event."""
        return self["store"]  # type: ignore[return-value]

    @property
    def event(self) -> str:
        """Machine readable event kind, e.g. ``status_change``."""
        return self["event"]  # type: ignore[return-value]

    @property
    def detail(self) -> str:
        """Human readable description of the event."""
        return self["detail"]  # type: ignore[return-value]

    def render(self) -> str:
        """Render the event on a single, screen-friendly line (used by ``store_status``)."""
        stamp = _datetime.datetime.fromtimestamp(self.timestamp).strftime("%Y-%m-%d %H:%M:%S")
        return f"[{stamp}] {self.store}: {self.event} -- {self.detail}"


class _ExtendedEncoder(json.JSONEncoder):
    """JSON encoder with conservative, lossless support for common non-builtin types."""

    def default(self, o: Any) -> Any:
        """Encode sets, tuples, bytes and timezone-aware/naive datetimes as tagged values."""
        if isinstance(o, set):
            return {"__diffsync_type__": "set", "value": list(o)}
        if isinstance(o, bytes):
            return {"__diffsync_type__": "bytes", "value": o.hex()}
        if isinstance(o, _datetime.datetime):
            return {"__diffsync_type__": "datetime", "value": o.isoformat()}
        return super().default(o)


def _json_object_hook(obj: Any) -> Any:
    """Reverse the tagged values produced by :class:`_ExtendedEncoder`."""
    tag = obj.get("__diffsync_type__") if isinstance(obj, dict) else None
    if tag == "set":
        return set(obj["value"])
    if tag == "bytes":
        return bytes.fromhex(obj["value"])
    if tag == "datetime":
        return _datetime.datetime.fromisoformat(obj["value"])
    return obj


def validate_namespace(namespace: str) -> None:
    r"""Validate a namespace identifier.

    Namespaces must be non-empty strings that do not contain characters that could escape the
    namespace boundary in key-value backends (``:`` is used to separate key segments and ``\\0``
    is reserved as a binary separator).

    Raises:
        NamespaceBoundaryError: if the namespace is invalid.
    """
    if not isinstance(namespace, str) or not namespace:
        raise NamespaceBoundaryError(f"namespace must be a non-empty string, got {namespace!r}")
    for char in (":", "\x00"):
        if char in namespace:
            raise NamespaceBoundaryError(f"invalid character {char!r} in namespace {namespace!r}")


class StoreContract:
    """Unified contract implemented by every store backend.

    Key/value data is partitioned into namespaces. A store may optionally be bound to a single
    namespace at construction time: in that case every operation must target that namespace and
    cross-namespace access raises :class:`NamespaceBoundaryError`.
    """

    def __init__(
        self,
        *,
        namespace: str = DEFAULT_NAMESPACE,
        allow_pickle: bool = False,
        name: str = "",
        **_: Any,
    ) -> None:
        """Initialize the store contract.

        Args:
            namespace: Namespace this store is bound to.
            allow_pickle: If True the pickle codec is enabled; JSON remains the default codec.
            name: Human readable store name; defaults to the class name.
        """
        validate_namespace(namespace)
        self.namespace = namespace
        self.allow_pickle = allow_pickle
        self.name = name or self.__class__.__name__
        self._lock = threading.RLock()
        self._status: StoreStatus = StoreStatus.READY
        self.events: Deque[SwitchEvent] = deque(maxlen=_MAX_EVENTS)
        self._log = structlog.get_logger().new(store=self)
        _register_instance(self)

    def __str__(self) -> str:
        """Render store name."""
        return self.name

    # -- status / observability -----------------------------------------------------------

    def ready(self) -> StoreStatus:
        """Return the current operational status of the backend.

        Returns:
            :attr:`StoreStatus.READY`, :attr:`StoreStatus.DEGRADED` or
            :attr:`StoreStatus.RECOVERING`.
        """
        return self._status

    def last_event(self) -> Optional[SwitchEvent]:
        """Return the most recent recorded event, if any."""
        return self.events[-1] if self.events else None

    def record_event(self, event: str, detail: str, *, status: Optional[StoreStatus] = None) -> SwitchEvent:
        """Record an observable event and, optionally, transition the store status.

        Args:
            event: Machine readable event kind.
            detail: Human readable description.
            status: If provided, transition the store to this status.

        Returns:
            The recorded :class:`SwitchEvent`.
        """
        if status is not None and status != self._status:
            self._log.warning("store.status_change", old_status=self._status.value, new_status=status.value, detail=detail)
            self._status = status
        elif status is None:
            self._log.info("store.event", kind=event, detail=detail)
        switch = SwitchEvent(
            timestamp=time.time(),
            store=self.name,
            event=event,
            detail=detail,
            status=self._status.value,
        )
        self.events.append(switch)
        _GLOBAL_EVENTS.append(switch)
        return switch

    # -- namespace handling ---------------------------------------------------------------

    def _resolve_namespace(self, namespace: Optional[str]) -> str:
        resolved = namespace or self.namespace
        validate_namespace(resolved)
        if resolved != self.namespace:
            raise NamespaceBoundaryError(
                f"store {self.name!r} is bound to namespace {self.namespace!r}, refusing access to {resolved!r}"
            )
        return resolved

    # -- serialization ---------------------------------------------------------------------

    def encode(self, value: Any) -> bytes:
        """Serialize a value for storage.

        JSON is the default codec. Pickle is only used when the store was explicitly constructed
        with ``allow_pickle=True``.

        Raises:
            StoreSerializationError: if the value cannot be serialized with the active codec.
        """
        if self.allow_pickle:
            try:
                return PICKLE_PREFIX + pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
            except Exception as exc:  # pylint: disable=broad-except
                raise StoreSerializationError(f"failed to pickle value: {exc}") from exc
        try:
            return JSON_PREFIX + json.dumps(value, cls=_ExtendedEncoder, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise StoreSerializationError(
                f"value is not JSON serializable; construct the store with allow_pickle=True to store it ({exc})"
            ) from exc

    def decode(self, raw: bytes) -> Any:
        """Deserialize a value previously written by this store.

        Current data carries an explicit codec prefix. Legacy, prefix-less data is always pickled
        and remains readable, but reading it requires the store to be constructed with
        ``allow_pickle=True`` and emits a deprecation warning log.
        """
        if raw is None:
            return None
        if raw.startswith(JSON_PREFIX):
            try:
                return json.loads(raw[len(JSON_PREFIX) :].decode("utf-8"), object_hook=_decode_tagged)
            except (TypeError, ValueError) as exc:
                raise StoreSerializationError(f"failed to decode JSON value: {exc}") from exc
        if raw.startswith(PICKLE_PREFIX):
            if not self.allow_pickle:
                raise StoreSerializationError(
                    "cannot read pickle-encoded value: construct the store with allow_pickle=True"
                )
            try:
                return pickle.loads(raw[len(PICKLE_PREFIX) :])  # noqa: S301
            except Exception as exc:  # pylint: disable=broad-except
                raise StoreSerializationError(f"failed to unpickle value: {exc}") from exc
        # Legacy, prefix-less data written by older DiffSync releases is always pickle.
        if not self.allow_pickle:
            raise StoreSerializationError(
                "encountered legacy pickle data: construct the store with allow_pickle=True to read it"
            )
        self._log.warning("store.legacy_pickle_read", detail="reading prefix-less pickle data written by older diffsync")
        try:
            return pickle.loads(raw)  # noqa: S301
        except Exception as exc:  # pylint: disable=broad-except
            raise StoreSerializationError(f"failed to decode legacy pickle value: {exc}") from exc

    def verify_roundtrip(self, sample: Any) -> None:
        """Verify that ``sample`` survives an encode/decode roundtrip with the active codec.

        Raises:
            StoreSerializationError: if the roundtrip fails or changes the value.
        """
        try:
            restored = self.decode(self.encode(sample))
        except StoreSerializationError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            raise StoreSerializationError(f"serialization roundtrip failed: {exc}") from exc
        if restored != sample:
            raise StoreSerializationError(
                f"serialization roundtrip changed the value: {sample!r} -> {restored!r}"
            )

    # -- key/value contract ---------------------------------------------------------------

    def get(self, key: str, namespace: Optional[str] = None) -> Any:
        """Return the value stored at ``key`` in ``namespace``.

        Raises:
            NamespaceBoundaryError: if the store is bound to another namespace.
            diffsync.exceptions.StoreKeyNotFound: if the key is not present.
        """
        raise NotImplementedError

    def set(self, key: str, value: Any, namespace: Optional[str] = None) -> None:
        """Store ``value`` at ``key`` in ``namespace`` (insert or replace)."""
        raise NotImplementedError

    def delete(self, key: str, namespace: Optional[str] = None) -> bool:
        """Delete ``key`` from ``namespace``; deleting a missing key is a no-op.

        Returns:
            True if a value was present and removed, False otherwise.
        """
        raise NotImplementedError

    def keys(self, namespace: Optional[str] = None) -> List[str]:
        """List all keys present in ``namespace``."""
        raise NotImplementedError


# Registry --------------------------------------------------------------------------------------

_REGISTRY: Dict[str, type] = {}
_INSTANCES: "deque[StoreContract]" = deque(maxlen=200)
_GLOBAL_EVENTS: Deque[SwitchEvent] = deque(maxlen=500)
_INSTANCES_LOCK = threading.Lock()


def register_store(name: str) -> Callable[[type], type]:
    """Class decorator registering a store backend under ``name``."""

    def decorator(store_class: type) -> type:
        _REGISTRY[name] = store_class
        return store_class

    return decorator


def get_store_class(store_name: str) -> type:
    """Look up a registered store class by name.

    Raises:
        KeyError: if no backend is registered under ``name``.
    """
    if store_name == "redis" and store_name not in _REGISTRY:
        # Lazy import so the optional redis dependency is only needed for the redis backend.
        import diffsync.store.redis  # noqa: F401  pylint: disable=import-outside-toplevel
    if store_name not in _REGISTRY:
        known = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise KeyError(f"unknown store backend {store_name!r}; registered backends: {known}")
    return _REGISTRY[store_name]


def registered_stores() -> List[str]:
    """Return the sorted names of all registered backends."""
    return sorted(_REGISTRY)


def create_store(store_name: str, **kwargs: Any) -> StoreContract:
    """Instantiate a registered backend by name."""
    if store_name == "redis" and store_name not in _REGISTRY:
        # Lazy import so the optional redis dependency is only needed for the redis backend.
        import diffsync.store.redis  # noqa: F401  pylint: disable=import-outside-toplevel
    return get_store_class(store_name)(**kwargs)


def _register_instance(store: StoreContract) -> None:
    with _INSTANCES_LOCK:
        _INSTANCES.append(store)


def all_stores() -> List[StoreContract]:
    """Return all live store instances known to this process (used by the ``store_status`` command)."""
    with _INSTANCES_LOCK:
        return list(_INSTANCES)


def recent_events(limit: int = 20) -> List[SwitchEvent]:
    """Return the most recent store events recorded across all stores, newest last."""
    events = list(_GLOBAL_EVENTS)
    return events[-limit:]


_decode_tagged = _json_object_hook
