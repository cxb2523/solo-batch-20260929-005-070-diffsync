"""Unified store contract for DiffSync backends.

This module defines the :class:`Store` contract shared by every key/value storage
backend (local memory, Redis, ...):

* ``get`` / ``set`` / ``delete`` (plus ``keys`` / ``count`` / ``clear``)
* namespace isolation: every store instance is bound to a namespace and
  :class:`~diffsync.exceptions.NamespaceViolation` is raised for empty, malformed
  or boundary-crossing namespaces
* a :meth:`Store.ready` probe that distinguishes the three operational states
  :class:`StoreState` (``READY`` / ``DEGRADED`` / ``RECOVERING``)
* a backend registry (:func:`register_store` / :func:`create_store`) allowing a
  store to be referenced by name, for example from the ``Adapter`` constructor
* an in-process registry of live store instances used by the ``store_status``
  command

Backends must never silently swallow backend errors.  They record an
:class:`StoreEvent`, expose it through :meth:`Store.status` and transition the
state explicitly (for example ``RedisStore`` degrading to an in-memory mirror).
"""

from __future__ import annotations

import itertools
import re
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Deque, Dict, List, Optional, Type

import structlog  # type: ignore

from diffsync.exceptions import BackendNotFound, NamespaceViolation

#: Namespaces are non-empty tokens restricted to common path characters so that
#: backends can safely use them as key prefixes without any escaping ambiguity.
NAMESPACE_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


class StoreState(str, Enum):
    """Operational state of a store backend."""

    READY = "ready"
    """The backend is fully operational and serves requests from its real storage."""

    DEGRADED = "degraded"
    """The backend is unavailable; requests are served by a fallback (e.g. memory)."""

    RECOVERING = "recovering"
    """The backend came back; buffered writes are being merged back in order."""


@dataclass
class StoreEvent:
    """A state-transition or error event emitted by a store."""

    timestamp: float
    from_state: Optional[StoreState]
    to_state: StoreState
    reason: str
    detail: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        """Render the event as a JSON-friendly dictionary."""
        return {
            "timestamp": self.timestamp,
            "from": self.from_state.value if self.from_state else None,
            "to": self.to_state.value,
            "reason": self.reason,
            "detail": self.detail,
        }


@dataclass
class Operation:
    """An ordered write operation buffered while a backend is degraded.

    A monotonically increasing sequence number keeps writes from the same
    namespace totally ordered, which allows backends to replay them back to the
    real storage in the exact order they were observed.
    """

    seq: int
    namespace: str
    key: str
    op: str
    value: Any = None


def validate_namespace(namespace: str) -> None:
    """Validate a (possibly nested) namespace string.

    Args:
        namespace: Namespace, e.g. ``"default"`` or ``"adapter-a/site"``.

    Raises:
        NamespaceViolation: if the namespace is empty, malformed or escapes its
            boundary (``..`` segments or leading/trailing separators).
    """
    if not isinstance(namespace, str) or not namespace:
        raise NamespaceViolation("Namespace must be a non-empty string")
    if namespace.startswith("/") or namespace.endswith("/") or "//" in namespace:
        raise NamespaceViolation(f"Malformed namespace: {namespace!r}")
    segments = namespace.split("/")
    if any(segment in ("", ".", "..") for segment in segments):
        raise NamespaceViolation(f"Namespace {namespace!r} crosses its boundary")
    if any(not NAMESPACE_RE.match(segment) for segment in segments):
        raise NamespaceViolation(
            f"Namespace {namespace!r} contains invalid characters; allowed: letters, digits, '_', '.', '-', '/'"
        )


def is_within_namespace(namespace: str, child: str) -> bool:
    """Return whether ``child`` is the same as or nested under ``namespace``."""
    validate_namespace(namespace)
    validate_namespace(child)
    return child == namespace or child.startswith(f"{namespace}/")


def _validate_key(key: str) -> None:
    """Validate a storage key."""
    if not isinstance(key, str) or not key:
        raise NamespaceViolation("Key must be a non-empty string")
    if key in (".", "..") or "/" in key or "\x00" in key:
        raise NamespaceViolation(f"Key {key!r} is invalid: it must not contain '/' or NUL or be a '.'/'..' component")


class Store(ABC):
    """Abstract key/value store contract.

    A store is bound to a ``namespace`` (default ``"default"``) and optionally
    to a ``namespace_root`` restricting which namespaces it may serve.  The
    :meth:`namespace`, :meth:`get`, :meth:`set` and :meth:`delete` methods all
    accept an explicit per-call ``namespace`` which must be nested inside the
    root, otherwise :class:`~diffsync.exceptions.NamespaceViolation` is raised.

    Attributes:
        name: Human-readable backend instance name.
        backend_name: Name under which the backend class is registered.
    """

    def __init__(self, namespace: str = "default", namespace_root: Optional[str] = None, name: str = "") -> None:
        """Initialize the store and validate its namespace boundaries."""
        validate_namespace(namespace)
        if namespace_root is not None:
            validate_namespace(namespace_root)
            if not is_within_namespace(namespace_root, namespace):
                raise NamespaceViolation(
                    f"Default namespace {namespace!r} is outside of the allowed root {namespace_root!r}"
                )
        self.name = name or self.__class__.__name__
        self.backend_name = getattr(self, "backend_name", self.__class__.__name__.lower().removesuffix("store"))
        self.namespace = namespace
        self.namespace_root = namespace_root
        self._state = StoreState.READY
        self._events: Deque[StoreEvent] = deque(maxlen=100)
        self._seq_counter = itertools.count(1)
        self._events_lock = threading.RLock()
        self._log = structlog.get_logger().bind(store=self.name, backend=self.backend_name)
        register_live_store(self)

    def __str__(self) -> str:
        """Render the store instance."""
        return f"{self.backend_name}:{self.name}"

    # -- namespace helpers -------------------------------------------------

    def _resolve_namespace(self, namespace: Optional[str]) -> str:
        resolved = namespace if namespace is not None else self.namespace
        validate_namespace(resolved)
        if self.namespace_root is not None and not is_within_namespace(self.namespace_root, resolved):
            raise NamespaceViolation(f"Namespace {resolved!r} is outside of the allowed root {self.namespace_root!r}")
        return resolved

    def namespace_key(self, namespace: Optional[str], key: str) -> str:
        """Return the fully-qualified ``namespace/key`` identifier after checks."""
        _validate_key(key)
        return f"{self._resolve_namespace(namespace)}/{key}"

    # -- state / observability --------------------------------------------

    @property
    def state(self) -> StoreState:
        """Current operational state."""
        return self._state

    def ready(self) -> bool:
        """Health probe.

        Returns:
            ``True`` only when the backend serves requests from its real
            storage (:attr:`StoreState.READY`).  ``False`` covers both
            ``DEGRADED`` (fallback in use) and ``RECOVERING`` (merge in flight).
        """
        return self._state is StoreState.READY

    def _set_state(self, new_state: StoreState, reason: str, **detail: Any) -> StoreEvent:
        """Transition to a new state and append an observable event."""
        with self._events_lock:
            previous = self._state
            event = StoreEvent(
                timestamp=time.time(), from_state=previous, to_state=new_state, reason=reason, detail=detail
            )
            self._state = new_state
            self._events.append(event)
        level = self._log.warning if new_state is not StoreState.READY else self._log.info
        level("store-state-change", **event.as_dict())
        return event

    def recent_events(self, limit: Optional[int] = None) -> List[StoreEvent]:
        """Return the most recent state-transition events, newest last."""
        with self._events_lock:
            events = list(self._events)
        return events[-limit:] if limit else events

    def status(self) -> Dict[str, Any]:
        """Return a JSON-friendly snapshot of the store status."""
        with self._events_lock:
            events = [event.as_dict() for event in self._events]
        return {
            "name": self.name,
            "backend": self.backend_name,
            "namespace": self.namespace,
            "namespace_root": self.namespace_root,
            "state": self._state.value,
            "ready": self.ready(),
            "pending_writes": self._pending_count(),
            "last_event": events[-1] if events else None,
            "recent_events": events[-5:],
        }

    # -- abstract CRUD -----------------------------------------------------

    @abstractmethod
    def get(self, key: str, namespace: Optional[str] = None, default: Any = None) -> Any:
        """Return the value stored under ``key`` or ``default`` when missing."""

    @abstractmethod
    def set(self, key: str, value: Any, namespace: Optional[str] = None) -> None:
        """Store ``value`` under ``key`` within the namespace."""

    @abstractmethod
    def delete(self, key: str, namespace: Optional[str] = None) -> bool:
        """Delete ``key``; return whether something was deleted."""

    @abstractmethod
    def keys(self, namespace: Optional[str] = None) -> List[str]:
        """List the (unqualified) keys present in the namespace."""

    @abstractmethod
    def namespaces(self) -> List[str]:
        """List all namespaces that currently hold data."""

    @abstractmethod
    def clear(self, namespace: Optional[str] = None) -> None:
        """Remove every key from the namespace."""

    def count(self, namespace: Optional[str] = None) -> int:
        """Return the number of keys stored in the namespace."""
        return len(self.keys(namespace))

    def __contains__(self, key: str) -> bool:
        """Support ``key in store`` checks against the default namespace."""
        sentinel = object()
        return self.get(key, default=sentinel) is not sentinel

    # -- hooks for subclasses ---------------------------------------------

    def _pending_count(self) -> int:
        """Number of buffered writes awaiting replay (0 unless degraded)."""
        return 0

    def close(self) -> None:  # pragma: no cover - overridden where relevant
        """Release backend resources and unregister the live instance."""
        unregister_live_store(self)


# ---------------------------------------------------------------------------
# Backend registry (load by name) and live-instance tracking
# ---------------------------------------------------------------------------

_BACKENDS: Dict[str, Type[Store]] = {}
_LIVE_STORES: Dict[str, "Store"] = {}
_REGISTRY_LOCK = threading.RLock()


def register_store(name: str, store_cls: Type[Store], *, override: bool = False) -> None:
    """Register a store backend class under ``name``."""
    with _REGISTRY_LOCK:
        if name in _BACKENDS and not override and _BACKENDS[name] is not store_cls:
            raise ValueError(f"Store backend {name!r} is already registered to {_BACKENDS[name]!r}")
        _BACKENDS[name] = store_cls
        if not hasattr(store_cls, "backend_name"):
            store_cls.backend_name = name


def registered_stores() -> List[str]:
    """Return the names of all registered backends."""
    with _REGISTRY_LOCK:
        return sorted(_BACKENDS)


def get_store_class(name: str) -> Type[Store]:
    """Look up a registered backend class by name."""
    try:
        return _BACKENDS[name]
    except KeyError as exc:
        available = ", ".join(registered_stores()) or "<none>"
        raise BackendNotFound(f"Unknown store backend {name!r}; registered backends: {available}") from exc


def create_store(backend: Any = "local", *, name: str = "", namespace: str = "default", **kwargs: Any) -> Store:
    """Instantiate a store from a backend name or return a ready instance.

    Args:
        backend: Registered backend name (``"local"``/``"redis"``) or an
            existing :class:`Store` instance (returned as-is).
        name: Optional explicit instance name used by ``store_status``.
        namespace: Default namespace bound to the store.
        **kwargs: Forwarded to the backend constructor.
    """
    if isinstance(backend, Store):
        return backend
    store_cls = get_store_class(backend)
    kwargs.setdefault("name", name or str(backend))
    return store_cls(namespace=namespace, **kwargs)


def register_live_store(store: Store) -> None:
    """Track a live store instance so ``store_status`` can report on it."""
    with _REGISTRY_LOCK:
        unique_name = store.name
        suffix = 1
        while unique_name in _LIVE_STORES and _LIVE_STORES[unique_name] is not store:
            suffix += 1
            unique_name = f"{store.name}#{suffix}"
        store.name = unique_name
        _LIVE_STORES[unique_name] = store


def unregister_live_store(store: Store) -> None:
    """Stop tracking a store instance."""
    with _REGISTRY_LOCK:
        _LIVE_STORES.pop(store.name, None)


def live_stores() -> List[Store]:
    """Return all tracked live store instances."""
    with _REGISTRY_LOCK:
        return list(_LIVE_STORES.values())
