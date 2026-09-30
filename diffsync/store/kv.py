"""Synchronous in-memory implementation of the unified store contract."""

from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from diffsync.store.contract import Store, register_store


class LocalStore(Store):
    """Thread-safe in-process key/value store.

    Writes are applied directly to a plain dictionary behind a re-entrant
    lock: there is no network, serialization or other per-operation overhead,
    keeping the default backend effectively zero-cost.
    """

    def __init__(self, namespace: str = "default", namespace_root: Optional[str] = None, name: str = "") -> None:
        """Initialize an empty local store."""
        self._data: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        super().__init__(namespace=namespace, namespace_root=namespace_root, name=name)

    def get(self, key: str, namespace: Optional[str] = None, default: Any = None) -> Any:
        """Return the value for ``key`` or ``default``."""
        nspace = self._resolve_namespace(namespace)
        with self._lock:
            return self._data.get(nspace, {}).get(key, default)

    def set(self, key: str, value: Any, namespace: Optional[str] = None) -> None:
        """Store ``value`` under ``key``."""
        nspace = self._resolve_namespace(namespace)
        self.namespace_key(namespace, key)
        with self._lock:
            self._data.setdefault(nspace, {})[key] = value

    def delete(self, key: str, namespace: Optional[str] = None) -> bool:
        """Delete ``key`` if present."""
        nspace = self._resolve_namespace(namespace)
        self.namespace_key(namespace, key)
        with self._lock:
            bucket = self._data.get(nspace)
            if bucket is None or key not in bucket:
                return False
            del bucket[key]
            return True

    def keys(self, namespace: Optional[str] = None) -> List[str]:
        """List keys stored directly in the namespace."""
        nspace = self._resolve_namespace(namespace)
        with self._lock:
            return list(self._data.get(nspace, {}))

    def namespaces(self) -> List[str]:
        """List namespaces that currently hold data."""
        with self._lock:
            return [nspace for nspace, bucket in self._data.items() if bucket]

    def clear(self, namespace: Optional[str] = None) -> None:
        """Empty the namespace."""
        nspace = self._resolve_namespace(namespace)
        with self._lock:
            self._data.pop(nspace, None)


register_store("local", LocalStore, override=True)
