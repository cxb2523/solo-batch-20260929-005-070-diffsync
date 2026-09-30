"""Adapter bridging the model-oriented ``BaseStore`` API onto a key/value Store.

The :class:`Adapter` (formerly ``DiffSync``) works with
:class:`~diffsync.DiffSyncModel` instances through :class:`diffsync.store.BaseStore`.
This bridge maps each ``modelname`` to a namespace and each unique id to a key,
storing the model as plain JSON-compatible data through the unified
:class:`~diffsync.store.contract.Store` contract.  That way both the local and
the degrading Redis backend can power a DiffSync adapter.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Type, Union

from diffsync.exceptions import ObjectAlreadyExists, ObjectNotFound
from diffsync.store import BaseStore
from diffsync.store.contract import Store, create_store

if TYPE_CHECKING:
    from diffsync import Adapter as DiffSyncAdapter
    from diffsync import DiffSyncModel


def _escape_uid(uid: str) -> str:
    """Escape a model unique id into a single valid store key."""
    return uid.replace("\\", "\\\\").replace("/", "\\x2f").replace("\x00", "\\x00") or "_"


class ModelStoreAdapter(BaseStore):
    """Present a :class:`Store` (key/value contract) as a :class:`BaseStore`."""

    def __init__(
        self,
        store: Union[Store, str] = "local",
        *,
        adapter: Optional["DiffSyncAdapter"] = None,
        name: str = "",
        namespace_root: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Wrap a store instance or create one from a registered backend name."""
        if isinstance(store, Store):
            self.kv = store
        else:
            self.kv = create_store(
                store, name=name or f"model-{store}", namespace=(namespace_root or "default"), **kwargs
            )
        if namespace_root is not None:
            self.kv.namespace_root = namespace_root
            if not self.kv.namespace.startswith(namespace_root):
                self.kv.namespace = namespace_root
        super().__init__(adapter=adapter, name=name or self.kv.name)
        self.kv.adapter = adapter  # convenience mirror

    def _bind_adapter(self, adapter: "DiffSyncAdapter") -> None:
        """Attach the owning DiffSync adapter (both here and on the KV store)."""
        self.adapter = adapter
        self.kv.adapter = adapter  # type: ignore[attr-defined]

    def _namespace(self, modelname: str) -> str:
        root = self.kv.namespace_root
        return modelname if root is None else f"{root}/{modelname}"

    def _deserialize(self, object_class: Any, modelname: str, data: Dict[str, Any]) -> "DiffSyncModel":
        if object_class is None:
            raise ObjectNotFound(f"{modelname} is not declared on adapter {self.adapter}; unable to reconstruct object")
        obj = object_class(**data)
        obj.adapter = self.adapter
        return obj

    def get_all_model_names(self) -> Set[str]:
        """Get all model names present in the store."""
        prefix = self.kv.namespace_root
        names: Set[str] = set()
        if prefix is None:
            for namespace in list(self.kv.namespaces() if hasattr(self.kv, "namespaces") else []):
                names.add(namespace)
            return names
        for namespace in self.kv.namespaces() if hasattr(self.kv, "namespaces") else []:
            if namespace.startswith(f"{prefix}/"):
                names.add(namespace[len(prefix) + 1 :])
        return names

    def get(
        self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]], identifier: Union[str, Dict]
    ) -> "DiffSyncModel":
        """Get one object from the store by model and unique id."""
        object_class, modelname = self._get_object_class_and_model(model)
        uid = self._get_uid(model, object_class, identifier)
        data = self.kv.get(_escape_uid(uid), namespace=self._namespace(modelname), default=None)
        if data is None:
            raise ObjectNotFound(f"{modelname} {uid} not present in {str(self)}")
        return self._deserialize(object_class, modelname, data)

    def get_all(self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]]) -> List["DiffSyncModel"]:
        """Get all objects of a given model type."""
        object_class, modelname = self._get_object_class_and_model(model)
        namespace = self._namespace(modelname)
        results: List["DiffSyncModel"] = []
        for key in self.kv.keys(namespace=namespace):
            data = self.kv.get(key, namespace=namespace)
            results.append(self._deserialize(object_class, modelname, data))
        return results

    def get_by_uids(
        self, *, uids: List[str], model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]]
    ) -> List["DiffSyncModel"]:
        """Get multiple objects by uid."""
        object_class, modelname = self._get_object_class_and_model(model)
        namespace = self._namespace(modelname)
        results = []
        for uid in uids:
            data = self.kv.get(_escape_uid(uid), namespace=namespace, default=None)
            if data is None:
                raise ObjectNotFound(f"{modelname} {uid} not present in {str(self)}")
            results.append(self._deserialize(object_class, modelname, data))
        return results

    def add(self, *, obj: "DiffSyncModel") -> None:
        """Store an object, rejecting a different object with the same uid."""
        modelname = obj.get_type()
        uid = obj.get_unique_id()
        key = _escape_uid(uid)
        namespace = self._namespace(modelname)
        existing = self.kv.get(key, namespace=namespace, default=None)
        if existing is not None:
            if existing == self._dump(obj):
                return
            raise ObjectAlreadyExists(f"Object {uid} already present", obj)
        if not obj.adapter:
            obj.adapter = self.adapter
        self.kv.set(key, self._dump(obj), namespace=namespace)

    def update(self, *, obj: "DiffSyncModel") -> None:
        """Overwrite an object's stored data."""
        modelname = obj.get_type()
        uid = obj.get_unique_id()
        if not obj.adapter:
            obj.adapter = self.adapter
        self.kv.set(_escape_uid(uid), self._dump(obj), namespace=self._namespace(modelname))

    @staticmethod
    def _dump(obj: "DiffSyncModel") -> Dict[str, Any]:
        """Serialize a model to plain JSON-friendly data, dropping internal bookkeeping."""
        data = obj.dict()
        data.pop("model_flags", None)
        return data

    def remove_item(self, modelname: str, uid: str) -> None:
        """Remove one item by model name and uid."""
        if not self.kv.delete(_escape_uid(uid), namespace=self._namespace(modelname)):
            raise ObjectNotFound(f"{modelname} {uid} not present in {str(self)}")

    def count(self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"], None] = None) -> int:
        """Count objects of a model or across all models."""
        if model is None:
            return sum(len(self.kv.keys(namespace=self._namespace(name))) for name in self.get_all_model_names())
        if isinstance(model, str):
            modelname = model
        else:
            modelname = model.get_type()
        return len(self.kv.keys(namespace=self._namespace(modelname)))

    def ready(self) -> bool:
        """Proxy the underlying key/value store health probe."""
        return self.kv.ready()

    def status(self) -> Dict[str, Any]:
        """Proxy the underlying key/value store status snapshot."""
        return self.kv.status()
