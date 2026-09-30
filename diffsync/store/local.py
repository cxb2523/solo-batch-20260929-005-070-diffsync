"""LocalStore module."""

import threading
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Type, Union

from diffsync.exceptions import ObjectAlreadyExists, ObjectNotFound
from diffsync.store import BaseStore
from diffsync.store.contract import DEFAULT_NAMESPACE, register_store, validate_namespace

if TYPE_CHECKING:
    from diffsync import DiffSyncModel


@register_store("local")
class LocalStore(BaseStore):
    """Synchronous, zero-dependency, in-process store.

    Key/value data is kept in plain dictionaries partitioned by namespace. A single short-lived
    lock only guards individual read-modify-write operations; reads and writes stay synchronous
    and allocation-free on the hot path.
    """

    def __init__(self, *args: Any, namespace: str = DEFAULT_NAMESPACE, multi_namespace: bool = False, **kwargs: Any) -> None:
        """Init method for LocalStore."""
        super().__init__(*args, namespace=namespace, **kwargs)
        self._data: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._models: Dict[str, Dict[str, "DiffSyncModel"]] = defaultdict(dict)
        self._kv_lock = threading.RLock()
        # Internal use only: a multi-namespace mirror (e.g. RedisStore fallback) serves KV data
        # across namespaces while model data keeps using the store's own namespace.
        self._multi_namespace = multi_namespace

    def _kv_namespace(self, namespace: Optional[str]) -> str:
        """Resolve the target namespace for a KV operation."""
        if self._multi_namespace:
            ns = namespace or self.namespace
            validate_namespace(ns)
            return ns
        return self._resolve_namespace(namespace)

    # -- contract key/value API -----------------------------------------------------------

    def get_item(self, key: str, namespace: Optional[str] = None) -> Any:
        """Return the value stored at ``key`` in ``namespace``."""
        ns = self._kv_namespace(namespace)
        with self._kv_lock:
            if key not in self._data[ns]:
                raise self.missing_key(ns, key, str(self))
            return self._data[ns][key]

    def set(self, key: str, value: Any, namespace: Optional[str] = None) -> None:
        """Store ``value`` at ``key`` in ``namespace``."""
        ns = self._kv_namespace(namespace)
        with self._kv_lock:
            self._data[ns][key] = value

    def delete(self, key: str, namespace: Optional[str] = None) -> bool:
        """Delete ``key`` from ``namespace``; return whether a value was present."""
        ns = self._kv_namespace(namespace)
        with self._kv_lock:
            if key not in self._data[ns]:
                return False
            del self._data[ns][key]
            return True

    def keys(self, namespace: Optional[str] = None) -> List[str]:
        """List all keys present in ``namespace``."""
        ns = self._kv_namespace(namespace)
        with self._kv_lock:
            return list(self._data[ns].keys())

    # -- model-level API -------------------------------------------------------------------

    def get_all_model_names(self) -> Set[str]:
        """Get all the model names stored.

        Return:
            Set of all the model names.
        """
        return set(self._models.keys())

    def get_model(
        self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]], identifier: Union[str, Dict]
    ) -> "DiffSyncModel":
        """Get one object from the data store based on its unique id.

        Args:
            model: DiffSyncModel class or instance, or modelname string, that defines the type of the object to retrieve
            identifier: Unique ID of the object to retrieve, or dict of unique identifier keys/values

        Raises:
            ValueError: if obj is a str and identifier is a dict (can't convert dict into a uid str without a model class)
            ObjectNotFound: if the requested object is not present
        """
        object_class, modelname = self._get_object_class_and_model(model)

        uid = self._get_uid(model, object_class, identifier)

        if uid not in self._models[modelname]:
            raise ObjectNotFound(f"{modelname} {uid} not present in {str(self)}")
        return self._models[modelname][uid]

    def get_all(self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]]) -> List["DiffSyncModel"]:
        """Get all objects of a given type.

        Args:
            model: DiffSyncModel class or instance, or modelname string, that defines the type of the objects to retrieve

        Returns:
            List of Object
        """
        if isinstance(model, str):
            modelname = model
        else:
            modelname = model.get_type()

        return list(self._models[modelname].values())

    def get_by_uids(
        self, *, uids: List[str], model: Union[str, "DiffSyncModel", Type["DiffSyncModel"]]
    ) -> List["DiffSyncModel"]:
        """Get multiple objects from the store by their unique IDs/Keys and type.

        Args:
            uids: List of unique id / key identifying object in the database.
            model: DiffSyncModel class or instance, or modelname string, that defines the type of the objects to retrieve

        Raises:
            ObjectNotFound: if any of the requested UIDs are not found in the store
        """
        if isinstance(model, str):
            modelname = model
        else:
            modelname = model.get_type()

        results = []
        for uid in uids:
            if uid not in self._models[modelname]:
                raise ObjectNotFound(f"{modelname} {uid} not present in {str(self)}")
            results.append(self._models[modelname][uid])
        return results

    def add(self, *, obj: "DiffSyncModel") -> None:
        """Add a DiffSyncModel object to the store.

        Args:
            obj: Object to store

        Raises:
            ObjectAlreadyExists: if a different object with the same uid is already present.
        """
        modelname = obj.get_type()
        uid = obj.get_unique_id()

        existing_obj = self._models[modelname].get(uid)
        if existing_obj:
            if existing_obj is not obj:
                raise ObjectAlreadyExists(f"Object {uid} already present", obj)
            # Return so we don't have to change anything on the existing object and underlying data
            return

        if not obj.adapter:
            obj.adapter = self.adapter

        self._models[modelname][uid] = obj

    def update(self, *, obj: "DiffSyncModel") -> None:
        """Update a DiffSyncModel object to the store.

        Args:
            obj: Object to update
        """
        modelname = obj.get_type()
        uid = obj.get_unique_id()

        existing_obj = self._models[modelname].get(uid)
        if existing_obj is obj:
            return

        self._models[modelname][uid] = obj

    def remove_item(self, modelname: str, uid: str) -> None:
        """Remove one item from store."""
        if uid not in self._models[modelname]:
            raise ObjectNotFound(f"{modelname} {uid} not present in {str(self)}")
        del self._models[modelname][uid]

    def count(self, *, model: Union[str, "DiffSyncModel", Type["DiffSyncModel"], None] = None) -> int:
        """Returns the number of elements of a specific model, or all elements in the store if unspecified."""
        if not model:
            return sum(len(entries) for entries in self._models.values())

        if isinstance(model, str):
            modelname = model
        else:
            modelname = model.get_type()
        return len(self._models[modelname])
