"""Exception classes used in DiffSync.

Copyright (c) 2020 Network To Code, LLC <info@networktocode.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from diffsync import DiffSyncModel
    from diffsync.diff import DiffElement


class ObjectCrudException(Exception):
    """Base class for various failures during CRUD operations."""


class ObjectNotCreated(ObjectCrudException):
    """Exception raised if an object Create operation failed."""


class ObjectNotUpdated(ObjectCrudException):
    """Exception raised if an object Update operation failed."""


class ObjectNotDeleted(ObjectCrudException):
    """Exception raised if an object Delete operation failed."""


class ObjectStoreException(Exception):
    """Base class for various failures during object storage in local caches."""


class ObjectAlreadyExists(ObjectStoreException):
    """Exception raised when trying to store a DiffSyncModel or DiffElement that is already being stored."""

    def __init__(self, message: str, existing_object: Union["DiffSyncModel", "DiffElement"], *args: Any, **kwargs: Any):
        """Add existing_object to the exception to provide user with existing object."""
        self.existing_object = existing_object
        super().__init__(message, existing_object, *args, **kwargs)


class ObjectNotFound(ObjectStoreException):
    """Exception raised when trying to access a DiffSyncModel that isn't in storage."""


class ObjectStoreWrongType(ObjectStoreException):
    """Exception raised when trying to store a DiffSyncModel of the wrong type."""


class StoreKeyNotFound(ObjectStoreException, KeyError):
    """Exception raised when a store contract get/delete references a key that is not present.

    Inherits from both ObjectStoreException and KeyError so that callers can handle it either as a
    diffsync store error or as a standard mapping miss.
    """


class NamespaceBoundaryError(ObjectStoreException):
    """Exception raised when an operation crosses a store namespace boundary."""


class StoreBackendUnavailable(ObjectStoreException):
    """Exception raised when a remote store backend cannot be reached."""


class StoreSerializationError(ObjectStoreException):
    """Exception raised when a value cannot be (de)serialized by the chosen store codec."""


class DiffException(Exception):
    """Base class for various failures related to Diff operations."""


class DiffClassMismatch(DiffException):
    """Exception raised when a diff object is not the same as the expected diff_class."""
