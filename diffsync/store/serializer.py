"""Serialization codecs for store backends.

JSON is the default (human-readable, portable and safe).  Pickle support is
opt-in only (``allow_pickle=True``) because it can execute arbitrary code on
load.  Every serialized value is wrapped so that later readers can:

* decode plain JSON values by default,
* fall back to pickle only when pickle was explicitly enabled and the payload
  carries the pickle marker,
* still read legacy data: bare JSON documents and bare pickled objects produced
  before this wrapper format existed.

Every ``encode`` performs a round-trip check (encode -> decode -> compare) so
that a lossy or unreproducible serialization fails loudly instead of silently
storing data that cannot be read back.
"""

from __future__ import annotations

import base64
import enum
import json
import pickle
from typing import Any, Optional

from diffsync.exceptions import SerializationError

#: Payload envelope version marker prefixes; the payload is otherwise opaque.
JSON_MARKER = "diffsync-json:v1:"
PICKLE_MARKER = "diffsync-pickle:v1:"


def _json_default(value: Any) -> Any:
    """JSON fallback: represent enums by their value instead of raising."""
    if isinstance(value, enum.Enum):
        return value.value
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class Serializer:
    """Encode/decode values for a backend.

    Args:
        allow_pickle: If ``False`` (default), non-JSON-serializable values raise
            :class:`~diffsync.exceptions.SerializationError` on encode and
            pickle payloads raise on decode.  If ``True``, pickle is used only
            when JSON serialization fails.
        verify_roundtrip: When ``True`` (default), re-decode the encoded value
            and compare it to the input to guarantee the round trip is exact.
    """

    def __init__(self, allow_pickle: bool = False, verify_roundtrip: bool = True) -> None:
        """Configure the codec behavior."""
        self.allow_pickle = allow_pickle
        self.verify_roundtrip = verify_roundtrip

    def encode(self, value: Any) -> bytes:
        """Serialize ``value`` to bytes, verifying the round trip."""
        try:
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, default=_json_default).encode("utf-8")
            payload = JSON_MARKER.encode("ascii") + encoded
        except (TypeError, ValueError) as exc:
            if not self.allow_pickle:
                raise SerializationError(
                    f"Value of type {type(value).__name__} is not JSON serializable; "
                    "enable pickle explicitly with allow_pickle=True"
                ) from exc
            payload = PICKLE_MARKER.encode("ascii") + self._pickle_dumps(value)

        if self.verify_roundtrip:
            try:
                decoded = self.decode(payload)
            except Exception as exc:  # noqa: BLE001 - surface round-trip failure context
                raise SerializationError("Serialized value failed the round-trip check (undecodable)") from exc
            if decoded != value:
                raise SerializationError(f"Serialized value failed the round-trip check: {value!r} != {decoded!r}")
        return payload

    def decode(self, raw: Optional[bytes]) -> Any:
        """Deserialize bytes produced by :meth:`encode`, honoring legacy data."""
        if raw is None:
            return None
        if isinstance(raw, str):  # tolerant clients / fakes
            raw = raw.encode("utf-8")
        if raw.startswith(JSON_MARKER.encode("ascii")):
            return json.loads(raw[len(JSON_MARKER) :].decode("utf-8"))
        if raw.startswith(PICKLE_MARKER.encode("ascii")):
            if not self.allow_pickle:
                raise SerializationError("Refusing to decode a pickle payload unless allow_pickle=True is set")
            return self._pickle_loads(raw[len(PICKLE_MARKER) :])

        # Legacy data: try bare JSON first, then legacy bare pickle.
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
            if not self.allow_pickle:
                raise SerializationError(
                    "Unrecognized non-JSON payload (likely legacy pickle data); set allow_pickle=True to read it"
                )
            try:
                return pickle.loads(raw)  # noqa: S301
            except Exception as exc:  # noqa: BLE001
                raise SerializationError("Unable to unpickle stored payload") from exc

    @staticmethod
    def _pickle_dumps(value: Any) -> bytes:
        try:
            return base64.b64encode(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))
        except Exception as exc:  # noqa: BLE001
            raise SerializationError(f"Unable to pickle value of type {type(value).__name__}") from exc

    @staticmethod
    def _pickle_loads(raw: bytes) -> Any:
        try:
            return pickle.loads(base64.b64decode(raw))  # noqa: S301
        except Exception as exc:  # noqa: BLE001
            raise SerializationError("Unable to unpickle stored payload") from exc
