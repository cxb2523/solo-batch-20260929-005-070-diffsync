"""Serialization tests: JSON default, opt-in pickle, round-trip and legacy data."""

from __future__ import annotations

import pickle

import pytest

from diffsync.exceptions import SerializationError
from diffsync.store.serializer import JSON_MARKER, PICKLE_MARKER, Serializer


def test_json_is_default_envelope():
    serializer = Serializer()
    encoded = serializer.encode({"name": "site-1", "vlan": 100, "tags": ["a", "b"]})
    assert encoded.startswith(JSON_MARKER.encode())
    assert serializer.decode(encoded) == {"name": "site-1", "vlan": 100, "tags": ["a", "b"]}


def test_non_json_value_rejected_without_pickle():
    serializer = Serializer()
    with pytest.raises(SerializationError):
        serializer.encode({1, 2, 3})  # sets are not JSON serializable


def test_pickle_requires_explicit_opt_in_for_decode():
    pickled = PICKLE_MARKER.encode() + pickle.dumps({1, 2, 3}).hex().encode()  # not base64 - decode path guarded
    with pytest.raises(SerializationError):
        Serializer(allow_pickle=False).decode(pickled)


def test_pickle_used_when_explicitly_enabled():
    serializer = Serializer(allow_pickle=True)
    value = {1, 2, 3}
    encoded = serializer.encode(value)
    assert encoded.startswith(PICKLE_MARKER.encode())
    assert serializer.decode(encoded) == value


def test_roundtrip_check_detects_lossy_serialization(monkeypatch):
    serializer = Serializer()

    def broken_json_dumps(value, **kwargs):  # noqa: ANN001
        return "{}"

    monkeypatch.setattr("diffsync.store.serializer.json.dumps", broken_json_dumps)
    with pytest.raises(SerializationError, match="round-trip"):
        serializer.encode({"a": 1})


def test_tuples_fail_roundtrip_under_json():
    # JSON silently turns tuples into lists, so the round-trip guard must reject them.
    with pytest.raises(SerializationError, match="round-trip"):
        Serializer().encode(("a", "b"))


def test_legacy_bare_json_is_still_readable():
    legacy = b'{"legacy": true, "count": 3}'
    assert Serializer().decode(legacy) == {"legacy": True, "count": 3}


def test_legacy_bare_pickle_readable_only_when_allowed():
    legacy = pickle.dumps({"old": (1, 2)})
    with pytest.raises(SerializationError):
        Serializer().decode(legacy)
    assert Serializer(allow_pickle=True).decode(legacy) == {"old": (1, 2)}


def test_corrupt_payload_raises():
    with pytest.raises(SerializationError):
        Serializer(allow_pickle=True).decode(b"diffsync-pickle:v1:not-base64!!")
