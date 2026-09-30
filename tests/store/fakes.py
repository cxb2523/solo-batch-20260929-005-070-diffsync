"""In-memory Redis double used by the store tests (no external Redis needed)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

try:  # subclass the real exceptions so production code classifies them correctly
    from redis import exceptions as _rex

    RedisErrorBase = _rex.RedisError
    ConnectionErrorBase = _rex.ConnectionError
    TimeoutErrorBase = _rex.TimeoutError
except ImportError:  # pragma: no cover - redis is a test dependency in this repo
    RedisErrorBase = OSError

    class ConnectionErrorBase(OSError):
        """Stand-in connection error when redis-py is unavailable."""

    class TimeoutErrorBase(OSError):
        """Stand-in timeout error when redis-py is unavailable."""


class RedisError(RedisErrorBase):
    """Base class for fake Redis errors, mirroring redis-py hierarchy."""


class ConnectionError(ConnectionErrorBase):  # noqa: A001 - mirrors redis.exceptions.ConnectionError
    """Raised to simulate a connectivity problem."""


class TimeoutError(TimeoutErrorBase):  # noqa: A001 - mirrors redis.exceptions.TimeoutError
    """Raised to simulate a socket timeout."""


class FakePipeline:
    """Ministic pipeline recording queued commands until ``execute()``."""

    def __init__(self, client: "FakeRedis") -> None:
        self._client = client
        self._commands: List[Tuple[str, tuple]] = []

    def set(self, key: str, value: bytes) -> "FakePipeline":
        """Queue a SET."""
        self._commands.append(("set", (key, value)))
        return self

    def delete(self, *keys: str) -> "FakePipeline":
        """Queue a DELETE."""
        self._commands.append(("delete", tuple(keys)))
        return self

    def execute(self) -> list:
        """Execute all queued commands in order and return their results."""
        results = []
        for command, args in self._commands:
            results.append(getattr(self._client, command)(*args))
        self._commands.clear()
        return results


class FakeRedis:
    """Thread-safe in-memory stand-in for a redis-py client."""

    def __init__(self, url: Optional[str] = None, **kwargs: Any) -> None:
        self.url = url
        self.kwargs = kwargs
        self._data: Dict[str, bytes] = {}
        self.connected = True
        self.fail_until = 0  # when set, operations before this many successes fail
        self.calls: List[str] = []
        self._pipeline_history: List[List[Tuple[str, tuple]]] = []

    # -- fault injection helpers ------------------------------------------

    def disconnect(self) -> None:
        """Simulate Redis going away."""
        self.connected = False

    def reconnect(self) -> None:
        """Simulate Redis coming back."""
        self.connected = True

    def _check(self, action: str) -> None:
        self.calls.append(action)
        if not self.connected:
            raise ConnectionError(f"fake redis is disconnected during {action}")

    # -- redis-py API subset ----------------------------------------------

    def ping(self) -> bool:
        """Health probe."""
        self._check("ping")
        return True

    def get(self, key: str) -> Optional[bytes]:
        """Read a key."""
        self._check("get")
        return self._data.get(key)

    def set(self, key: str, value: bytes) -> bool:
        """Write a key."""
        self._check("set")
        self._data[key] = bytes(value)
        return True

    def delete(self, *keys: str) -> int:
        """Delete keys, returning how many existed."""
        self._check("delete")
        removed = 0
        for key in keys:
            if key in self._data:
                del self._data[key]
                removed += 1
        return removed

    def exists(self, key: str) -> int:
        """Check key existence."""
        self._check("exists")
        return int(key in self._data)

    def scan_iter(self, pattern: str) -> List[str]:
        """Extremely small glob matcher supporting a single trailing ``*``."""
        self._check("scan_iter")
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            return sorted(key for key in self._data if key.startswith(prefix))
        return [pattern] if pattern in self._data else []

    def pipeline(self) -> FakePipeline:
        """Return a pipeline that records itself on execution."""
        pipeline = FakePipeline(self)
        original_execute = pipeline.execute

        def tracked_execute() -> list:
            self._pipeline_history.append(list(pipeline._commands))  # noqa: SLF001
            return original_execute()

        pipeline.execute = tracked_execute  # type: ignore[method-assign]
        return pipeline

    @property
    def pipeline_history(self) -> List[List[Tuple[str, tuple]]]:
        """Recorded command batches executed through pipelines."""
        return self._pipeline_history

    def close(self) -> None:
        """Mark the client closed."""
        self.connected = False
