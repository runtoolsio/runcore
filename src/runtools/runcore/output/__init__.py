"""
Output storage layer.

Each environment can have one or more output backends (file, S3, etc.) for storing and reading
job instance output. The storage type is driven by environment configuration.

Key Components:
    OutputLine: Canonical structured output event (message, timestamp, level, fields).
    OutputBackend: ABC for reading stored output.
    MultiSourceOutputReader: Tries backends in priority order.
    OutputConfig / OutputStorageConfig: Pydantic configuration models.

Factory Functions:
    load_output_module: Dynamically loads an output backend module (e.g., file).
    create_backends: Creates all enabled backends from storage configs.

Backend Module Contract:
    Each backend module must expose:
        create_backend(env_id, config) -> OutputBackend

See Also:
    runtools.runcore.output.file: File/JSONL implementation.
"""

import importlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import Any, List, Optional, Dict, Iterable
from urllib.parse import urlparse, unquote
from urllib.request import pathname2url

from itertools import count
from pydantic import BaseModel, ConfigDict, Field

from runtools.runcore import util
from runtools.runcore.err import InvalidStateError, RuntoolsException

log = logging.getLogger(__name__)


class Mode(Enum):
    HEAD = auto()
    TAIL = auto()


@dataclass(frozen=True)
class OutputLocation:
    """URI-based descriptor for output storage location (e.g. ``file:///path/to/output.jsonl``).

    Single ``uri`` field encodes both the backend type (scheme) and the resource address.
    """
    uri: str

    @staticmethod
    def for_file(path: Path) -> 'OutputLocation':
        """Create an OutputLocation from a file path."""
        return OutputLocation(uri="file://" + pathname2url(str(path)))

    @property
    def scheme(self) -> str:
        return urlparse(self.uri).scheme

    @property
    def is_file(self) -> bool:
        return self.scheme == "file"

    def as_path(self) -> Path:
        """Return the file path. Raises ValueError if not a file:// URI."""
        if not self.is_file:
            raise ValueError(f"Not a file URI: {self.uri}")
        return Path(unquote(urlparse(self.uri).path))

    def serialize(self) -> str:
        return self.uri

    @classmethod
    def deserialize(cls, data: str) -> 'OutputLocation':
        return cls(uri=data)

    def __str__(self) -> str:
        return self.uri


class Output(ABC):

    @abstractmethod
    def tail(self, mode: Mode = Mode.TAIL, max_lines: int = 0):
        pass

    @property
    @abstractmethod
    def locations(self):
        """

        Returns:

        """
        pass


def parse_timestamp(raw) -> Optional[datetime]:
    """Parse a timestamp into a timezone-aware datetime.

    Accepts common formats liberally:
        2026-04-21T02:05:31.062Z          ISO 8601 with Z
        2026-04-21T02:05:31.062+00:00     ISO 8601 with offset
        2026-04-21T02:05:31.062+05:30     ISO 8601 with non-UTC offset
        2026-04-21 02:05:31,062           Python logging (space + comma millis)
        2026-04-21T02:05:31               No fractional seconds
        2026-04-21 02:05:31               Space separator, no millis
        03:34:49.026                      Time-only (today's date assumed)
        1777196905123                     Epoch milliseconds (numeric or string)

    Returns None if the value cannot be parsed.
    """
    if isinstance(raw, (int, float)):
        try:
            epoch_s = raw / 1000 if raw > 1e12 else raw
            return datetime.fromtimestamp(epoch_s, tz=timezone.utc)
        except (OSError, ValueError, OverflowError):
            return None

    s = str(raw).strip()

    # Numeric string: epoch millis or seconds
    if s.isdigit() and len(s) >= 10:
        try:
            epoch_s = int(s) / 1000 if len(s) >= 13 else int(s)
            return datetime.fromtimestamp(epoch_s, tz=timezone.utc)
        except (OSError, ValueError, OverflowError):
            return None

    s = s.replace(',', '.')
    # Time-only: prepend today's date
    if len(s) <= 12 and s[0].isdigit() and ':' in s[:3] and '-' not in s:
        s = datetime.now(timezone.utc).strftime('%Y-%m-%d') + 'T' + s
    elif ' ' in s[:11] and 'T' not in s[:11]:
        s = s.replace(' ', 'T', 1)
    try:
        if s.endswith('Z'):
            return datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc)
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, IndexError):
        return None


def format_timestamp(dt: datetime) -> str:
    """Format a datetime to canonical ISO 8601 UTC string for storage.

    Naive datetimes are treated as UTC to avoid silent local-time shifts.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts = dt.astimezone(timezone.utc).isoformat(timespec='milliseconds')
    return ts[:-6] + 'Z' if ts.endswith('+00:00') else ts


# Top-level keys reserved for canonical OutputLine fields. User-supplied fields
# that collide with these have undefined behavior (last write wins). The `_rt`
# namespace holds runtools-internal metadata (ordinal, source/phase, error flag).
_RESERVED_KEYS = frozenset({
    "_rt", "message", "timestamp", "level", "logger", "thread",
    "msg", "ts", "lvl",  # short aliases accepted on read
})


@dataclass(frozen=True)
class OutputLine:
    """A single structured output event from a job.

    Canonical envelope fields (message, timestamp, level, logger) are first-class.
    Application-specific fields live in the flat ``fields`` dict.
    Framework metadata (ordinal, source, error flag) is internal to runtools.
    """

    message: str
    ordinal: int
    is_error: bool = False
    source: Optional[str] = None
    timestamp: Optional[datetime] = None
    level: Optional[str] = None
    logger: Optional[str] = None
    thread: Optional[str] = None
    fields: Optional[Dict[str, Any]] = None

    @property
    def has_tracking(self) -> bool:
        """Whether this line contains any rt_ tracking fields."""
        return bool(self.fields and any(k.startswith('rt_') for k in self.fields))

    @property
    def is_tracking_only(self) -> bool:
        """Whether this line carries only tracking data with no human-readable message."""
        return not self.message.strip() and self.has_tracking

    @property
    def tracking_fields(self) -> Optional[Dict[str, Any]]:
        """Return only the rt_ tracking fields, or None if there are none."""
        if not self.fields:
            return None
        tracking = {k: v for k, v in self.fields.items() if k.startswith('rt_')}
        return tracking or None

    def with_fields(self, fields: Optional[Dict[str, Any]]) -> 'OutputLine':
        """Return a copy with replaced fields."""
        return replace(self, fields=fields)

    def without_tracking_fields(self) -> 'OutputLine':
        """Return a copy with rt_ tracking fields stripped from fields."""
        if not self.fields:
            return self
        clean = {k: v for k, v in self.fields.items() if not k.startswith('rt_')}
        return replace(self, fields=clean or None) if len(clean) != len(self.fields) else self

    @classmethod
    def deserialize(cls, data: dict) -> 'OutputLine':
        rt = data.get("_rt", {})
        fields = {k: v for k, v in data.items() if k not in _RESERVED_KEYS}
        raw_ts = data.get("timestamp") or data.get("ts")
        return cls(
            message=data.get("message") or data.get("msg", ""),
            ordinal=rt.get("n", 0),
            timestamp=parse_timestamp(raw_ts) if raw_ts else None,
            level=data.get("level") or data.get("lvl"),
            logger=data.get("logger"),
            thread=data.get("thread"),
            is_error=rt.get("err", False),
            source=rt.get("src"),
            fields=fields or None,
        )

    def serialize(self, truncate_length: Optional[int] = None, truncated_suffix: str = ".. (truncated)"):
        message = util.truncate(self.message, truncate_length, truncated_suffix) if truncate_length is not None else self.message
        rt = {"n": self.ordinal}
        if self.source is not None:
            rt["src"] = self.source
        if self.is_error:
            rt["err"] = True
        data = {"message": message, "_rt": rt}
        if self.timestamp is not None:
            data["timestamp"] = format_timestamp(self.timestamp)
        if self.level is not None:
            data["level"] = self.level
        if self.logger is not None:
            data["logger"] = self.logger
        if self.thread is not None:
            data["thread"] = self.thread
        if self.fields:
            data.update(self.fields)
        return data


class OutputLineFactory:
    def __init__(self, default_source=None):
        self.default_source = default_source
        self._counter = count(1)

    def __call__(self, message, is_error=False, source=None,
                 timestamp: str | datetime | None = None,
                 level=None, logger=None, thread=None, fields=None) -> OutputLine:
        ordinal = next(self._counter)
        if isinstance(timestamp, str):
            timestamp = parse_timestamp(timestamp)
        elif isinstance(timestamp, datetime) and timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return OutputLine(message, ordinal, is_error, source or self.default_source, timestamp, level, logger, thread, fields)

    def __getstate__(self):
        return {'default_source': self.default_source, 'counter_value': next(self._counter)}

    def __setstate__(self, state):
        self.default_source = state['default_source']
        self._counter = count(state['counter_value'])


class OutputObserver(ABC):

    def new_output(self, output_line):
        pass


class TailBuffer(ABC):

    def add_line(self, line: OutputLine):
        pass

    def get_lines(self, mode: Mode = Mode.TAIL, max_lines: int = 0) -> List[OutputLine]:
        pass


class TailNotSupportedError(InvalidStateError):
    pass


class OutputReadError(InvalidStateError):
    """Raised when stored output exists but cannot be read (corrupt data, I/O errors)."""

    def __init__(self, source: str, cause: Exception):
        super().__init__(f"Failed to read output from {source}: {cause}")
        self.source = source
        self.__cause__ = cause


class OutputBackend(ABC):
    """Access to stored output. Created from config, used by connectors."""

    @property
    @abstractmethod
    def type(self) -> str:
        """Backend type identifier (e.g., "file")."""

    @abstractmethod
    def read_output(self, instance_id, sources: set[str] | None = None,
                    max_lines: int = 0) -> List['OutputLine']:
        """Read stored output lines for an instance."""

    def delete_output(self, *instance_ids) -> None:
        """Delete stored output for the given instances. No-op by default."""

    def close(self):
        """Release any resources held by this backend. No-op by default."""


class MultiSourceOutputReader:
    """Reads output by trying backends in priority order, returning the first non-empty result."""

    def __init__(self, backends: Iterable[OutputBackend] = ()):
        self._backends = tuple(backends)

    def read_output(self, instance_id, sources: set[str] | None = None,
                    max_lines: int = 0) -> List['OutputLine']:
        """Read output lines for an instance.

        Raises:
            OutputReadError: If output exists but all backends fail to read it.
        """
        last_error = None
        for backend in self._backends:
            try:
                lines = backend.read_output(instance_id, sources, max_lines)
            except OutputReadError as e:
                log.warning("Backend read failed, trying next", extra={"detail": str(e)})
                last_error = e
                continue
            if lines:
                return lines
            last_error = None
        if last_error:
            raise last_error
        return []


# ---------------------------------------------------------------------------
# Configuration models
# ---------------------------------------------------------------------------

class OutputStorageConfig(BaseModel):
    """Generic output storage entry. Core owns `type` and `enabled`; backend-specific
    fields are preserved as extras and passed through to the backend module for validation.
    """
    model_config = ConfigDict(frozen=True, extra="allow")

    type: str = Field(default="file", description="Storage backend type")
    enabled: bool = Field(default=True, description="Enable output storage")


DEFAULT_TAIL_BUFFER_SIZE = 2 * 1024 * 1024  # 2 MB


class OutputConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    default_tail_buffer_size: int = Field(default=DEFAULT_TAIL_BUFFER_SIZE,
                                          description="Default max bytes for in-memory tail buffer")
    storages: list[OutputStorageConfig] = Field(
        default_factory=lambda: [OutputStorageConfig(type="file")],
        description="Output storage configurations",
    )


# ---------------------------------------------------------------------------
# Backend module loader (same pattern as db module loader)
# ---------------------------------------------------------------------------

class OutputBackendNotFoundError(RuntoolsException):

    def __init__(self, backend_type):
        super().__init__(f'Cannot find output backend module for type {backend_type!r}. '
                         f'Ensure the module is installed.')


_output_modules = {}


def load_output_module(backend_type: str):
    """Load an output backend module by type name.

    Imports ``runtools.runcore.output.<backend_type>`` directly. The module must
    expose a ``create_backend(env_id, config)`` factory function.

    Args:
        backend_type: Backend type identifier (e.g., "file").

    Returns:
        The loaded backend module.

    Raises:
        OutputBackendNotFoundError: If no matching module is found.
    """
    module = _output_modules.get(backend_type)
    if module:
        return module
    module_name = f"runtools.runcore.output.{backend_type}"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        if e.name == module_name:
            raise OutputBackendNotFoundError(backend_type)
        raise
    _output_modules[backend_type] = module
    return module


def create_backends(env_id: str, storage_configs: Iterable[OutputStorageConfig]) -> list[OutputBackend]:
    """Create all enabled output backends from storage configurations, in config order."""
    backends: list[OutputBackend] = []
    for cfg in storage_configs:
        if not cfg.enabled:
            continue
        module = load_output_module(cfg.type)
        backends.append(module.create_backend(env_id, cfg))
    return backends