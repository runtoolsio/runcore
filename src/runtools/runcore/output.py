import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import List, Optional, Dict, Iterable, Literal
from urllib.parse import urlparse, unquote
from urllib.request import pathname2url

from itertools import count
from pydantic import BaseModel, ConfigDict, Field

from runtools.runcore import paths, util
from runtools.runcore.err import InvalidStateError

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


@dataclass(frozen=True)
class OutputLine:
    """
    A single unit of output from a job, supporting both plain text and structured data.

    Attributes:
        message: Human-readable text content
        ordinal: Sequence number for ordering
        is_error: Whether this is error output (stderr)
        source: Optional identifier of the output source
        fields: Structured key-value data (e.g., from logging extras)
    """
    message: str
    ordinal: int
    is_error: bool = False
    source: Optional[str] = None
    fields: Dict[str, any] = None

    @classmethod
    def deserialize(cls, data: dict) -> 'OutputLine':
        return cls(
            message=data["msg"],
            ordinal=data["n"],
            is_error=data.get("err", False),
            source=data.get("src"),
            fields=data.get("f"),
        )

    def serialize(self, truncate_length: Optional[int] = None, truncated_suffix: str = ".. (truncated)"):
        message = util.truncate(self.message, truncate_length, truncated_suffix) if truncate_length is not None else self.message
        data = {"n": self.ordinal}
        if self.source is not None:
            data["src"] = self.source
        if self.is_error:
            data["err"] = True
        data["msg"] = message
        if self.fields is not None:
            data["f"] = self.fields
        return data


class OutputLineFactory:
    def __init__(self, default_source=None):
        self.default_source = default_source
        self._counter = count(1)

    def __call__(self, message, is_error=False, source=None, fields=None) -> OutputLine:
        ordinal = next(self._counter)
        return OutputLine(message, ordinal, is_error, source or self.default_source, fields)

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
        """Read stored output lines for an instance.

        Args:
            instance_id: InstanceID of the job run.
            sources: If provided, only return lines from these sources. None returns all.
            max_lines: If > 0, return only the last N lines. 0 means all.

        Returns:
            List of output lines, sorted by ordinal. Empty list if not found.

        Raises:
            OutputReadError: If the output exists but cannot be read.
        """

    def delete_output(self, *instance_ids) -> None:
        """Delete stored output for the given instances. No-op by default."""

    def close(self):
        """Release any resources held by this backend. No-op by default."""


class FileOutputBackend(OutputBackend):
    """File-backed output backend. Reads JSONL files from ``{base_dir}/{job_id}/{run_id}__{ordinal}.jsonl``."""

    type = "file"

    def __init__(self, base_dir: Path):
        self._base_dir = base_dir

    def _output_path(self, instance_id) -> Path:
        return self._base_dir / instance_id.job_id / f"{instance_id.run_id}__{instance_id.ordinal}.jsonl"

    def read_output(self, instance_id, sources: set[str] | None = None,
                    max_lines: int = 0) -> List['OutputLine']:
        path = self._output_path(instance_id)
        try:
            if max_lines > 0:
                if sources is not None:
                    return _read_jsonl_indexed_tail(path, sources, max_lines)
                return _read_jsonl_tail(path, max_lines)
            if sources is not None:
                return _read_jsonl_indexed(path, sources)
            return read_jsonl_file(str(path))
        except FileNotFoundError:
            return []
        except (json.JSONDecodeError, KeyError, OSError) as e:
            raise OutputReadError(str(path), e) from e

    def delete_output(self, *instance_ids) -> None:
        for iid in instance_ids:
            path = self._output_path(iid)
            path.unlink(missing_ok=True)
            SourceIndex.path_for(path).unlink(missing_ok=True)
            try:
                path.parent.rmdir()
            except OSError:
                pass  # Not empty or already gone


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
                log.warning("Backend read failed, trying next: %s", e)
                last_error = e
                continue
            if lines:
                return lines
            last_error = None  # Successful read (even if empty) clears prior errors
        if last_error:
            raise last_error
        return []


class OutputStorageConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = Field(default=False, description="Enable output storage")
    type: str = Field(default="file", description="Storage backend type")


class FileOutputStorageConfig(OutputStorageConfig):
    type: Literal["file"] = "file"
    dir: Optional[Path] = Field(default=None, description="Base output directory; XDG default if None")


DEFAULT_TAIL_BUFFER_SIZE = 2 * 1024 * 1024  # 2 MB


# When a second storage type is added (e.g. S3), wrap with Annotated[Union[...], Discriminator('type')]
OutputStorageConfigUnion = FileOutputStorageConfig


class OutputConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    default_tail_buffer_size: int = Field(default=DEFAULT_TAIL_BUFFER_SIZE,
                                          description="Default max bytes for in-memory tail buffer")
    storages: list[OutputStorageConfigUnion] = Field(
        default_factory=lambda: [FileOutputStorageConfig(enabled=True)],
        description="Output storage configurations",
    )


def create_backends(env_id: str, storage_configs: Iterable[OutputStorageConfig]) -> list[OutputBackend]:
    """Create all enabled output backends from storage configurations, in config order.

    Args:
        env_id: Environment identifier (used for default path resolution).
        storage_configs: Storage configurations.

    Returns:
        List of enabled backends in config order. Empty list if none are enabled.
    """
    backends: list[OutputBackend] = []
    for cfg in storage_configs:
        if not cfg.enabled:
            continue
        if isinstance(cfg, FileOutputStorageConfig):
            base_dir = Path(cfg.dir).expanduser() if cfg.dir else paths.output_dir(env_id, create=False)
            backends.append(FileOutputBackend(base_dir))
        else:
            assert False, f"Unknown output storage config type: {cfg.type}"
    return backends


def read_jsonl_file(file_path: str) -> List['OutputLine']:
    """Read output lines from a JSON Lines file."""
    lines = []
    with open(file_path, encoding="utf-8") as f:
        for raw_line in f:
            raw_line = raw_line.strip()
            if raw_line:
                lines.append(OutputLine.deserialize(json.loads(raw_line)))
    lines.sort(key=lambda ol: ol.ordinal)
    return lines


class SourceIndex:
    """Immutable source index mapping source IDs to byte-offset spans in a JSONL file."""

    VERSION = 1

    def __init__(self, sources: dict[str, list[list[int]]], jsonl_size: int):
        self.sources = sources
        self.jsonl_size = jsonl_size

    @staticmethod
    def path_for(jsonl_path: Path) -> Path:
        return Path(str(jsonl_path) + ".idx")

    @classmethod
    def load(cls, jsonl_path: Path) -> 'SourceIndex | None':
        """Load and validate a source index. Returns None if missing, corrupt, or stale."""
        try:
            with open(cls.path_for(jsonl_path), encoding="utf-8") as f:
                data = json.load(f)
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return None
        if data.get("version") != cls.VERSION:
            return None
        try:
            if jsonl_path.stat().st_size != data.get("jsonl_size"):
                return None
        except OSError:
            return None
        jsonl_size = data.get("jsonl_size")
        if not isinstance(jsonl_size, int):
            return None
        return cls(data.get("sources", {}), jsonl_size)

    def save(self, jsonl_path: Path):
        """Atomically write the index file."""
        index_path = self.path_for(jsonl_path)
        tmp_path = Path(str(index_path) + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({"version": self.VERSION, "jsonl_size": self.jsonl_size, "sources": self.sources},
                      f, separators=(",", ":"))
        os.replace(tmp_path, index_path)

    def spans_for(self, sources: set[str]) -> list[list[int]]:
        """Collect spans for requested sources, sorted by byte offset for correct line order."""
        spans = []
        for source in sources:
            if source in self.sources:
                spans.extend(self.sources[source])
        spans.sort(key=lambda s: s[0])
        return spans


class SourceIndexBuilder:
    """Accumulates byte-offset spans per source during writing."""

    def __init__(self):
        self._byte_offset = 0
        self._source_spans: dict[str, list[list[int]]] = {}
        self._current_source: str | None = None
        self._current_span_start = 0
        self._current_span_length = 0

    def track(self, source: str | None, byte_len: int):
        if source != self._current_source:
            self._flush_span()
            self._current_source = source
            self._current_span_start = self._byte_offset
            self._current_span_length = 0

        if source is not None:
            self._current_span_length += byte_len

        self._byte_offset += byte_len

    def build(self) -> SourceIndex | None:
        """Flush and produce the immutable index. Returns None if no sources were tracked."""
        self._flush_span()
        if not self._source_spans:
            return None
        return SourceIndex(self._source_spans, self._byte_offset)

    def _flush_span(self):
        if self._current_source is not None and self._current_span_length > 0:
            spans = self._source_spans.setdefault(self._current_source, [])
            spans.append([self._current_span_start, self._current_span_length])


def _read_jsonl_tail(path: Path, max_lines: int) -> List['OutputLine']:
    """Read the last max_lines from a JSONL file without loading the entire file."""
    if max_lines <= 0:
        return []
    file_size = path.stat().st_size
    if file_size == 0:
        return []
    lines = _read_tail_bytes(path, 0, file_size, max_lines)
    lines.reverse()
    return lines


def _read_jsonl_indexed_tail(jsonl_path: Path, sources: set[str],
                             max_lines: int) -> List['OutputLine']:
    """Read the last max_lines for specific sources, using index if available."""
    if max_lines <= 0:
        return []

    index = SourceIndex.load(jsonl_path)
    if index is None:
        all_filtered = _read_jsonl_filtered(jsonl_path, sources)
        return all_filtered[-max_lines:]

    spans = index.spans_for(sources)
    if not spans:
        return []

    try:
        # Iterate spans from the end; each _read_tail_bytes returns reverse-chronological
        lines = []
        for offset, length in reversed(spans):
            chunk_lines = _read_tail_bytes(jsonl_path, offset, length, max_lines - len(lines))
            lines.extend(chunk_lines)
            if len(lines) >= max_lines:
                break
        # Collected in reverse order overall — reverse to chronological
        lines = lines[:max_lines]
        lines.reverse()
        return lines
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        log.warning("Indexed tail read failed for %s, falling back to full scan", jsonl_path, exc_info=True)
        all_filtered = _read_jsonl_filtered(jsonl_path, sources)
        return all_filtered[-max_lines:]


_TAIL_CHUNK_SIZE = 64 * 1024


def _read_tail_bytes(path: Path, start: int, length: int, max_lines: int) -> List['OutputLine']:
    """Read the last max_lines from byte range [start, start+length) of a JSONL file.

    Reads backwards in chunks. Returns lines in reverse-chronological order.
    """
    lines = []
    end = start + length

    with open(path, "rb") as f:
        read_end = end
        leftover = b""

        while read_end > start and len(lines) < max_lines:
            chunk_start = max(start, read_end - _TAIL_CHUNK_SIZE)
            f.seek(chunk_start)
            chunk = f.read(read_end - chunk_start) + leftover

            raw_lines = chunk.split(b"\n")

            if chunk_start > start:
                leftover = raw_lines[0]
                raw_lines = raw_lines[1:]
            else:
                leftover = b""

            for raw_line in reversed(raw_lines):
                if len(lines) >= max_lines:
                    break
                raw_line = raw_line.strip()
                if raw_line:
                    lines.append(OutputLine.deserialize(json.loads(raw_line)))

            read_end = chunk_start

        if leftover and len(lines) < max_lines:
            leftover = leftover.strip()
            if leftover:
                lines.append(OutputLine.deserialize(json.loads(leftover)))

    return lines


def _read_jsonl_indexed(jsonl_path: Path, sources: set[str]) -> List['OutputLine']:
    """Read output lines for specific sources, using index if available."""
    index = SourceIndex.load(jsonl_path)
    if index is None:
        return _read_jsonl_filtered(jsonl_path, sources)

    spans = index.spans_for(sources)
    if not spans:
        return []

    try:
        lines = []
        with open(jsonl_path, "rb") as f:
            for offset, length in spans:
                f.seek(offset)
                chunk = f.read(length).decode("utf-8")
                for raw_line in chunk.splitlines():
                    if raw_line:
                        lines.append(OutputLine.deserialize(json.loads(raw_line)))
        return lines
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        log.warning("Indexed read failed for %s, falling back to full scan", jsonl_path, exc_info=True)
        return _read_jsonl_filtered(jsonl_path, sources)


def _read_jsonl_filtered(path: Path, sources: set[str]) -> List['OutputLine']:
    """Fallback: read entire file and filter by source."""
    lines = []
    with open(path, encoding="utf-8") as f:
        for raw_line in f:
            if raw_line := raw_line.strip():
                ol = OutputLine.deserialize(json.loads(raw_line))
                if ol.source in sources:
                    lines.append(ol)
    return lines
