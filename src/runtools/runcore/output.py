import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from enum import Enum, auto
from pathlib import Path
from typing import List, Optional, Dict, Iterable, Literal

from itertools import count
from pydantic import BaseModel, Field

from runtools.runcore import paths, util
from runtools.runcore.err import InvalidStateError

log = logging.getLogger(__name__)


class Mode(Enum):
    HEAD = auto()
    TAIL = auto()


@dataclass(frozen=True)
class OutputLocation:
    """
    Immutable descriptor for a location from which output can be read.

    Attributes:
        type: str - the kind of location (e.g., "file", "sqlite").
        source: str - identifier or address of the resource (e.g., file path, table name, URI).
    """
    type: str
    source: str

    def serialize(self) -> Dict[str, str]:
        """Serialize the location to a dictionary."""
        return {
            "type": self.type,
            "source": self.source
        }

    @classmethod
    def deserialize(cls, data: Dict[str, str]) -> 'OutputLocation':
        """Deserialize a location from a dictionary."""
        return cls(
            type=data["type"],
            source=data["source"]
        )


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
            ordinal=data["no"],
            is_error=data.get("err", False),
            source=data.get("src"),
            fields=data.get("f"),
        )

    def with_source(self, source: str) -> 'OutputLine':
        return replace(self, source=source)

    def serialize(self, truncate_length: Optional[int] = None, truncated_suffix: str = ".. (truncated)"):
        message = util.truncate(self.message, truncate_length, truncated_suffix) if truncate_length is not None else self.message
        data = {"no": self.ordinal}
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
    """Read-only access to stored output. Created from config, used by connectors."""

    @property
    @abstractmethod
    def type(self) -> str:
        """Backend type identifier (e.g., "file")."""

    @abstractmethod
    def read_output(self, instance_id) -> List['OutputLine']:
        """Read stored output lines for an instance.

        Args:
            instance_id: InstanceID of the job run.

        Returns:
            List of output lines, sorted by ordinal. Empty list if not found.

        Raises:
            OutputReadError: If the output exists but cannot be read.
        """

    def close(self):
        """Release any resources held by this backend. No-op by default."""


class FileOutputBackend(OutputBackend):
    """File-backed output backend. Reads JSONL files from ``{base_dir}/{job_id}/{run_id}.jsonl``."""

    type = "file"

    def __init__(self, base_dir: Path):
        self._base_dir = base_dir

    def read_output(self, instance_id) -> List['OutputLine']:
        path = self._base_dir / instance_id.job_id / f"{instance_id.run_id}.jsonl"
        try:
            return read_jsonl_file(str(path))
        except FileNotFoundError:
            return []
        except (json.JSONDecodeError, KeyError, OSError) as e:
            raise OutputReadError(str(path), e) from e


class MultiSourceOutputReader:
    """Reads output by trying backends in priority order, returning the first non-empty result."""

    def __init__(self, backends: Iterable[OutputBackend] = ()):
        self._backends = tuple(backends)

    def read_output(self, instance_id) -> List['OutputLine']:
        """Read output lines for an instance.

        Raises:
            OutputReadError: If output exists but all backends fail to read it.
        """
        last_error = None
        for backend in self._backends:
            try:
                lines = backend.read_output(instance_id)
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
    enabled: bool = Field(default=False, description="Enable output storage")
    type: str = Field(default="file", description="Storage backend type")


class FileOutputStorageConfig(OutputStorageConfig):
    type: Literal["file"] = "file"
    dir: Optional[Path] = Field(default=None, description="Base output directory; XDG default if None")


DEFAULT_TAIL_BUFFER_SIZE = 2 * 1024 * 1024  # 2 MB


# When a second storage type is added (e.g. S3), wrap with Annotated[Union[...], Discriminator('type')]
OutputStorageConfigUnion = FileOutputStorageConfig


class OutputConfig(BaseModel):
    tail_buffer_size: int = Field(default=DEFAULT_TAIL_BUFFER_SIZE, description="Max bytes for in-memory tail buffer")
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
