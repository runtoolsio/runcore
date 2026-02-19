import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from enum import Enum, auto
from typing import List, Optional, Dict, Iterable

from itertools import count
from runtools.runcore import util
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


def read_output(locations: Iterable[OutputLocation]) -> List['OutputLine']:
    """Read output lines from the given locations.

    Tries each location in order and returns lines from the first one that succeeds.

    Args:
        locations: Output locations to read from.

    Returns:
        List of output lines, sorted by ordinal.
    """
    for location in locations:
        try:
            if location.type == "file":
                return _read_jsonl_file(location.source)
            else:
                log.warning("Unsupported output location type: %s", location.type)
        except FileNotFoundError:
            log.debug("Output file not found: %s", location.source)
        except (json.JSONDecodeError, KeyError, OSError) as e:
            log.warning("Failed to read output from %s: %s", location.source, e)
    return []


def _read_jsonl_file(file_path: str) -> List['OutputLine']:
    """Read output lines from a JSON Lines file."""
    lines = []
    with open(file_path, encoding="utf-8") as f:
        for raw_line in f:
            raw_line = raw_line.strip()
            if raw_line:
                lines.append(OutputLine.deserialize(json.loads(raw_line)))
    lines.sort(key=lambda ol: ol.ordinal)
    return lines
