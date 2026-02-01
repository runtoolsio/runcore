from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from itertools import count
from typing import List, Optional, Dict

from runtools.runcore import util
from runtools.runcore.err import InvalidStateError


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
            message=data["message"],
            ordinal=data["ordinal"],
            is_error=data["is_error"],
            source=data.get("source"),
            fields=data.get("fields"),
        )

    def serialize(self, truncate_length: Optional[int] = None, truncated_suffix: str = ".. (truncated)"):
        message = util.truncate(self.message, truncate_length, truncated_suffix) if truncate_length is not None else self.message
        return {
            "message": message,
            "ordinal": self.ordinal,
            "is_error": self.is_error,
            "source": self.source,
            "fields": self.fields,
        }


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
