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
        # TODO Rename to tail buffer?
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
    text: str
    ordinal: int
    is_error: bool = False
    source: Optional[str] = None

    @classmethod
    def deserialize(cls, data: dict) -> 'OutputLine':
        return cls(
            text=data["text"],
            ordinal=data["ordinal"],
            is_error=data["is_error"],
            source=data.get("source"),
        )

    def serialize(self, truncate_length: Optional[int] = None, truncated_suffix: str = ".. (truncated)"):
        text = util.truncate(self.text, truncate_length, truncated_suffix) if truncate_length is not None else self.text
        return {
            "text": text,
            "ordinal": self.ordinal,
            "is_error": self.is_error,
            "source": self.source
        }


class OutputLineFactory:
    def __init__(self, default_source=None):
        self.default_source = default_source
        self._counter = count(1)

    def __call__(self, text, is_error=False, source=None) -> OutputLine:
        ordinal = next(self._counter)
        return OutputLine(text, ordinal, is_error, source)


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
