from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from threading import Lock
from typing import List, Optional

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
        self._atomic_lock = Lock()
        self._last = 0

    def __call__(self, text, is_error=False, source=None) -> OutputLine:
        with self._atomic_lock:
            self._last = ordinal = self._last + 1
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
