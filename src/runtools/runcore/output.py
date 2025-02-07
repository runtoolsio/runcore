from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional

from runtools.runcore import util
from runtools.runcore.common import InvalidStateError



class Mode(Enum):
    HEAD = auto()
    TAIL = auto()

class Output(ABC):

    @abstractmethod
    def tail(self, mode: Mode = Mode.TAIL, max_lines: int = 0):
        pass


@dataclass(frozen=True)
class OutputLine:
    text: str
    is_error: bool = False
    source: Optional[str] = None

    @classmethod
    def deserialize(cls, data: dict) -> 'OutputLine':
        return cls(
            text=data["text"],
            is_error=data["is_error"],
            source=data.get("source"),
        )

    def serialize(self, truncate_length: Optional[int] = None, truncated_suffix: str = ".. (truncated)"):
        text = util.truncate(self.text, truncate_length, truncated_suffix) if truncate_length is not None else self.text
        return {
            "text": text,
            "is_error": self.is_error,
            "source": self.source
        }

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
