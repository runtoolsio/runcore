from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Tuple, Dict, Optional

from runtools.runcore.common import InvalidStateError


class Output(ABC):

    @abstractmethod
    def tail(self, count: int = 0):
        pass


class Mode(Enum):
    HEAD = auto()
    TAIL = auto()


@dataclass
class OutputLine:
    text: str
    is_error: bool
    source: Optional[str] = None

    @classmethod
    def deserialize(cls, data: dict) -> 'OutputLine':
        return cls(
            text=data["text"],
            is_error=data["is_error"],
            source=data.get("source"),
        )

    def serialize(self):
        return {"text": self.text, "is_error": self.is_error, "source": self.source}


class TailBuffer(ABC):

    def add_line(self, line: OutputLine):
        pass

    def get_lines(self, mode: Mode = Mode.HEAD, lines: int = 0) -> List[OutputLine]:
        pass


class InMemoryTailBuffer(TailBuffer):

    def __init__(self, max_capacity: int = 0):
        if max_capacity < 0:
            raise ValueError("max_capacity cannot be negative")
        self._max_capacity = max_capacity or None
        self._lines = deque(maxlen=self._max_capacity)

    def add_line(self, output_line: OutputLine):
        self._lines.append(output_line)

    def get_lines(self, mode: Mode = Mode.TAIL, count: int = 0) -> List[OutputLine]:
        if count < 0:
            raise ValueError("Count cannot be negative")

        output = list(self._lines)
        if not count:
            return output

        if mode == Mode.TAIL:
            return output[-count:]

        if mode == Mode.HEAD:
            return output[:count]

class TailNotSupportedError(InvalidStateError):
    pass
