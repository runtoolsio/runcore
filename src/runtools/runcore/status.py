from __future__ import annotations

import re
from abc import ABC
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Optional, List


@dataclass(frozen=True)
class Event:
    text: str
    timestamp: datetime

    @classmethod
    def deserialize(cls, data: dict) -> 'Event':
        return cls(
            text=data['text'],
            timestamp=datetime.fromisoformat(data['timestamp'])
        )

    def serialize(self) -> dict:
        return {
            'text': self.text,
            'timestamp': self.timestamp.isoformat(),
        }


@dataclass(frozen=True)
class Operation:
    name: str
    completed: Optional[float]
    total: Optional[float]
    unit: Optional[str]
    created_at: datetime
    updated_at: datetime
    active: bool = True

    @property
    def pct_done(self) -> Optional[float]:
        if isinstance(self.completed, (int, float)) and isinstance(self.total, (int, float)):
            return self.completed / self.total
        return None

    @classmethod
    def deserialize(cls, data: dict) -> 'Operation':
        return cls(
            name=data['name'],
            completed=data['completed'],
            total=data['total'],
            unit=data['unit'],
            created_at=datetime.fromisoformat(data['created_at']),
            updated_at=datetime.fromisoformat(data['updated_at'])
        )

    def serialize(self) -> dict:
        return {
            'name': self.name,
            'completed': self.completed,
            'total': self.total,
            'unit': self.unit,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }

    @property
    def finished(self):
        return self.completed and self.total and (self.completed == self.total)

    @property
    def has_progress(self):
        return self.completed or self.total or self.unit

    def _progress_str(self):
        val = f"{self.completed or '?'}"
        if self.total:
            val += f"/{self.total}"
        if self.unit:
            val += f" {self.unit}"
        if pct_done := self.pct_done:
            val += f" ({round(pct_done * 100, 0):.0f}%)"

        return val

    def __str__(self):
        parts = []
        if self.name:
            parts.append(self.name)
        if self.has_progress:
            parts.append(self._progress_str())

        return f"[{' '.join(parts)}]"


@dataclass(frozen=True)
class Status:
    # TODO Warnings
    last_event: Optional[Event]
    operations: List[Operation]
    result: Optional[str]

    @classmethod
    def deserialize(cls, data: dict) -> 'Status':
        return cls(
            last_event=Event.deserialize(data['last_event']) if data['last_event'] else None,
            operations=[Operation.deserialize(op) for op in data['operations']],
            result=data['result']
        )

    def serialize(self) -> dict:
        return {
            'last_event': self.last_event.serialize() if self.last_event else None,
            'operations': [op.serialize() for op in self.operations],
            'result': self.result
        }

    def find_operation(self, name: str) -> Optional[Operation]:
        """
        Find an operation by its name.

        Args:
            name: The name of the operation to find

        Returns:
            The matching Operation if found, None otherwise
        """
        for operation in self.operations:
            if operation.name == name:
                return operation
        return None


    def __str__(self) -> str:
        """
        Formats a status line showing active operations and the last event.
        Format: [op1] [op2]...  last_event_text
        Only shows active operations. If there's a result, it shows that instead.
        """
        if self.result:
            return self.result

        parts = []

        # Add active operations
        active_ops = [str(op) for op in self.operations if op.active]
        if active_ops:
            parts.append(" ".join(active_ops))

        # Add last event if present
        if self.last_event:
            if parts:  # If we have operations, add the separator without leading space
                parts[-1] = parts[-1] + "..."  # Append directly to last part
            parts.append(self.last_event.text)

        return "  ".join(parts) if parts else ""

class OperationTracker:

    def __init__(self, name: str, created_at: datetime = None):
        self.name = name
        self.completed = None
        self.total = None
        self.unit = ''
        self.created_at = created_at or datetime.now(UTC).replace(tzinfo=None)
        self.updated_at = self.created_at
        self.active = True

    def update(self,
               completed: Optional[float] = None,
               total: Optional[float] = None,
               unit: Optional[str] = None,
               updated_at: Optional[datetime] = None) -> None:
        if completed is not None:
            if not self.completed or completed > self.completed:
                self.completed = completed  # Assuming is total completed
            else:
                self.completed += completed  # Assuming it is an increment
        if total is not None:
            self.total = total
        if unit is not None:
            self.unit = unit
        self.updated_at = updated_at or datetime.now(UTC).replace(tzinfo=None)

    def parse_value(self, value):
        # Check if value is a string and extract number and unit
        if isinstance(value, str):
            match = re.match(r"(\d+(\.\d+)?)(\s*)(\w+)?", value)
            if match:
                number = float(match.group(1))
                unit = match.group(4) if match.group(4) else ''
                return number, unit
            else:
                raise ValueError("String format is not correct. Expected format: {number}{unit} or {number} {unit}")
        elif isinstance(value, (float, int)):
            return float(value), self.unit
        else:
            raise TypeError("Value must be in the format `{number}{unit}` or `{number} {unit}`, but it was: "
                            + str(value))

    @property
    def finished(self):
        return self.completed and self.total and (self.completed == self.total)

    def to_operation(self) -> Operation:
        return Operation(
            self.name,
            self.completed,
            self.total,
            self.unit,
            self.created_at,
            self.updated_at,
            self.active
        )


class StatusTracker:

    def __init__(self):
        self._last_event: Optional[Event] = None
        self._operations: List[OperationTracker] = []
        self._result: Optional[str] = None

    def event(self, text: str, timestamp=None) -> None:
        timestamp = timestamp or datetime.now(UTC).replace(tzinfo=None)
        self._last_event = Event(text, timestamp)
        for op in self._operations:
            if op.finished:
                op.active = False

    def operation(self, name: str, timestamp=None) -> OperationTracker:
        op = self._get_operation(name)
        if not op:
            op = OperationTracker(name, timestamp)
            self._operations.append(op)

        return op

    def _get_operation(self, name: str) -> Optional[OperationTracker]:
        return next((op for op in self._operations if op.name == name), None)

    def result(self, result: str) -> None:
        self._result = result

    def to_status(self) -> Status:
        return Status(self._last_event, [op.to_operation() for op in self._operations], self._result)


class StatusObserver(ABC):

    def new_status_update(self):
        pass
