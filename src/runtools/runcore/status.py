from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Optional, List


@dataclass(frozen=True)
class Event:
    text: str
    timestamp: datetime

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
    unit: str
    created_at: datetime
    updated_at: datetime

    @property
    def pct_done(self) -> Optional[float]:
        if isinstance(self.completed, (int, float)) and isinstance(self.total, (int, float)):
            return self.completed / self.total
        return None

    def serialize(self) -> dict:
        return {
            'name': self.name,
            'completed': self.completed,
            'total': self.total,
            'unit': self.unit,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }


@dataclass(frozen=True)
class Status:
    last_event: Optional[Event]
    operations: List[Operation]
    result: Optional[str]

    def serialize(self) -> dict:
        return {
            'last_event': self.last_event.serialize() if self.last_event else None,
            'operations': [op.serialize() for op in self.operations],
            'result': self.result
        }


class OperationTracker:

    def __init__(self, name: str, created_at: datetime = None):
        self.name = name
        self.completed = None
        self.total = None
        self.unit = ''
        self.created_at = created_at or datetime.now(UTC).replace(tzinfo=None)
        self.updated_at = self.created_at

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

    def to_operation(self) -> Operation:
        return Operation(
            self.name,
            self.completed,
            self.total,
            self.unit,
            self.created_at,
            self.updated_at
        )


class StatusTracker:

    def __init__(self):
        self.last_event = None
        self._operations: List[OperationTracker] = []
        self.result = None

    def event(self, text: str, timestamp=None) -> None:
        timestamp = timestamp or datetime.now(UTC).replace(tzinfo=None)
        self.last_event = Event(text, timestamp)

    def operation(self, name: str, timestamp=None) -> OperationTracker:
        op = self._get_operation(name)
        if not op:
            op = OperationTracker(name, timestamp)
            self._operations.append(op)

        return op

    def _get_operation(self, name: str) -> Optional[OperationTracker]:
        return next((op for op in self._operations if op.name == name), None)

    def result(self, result: str) -> None:
        self.result = result

    def to_status(self) -> Status:
        return Status(
            self.last_event,
            [op.to_operation() for op in self._operations],
            self.result
        )
