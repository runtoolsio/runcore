from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
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
    is_active: bool = True
    result: Optional[str] = None

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
            updated_at=datetime.fromisoformat(data['updated_at']),
            is_active=data['is_active'],
            result=data['result'],
        )

    def serialize(self) -> dict:
        return {
            'name': self.name,
            'completed': self.completed,
            'total': self.total,
            'unit': self.unit,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'is_active': self.is_active,
            'result': self.result,
        }

    @property
    def finished(self):
        return self.result is not None or (
                self.has_progress and
                self.completed and
                self.total and
                (self.completed == self.total)
        )

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
        if self.result:
            parts.append(self.result)
        return f"[{' '.join(parts)}]"

@dataclass(frozen=True)
class Status:
    last_event: Optional[Event]
    operations: List[Operation]
    warnings: List[Event]
    result: Optional[Event]

    @classmethod
    def deserialize(cls, data: dict) -> 'Status':
        return cls(
            last_event=Event.deserialize(data['last_event']) if data['last_event'] else None,
            operations=[Operation.deserialize(op) for op in data['operations']],
            warnings=[Event.deserialize(w) for w in data['warnings']],
            result=Event.deserialize(data['result']),
        )

    def serialize(self) -> dict:
        return {
            'last_event': self.last_event.serialize() if self.last_event else None,
            'operations': [op.serialize() for op in self.operations],
            'warnings': [w.serialize() for w in self.warnings],
            'result': self.result.serialize(),
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
        Format: [op1] [op2]...  last_event_text  (!warning1, warning2)
        If there's a result, shows: result  (!warning1, warning2)
        """
        parts = []

        # Add result or active operations
        if self.result:
            parts.append(self.result.text)
        else:
            # Add active operations
            active_ops = [str(op) for op in self.operations if op.is_active]
            if active_ops:
                parts.append(" ".join(active_ops))

            # Add last event if present
            if self.last_event:
                if parts:  # If we have operations, add the separator
                    parts[-1] = parts[-1] + "..."  # Append directly to last part
                parts.append(self.last_event.text)

        # Add warnings if present
        if self.warnings:
            warnings_str = ", ".join(w.text for w in self.warnings)
            parts.append(f"(!{warnings_str})")

        return "  ".join(parts) if parts else ""


class StatusObserver(ABC):

    def new_status_update(self):
        pass
