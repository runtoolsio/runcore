from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

from runtools.runcore.util.dt import format_timedelta_compact

MAX_OPS_IN_SUMMARY = 3


def format_number(value: float) -> str:
    return str(int(value)) if value == int(value) else str(value)


@dataclass(frozen=True)
class Event:
    message: str
    timestamp: datetime
    source: Optional[str] = None

    @classmethod
    def deserialize(cls, data: dict) -> 'Event':
        return cls(
            message=data['message'],
            timestamp=datetime.fromisoformat(data['timestamp']),
            source=data.get('source'),
        )

    def serialize(self) -> dict:
        return {
            'message': self.message,
            'timestamp': self.timestamp.isoformat(),
            'source': self.source,
        }


@dataclass(frozen=True)
class Operation:
    name: str
    completed: Optional[float]
    total: Optional[float]
    unit: Optional[str]
    created_at: datetime
    updated_at: datetime
    result: Optional[str] = None
    failed: bool = False
    source: Optional[str] = None
    scope: Optional[str] = None

    @property
    def scoped(self) -> bool:
        return self.scope is not None

    @property
    def display_name(self) -> str:
        return f"{self.name}:{self.scope}" if self.scope else self.name

    @property
    def pct_done(self) -> Optional[float]:
        if isinstance(self.total, (int, float)) and self.total > 0:
            completed = self.completed if isinstance(self.completed, (int, float)) else 0
            return completed / self.total
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
            result=data.get('result'),
            failed=data.get('failed', False),
            source=data.get('source'),
            scope=data.get('scope'),
        )

    def serialize(self) -> dict:
        d = {
            'name': self.name,
            'completed': self.completed,
            'total': self.total,
            'unit': self.unit,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'result': self.result,
            'source': self.source,
        }
        if self.failed:
            d['failed'] = True
        if self.scope is not None:
            d['scope'] = self.scope
        return d

    @property
    def finished(self):
        return self.result is not None or (
                self.total is not None and
                self.completed is not None and
                self.completed >= self.total
        )

    @property
    def has_progress(self):
        return self.completed is not None or self.total is not None or self.unit is not None

    def _progress_str(self):
        val = f"{format_number(self.completed) if self.completed is not None else '?'}"
        if self.total:
            val += f"/{self.total}"
        if self.unit:
            val += f" {self.unit}"
        pct_done = self.pct_done
        if pct_done is not None:
            val += f" ({round(pct_done * 100, 0):.0f}%)"

        return val

    @property
    def elapsed(self) -> Optional[str]:
        """Compact elapsed time string, or None if timestamps are missing."""
        if self.created_at and self.updated_at:
            return format_timedelta_compact(self.updated_at - self.created_at)
        return None

    @property
    def finished_summary(self) -> str:
        """Short summary for a finished op: ``name ✓ 5000 keys (done) 2m34s`` or ``name ✗ reason 2m34s``."""
        mark = "✗" if self.failed else "✓"
        parts = []
        if self.completed is not None:
            s = format_number(self.completed)
            if self.unit:
                s += f" {self.unit}"
            parts.append(s)
        if self.result:
            if parts:
                parts.append(f"({self.result})")
            else:
                parts.append(self.result)
        elapsed = self.elapsed
        if elapsed:
            parts.append(elapsed)
        return f"{self.display_name} {mark} {' '.join(parts)}" if parts else f"{self.display_name} {mark}"

    def __str__(self):
        parts = []
        if self.name:
            parts.append(self.display_name)
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
            last_event=Event.deserialize(data['last_event']) if data.get('last_event') else None,
            operations=[Operation.deserialize(op) for op in data.get('operations', ())],
            warnings=[Event.deserialize(w) for w in data.get('warnings', ())],
            result=Event.deserialize(data['result']) if data.get('result') else None,
        )

    def serialize(self) -> dict:
        dto = {}
        if self.last_event:
            dto['last_event'] = self.last_event.serialize()
        if self.operations:
            dto['operations'] = [op.serialize() for op in self.operations]
        if self.warnings:
            dto['warnings'] = [w.serialize() for w in self.warnings]
        if self.result:
            dto['result'] = self.result.serialize()
        return dto

    def find_operation(self, name: str, scope: Optional[str] = None) -> Optional[Operation]:
        """Find an operation by name and scope.

        Args:
            name: The name of the operation to find.
            scope: The scope to match. None matches only unscoped operations.

        Returns:
            The matching Operation if found, None otherwise.
        """
        for operation in self.operations:
            if operation.name == name and operation.scope == scope:
                return operation
        return None

    @property
    def finished_ops_summary(self) -> str:
        """Summary of finished operations: ``Copy ✓ 50 files · Scan ✓ complete (+2 more)``."""
        finished = [op for op in self.operations if op.finished and not op.scoped]
        if not finished:
            return ""
        shown = finished[:MAX_OPS_IN_SUMMARY]
        parts = [op.finished_summary for op in shown]
        extra = len(finished) - len(shown)
        summary = " · ".join(parts)
        if extra > 0:
            summary += f" (+{extra} more)"
        return summary

    def __bool__(self) -> bool:
        return self.last_event is not None or bool(self.operations) or bool(self.warnings) or self.result is not None

    def __str__(self) -> str:
        """
        Formats a status line showing active operations or the last event.
        Format: [op1] [op2]  (!warning1, warning2)
        Or if no active operations: last_event_text  (!warning1, warning2)
        If there's a result, shows: result  (!warning1, warning2)
        """
        parts = []

        if self.result:
            parts.append(self.result.message)
        else:
            active_ops = [str(op) for op in self.operations if not op.finished and not op.scoped]
            if active_ops:
                parts.append(" ".join(active_ops))
            elif self.last_event:
                parts.append(self.last_event.message)
            elif ops_summary := self.finished_ops_summary:
                parts.append(ops_summary)

        if self.warnings:
            warnings_str = ", ".join(w.message for w in self.warnings)
            parts.append(f"(!{warnings_str})")

        return "  ".join(parts) if parts else ""
