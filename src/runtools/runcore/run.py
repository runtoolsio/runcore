"""
A run is an abstract concept that consists of a sequence of individual phase runs. A 'run' refers to any sequence of
phases, whether they are processes, programs, tasks, conditions, or other constructs, executed in a specific order.
Each phase has a unique name and defines its run state, which determines the nature of the phase's activity during
its run (like waiting, evaluating, executing, etc.). Phases operate in a predefined order; when one phase ends, the
subsequent phase begins. However, if a phase ends and signals premature termination by providing a termination status,
the next phase may not commence. Regardless of how the entire run finishes, the final phase must be a terminal phase,
and a termination status must be provided. This module includes a class, 'Phaser,' which implements the run concept
by orchestrating the given phase phases.
"""

import datetime
import inspect
import traceback
import weakref
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Dict, Any, TypeVar, Callable, Tuple, Iterator

from runtools.runcore import util
from runtools.runcore.util import format_dt_iso, utc_now


class Stage(Enum):
    CREATED = "created"
    RUNNING = "running"
    ENDED = "ended"


class Outcome(Enum):
    NONE = range(-1, 1)  # Null value.
    ANY = range(-1, 9999)
    SUCCESS = range(1, 11)  # Completed successfully.
    NON_SUCCESS = range(11, 99)  # Not completed successfully.
    ABORTED = range(11, 21)  # Aborted by user.
    REJECTED = range(21, 31)  # Rejected by not satisfying a condition.
    FAULT = range(31, 41)  # Failed.


class TerminationStatus(Enum):
    UNKNOWN = -1
    NONE = 0

    COMPLETED = 1

    CANCELLED = 11
    STOPPED = 12
    INTERRUPTED = 13
    SIGNAL = 14

    TIMEOUT = 21
    OVERLAP = 22
    UNSATISFIED = 23
    DENIED = 24

    FAILED = 31
    ERROR = 32

    @classmethod
    def from_code(cls, code):
        for member in cls:
            if member.value == code:
                return member

        return TerminationStatus.UNKNOWN

    def is_outcome(self, outcome):
        return self.value in outcome.value

    def __bool__(self):
        return self != TerminationStatus.NONE


class StopReason(Enum):
    STOPPED = auto()  # User explicitly cancelled
    INTERRUPTED = auto()  # Keyboard interrupt
    SIGNAL = auto()  # Termination signal received
    TIMEOUT = auto()  # Execution time exceeded

    @property
    def termination_status(self):
        mapping = {
            StopReason.STOPPED: TerminationStatus.STOPPED,
            StopReason.INTERRUPTED: TerminationStatus.INTERRUPTED,
            StopReason.SIGNAL: TerminationStatus.SIGNAL,
            StopReason.TIMEOUT: TerminationStatus.TIMEOUT,
        }
        return mapping.get(self, TerminationStatus.STOPPED)


@dataclass
class Fault:
    category: str
    reason: str
    stack_trace: Optional[str] = None

    def serialize(self):
        data = {"cat": self.category, "reason": self.reason}
        if self.stack_trace:
            data["stack_trace"] = self.stack_trace
        return data

    @classmethod
    def deserialize(cls, as_dict):
        return cls(
            as_dict["cat"],
            as_dict["reason"],
            as_dict.get("stack_trace")
        )

    @classmethod
    def from_exception(cls, category: str, exception: BaseException) -> 'Fault':
        stack_trace = ''.join(traceback.format_exception(type(exception), exception, exception.__traceback__))
        return cls(
            category=category,
            reason=f"{exception.__class__.__name__}: {exception}",
            stack_trace=stack_trace
        )


@dataclass(frozen=True)
class TerminationInfo:
    status: TerminationStatus
    terminated_at: datetime.datetime
    message: Optional[str] = None
    stack_trace: Optional[str] = None

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]):
        return cls(
            status=TerminationStatus[as_dict['termination_status']],
            terminated_at=util.parse_datetime(as_dict['terminated_at']),
            message=as_dict.get('message'),
            stack_trace=as_dict.get('stack_trace'),
        )

    def serialize(self) -> Dict[str, Any]:
        return {
            "termination_status": self.status.name,
            "terminated_at": format_dt_iso(self.terminated_at),
            "message": self.message,
            "stack_trace": self.stack_trace,
        }

    def __str__(self) -> str:
        """Compact string representation of termination info."""
        parts = [f"{self.status.name}@{self.terminated_at.strftime('%Y-%m-%dT%H:%M:%S')}"]
        if self.message:
            parts.append(f"msg={self.message}")
        return " ".join(parts)


@dataclass(frozen=True)
class RunLifecycle:
    """
    Encapsulates lifecycle information of a runnable unit, tracking its progression through
    different stages from creation to termination.
    """
    created_at: datetime
    started_at: Optional[datetime] = None
    termination: Optional[TerminationInfo] = None

    @property
    def stage(self) -> Stage:
        """Determines the current stage of the phase in its lifecycle."""
        if self.termination:
            return Stage.ENDED
        if self.started_at:
            return Stage.RUNNING
        return Stage.CREATED

    @property
    def last_transition_at(self) -> datetime:
        """Returns the timestamp of the last lifecycle transition."""
        if self.termination:
            return self.termination.terminated_at
        return self.started_at or self.created_at

    def transition_at(self, stage):
        match stage:
            case Stage.CREATED:
                return self.created_at
            case Stage.RUNNING:
                return self.started_at
            case Stage.ENDED:
                return self.termination.terminated_at if self.termination else None

        return None

    @property
    def elapsed(self) -> Optional[datetime.timedelta]:
        """
        For naive-UTC timestamps:
          - If started_at is None → None
          - Otherwise: (termination time if ended) or utc_now() minus started_at
        """
        if not self.started_at:
            return None

        # assumed also naive UTC, from your parser
        end = self.termination.terminated_at if self.termination else utc_now()
        return end - self.started_at

    @property
    def total_run_time(self) -> Optional[datetime.timedelta]:
        """Calculates the duration between start and end of the phase."""
        if not self.started_at or not self.termination:
            return None
        return self.termination.terminated_at - self.started_at

    @property
    def is_running(self) -> bool:
        """Returns True if this phase is in the running stage."""
        return self.stage == Stage.RUNNING

    @property
    def is_ended(self) -> bool:
        """Returns True if this phase has reached its end stage."""
        return self.stage == Stage.ENDED

    @property
    def completed_successfully(self) -> bool:
        """Returns True if this phase completed its lifecycle successfully."""
        return self.termination is not None and self.termination.status == TerminationStatus.COMPLETED

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'RunLifecycle':
        return cls(
            created_at=util.parse_datetime(as_dict['created_at']),
            started_at=util.parse_datetime(as_dict['started_at']) if as_dict.get('started_at') else None,
            termination=TerminationInfo.deserialize(as_dict['termination']) if as_dict.get('termination') else None
        )

    def serialize(self) -> Dict[str, Any]:
        dto = {
            'created_at': format_dt_iso(self.created_at),
        }
        if self.started_at:
            dto['started_at'] = format_dt_iso(self.started_at)
        if self.termination:
            dto['termination'] = self.termination.serialize()
        return dto


class PhasePath(list):
    """
    Represents a path from root to a specific phase in the phase hierarchy.

    Provides convenient methods for accessing parent phases and the root phase.
    """

    def __init__(self, phases=None):
        """
        Initialize with optional list of phases from root to current (excluding current).
        Args:
            phases (Optional[List['PhaseDetail']]: phases starting from root
        """
        super().__init__(phases or [])

    @property
    def parent(self) -> Optional['PhaseDetail']:
        """Returns the immediate parent phase, or None if at root."""
        return self[-1] if self else None

    @property
    def root(self) -> Optional['PhaseDetail']:
        """Returns the root phase of the path, or None if path is empty."""
        return self[0] if self else None

    @property
    def depth(self) -> int:
        """Returns the depth in the tree (number of ancestors)."""
        return len(self)

    def get_ancestor(self, levels_up: int) -> Optional['PhaseDetail']:
        """
        Get an ancestor at a specific level up from the current position.

        Args:
            levels_up: Number of levels to go up (1 = parent, 2 = grandparent, etc.)

        Returns:
            The ancestor phase or None if levels_up exceeds the path length
        """
        if levels_up <= 0 or levels_up > len(self):
            return None
        return self[-levels_up]

    def iter_ancestors(self, *, include_root: bool = True, reverse: bool = False) -> Iterator['PhaseDetail']:
        """
        Iterate through ancestors.

        Args:
            include_root: Whether to include the root phase in iteration
            reverse: If False (default), iterate from parent to root. If True, iterate from root to parent.

        Yields:
            Ancestor phases in the specified order
        """
        if reverse:
            # Root to parent order
            start = 0 if include_root else 1
            for i in range(start, len(self)):
                yield self[i]
        else:
            # Parent to root order (default)
            for i in range(len(self) - 1, -1 if include_root else 0, -1):
                yield self[i]

    def has_ancestor(self, phase_id: str) -> bool:
        """Check if a phase with the given ID exists in the ancestor path."""
        return any(p.phase_id == phase_id for p in self)

    def any_match(self, predicate: Callable[['PhaseDetail'], bool]) -> bool:
        """
        Check if any ancestor phase matches the given predicate.

        Args:
            predicate: A function that takes a PhaseDetail and returns bool

        Returns:
            True if any ancestor matches, False otherwise
        """
        return any(predicate(phase) for phase in self)

    def extend_with(self, phase) -> 'PhasePath':
        """
        Create a new PhasePath with the given phase appended.

        Args:
            phase (PhaseDetail): The phase to add to the path

        Returns:
            A new PhasePath instance with the phase added
        """
        new_path = PhasePath(self)
        new_path.append(phase)
        return new_path

    def __repr__(self) -> str:
        """String representation showing the phase IDs in the path."""
        if not self:
            return "PhasePath([])"
        path_str = " -> ".join(p.phase_id for p in self)
        return f"PhasePath([{path_str}])"


class PhaseVisitor(ABC):
    """
    Abstract base class for phase visitors.
    Implement this to define custom traversal behavior.
    """

    @abstractmethod
    def visit_phase(self, phase_detail, parent_path) -> Any:
        """
        Visit a single phase node.

        Args:
            phase_detail (PhaseDetail): The current phase being visited
            parent_path (PhasePath): Path containing parent phases from root to current (excluding current)

        Returns:
            Any value (visitor-specific)
        """
        pass


@dataclass(frozen=True)
class PhaseDetail:
    """
    A complete immutable view of a Phase instance, containing all state and metadata.
    """
    # Core phase information
    phase_id: str
    phase_type: str
    is_idle: bool
    phase_name: Optional[str]
    attributes: Optional[Dict[str, Any]]
    variables: Optional[Dict[str, Any]]

    # Lifecycle information encapsulated in a dedicated class
    lifecycle: RunLifecycle

    # Hierarchical information
    children: Tuple['PhaseDetail', ...]

    @classmethod
    def from_phase(cls, phase) -> 'PhaseDetail':
        return cls(
            phase_id=phase.id,
            phase_type=phase.type,
            is_idle=phase.is_idle,
            phase_name=phase.name,
            attributes=phase.attributes,
            variables=phase.variables,
            lifecycle=RunLifecycle(
                created_at=phase.created_at,
                started_at=phase.started_at,
                termination=phase.termination
            ),
            children=tuple([cls.from_phase(child) for child in phase.children] if phase.children else [])
        )

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'PhaseDetail':
        """
        Creates a PhaseView from a dictionary representation.

        Args:
            as_dict: Dictionary containing serialized phase data

        Returns:
            PhaseDetail: The deserialized phase view
        """
        return cls(
            phase_id=as_dict['phase_id'],
            phase_type=as_dict['phase_type'],
            is_idle=as_dict.get('is_idle', False),
            phase_name=as_dict.get('phase_name'),
            attributes=as_dict.get('attributes'),
            variables=as_dict.get('variables'),
            lifecycle=RunLifecycle.deserialize(as_dict.get('lifecycle')),
            children=tuple(cls.deserialize(child) for child in as_dict.get('children', [])),
        )

    def serialize(self) -> Dict[str, Any]:
        """
        Creates a dictionary representation of this PhaseView.

        Returns:
            Dict[str, Any]: The serialized phase view
        """
        dto = {
            'phase_id': self.phase_id,
            'phase_type': self.phase_type,
            'is_idle': self.is_idle,
            'lifecycle': self.lifecycle.serialize(),
        }
        if self.phase_name:
            dto['phase_name'] = self.phase_name
        if self.attributes:
            dto['attributes'] = self.attributes
        if self.variables:
            dto['variables'] = self.variables
        if self.children:
            dto['children'] = [child.serialize() for child in self.children]

        return dto

    def search_phases(self,
                      predicate: Optional[Callable[['PhaseDetail'], bool]] = None,
                      *,
                      include_self: bool = True) -> List['PhaseDetail']:
        """
        Searches phases in this view's hierarchy, with an option to include self.
        The search is performed in a pre-order (depth-first) manner.

        Args:
            predicate: Optional function to filter phases. If None, all phases in scope are returned.
            include_self: If True, the current phase (self) is included in the search.
                          If False, only descendant phases are searched.
                          Defaults to True.

        Returns:
            List[PhaseDetail]: A list of matching phase details.
        """
        results: List[PhaseDetail] = []

        def _collect_recursively(node: 'PhaseDetail', target_list: List['PhaseDetail']):
            if not predicate or predicate(node):
                target_list.append(node)
            for child in node.children:
                _collect_recursively(child, target_list)

        if include_self:
            _collect_recursively(self, results)
        else:
            for child_node in self.children:
                _collect_recursively(child_node, results)

        return results

    def search_descendants(self, predicate: Optional[Callable[['PhaseDetail'], bool]] = None) -> List['PhaseDetail']:
        """
        Returns all descendant phases in depth-first order (pre-order).
        This is a convenience method, equivalent to calling
        search_phases(predicate, include_self=False).
        """
        return self.search_phases(predicate, include_self=False)

    def find_first_phase(self, predicate: Callable[['PhaseDetail'], bool]) -> Optional['PhaseDetail']:
        """
        Finds a phase in this view's hierarchy that matches the given predicate.

        Args:
            predicate: A function that takes a PhaseView and returns bool

        Returns:
            Optional[PhaseDetail]: The matching phase view or None if not found
        """
        if predicate(self):
            return self

        if self.children:
            for child in self.children:
                result = child.find_first_phase(predicate)
                if result:
                    return result

        return None

    def find_phase_by_id(self, phase_id):
        return self.find_first_phase(lambda p: p.phase_id == phase_id)

    @property
    def any_child_running(self) -> bool:
        """Returns True if any child phase has started."""
        return any(child.lifecycle.stage == Stage.RUNNING for child in self.children)

    def accept_visitor(self, visitor, parent_path=None):
        """
        Accept a visitor for tree traversal.

        Args:
            visitor (PhaseVisitor): The visitor to accept
            parent_path (Optional[PhasePath]): Path from root to this phase (excluding self)
        """
        if parent_path is None:
            parent_path = PhasePath()

        visitor.visit_phase(self, parent_path)

        new_path = parent_path.extend_with(self)
        for child in self.children:
            child.accept_visitor(visitor, new_path)

        return visitor


@dataclass
class PhaseTransitionEvent:
    phase_detail: PhaseDetail
    new_stage: Stage
    timestamp: datetime


class PhaseTransitionObserver(ABC):

    @abstractmethod
    def new_phase_transition(self, event: PhaseTransitionEvent):
        pass


C = TypeVar('C')


class _ControlProperty(property):
    _expose_to_control = True


def control_api(func):
    if isinstance(func, property):
        return _ControlProperty(func.fget, func.fset, func.fdel)
    else:
        func._expose_to_control = True
        return func


class PhaseControl:

    def __init__(self, phase):
        self._phase = weakref.proxy(phase)

        self._allowed_methods = {}
        for name, attr in inspect.getmembers(phase.__class__):
            if getattr(attr, '_expose_to_control', False):
                self._allowed_methods[name] = attr

    def __getattr__(self, name):
        if name not in self._allowed_methods:
            raise AttributeError(f"'{self.__class__.__name__}' has no attribute '{name}'")
        return getattr(self._phase, name)


class RunCompletionError(Exception):
    """
    Represents an error that occurred during run execution, capturing the phase ID and reason.

    This exception is used to track errors through the phase hierarchy, maintaining a chain
    of phase IDs that failed during execution.

    Attributes:
        phase_id (str): ID of the phase where the error occurred.
    """

    def __init__(self, phase_id, reason):
        super().__init__(reason)
        self.phase_id = phase_id

    def original_message(self) -> Optional[str]:
        """
        Walk the chain of RunCompletionError causes and return
        the message from the very first one.
        """
        exc = self
        while isinstance(exc.__cause__, RunCompletionError):
            exc = exc.__cause__
        return str(exc)
