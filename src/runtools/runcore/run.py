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
from collections import OrderedDict
from copy import copy
from dataclasses import dataclass
from enum import Enum, EnumMeta
from typing import Optional, List, Dict, Any, Tuple, TypeVar, Generic, Callable

from runtools.runcore import util
from runtools.runcore.job import Stage
from runtools.runcore.util import format_dt_iso, is_empty


class RunStateMeta(EnumMeta):
    _value2member_map_ = {}

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._value2member_map_.get(item, RunState.UNKNOWN)
        elif isinstance(item, str):
            return super().__getitem__(item.upper())
        else:
            raise KeyError("Invalid key: must be integer or string")


class RunState(Enum, metaclass=RunStateMeta):
    NONE = 0
    UNKNOWN = -1
    CREATED = 1  # TBD
    PENDING = 2
    WAITING = 3
    EVALUATING = 4
    IN_QUEUE = 5
    EXECUTING = 6
    EXECUTING_CHILDREN = 7
    ENDED = 100


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
    fault: Optional[Fault] = None

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]):
        return cls(
            status=TerminationStatus[as_dict['termination_status']],
            terminated_at=util.parse_datetime(as_dict['terminated_at']),
            fault=Fault.deserialize(as_dict['fault']) if as_dict.get('fault') else None,
        )

    def serialize(self) -> Dict[str, Any]:
        return {
            "termination_status": self.status.name,
            "terminated_at": format_dt_iso(self.terminated_at),
            "fault": self.fault.serialize() if self.fault else None,
        }


@dataclass
class PhaseRun:
    phase_id: str
    run_state: RunState
    started_at: Optional[datetime.datetime]
    ended_at: Optional[datetime.datetime] = None

    @classmethod
    def deserialize(cls, d):
        return cls(
            d['phase_id'],
            RunState[d['run_state']],
            util.parse_datetime(d['started_at']),
            util.parse_datetime(d['ended_at']),
        )

    def serialize(self):
        return {
            'phase_id': self.phase_id,
            'run_state': self.run_state.name,
            'started_at': format_dt_iso(self.started_at),
            'ended_at': format_dt_iso(self.ended_at),
        }

    @property
    def run_time(self):
        if self.started_at and self.ended_at:
            return self.ended_at - self.started_at
        return None

    def __bool__(self):
        return bool(self.phase_id) and self.run_state != RunState.NONE

    def __copy__(self):
        return PhaseRun(self.phase_id, self.run_state, self.started_at, self.ended_at)


NONE_PHASE_RUN = PhaseRun('', RunState.NONE, None, None)


class Lifecycle:
    """
    This class represents the lifecycle of a run. A lifecycle consists of a chronological sequence of phase transitions.
    Each phase has a timestamp that indicates when the transition to that phase occurred.
    """

    def __init__(self, *phase_runs: PhaseRun):
        self._phase_runs: OrderedDict[str, PhaseRun] = OrderedDict()
        self._current_run: Optional[PhaseRun] = None
        self._previous_run: Optional[PhaseRun] = None
        for run in phase_runs:
            self.add_phase_run(run)

    def add_phase_run(self, phase_run: PhaseRun):
        """
        Adds a new phase run to the lifecycle.
        """
        if phase_run.phase_id in self._phase_runs:
            raise ValueError(f"Phase with ID `{phase_run.phase_id}` is already present: {self.phase_ids}")

        if self.current_run:
            self._previous_run = self._current_run
            self._previous_run.ended_at = phase_run.started_at

        self._current_run = phase_run
        self._phase_runs[phase_run.phase_id] = phase_run

    @classmethod
    def deserialize(cls, as_dict):
        phase_runs = []
        for transition in as_dict['transitions']:
            phase_id = transition['phase_id']
            run_state = RunState[transition['state']]
            started_at = util.parse_datetime(transition['ts'])

            # Determine the ended_at for each phase
            # The end of a phase is the start of the next phase, if there is one
            if phase_runs:
                phase_runs[-1].ended_at = started_at

            phase_runs.append(PhaseRun(phase_id, run_state, started_at, None))

        return cls(*phase_runs)

    def serialize(self) -> Dict[str, Any]:
        return {
            "transitions": [
                {'phase_id': run.phase_id, 'state': run.run_state.value, 'ts': format_dt_iso(run.started_at)}
                for run in self._phase_runs.values()
            ]
        }

    def to_dto(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "phase_runs": [run.serialize() for run in self._phase_runs.values()],
            "current_run": self.current_run.serialize(),
            "previous_run": self.previous_run.serialize(),
            "last_transition_at": format_dt_iso(self.last_transition_at),
            "created_at": format_dt_iso(self.created_at),
            "executed_at": format_dt_iso(self.executed_at),
            "ended_at": format_dt_iso(self.ended_at),
            "execution_time": self.total_executing_time.total_seconds() if self.ended_at else None,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    @property
    def current_run(self) -> Optional[PhaseRun]:
        return self._current_run or NONE_PHASE_RUN

    @property
    def current_phase_id(self) -> Optional[str]:
        return self._current_run.phase_id if self._current_run else None

    @property
    def previous_run(self) -> Optional[PhaseRun]:
        return self._previous_run or NONE_PHASE_RUN

    @property
    def previous_phase_id(self) -> Optional[str]:
        return self._previous_run.phase_id if self._previous_run else None

    @property
    def run_state(self):
        if not self._current_run:
            return RunState.NONE

        return self._current_run.run_state

    @property
    def phase_count(self):
        return len(self._phase_runs)

    def get_ordinal(self, phase_key) -> int:
        for index, current_phase in enumerate(self._phase_runs.keys()):
            if current_phase == phase_key:
                return index + 1
        raise ValueError(f"Phase {phase_key} not found in lifecycle")

    @property
    def phase_ids(self) -> List[str]:
        return list(self._phase_runs.keys())

    @property
    def phase_runs(self) -> List[PhaseRun]:
        return list(self._phase_runs.values())

    def phase_run(self, phase_id) -> Optional[PhaseRun]:
        return self._phase_runs.get(phase_id) or NONE_PHASE_RUN

    def runs_between(self, phase_from, phase_to) -> List[PhaseRun]:
        runs = []
        for run in self._phase_runs.values():
            if run.phase_id == phase_to:
                if not runs:
                    if phase_from == phase_to:
                        return [run]
                    else:
                        return []
                runs.append(run)
                return runs
            elif run.phase_id == phase_from or runs:
                runs.append(run)

        return []

    def phase_ids_between(self, phase_from, phase_to) -> List[str]:
        return [run.phase_id for run in self.runs_between(phase_from, phase_to)]

    def phase_start_dt(self, phase_id) -> Optional[datetime.datetime]:
        phase_run = self._phase_runs.get(phase_id)
        return phase_run.started_at if phase_run else None

    @property
    def last_transition_at(self) -> Optional[datetime.datetime]:
        if not self._current_run:
            return None

        return self._current_run.started_at

    def state_first_transition_at(self, state: RunState) -> Optional[datetime.datetime]:
        return next((run.started_at for run in self._phase_runs.values() if run.run_state == state), None)

    def state_last_transition_at(self, state: RunState) -> Optional[datetime.datetime]:
        return next((run.started_at for run in reversed(self._phase_runs.values()) if run.run_state == state), None)

    def contains_state(self, state: RunState):
        return any(run.run_state == state for run in self._phase_runs.values())

    @property
    def created_at(self) -> Optional[datetime.datetime]:
        return self.state_first_transition_at(RunState.CREATED)

    @property
    def executed_at(self) -> Optional[datetime.datetime]:
        return self.state_first_transition_at(RunState.EXECUTING)

    @property
    def ended_at(self) -> Optional[datetime.datetime]:
        return self.state_last_transition_at(RunState.ENDED)

    @property
    def is_ended(self):
        return self.contains_state(RunState.ENDED)

    def run_time_in_state(self, state: RunState) -> datetime.timedelta:
        """
        Calculate the total time spent in the given state.

        Args:
            state (RunState): The state to calculate run time for.

        Returns:
            datetime.timedelta: Total time spent in the given state.
        """
        durations = [run.run_time for run in self._phase_runs.values() if run.run_state == state and run.run_time]
        return sum(durations, datetime.timedelta())

    @property
    def total_executing_time(self) -> Optional[datetime.timedelta]:
        return self.run_time_in_state(RunState.EXECUTING)

    def __copy__(self):
        copied = Lifecycle()
        copied._phase_runs = OrderedDict((name, copy(run)) for name, run in self._phase_runs.items())
        copied._current_run = copy(self._current_run)
        copied._previous_run = copy(self._previous_run)
        return copied

    def __eq__(self, other):
        if not isinstance(other, Lifecycle):
            return NotImplemented

        return self._phase_runs == other._phase_runs

    def __repr__(self):
        phase_runs_repr = ', '.join(repr(run) for run in self._phase_runs.values())
        return f"{self.__class__.__name__}({phase_runs_repr})"


@dataclass(frozen=True)
class PhaseInfo:
    phase_id: str
    phase_type: str
    run_state: RunState
    phase_name: Optional[str] = None
    attributes: Optional[Dict[str, Any]] = None  # or frozendict for true immutability

    @classmethod
    def deserialize(cls, as_dict) -> 'PhaseInfo':
        return cls(
            as_dict["phase_id"],
            as_dict["phase_type"],
            RunState[as_dict["run_state"]],
            as_dict.get("phase_name"),
            as_dict.get("attributes"),
        )

    def serialize(self) -> Dict:
        d = {
            "phase_id": self.phase_id,
            "phase_type": self.phase_type,
            "run_state": self.run_state.name,
        }
        if self.phase_name:
            d["phase_name"] = self.phase_name
        if self.attributes:
            d["attributes"] = self.attributes

        return d


@dataclass(frozen=True)
class PhaseDetail:
    """
    A complete immutable view of a Phase instance, containing all state and metadata.
    Provides a snapshot of the phase's current state including runtime information.
    """
    # Core phase information
    phase_id: str
    phase_type: str
    run_state: RunState
    phase_name: Optional[str]
    attributes: Optional[Dict[str, Any]]

    # Runtime information
    created_at: datetime
    started_at: Optional[datetime]
    termination: Optional[TerminationInfo]

    # Hierarchical information
    children: List['PhaseDetail']

    @classmethod
    def from_phase(cls, phase) -> 'PhaseDetail':
        """
        Creates a PhaseView from a PhaseV2 instance.

        Args:
            phase: A PhaseV2 instance

        Returns:
            PhaseDetail: An immutable view of the phase's current state
        """
        return cls(
            phase_id=phase.id,
            phase_type=phase.type,
            run_state=phase.run_state,
            phase_name=phase.name,
            attributes=phase.attributes,
            created_at=phase.created_at,
            started_at=phase.started_at,
            termination=phase.termination,
            children=[cls.from_phase(child) for child in phase.children] if phase.children else []
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
        children = [
            cls.deserialize(child)
            for child in as_dict.get('children', [])
        ]

        return cls(
            phase_id=as_dict['phase_id'],
            phase_type=as_dict['phase_type'],
            run_state=RunState[as_dict['run_state']],
            phase_name=as_dict.get('phase_name'),
            attributes=as_dict.get('attributes'),
            created_at=as_dict['created_at'],
            started_at=util.parse_datetime(as_dict['started_at']) if as_dict.get('started_at') else None,
            termination=TerminationInfo.deserialize(as_dict['termination']) if as_dict.get('termination') else None,
            children=children,
        )

    def serialize(self) -> Dict[str, Any]:
        """
        Creates a dictionary representation of this PhaseView.

        Returns:
            Dict[str, Any]: The serialized phase view
        """
        result = {
            'phase_id': self.phase_id,
            'phase_type': self.phase_type,
            'run_state': self.run_state.name,
        }

        if self.phase_name:
            result['phase_name'] = self.phase_name
        if self.attributes:
            result['attributes'] = self.attributes
        if self.started_at:
            result['started_at'] = format_dt_iso(self.started_at)
        if self.termination:
            result['termination'] = self.termination.serialize()
        if self.children:
            result['children'] = [child.serialize() for child in self.children]

        return result

    def descendants(self, predicate: Optional[Callable[['PhaseDetail'], bool]] = None) -> List['PhaseDetail']:
        """
        Returns all descendant phases in depth-first order.
        Includes children, grandchildren, and so on.

        Args:
            predicate: Optional function to filter phases.

        Returns:
            List of descendant phase snapshots, optionally filtered
        """
        result = []
        for child in self.children or []:
            if not predicate or predicate(child):
                result.append(child)
            result.extend(child.descendants(predicate))
        return result

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

    @property
    def stage(self):
        if self.termination:
            return Stage.ENDED
        if self.started_at:
            return Stage.RUNNING
        return Stage.CREATED

    @property
    def last_change_at(self):
        if self.termination:
            return self.termination.terminated_at
        return self.started_at or self.created_at

    def is_running(self) -> bool:
        """
        Returns True if this phase is currently running.
        """
        return self.stage == Stage.RUNNING

    def is_ended(self) -> bool:
        """
        Returns True if this phase has ended execution (successfully or not).
        """
        return self.stage == Stage.ENDED

    def completed_successfully(self) -> bool:
        """
        Returns True if this phase completed successfully.
        """
        return self.termination is not None and self.termination.status == TerminationStatus.COMPLETED


@dataclass
class PhaseUpdateEvent:
    phase_detail: PhaseDetail
    new_stage: Stage
    timestamp: datetime


class PhaseUpdateObserver(ABC):

    @abstractmethod
    def new_phase_update(self, event: PhaseUpdateEvent):
        pass


class TerminateRun(Exception):
    def __init__(self, term_status: TerminationStatus):
        if term_status.value <= 1:
            raise ValueError("Termination status code must be >1 but it was: " + str(term_status.value))
        self.term_status = term_status
        super().__init__(f"Termination status: {term_status}")


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


class Phase(ABC, Generic[C]):

    @property
    @abstractmethod
    def id(self):
        pass

    @property
    @abstractmethod
    def type(self) -> str:
        """
        The type of this phase. Should be defined as a constant value in each implementing class.
        """
        pass

    @property
    @abstractmethod
    def run_state(self) -> RunState:
        """
        The run state of this phase. Should be defined as a constant value in each implementing class.
        """
        pass

    @property
    def name(self) -> Optional[str]:
        return None

    @property
    def attributes(self):
        return {}

    @property
    def info(self) -> PhaseInfo:
        return PhaseInfo(self.id, self.type, self.run_state, self.name, self.attributes)

    @property
    @abstractmethod
    def stop_status(self):
        pass

    @property
    def control(self):
        return PhaseControl(self)

    @abstractmethod
    def run(self, ctx: Optional[C]):
        pass

    @abstractmethod
    def stop(self):
        pass


class FailedRun(Exception):
    """
    This exception is used to provide additional information about a run failure.
    """

    def __init__(self, fault):
        super().__init__(fault.reason)
        self.fault = fault


@dataclass(frozen=True)
class Run:
    phases: Tuple[PhaseInfo, ...]
    lifecycle: Lifecycle
    termination: Optional[TerminationInfo]

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]):
        return cls(
            phases=tuple(PhaseInfo.deserialize(phase) for phase in as_dict['phases']),
            lifecycle=Lifecycle.deserialize(as_dict['lifecycle']),
            termination=TerminationInfo.deserialize(as_dict['termination']) if as_dict.get('termination') else None,
        )

    def serialize(self) -> Dict[str, Any]:
        return {
            "phases": [phase.serialize() for phase in self.phases],
            "lifecycle": self.lifecycle.serialize(),
            "termination": self.termination.serialize() if self.termination else None,
        }

    @property
    def current_phase(self) -> Optional[PhaseInfo]:
        current_phase_id = self.lifecycle.current_phase_id
        if not current_phase_id:
            return None

        for p in self.phases:
            if p.phase_id == current_phase_id:
                return p

        return None

    def find_phase(self, criterion) -> Optional[PhaseInfo]:
        for phase in self.phases:
            if criterion(phase):
                return phase

        return None

    def phase_after(self, phase: PhaseInfo) -> Optional[PhaseInfo]:
        """
        Returns the phase that comes immediately after the given phase in the sequence.

        Args:
            phase: The reference phase to find the next phase from

        Returns:
            The phase that follows the given phase, or None if the given phase
            is the last one or not found in the sequence
        """
        try:
            idx = self.phases.index(phase)
            return self.phases[idx + 1] if idx + 1 < len(self.phases) else None
        except ValueError:
            return None
