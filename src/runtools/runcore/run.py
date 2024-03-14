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
import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from copy import copy
from dataclasses import dataclass
from enum import Enum, EnumMeta
from threading import Event, Condition
from typing import Optional, List, Dict, Any, TypeVar, Callable, Tuple, Iterable, Generic, NamedTuple

from runtools.runcore import util
from runtools.runcore.common import InvalidStateError
from runtools.runcore.util import format_dt_iso, is_empty

log = logging.getLogger(__name__)


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
    CREATED = 1
    PENDING = 2
    WAITING = 3
    EVALUATING = 4
    IN_QUEUE = 5
    EXECUTING = 6
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


class PhaseKey(NamedTuple):
    type: str
    id: str

    def serialize(self) -> dict:
        return {"type": self.type, "id": self.id}

    @classmethod
    def deserialize(cls, data: dict) -> 'PhaseKey':
        return cls(data["type"], data["id"])

    def __str__(self) -> str:
        return f"{self.type}-{self.id}"


@dataclass
class PhaseRun:
    phase_key: PhaseKey
    run_state: RunState
    started_at: Optional[datetime.datetime]
    ended_at: Optional[datetime.datetime] = None

    @classmethod
    def deserialize(cls, d):
        return cls(
            PhaseKey.deserialize(d['phase_key']),
            RunState[d['run_state']],
            util.parse_datetime(d['started_at']),
            util.parse_datetime(d['ended_at'])
        )

    def serialize(self):
        return {
            'phase_key': self.phase_key.serialize(),
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
        return bool(self.phase_key) and self.run_state != RunState.NONE

    def __copy__(self):
        return PhaseRun(self.phase_key, self.run_state, self.started_at, self.ended_at)


NONE_PHASE_RUN = PhaseRun(PhaseKey('', ''), RunState.NONE, None, None)


class Lifecycle:
    """
    This class represents the lifecycle of a run. A lifecycle consists of a chronological sequence of phase transitions.
    Each phase has a timestamp that indicates when the transition to that phase occurred.
    """

    def __init__(self, *phase_runs: PhaseRun):
        self._phase_runs: OrderedDict[PhaseKey, PhaseRun] = OrderedDict()
        self._current_run: Optional[PhaseRun] = None
        self._previous_run: Optional[PhaseRun] = None
        for run in phase_runs:
            self.add_phase_run(run)

    def add_phase_run(self, phase_run: PhaseRun):
        """
        Adds a new phase run to the lifecycle.
        """
        if phase_run.phase_key in self._phase_runs:
            raise ValueError(f"Phase {phase_run.phase_key} already in this lifecycle: {self.phases}")

        if self.current_run:
            self._previous_run = self._current_run
            self._previous_run.ended_at = phase_run.started_at

        self._current_run = phase_run
        self._phase_runs[phase_run.phase_key] = phase_run

    @classmethod
    def deserialize(cls, as_dict):
        phase_runs = []
        for transition in as_dict['transitions']:
            phase_key = PhaseKey.deserialize(transition['phase'])
            run_state = RunState[transition['state']]
            started_at = util.parse_datetime(transition['ts'])

            # Determine the ended_at for each phase
            # The end of a phase is the start of the next phase, if there is one
            if phase_runs:
                phase_runs[-1].ended_at = started_at

            phase_runs.append(PhaseRun(phase_key, run_state, started_at, None))

        return cls(*phase_runs)

    def serialize(self) -> Dict[str, Any]:
        return {
            "transitions": [
                {'phase': run.phase_key.serialize(), 'state': run.run_state.value, 'ts': format_dt_iso(run.started_at)}
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
    def current_phase(self) -> Optional[PhaseKey]:
        return self._current_run.phase_key if self._current_run else None

    @property
    def previous_run(self) -> Optional[PhaseRun]:
        return self._previous_run or NONE_PHASE_RUN

    @property
    def previous_phase_name(self) -> Optional[str]:
        return self._previous_run.phase_key if self._previous_run else None

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
    def phases(self) -> List[PhaseKey]:
        return list(self._phase_runs.keys())

    @property
    def phase_runs(self) -> List[PhaseRun]:
        return list(self._phase_runs.values())

    def phase_run(self, phase_key) -> Optional[PhaseRun]:
        return self._phase_runs.get(phase_key) or NONE_PHASE_RUN

    def runs_between(self, phase_from, phase_to) -> List[PhaseRun]:
        runs = []
        for run in self._phase_runs.values():
            if run.phase_key == phase_to:
                if not runs:
                    if phase_from == phase_to:
                        return [run]
                    else:
                        return []
                runs.append(run)
                return runs
            elif run.phase_key == phase_from or runs:
                runs.append(run)

        return []

    def phases_between(self, phase_from, phase_to) -> List[PhaseKey]:
        return [run.phase_key for run in self.runs_between(phase_from, phase_to)]

    def phase_started_at(self, phase_key) -> Optional[datetime.datetime]:
        phase_run = self._phase_runs.get(phase_key)
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


PHASE_INFO_REGISTRY = {}


def register_phase_info(phase_type: Enum | str):
    if isinstance(phase_type, Enum):
        phase_type = phase_type.value

    def decorator(cls):
        PHASE_INFO_REGISTRY[phase_type] = cls
        return cls

    return decorator


@dataclass(frozen=True)
class PhaseInfo:
    phase_type: str
    phase_id: str
    run_state: RunState
    phase_name: Optional[str]
    protection_id: Optional[str]
    last_protected_phase: Optional[PhaseKey]

    @classmethod
    def deserialize(cls, as_dict) -> 'PhaseInfo':
        info_cls = PHASE_INFO_REGISTRY.get(as_dict["phase_type"])
        if info_cls and info_cls != PhaseInfo and 'deserialize' in info_cls.__dict__:
            return info_cls.deserialize(as_dict)

        return cls(
            as_dict["phase_type"],
            as_dict["phase_id"],
            RunState[as_dict["run_state"]],
            as_dict.get("phase_name"),
            as_dict.get("protection_id"),
            PhaseKey.deserialize(as_dict["last_protected_phase"]) if "last_protected_phase" in as_dict else None,
        )

    def serialize(self) -> Dict:
        d = {
            "phase_type": self.phase_type,
            "phase_id": self.phase_id,
            "run_state": self.run_state.name,
        }
        if self.phase_name:
            d["phase_name"] = self.phase_name
        if self.protection_id:
            d["protection_id"] = self.protection_id
        if self.last_protected_phase:
            d["last_protected_phase"] = self.last_protected_phase.serialize()

        return d

    @property
    def key(self):
        return PhaseKey(self.phase_type, self.phase_id)


class TerminateRun(Exception):
    def __init__(self, term_status: TerminationStatus):
        if term_status.value <= 1:
            raise ValueError("Termination status code must be >1 but it was: " + str(term_status.value))
        self.term_status = term_status
        super().__init__(f"Termination status: {term_status}")


class RunContext(ABC):

    @property
    @abstractmethod
    def task_tracker(self):
        pass

    @abstractmethod
    def new_output(self, output, is_err=False):
        pass

    def create_logging_handler(self):
        """
        Creates and returns a logging.Handler instance that forwards log records
        to this OutputToTask instance.
        """

        class InternalHandler(logging.Handler):
            def __init__(self, outer_instance):
                super().__init__()
                self.outer_instance = outer_instance

            def emit(self, record):
                output = self.format(record)  # Convert log record to a string
                is_error = record.levelno >= logging.ERROR
                self.outer_instance.new_output(output, is_error)

        return InternalHandler(self)


class PhaseKeys:
    INIT = PhaseKey('INIT', 'init')
    TERMINAL = PhaseKey('TERMINAL', 'term')


class Phase(ABC):
    """
    TODO repr
    """

    def __init__(self, phase_type: str | Enum, phase_id: str, run_state: RunState, phase_name: Optional[str] = None,
                 *, protection_id=None, last_protected_phase=None):
        if isinstance(phase_type, Enum):
            phase_type = phase_type.value
        self._phase_type = phase_type
        self._phase_id = phase_id
        self._run_state = run_state
        self._phase_name = phase_name
        self._protection_id = protection_id
        self._last_protected_phase = last_protected_phase

    @property
    def key(self):
        return PhaseKey(self._phase_type, self._phase_id)

    @property
    def type(self):
        return self._phase_type

    @property
    def id(self):
        return self._phase_id

    @property
    def run_state(self):
        return self._run_state

    def info(self) -> PhaseInfo:
        return PhaseInfo(self._phase_type, self._phase_id, self._run_state, self._phase_name, self._protection_id,
                         self._last_protected_phase)

    @property
    @abstractmethod
    def stop_status(self):
        pass

    @abstractmethod
    def run(self, run_ctx):
        pass

    @abstractmethod
    def stop(self):
        pass


class NoOpsPhase(Phase):

    def __init__(self, phase_type, phase_id, run_state, stop_status):
        super().__init__(phase_type, phase_id, run_state)
        self._stop_status = stop_status

    @property
    def stop_status(self):
        return self._stop_status

    def run(self, run_ctx):
        """No activity on run"""
        pass

    def stop(self):
        """Nothing to stop"""
        pass


class InitPhase(NoOpsPhase):

    def __init__(self):
        super().__init__(PhaseKeys.INIT.type, PhaseKeys.INIT.id, RunState.CREATED, TerminationStatus.STOPPED)


class TerminalPhase(NoOpsPhase):

    def __init__(self):
        super().__init__(PhaseKeys.TERMINAL.type, PhaseKeys.TERMINAL.id, RunState.ENDED, TerminationStatus.NONE)


class WaitWrapperPhase(Phase):

    def __init__(self, wrapped_phase):
        super().__init__(wrapped_phase.type, wrapped_phase.id, wrapped_phase.run_state)
        self.wrapped_phase = wrapped_phase
        self._run_event = Event()

    @property
    def stop_status(self):
        return self.wrapped_phase.stop_status

    def wait(self, timeout):
        self._run_event.wait(timeout)

    def run(self, run_ctx):
        self._run_event.set()
        self.wrapped_phase.run(run_ctx)

    def stop(self):
        self.wrapped_phase.stop()


@dataclass
class Fault:
    category: str
    reason: str


@dataclass
class RunFailure(Fault):

    def serialize(self):
        return {"cat": self.category, "reason": self.reason}

    @classmethod
    def deserialize(cls, as_dict):
        return cls(as_dict["cat"], as_dict["reason"])


@dataclass
class RunError(Fault):

    def serialize(self):
        return {"cat": self.category, "reason": self.reason}

    @classmethod
    def deserialize(cls, as_dict):
        return cls(as_dict["cat"], as_dict["reason"])


class FailedRun(Exception):
    """
    This exception is used to provide additional information about a run failure.
    """

    def __init__(self, fault_type: str, reason: str):
        super().__init__(f"{fault_type}: {reason}")
        self.fault = RunFailure(fault_type, reason)


@dataclass(frozen=True)
class TerminationInfo:
    status: TerminationStatus
    terminated_at: datetime.datetime
    failure: Optional[RunFailure] = None
    error: Optional[RunError] = None

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]):
        return cls(
            status=TerminationStatus[as_dict['termination_status']],
            terminated_at=util.parse_datetime(as_dict['terminated_at']),
            failure=RunFailure.deserialize(as_dict['failure']) if as_dict.get('failure') else None,
            error=RunError.deserialize(as_dict['error']) if as_dict.get('error') else None
        )

    def serialize(self) -> Dict[str, Any]:
        return {
            "termination_status": self.status.name,
            "terminated_at": format_dt_iso(self.terminated_at),
            "failure": self.failure.serialize() if self.failure else None,
            "error": self.error.serialize() if self.error else None
        }


@dataclass(frozen=True)
class Run:
    phases: Tuple[PhaseInfo]
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

    def current_phase(self):
        current_phase_key = self.lifecycle.current_phase
        if not current_phase_key:
            return None

        for p in self.phases:
            if p.key == current_phase_key:
                return p

        return None

    def in_protected_phase(self, protection_phase_type, protection_id) -> bool:
        if isinstance(protection_phase_type, Enum):
            protection_phase_type = protection_phase_type.value
        return self.lifecycle.current_phase in self.protected_phases(protection_phase_type, protection_id)

    def protected_phases(self, protection_phase_type, protection_id) -> List[PhaseKey]:
        if isinstance(protection_phase_type, Enum):
            protection_phase_type = protection_phase_type.value
        protected_phase_start = None
        protected_phase_end = None

        for idx, phase_info in enumerate(self.phases):
            if phase_info.phase_type == protection_phase_type and phase_info.protection_id == protection_id:
                if (idx + 1) < len(self.phases):
                    protected_phase_start = self.phases[idx + 1].key
                    protected_phase_end = phase_info.last_protected_phase
                break

        if not protected_phase_start:
            return []

        return self.lifecycle.phases_between(protected_phase_start, protected_phase_end or protected_phase_start)


def unique_phases_to_dict(phases) -> Dict[PhaseKey, Phase]:
    key_to_phase = {}
    for phase in phases:
        if phase.key in key_to_phase:
            raise ValueError(f"Duplicate phase found: {phase.key}")
        key_to_phase[phase.key] = phase
    return key_to_phase


class CommonEntities(Enum):
    JOB = 'job'
    SERVICE = 'service'


@dataclass
class InstanceMetadata(ABC):
    """
    A dataclass that contains metadata information related to a specific job run. This object is designed
    to represent essential information about a job run in a compact and serializable format. By using this object
    instead of a full `JobRun` snapshot, you can reduce the amount of data transmitted when sending information
    across a network or between different parts of a system.

    Attributes:
        entity_id (str):
            The unique identifier of the job associated with the instance.
        run_id (str):
            The unique identifier of the job instance run.
        instance_id (str):
            The reference identifier of the job instance.
        system_parameters (Dict[str, Any]):
            A dictionary containing system parameters for the job instance.
            These parameters are implementation-specific and contain information needed by the system to
            perform certain tasks or enable specific features.
        user_params (Dict[str, Any]):
            A dictionary containing user-defined parameters associated with the instance.
            These are arbitrary parameters set by the user, and they do not affect the functionality.
    """
    entity_id: str
    run_id: str
    instance_id: str
    system_parameters: Dict[str, Any]
    user_params: Dict[str, Any]

    def serialize(self) -> Dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "entity_type": self.entity_type,
            "run_id": self.run_id,
            "instance_id": self.instance_id,
            "system_parameters": self.system_parameters,
            "user_params": self.user_params,
        }

    @property
    @abstractmethod
    def entity_type(self):
        pass

    def contains_system_parameters(self, *params):
        return all(param in self.system_parameters for param in params)

    def __repr__(self) -> str:
        return f"{self.entity_id}@{self.run_id}:{self.instance_id}"


class JobInstanceMetadata(InstanceMetadata):

    @classmethod
    def deserialize(cls, as_dict):
        return cls(
            as_dict['entity_id'],
            as_dict['run_id'],
            as_dict['instance_id'],
            as_dict['system_parameters'],
            as_dict['user_params'],
        )

    @property
    def entity_type(self):
        return CommonEntities.JOB.value


M = TypeVar('M', bound=InstanceMetadata)


@dataclass(frozen=True)
class EntityRun(Generic[M]):
    """Descriptive information about this run"""
    metadata: M
    """The snapshot of the run represented by this instance"""
    run: Run


P = TypeVar('P')


class AbstractPhaser:

    def __init__(self, phases: Iterable[Phase], *, timestamp_generator=util.utc_now):
        self._key_to_phase: Dict[PhaseKey, Phase] = unique_phases_to_dict(phases)
        self._timestamp_generator = timestamp_generator

        self.transition_hook: Optional[Callable[[PhaseRun, PhaseRun, int], None]] = None
        self.output_hook: Optional[Callable[[PhaseInfo, str, bool], None]] = None

    def get_phase(self, phase_type: str | Enum, phase_id):
        if isinstance(phase_type, Enum):
            phase_type = phase_type.value
        return self._key_to_phase.get(PhaseKey(phase_type, phase_id))

    @property
    def phases(self) -> Dict[PhaseKey, Phase]:
        return self._key_to_phase.copy()


class Phaser(AbstractPhaser):

    def __init__(self, phases: Iterable[Phase], lifecycle=None, *, timestamp_generator=util.utc_now):
        super().__init__(phases, timestamp_generator=timestamp_generator)

        self._transition_lock = Condition()
        # Guarded by the transition/state lock:
        self._lifecycle = lifecycle or Lifecycle()
        self._current_phase = None
        self._stop_status = TerminationStatus.NONE
        self._abort = False
        self._termination: Optional[TerminationInfo] = None
        # ----------------------- #

    @property
    def current_phase(self):
        return self._current_phase

    def _term_info(self, termination_status, failure=None, error=None):
        return TerminationInfo(termination_status, self._timestamp_generator(), failure, error)

    def run_info(self) -> Run:
        with self._transition_lock:
            phases = tuple(p.info() for p in self._key_to_phase.values())
            return Run(phases, copy(self._lifecycle), self._termination)

    def prime(self):
        with self._transition_lock:
            if self._current_phase:
                raise InvalidStateError("Primed already")
            self._next_phase(InitPhase())

    def run(self, task_tracker=None):
        if not self._current_phase:
            raise InvalidStateError('Prime not executed before run')

        task_tracker = task_tracker

        class _RunContext(RunContext):

            def __init__(self, phaser: Phaser, ctx_phase):
                self._phaser = phaser
                self._ctx_phase = ctx_phase
                self._task_tracker = task_tracker

            @property
            def task_tracker(self):
                return self._task_tracker

            def new_output(self, output, is_err=False):
                self._phaser.output_hook(self._ctx_phase.info(), output, is_err)

        for phase in self._key_to_phase.values():
            with self._transition_lock:
                if self._abort:
                    return

                self._next_phase(phase)

            term_info, exc = self._run_handle_errors(phase, _RunContext(self, phase))

            with self._transition_lock:
                if self._stop_status:
                    self._termination = self._term_info(self._stop_status)
                elif term_info:
                    self._termination = term_info

                if isinstance(exc, BaseException):
                    assert self._termination
                    self._next_phase(TerminalPhase())
                    raise exc
                if self._termination:
                    self._next_phase(TerminalPhase())
                    return

        with self._transition_lock:
            self._termination = self._term_info(TerminationStatus.COMPLETED)
            self._next_phase(TerminalPhase())

    def _run_handle_errors(self, phase: Phase, run_ctx: RunContext) \
            -> Tuple[Optional[TerminationInfo], Optional[BaseException]]:
        try:
            phase.run(run_ctx)
            return None, None
        except TerminateRun as e:
            return self._term_info(e.term_status), None
        except FailedRun as e:
            return self._term_info(TerminationStatus.FAILED, failure=e.fault), None
        except Exception as e:
            # TODO print exception
            run_error = RunError(e.__class__.__name__, str(e))
            return self._term_info(TerminationStatus.ERROR, error=run_error), e
        except KeyboardInterrupt as e:
            log.warning('keyboard_interruption')
            phase.stop()
            return self._term_info(TerminationStatus.INTERRUPTED), e
        except SystemExit as e:
            # Consider UNKNOWN (or new state DETACHED?) if there is possibility the execution is not completed
            term_status = TerminationStatus.COMPLETED if e.code == 0 else TerminationStatus.FAILED
            return self._term_info(term_status), e

    def _next_phase(self, phase):
        """
        Impl note: The execution must be guarded by the phase lock (except terminal phase)
        """
        assert self._current_phase != phase

        self._current_phase = phase
        self._lifecycle.add_phase_run(PhaseRun(phase.key, phase.run_state, self._timestamp_generator()))
        if self.transition_hook:
            self.execute_transition_hook_safely(self.transition_hook)
        with self._transition_lock:
            self._transition_lock.notify_all()

    def execute_transition_hook_safely(self, transition_hook: Optional[Callable[[PhaseRun, PhaseRun, int], None]]):
        with self._transition_lock:
            lc = copy(self._lifecycle)
            transition_hook(lc.previous_run, lc.current_run, lc.phase_count)

    def stop(self):
        with self._transition_lock:
            if self._termination:
                return

            self._stop_status = self._current_phase.stop_status if self._current_phase else TerminationStatus.STOPPED
            if not self._current_phase or (self._current_phase.key == PhaseKeys.INIT):
                # Not started yet
                self._abort = True  # Prevent phase transition...
                self._termination = self._term_info(self._stop_status)
                self._next_phase(TerminalPhase())

        self._current_phase.stop()

    def wait_for_transition(self, phase_name=None, run_state=RunState.NONE, *, timeout=None):
        with self._transition_lock:
            while True:
                for run in self._lifecycle.phase_runs:
                    if run.phase_key == phase_name or run.run_state == run_state:
                        return True

                if not self._transition_lock.wait(timeout):
                    return False
                if not phase_name and not run_state:
                    return True
