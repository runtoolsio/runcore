"""
This module defines the 'job' component of the job framework. Together with the 'instance' component
in the `inst` module, they establish the foundation of this framework.

A job consists of an ID and may have static attributes. When a job is executed, it creates a job instance.
Naturally, while the specific task a job instance performs is user-defined, each instance of the same job is expected
to perform the same task.
"""
import abc
import datetime
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, auto
from typing import Dict, Any, List, Optional, Tuple, Iterator, ClassVar, Set, Callable

from runtools.runcore import util
from runtools.runcore.output import OutputLine, Output, OutputLocation
from runtools.runcore.run import TerminationStatus, Fault, PhaseDetail, Stage, RunLifecycle, StopReason
from runtools.runcore.status import Status
from runtools.runcore.util import MatchingStrategy, format_dt_iso, unique_timestamp_hex
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY, ObservableNotification


class JobType(Enum):
    BATCH = auto()
    LONG_RUNNING = auto()


class Job:
    """
    Represents a job definition.

    This class encapsulates the properties and characteristics of a job. Each individual execution
    of a job is represented as an object of the `JobInstance` class.

    Attributes:
        _id (str): Unique identifier for the job.
        _properties (Dict[str, str]): Additional properties or metadata associated with the job.
    """

    def __init__(self, job_id: str, job_type: JobType, properties: Dict[str, str] = None):
        """
        Initialize a new Job object.

        Args:
            job_id (str): Unique identifier for the job.
            properties (Dict[str, str], optional): Additional properties or metadata. Defaults to an empty dictionary.
        """
        self._id = job_id
        self._type = job_type
        self._properties = properties or {}

    @property
    def id(self) -> str:
        """
        Returns the unique identifier of the job.

        Returns:
            str: The job's unique identifier.
        """
        return self._id

    @property
    def type(self) -> JobType:
        """
        Returns the type of the job.

        Returns:
            str: The job's type.
        """
        return self._type

    @property
    def properties(self) -> Dict[str, str]:
        """
        Returns the properties or metadata associated with the job.

        Returns:
            Dict[str, str]: Dictionary containing job properties.
        """
        return self._properties

    def __eq__(self, other: object) -> bool:
        """Checks if two Job objects are equal based on their unique ID and properties."""
        if not isinstance(other, Job):
            return False
        return self._id == other._id and self._type == other._type and self._properties == other._properties

    def __hash__(self) -> int:
        """Returns the hash based on the job's unique ID and properties."""
        return hash((self._id, self._type, frozenset(self._properties.items())))


class JobMatchingCriteria:
    """
    Specifies criteria for matching `Job` instances based on job properties and a matching strategy.

    Attributes:
        properties (Dict[str, str], optional): Dictionary of properties to match against.
        property_match_strategy (MatchingStrategy): Strategy function to use for matching property values.
    """

    def __init__(self, *, properties=None, property_match_strategy=MatchingStrategy.EXACT):
        """
        Initializes the JobMatchingCriteria with the given properties and matching strategy.

        Args:
            properties (Dict[str, str], optional):
                Dictionary of properties to match against.
            property_match_strategy (MatchingStrategy):
                Strategy function to use for matching property values. Defaults to an exact match.
        """
        self.properties = properties
        self.property_match_strategy = property_match_strategy

    def matches(self, job) -> bool:
        """
        Determines if the given job matches the set criteria.

        Args:
            job (Job): The job instance to check against the criteria.

        Returns:
            bool: True if the job matches the criteria, otherwise False.
        """
        if not self.properties:
            return True

        for k, v in self.properties.items():
            prop = job.properties.get(k)
            if not prop:
                return False
            if not self.property_match_strategy(prop, v):
                return False

        return True

    def matched(self, jobs) -> List[Job]:
        """
        Returns a list of jobs that match the set criteria.

        Args:
            jobs (List[Job]): A list of job instances to check against the criteria.

        Returns:
            List[Job]: A list of job instances that match the criteria.
        """
        return [job for job in jobs if self.matches(job)]


@dataclass
class JobStats:
    """
    Represents the statistics related to a specific job over a certain time period.

    These statistics provide insights into the job's performance and status trends during
    the specified timeframe.

    Attributes:
        job_id (str): Unique identifier for the job.
        count (int): Number of instances of the job within the time interval.
        first_created (datetime): Creation time of the first job instance in the interval.
        last_created (datetime): Creation time of the last job instance in the interval.
        fastest_time (timedelta): Shortest execution time among all instances in the interval.
        average_time (timedelta): Average execution time across all instances in the interval.
        slowest_time (timedelta): Longest execution time among all instances in the interval.
        last_time (timedelta): Execution time of the most recent instance in the interval.
        termination_status (TerminationStatus): State of the last executed instance in the interval.
        failed_count (int): Number of instances that failed during the time interval.
        warning_count (int): Number of instances with at least one warning during the time interval.
    """
    job_id: str
    count: int = 0
    first_created: Optional[datetime] = None
    last_created: Optional[datetime] = None
    fastest_time: Optional[timedelta] = None
    average_time: Optional[timedelta] = None
    slowest_time: Optional[timedelta] = None
    last_time: Optional[timedelta] = None
    termination_status: TerminationStatus = TerminationStatus.NONE
    failed_count: int = 0
    warning_count: int = 0


class StatsSortOption(str, Enum):
    """
    Options for sorting job statistics records.

    Attributes:
        JOB_ID: Sort alphabetically by job identifier
        FIRST_CREATED: Sort by first instance creation timestamp
        LAST_CREATED: Sort by last instance creation timestamp
        AVERAGE_TIME: Sort by average execution duration
        SLOWEST_TIME: Sort by slowest execution duration
        FASTEST_TIME: Sort by fastest execution duration
        LAST_TIME: Sort by most recent execution duration
        COUNT: Sort by number of executions
        FAILED_COUNT: Sort by number of failed executions
    """
    JOB_ID = "job"
    FIRST_CREATED = "oldest"
    LAST_CREATED = "newest"
    AVERAGE_TIME = "avg"
    SLOWEST_TIME = "slow"
    FASTEST_TIME = "fast"
    LAST_TIME = "last"
    COUNT = "runs"
    FAILED_COUNT = "fails"

    def sort_stats(self, job_stats_list, *, reverse=False):
        """
        Sorts a list of JobStats objects using this sort option.

        Args:
            job_stats_list: List of JobStats objects to sort
            reverse: If True, sort in descending order

        Returns:
            List[JobStats]: Sorted list of job statistics
        """
        match self:
            case StatsSortOption.JOB_ID:
                return sorted(job_stats_list, key=lambda s: s.job_id, reverse=reverse)
            case StatsSortOption.FIRST_CREATED:
                return sorted(job_stats_list, key=lambda s: s.first_created or s.last_created, reverse=reverse)
            case StatsSortOption.LAST_CREATED:
                return sorted(job_stats_list, key=lambda s: s.last_created, reverse=reverse)
            case StatsSortOption.AVERAGE_TIME:
                return sorted(job_stats_list, key=lambda s: s.average_time or s.last_time, reverse=reverse)
            case StatsSortOption.SLOWEST_TIME:
                return sorted(job_stats_list, key=lambda s: s.slowest_time or s.last_time, reverse=reverse)
            case StatsSortOption.FASTEST_TIME:
                return sorted(job_stats_list, key=lambda s: s.fastest_time or s.last_time, reverse=reverse)
            case StatsSortOption.LAST_TIME:
                return sorted(job_stats_list, key=lambda s: s.last_time, reverse=reverse)
            case StatsSortOption.COUNT:
                return sorted(job_stats_list, key=lambda s: s.count, reverse=reverse)
            case StatsSortOption.FAILED_COUNT:
                return sorted(job_stats_list, key=lambda s: s.failed_count, reverse=reverse)
            case _:
                raise AssertionError(f"Programmer error - unimplemented key for sort option: {self}")


def iid(job_id, run_id=None):
    return InstanceID(job_id, run_id) if run_id else InstanceID(job_id)


@dataclass(frozen=True)
class InstanceID(Sequence):
    FORBIDDEN_CHARS: ClassVar[Set[str]] = frozenset("!@#")
    job_id: str
    run_id: str = field(default_factory=unique_timestamp_hex)

    def __post_init__(self):
        if not self.job_id:
            raise ValueError("Instance `job_id` must be non‑empty")
        if not self.run_id:
            object.__setattr__(self, 'run_id', unique_timestamp_hex())
        if InstanceID.FORBIDDEN_CHARS.intersection(self.job_id) or InstanceID.FORBIDDEN_CHARS.intersection(self.run_id):
            raise ValueError(
                f"Instance ID identifiers cannot contain characters: {", ".join(InstanceID.FORBIDDEN_CHARS)}")

    @classmethod
    def parse(cls, id_string: str) -> "InstanceID":
        """
        Parse an instance ID from a string in the format 'job_id@run_id'.

        Args:
            id_string: A string in the format 'job_id@run_id'

        Returns:
            An InstanceID object

        Raises:
            ValueError: If the string is not in the expected format
        """
        if not id_string:
            raise ValueError("Instance ID string cannot be empty")

        if '@' not in id_string:
            raise ValueError("Instance ID must be in format 'job_id@run_id'")

        job_id, run_id = id_string.split('@', 1)

        if not job_id:
            raise ValueError("job_id part cannot be empty")
        if not run_id:
            raise ValueError("run_id part cannot be empty")

        return cls(job_id=job_id, run_id=run_id)

    def __len__(self) -> int:
        return 2

    def __getitem__(self, index: int) -> Any:
        if index == 0:
            return self.job_id
        if index == 1:
            return self.run_id
        raise IndexError(f"Index {index} out of range for InstanceID")

    def __iter__(self) -> Iterator[Any]:
        yield self.job_id
        yield self.run_id

    def serialize(self) -> Dict[str, str]:
        return {"job_id": self.job_id, "run_id": self.run_id}

    @classmethod
    def deserialize(cls, data: Dict[str, str]) -> "InstanceID":
        return cls(job_id=data["job_id"], run_id=data["run_id"])

    def __str__(self) -> str:
        return f"{self.job_id}@{self.run_id}"


@dataclass(frozen=True)
class JobInstanceMetadata:
    """
    A dataclass that contains descriptive information about a specific job instance. This object is designed
    to represent essential information about a job run in a compact and serializable format.
    TODO Add job type

    Attributes:
        instance_id (InstanceID):
            Unique identifier for the job instance, composed of job_id and run_id.
        user_params (Dict[str, Any]):
            A dictionary containing user-defined parameters associated with the instance.
            These are arbitrary parameters set by the user, and they do not affect the functionality.
    """
    instance_id: InstanceID
    user_params: Dict[str, Any] = field(default_factory=dict, compare=False, hash=False)

    @property
    def job_id(self) -> str:
        """The unique identifier of the job associated with the instance."""
        return self.instance_id.job_id

    @property
    def run_id(self) -> str:
        """The unique identifier of the job instance run."""
        return self.instance_id.run_id

    def serialize(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "run_id": self.run_id,
            "user_params": self.user_params,
        }

    @classmethod
    def deserialize(cls, as_dict):
        return cls(
            instance_id=InstanceID(as_dict['job_id'], as_dict['run_id']),
            user_params=as_dict['user_params'],
        )

    def __eq__(self, other):
        if not isinstance(other, JobInstanceMetadata):
            return NotImplemented
        return self.instance_id == other.instance_id

    def __hash__(self):
        return hash(self.instance_id)

    def __repr__(self) -> str:
        return str(self.instance_id)


class JobInstance(abc.ABC):
    """
    The `JobInstance` class is a central component of this package. It denotes a single occurrence of a job.
    While the job itself describes static attributes common to all its instances, the JobInstance class
    represents a specific run of that job.

    TODO add/remove status output
    """

    @property
    @abc.abstractmethod
    def metadata(self):
        """
        Returns:
            InstanceMetadata: Identifiers and descriptive information about this instance.
        """

    @property
    def id(self):
        """
        Returns:
            InstanceID: Unique identifier of this instance.
        """
        return self.metadata.instance_id

    @property
    def job_id(self):
        """
        Returns:
            str: Identifier of the job.
        """
        return self.metadata.job_id

    @property
    def run_id(self):
        """
        Returns:
            str: Identifier of the individual run.
        """
        return self.metadata.run_id

    @abc.abstractmethod
    def find_phase_control(self, phase_filter):
        """
        TODO
        """

    def find_phase_control_by_id(self, phase_id: str):
        return self.find_phase_control(lambda phase: phase.phase_id == phase_id)

    @abc.abstractmethod
    def to_run(self):
        """
        Creates a consistent, thread-safe snapshot of the job instance's current state.

        Returns:
            JobRun: A snapshot representing the current state of the job instance.
        """

    @property
    @abc.abstractmethod
    def output(self) -> Output:
        pass

    @abc.abstractmethod
    def run(self):
        """
        Run the job.

        This method is not expected to raise any errors. In case of any failure the error details can be retrieved
        by calling `exec_error` method.
        """

    @abc.abstractmethod
    def stop(self, reason=StopReason.STOPPED):
        """
        Attempts to cancel a scheduled job or stop a job that is already executing.

        Note:
            The way the stop request is handled can vary based on the implementation or the specific job.
            It's possible that not all instances will respond successfully to the stop request.
        """

    def add_observer_all_events(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self.add_observer_lifecycle(observer, priority)
        self.add_observer_transition(observer, priority)
        self.add_observer_output(observer, priority)

    def remove_observer_all_events(self, observer):
        self.remove_observer_lifecycle(observer)
        self.remove_observer_transition(observer)
        self.remove_observer_output(observer)

    @abc.abstractmethod
    def add_observer_lifecycle(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, reply_last_event=False):
        pass

    @abc.abstractmethod
    def remove_observer_lifecycle(self, observer):
        pass

    @abc.abstractmethod
    def add_observer_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """
        Register an instance state observer. Optionally, trigger a notification with the last known state
        upon registration.

        Notes for implementers: Prevent race-conditions when `notify_on_register` used.

        Args:
            observer:
                The observer to register. This can either be:
                1. An instance of `InstanceStateObserver`.
                2. A callable object with the signature of the `InstanceStateObserver.instance_phase_transition` method.
            priority (int, optional):
                Priority of the observer. Lower numbers are notified first.
        """

    @abc.abstractmethod
    def remove_observer_transition(self, observer):
        """
        De-register an execution state observer.
        Note: The implementation must cope with the scenario when this method is executed during notification.

        Args:
            observer: The observer to de-register.
        """

    def add_observer_output(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        pass

    def remove_observer_output(self, observer):
        pass


@dataclass(frozen=True)
class JobRun:
    """
    Immutable snapshot of job instance
    """
    metadata: JobInstanceMetadata
    lifecycle: RunLifecycle
    phases: Tuple[PhaseDetail, ...]
    output_locations: Tuple[OutputLocation, ...] = ()
    faults: Tuple[Fault, ...] = ()
    status: Optional[Status] = None

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'JobRun':
        return cls(
            metadata=JobInstanceMetadata.deserialize(as_dict['metadata']),
            lifecycle=RunLifecycle.deserialize(as_dict['lifecycle']),
            phases=tuple(PhaseDetail.deserialize(p) for p in as_dict['phases']),
            faults=tuple(Fault.deserialize(f) for f in as_dict.get('faults', [])),
            status=Status.deserialize(as_dict['status']) if as_dict.get('status') else None,
        )

    def serialize(self) -> Dict[str, Any]:
        d = {
            "metadata": self.metadata.serialize(),
            "lifecycle": self.lifecycle.serialize(),
            "phases": [p.serialize() for p in self.phases],
        }
        if self.faults:
            d["faults"] = [f.serialize() for f in self.faults]
        if self.status:
            d["status"] = self.status.serialize()
        return d

    @property
    def instance_id(self) -> InstanceID:
        """
        Returns:
            str: Unique identifier of the instance representing this job run.
        """
        return self.metadata.instance_id

    @property
    def job_id(self) -> str:
        """
        Returns:
            str: Job part of the job instance full identifier.
        """
        return self.metadata.job_id

    @property
    def run_id(self) -> str:
        """
        Returns:
            str: Run part of the job instance full identifier.
        """
        return self.metadata.run_id

    def search_phases(self, predicate: Optional[Callable[['PhaseDetail'], bool]] = None) -> List['PhaseDetail']:
        """
        Searches all phases within this job run (including root-level phases and
        all their descendants) that match the given predicate.

        The search for each root phase and its hierarchy is performed in a
        pre-order (depth-first) manner.

        Args:
            predicate: Optional function to filter phases. If None, all phases
                       (root and descendants across the entire job run) are returned.

        Returns:
            List[PhaseDetail]: A list of all matching phase details found.
        """
        matching_phases: List['PhaseDetail'] = []
        for phase in self.phases:
            matching_phases.extend(phase.search_phases(predicate=predicate, include_self=True))
        return matching_phases

    def find_first_phase(self, predicate):
        for p in self.phases:
            if found := p.find_first_phase(predicate):
                return found
        return None

    def find_phase_by_id(self, phase_id):
        for p in self.phases:
            if found := p.find_phase_by_id(phase_id):
                return found
        return None

    def accept_visitor(self, visitor):
        """
        Accept a visitor to traverse all phases in this job run.

        Args:
            visitor: The visitor to accept
        """
        for phase in self.phases:
            phase.accept_visitor(visitor)
        return visitor


class JobRuns(list):
    """
    List of job instances with auxiliary methods.
    """

    def __init__(self, runs):
        super().__init__(runs)

    @property
    def job_ids(self) -> List[str]:
        return [r.job_id for r in self]

    def in_phase(self, phase) -> 'JobRuns':
        return JobRuns([job_run for job_run in self if job_run.lifecycle.current_phase_id is phase])

    def in_protected_phase(self, protection_type, protection_id):
        return JobRuns([job_run for job_run in self if job_run.in_protected_phase(protection_type, protection_id)])

    def in_state(self, state):
        return [job_run for job_run in self if job_run.lifecycle.run_state is state]

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        return {"runs": [run.serialize(include_empty=include_empty) for run in self]}


@dataclass(frozen=True)
class InstanceLifecycleEvent:
    EVENT_TYPE = "instance_stage_update"

    instance: JobInstanceMetadata
    job_run: JobRun
    new_stage: Stage
    timestamp: datetime

    @property
    def event_type(self):
        return self.EVENT_TYPE

    def serialize(self) -> Dict[str, Any]:
        instance_meta = self.instance.serialize()
        return {
            "event_metadata": {
                "event_type": self.EVENT_TYPE,
                "instance": instance_meta,
            },
            "event": {
                "instance": instance_meta,
                "job_run": self.job_run.serialize(),
                "new_stage": self.new_stage.name,
                "timestamp": format_dt_iso(self.timestamp),
            }
        }

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'InstanceLifecycleEvent':
        return cls(
            instance=JobInstanceMetadata.deserialize(as_dict['instance']),
            job_run=JobRun.deserialize(as_dict['job_run']),
            new_stage=Stage[as_dict['new_stage']],
            timestamp=util.parse_datetime(as_dict['timestamp']),
        )


class InstanceLifecycleObserver(abc.ABC):

    @abstractmethod
    def instance_lifecycle_update(self, event: InstanceLifecycleEvent):
        pass


@dataclass(frozen=True)
class InstanceTransitionEvent:
    EVENT_TYPE = "instance_phase_update"

    instance: JobInstanceMetadata
    job_run: JobRun
    is_root_phase: bool
    phase_id: str
    new_stage: Stage
    timestamp: datetime

    @property
    def event_type(self):
        return self.EVENT_TYPE

    def serialize(self) -> Dict[str, Any]:
        instance_meta = self.instance.serialize()
        return {
            "event_metadata": {
                "event_type": self.EVENT_TYPE,
                "instance": instance_meta,
            },
            "event": {
                "instance": instance_meta,
                "job_run": self.job_run.serialize(),
                "is_root_phase": self.is_root_phase,
                "phase_id": self.phase_id,
                "new_stage": self.new_stage.name,
                "timestamp": format_dt_iso(self.timestamp),
            }
        }

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'InstanceTransitionEvent':
        return cls(
            instance=JobInstanceMetadata.deserialize(as_dict['instance']),
            job_run=JobRun.deserialize(as_dict['job_run']),
            is_root_phase=as_dict['is_root_phase'],
            phase_id=as_dict['phase_id'],
            new_stage=Stage[as_dict['new_stage']],
            timestamp=util.parse_datetime(as_dict['timestamp']),
        )


class InstanceTransitionObserver(abc.ABC):

    @abstractmethod
    def new_instance_transition(self, event: InstanceTransitionEvent):
        pass


@dataclass(frozen=True)
class InstanceOutputEvent:
    EVENT_TYPE = "instance_output_update"

    instance: JobInstanceMetadata
    output_line: OutputLine
    timestamp: datetime

    @property
    def event_type(self):
        return self.EVENT_TYPE

    def serialize(self, truncate_length: Optional[int] = None, truncated_suffix: str = ".. (truncated)") \
            -> Dict[str, Any]:
        instance_meta = self.instance.serialize()
        return {
            "event_metadata": {
                "event_type": self.EVENT_TYPE,
                "instance": instance_meta,
            },
            "event": {
                "instance": instance_meta,
                "output_line": self.output_line.serialize(truncate_length, truncated_suffix),
                "timestamp": format_dt_iso(self.timestamp),
            }
        }

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'InstanceOutputEvent':
        return cls(
            instance=JobInstanceMetadata.deserialize(as_dict['instance']),
            output_line=OutputLine.deserialize(as_dict['output_line']),
            timestamp=util.parse_datetime(as_dict['timestamp']),
        )


class InstanceOutputObserver(abc.ABC):

    @abc.abstractmethod
    def instance_output_update(self, event: InstanceOutputEvent):
        pass


class JobInstanceManager(ABC):
    """
    Interface for managing job instances. The ambiguous name 'Manager' is used because the
    subclasses may implement diverse functionalities for the instances registered to this object.
    """

    @abstractmethod
    def register_instance(self, job_instance):
        """
        Register a new job instance with the manager.

        The specifics of what occurs upon registering an instance depend on the implementing class.
        The class is not required to keep track of the instance if that is not needed for the provided functionality.

        Args:
            job_instance: The job instance to be registered.
        """
        pass

    @abstractmethod
    def unregister_instance(self, job_instance):
        """
        Unregister an existing job instance from the manager.

        This will trigger any necessary clean-up or de-initialization tasks if needed. The specifics of what occurs
        upon unregistering an instance depend on the implementing class. It can be ignored if the manager does not
        track the registered instances.

        Args:
            job_instance: The job instance to be unregistered.
        """
        pass


class JobInstanceObservable(ABC):
    """
    Interface defining the contract for objects that can be observed for job instance events.

    This interface provides methods to register and unregister observers for different
    types of job instance events: stage transitions, phase transitions, and output events.
    """

    @abstractmethod
    def add_observer_all_events(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """
        Register an observer for all event types: stage, transition, and output events.

        Args:
            observer: The observer to register for all event types
            priority: Priority level for the observer (lower numbers = higher priority)
        """
        pass

    @abstractmethod
    def remove_observer_all_events(self, observer):
        """
        Unregister an observer from all event types: stage, transition, and output events.

        Args:
            observer: The observer to unregister from all event types
        """
        pass

    @abstractmethod
    def add_observer_lifecycle(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """
        Register an observer for stage events.

        Args:
            observer: The observer to register for stage events
            priority: Priority level for the observer (lower numbers = higher priority)
        """
        pass

    @abstractmethod
    def remove_observer_lifecycle(self, observer):
        """
        Unregister an observer from stage events.

        Args:
            observer: The observer to unregister from stage events
        """
        pass

    @abstractmethod
    def add_observer_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """
        Register an observer for transition events.

        Args:
            observer: The observer to register for transition events
            priority: Priority level for the observer (lower numbers = higher priority)

        """
        pass

    @abstractmethod
    def remove_observer_transition(self, observer):
        """
        Unregister an observer from transition events.

        Args:
            observer: The observer to unregister from transition events
        """
        pass

    @abstractmethod
    def add_observer_output(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """
        Register an observer for output events.

        Args:
            observer: The observer to register for output events
            priority: Priority level for the observer (lower numbers = higher priority)
        """
        pass

    @abstractmethod
    def remove_observer_output(self, observer):
        """
        Unregister an observer from output events.

        Args:
            observer: The observer to unregister from output events
        """
        pass


class JobInstanceNotifications:

    def __init__(self):
        self._stage_notification = ObservableNotification[InstanceLifecycleObserver]()
        self._transition_notification = ObservableNotification[InstanceTransitionObserver]()
        self._output_notification = ObservableNotification[InstanceOutputObserver]()

    def add_observer_all_events(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self.add_observer_lifecycle(observer, priority)
        self.add_observer_transition(observer, priority)
        self.add_observer_output(observer, priority)

    def remove_observer_all_events(self, observer):
        self.remove_observer_lifecycle(observer)
        self.remove_observer_transition(observer)
        self.remove_observer_output(observer)

    def add_observer_lifecycle(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        self._stage_notification.add_observer(observer, priority)

    def remove_observer_lifecycle(self, observer):
        self._stage_notification.remove_observer(observer)

    def add_observer_transition(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        self._transition_notification.add_observer(observer, priority)

    def remove_observer_transition(self, observer):
        self._transition_notification.remove_observer(observer)

    def add_observer_output(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        self._output_notification.add_observer(observer, priority)

    def remove_observer_output(self, observer):
        self._output_notification.remove_observer(observer)


class InstanceEventsObserver(InstanceLifecycleObserver, InstanceTransitionObserver, InstanceOutputObserver, ABC):
    pass


class JobInstanceDelegate(JobInstance):
    """
    A generic delegation wrapper for JobInstance implementations.

    This class implements all JobInstance methods by forwarding calls to the
    wrapped instance, allowing subclasses to selectively override behaviors
    without duplicating code.
    """

    def __init__(self, wrapped: JobInstance):
        """
        Initialize with a delegate instance.

        Args:
            wrapped: The JobInstance implementation to wrap
        """
        self._wrapped = wrapped

    @property
    def metadata(self):
        """Delegates to the wrapped instance's metadata"""
        return self._wrapped.metadata

    @property
    def id(self):
        """Delegates to the wrapped instance's instance_id"""
        return self._wrapped.id

    @property
    def job_id(self):
        """Delegates to the wrapped instance's job_id"""
        return self._wrapped.job_id

    @property
    def run_id(self):
        """Delegates to the wrapped instance's run_id"""
        return self._wrapped.run_id

    def find_phase_control(self, phase_filter):
        """Delegates to the wrapped instance's find_phase_control method"""
        return self._wrapped.find_phase_control(phase_filter)

    def find_phase_control_by_id(self, phase_id: str):
        """Delegates to the wrapped instance's find_phase_control_by_id method"""
        return self._wrapped.find_phase_control_by_id(phase_id)

    def to_run(self) -> JobRun:
        """Delegates to the wrapped instance's snapshot method"""
        return self._wrapped.to_run()

    @property
    def output(self):
        """Delegates to the wrapped instance's output property"""
        return self._wrapped.output

    def run(self):
        """Delegates to the wrapped instance's run method"""
        return self._wrapped.run()

    def stop(self, reason=StopReason.STOPPED):
        """Delegates to the wrapped instance's stop method"""
        return self._wrapped.stop(reason)

    def add_observer_all_events(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """Delegates to the wrapped instance's add_observer_all_events method"""
        self._wrapped.add_observer_all_events(observer, priority)

    def remove_observer_all_events(self, observer):
        """Delegates to the wrapped instance's remove_observer_all_events method"""
        self._wrapped.remove_observer_all_events(observer)

    def add_observer_lifecycle(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, reply_last_event=False):
        """Delegates to the wrapped instance's add_observer_stage method"""
        self._wrapped.add_observer_lifecycle(observer, priority, reply_last_event)

    def remove_observer_lifecycle(self, observer):
        """Delegates to the wrapped instance's remove_observer_stage method"""
        self._wrapped.remove_observer_lifecycle(observer)

    def add_observer_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """Delegates to the wrapped instance's add_observer_transition method"""
        self._wrapped.add_observer_transition(observer, priority)

    def remove_observer_transition(self, observer):
        """Delegates to the wrapped instance's remove_observer_transition method"""
        self._wrapped.remove_observer_transition(observer)

    def add_observer_output(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """Delegates to the wrapped instance's add_observer_output method"""
        self._wrapped.add_observer_output(observer, priority)

    def remove_observer_output(self, observer):
        """Delegates to the wrapped instance's remove_observer_output method"""
        self._wrapped.remove_observer_output(observer)
