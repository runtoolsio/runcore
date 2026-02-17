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
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Iterator, ClassVar, Set, Callable, Protocol, override

from runtools.runcore import util
from runtools.runcore.output import OutputLine, Output, OutputLocation
from runtools.runcore.run import TerminationStatus, Fault, PhaseRun, Stage, RunLifecycle, StopReason
from runtools.runcore.status import Status
from runtools.runcore.util import MatchingStrategy, format_dt_iso, unique_timestamp_hex
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY, ObservableNotification


class Job:
    """
    Represents a job definition.

    This class encapsulates the properties and characteristics of a job. Each individual execution
    of a job is represented as an object of the `JobInstance` class.

    Attributes:
        _id (str): Unique identifier for the job.
        _properties (Dict[str, str]): Additional properties or metadata associated with the job.
    """

    def __init__(self, job_id: str, properties: Dict[str, str] = None):
        """
        Initialize a new Job object.

        Args:
            job_id (str): Unique identifier for the job.
            properties (Dict[str, str], optional): Additional properties or metadata. Defaults to an empty dictionary.
        """
        self._id = job_id
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
        return self._id == other._id and self._properties == other._properties

    def __hash__(self) -> int:
        """Returns the hash based on the job's unique ID and properties."""
        return hash((self._id, frozenset(self._properties.items())))


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
            if prop is None:
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
    termination_status: Optional[TerminationStatus] = None
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
        return sorted(job_stats_list, key=self.key_func, reverse=reverse)

    @property
    def key_func(self):
        match self:
            case StatsSortOption.JOB_ID:
                return lambda s: s.job_id
            case StatsSortOption.FIRST_CREATED:
                return lambda s: s.first_created or s.last_created
            case StatsSortOption.LAST_CREATED:
                return lambda s: s.last_created
            case StatsSortOption.AVERAGE_TIME:
                return lambda s: s.average_time or s.last_time
            case StatsSortOption.SLOWEST_TIME:
                return lambda s: s.slowest_time or s.last_time
            case StatsSortOption.FASTEST_TIME:
                return lambda s: s.fastest_time or s.last_time
            case StatsSortOption.LAST_TIME:
                return lambda s: s.last_time
            case StatsSortOption.COUNT:
                return lambda s: s.count
            case StatsSortOption.FAILED_COUNT:
                return lambda s: s.failed_count
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
        """Find a phase control matching the given filter."""

    def find_phase_control_by_id(self, phase_id: str):
        return self.find_phase_control(lambda phase: phase.phase_id == phase_id)

    @abc.abstractmethod
    def snap(self):
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

    @property
    @abc.abstractmethod
    def notifications(self) -> 'InstanceNotifications':
        """Register observers here to receive events from this instance."""


@dataclass(frozen=True)
class JobRun:
    """
    Immutable snapshot of job instance
    """
    metadata: JobInstanceMetadata
    root_phase: PhaseRun
    output_locations: Tuple[OutputLocation, ...] = ()
    faults: Tuple[Fault, ...] = ()
    status: Optional[Status] = None

    @property
    def lifecycle(self) -> RunLifecycle:
        return self.root_phase.lifecycle

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'JobRun':
        return cls(
            metadata=JobInstanceMetadata.deserialize(as_dict['metadata']),
            root_phase=PhaseRun.deserialize(as_dict['phases']),
            faults=tuple(Fault.deserialize(f) for f in as_dict.get('faults', [])),
            status=Status.deserialize(as_dict['status']) if as_dict.get('status') else None,
        )

    def serialize(self) -> Dict[str, Any]:
        d = {
            "metadata": self.metadata.serialize(),
            "phases": self.root_phase.serialize(),
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

    def search_phases(self, predicate: Optional[Callable[['PhaseRun'], bool]] = None) -> List['PhaseRun']:
        """
        Searches all phases within this job run (including root phase and
        all descendants) that match the given predicate.

        The search is performed in a pre-order (depth-first) manner.

        Args:
            predicate: Optional function to filter phases. If None, all phases
                       (root and descendants) are returned.

        Returns:
            List[PhaseRun]: A list of all matching phase details found.
        """
        return self.root_phase.search_phases(predicate=predicate, include_self=True)

    def find_first_phase(self, predicate):
        return self.root_phase.find_first_phase(predicate)

    def find_phase_by_id(self, phase_id):
        return self.root_phase.find_phase_by_id(phase_id)

    def accept_visitor(self, visitor):
        """
        Accept a visitor to traverse all phases in this job run.

        Args:
            visitor: The visitor to accept
        """
        self.root_phase.accept_visitor(visitor)
        return visitor


class InstanceEvent(Protocol):
    """Protocol for job instance events that have an instance metadata field."""
    instance: JobInstanceMetadata


@dataclass(frozen=True)
class InstanceLifecycleEvent:
    EVENT_TYPE = "instance_lifecycle_update"

    job_run: JobRun
    new_stage: Stage
    timestamp: datetime.datetime

    @property
    def instance(self) -> JobInstanceMetadata:
        return self.job_run.metadata

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
                "job_run": self.job_run.serialize(),
                "new_stage": self.new_stage.name,
                "timestamp": format_dt_iso(self.timestamp),
            },
        }

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'InstanceLifecycleEvent':
        return cls(
            job_run=JobRun.deserialize(as_dict['job_run']),
            new_stage=Stage[as_dict['new_stage']],
            timestamp=util.parse_datetime(as_dict['timestamp']),
        )


class InstanceLifecycleObserver(abc.ABC):

    @abstractmethod
    def instance_lifecycle_update(self, event: InstanceLifecycleEvent):
        pass


@dataclass(frozen=True)
class InstancePhaseEvent:
    EVENT_TYPE = "instance_phase_update"

    job_run: JobRun
    is_root_phase: bool
    phase_id: str
    new_stage: Stage
    timestamp: datetime.datetime

    @property
    def instance(self) -> JobInstanceMetadata:
        return self.job_run.metadata

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
                "job_run": self.job_run.serialize(),
                "is_root_phase": self.is_root_phase,
                "phase_id": self.phase_id,
                "new_stage": self.new_stage.name,
                "timestamp": format_dt_iso(self.timestamp),
            },
        }

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'InstancePhaseEvent':
        return cls(
            job_run=JobRun.deserialize(as_dict['job_run']),
            is_root_phase=as_dict['is_root_phase'],
            phase_id=as_dict['phase_id'],
            new_stage=Stage[as_dict['new_stage']],
            timestamp=util.parse_datetime(as_dict['timestamp']),
        )


class InstancePhaseObserver(abc.ABC):

    @abstractmethod
    def instance_phase_update(self, event: InstancePhaseEvent):
        pass


@dataclass(frozen=True)
class InstanceOutputEvent:
    EVENT_TYPE = "instance_output_update"

    instance: JobInstanceMetadata
    output_line: OutputLine
    timestamp: datetime.datetime

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


class InstanceNotifications(ABC):
    """Interface for registering observers for instance events."""

    @abstractmethod
    def add_observer_lifecycle(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        pass

    @abstractmethod
    def remove_observer_lifecycle(self, observer):
        pass

    @abstractmethod
    def add_observer_phase(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        pass

    @abstractmethod
    def remove_observer_phase(self, observer):
        pass

    @abstractmethod
    def add_observer_output(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        pass

    @abstractmethod
    def remove_observer_output(self, observer):
        pass

    def add_observer_all_events(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self.add_observer_lifecycle(observer, priority)
        self.add_observer_phase(observer, priority)
        self.add_observer_output(observer, priority)

    def remove_observer_all_events(self, observer):
        self.remove_observer_lifecycle(observer)
        self.remove_observer_phase(observer)
        self.remove_observer_output(observer)


class JobInstanceObservable(ABC):
    """Marker interface for components that emit instance events."""

    @property
    @abstractmethod
    def notifications(self) -> InstanceNotifications:
        pass


class InstanceObservableNotifications(InstanceNotifications):
    """
    Notification component for job instance events.

    Implements InstanceNotifications for external observer registration, and exposes internal
    ObservableNotification objects for firing events, chaining via bind/unbind, and filtering.
    """

    def __init__(self, instance_filter: 'MetadataCriterion' = None,
                 lifecycle_error_hook=None, phase_error_hook=None, output_error_hook=None,
                 force_reraise=False):
        event_filter = (lambda e: instance_filter.matches(e.instance)) if instance_filter else None
        self.lifecycle_notification = ObservableNotification[InstanceLifecycleObserver](
            event_filter=event_filter, error_hook=lifecycle_error_hook, force_reraise=force_reraise)
        self.phase_notification = ObservableNotification[InstancePhaseObserver](
            event_filter=event_filter, error_hook=phase_error_hook, force_reraise=force_reraise)
        self.output_notification = ObservableNotification[InstanceOutputObserver](
            event_filter=event_filter, error_hook=output_error_hook, force_reraise=force_reraise)

    def bind_to(self, source: InstanceNotifications, priority: int = DEFAULT_OBSERVER_PRIORITY) -> None:
        """Register this notification to receive events from source."""
        source.add_observer_lifecycle(self.lifecycle_notification.observer_proxy, priority)
        source.add_observer_phase(self.phase_notification.observer_proxy, priority)
        source.add_observer_output(self.output_notification.observer_proxy, priority)

    def unbind_from(self, source: InstanceNotifications) -> None:
        """Stop receiving events from source."""
        source.remove_observer_lifecycle(self.lifecycle_notification.observer_proxy)
        source.remove_observer_phase(self.phase_notification.observer_proxy)
        source.remove_observer_output(self.output_notification.observer_proxy)

    def add_observer_lifecycle(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        self.lifecycle_notification.add_observer(observer, priority)

    def remove_observer_lifecycle(self, observer):
        self.lifecycle_notification.remove_observer(observer)

    def add_observer_phase(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        self.phase_notification.add_observer(observer, priority)

    def remove_observer_phase(self, observer):
        self.phase_notification.remove_observer(observer)

    def add_observer_output(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        self.output_notification.add_observer(observer, priority)

    def remove_observer_output(self, observer):
        self.output_notification.remove_observer(observer)


class InstanceEventsObserver(InstanceLifecycleObserver, InstancePhaseObserver, InstanceOutputObserver, ABC):
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

    def snap(self) -> JobRun:
        """Delegates to the wrapped instance's snapshot method"""
        return self._wrapped.snap()

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

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        """Delegates to the wrapped instance's notifications"""
        return self._wrapped.notifications
