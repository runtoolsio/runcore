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
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum, auto
from typing import Dict, Any, List, Optional, Tuple

from runtools.runcore.output import OutputLine
from runtools.runcore.run import TerminationStatus, RunState, Run, PhaseRun, PhaseInfo, \
    Lifecycle, TerminationInfo, Fault
from runtools.runcore.status import Status
from runtools.runcore.util import MatchingStrategy, format_dt_iso
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

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        result = {
            'job_id': self.job_id,
            'count': self.count,
            'last_state': self.termination_status.name,
            'failed_count': self.failed_count,
            'warning_count': self.warning_count,
        }

        if self.first_created:
            result['first_created'] = format_dt_iso(self.first_created)
        else:
            result['first_created'] = None

        if self.last_created:
            result['last_created'] = format_dt_iso(self.last_created)
        else:
            result['last_created'] = None

        if self.fastest_time:
            result['fastest_time'] = self.fastest_time.total_seconds()
        else:
            result['fastest_time'] = None

        if self.average_time:
            result['average_time'] = self.average_time.total_seconds()
        else:
            result['average_time'] = None

        if self.slowest_time:
            result['slowest_time'] = self.slowest_time.total_seconds()
        else:
            result['slowest_time'] = None

        if self.last_time:
            result['last_time'] = self.last_time.total_seconds()
        else:
            result['last_time'] = None

        if not include_empty:
            result = {k: v for k, v in result.items() if v is not None}
        return result


@dataclass
class JobInstanceMetadata(ABC):
    """
    A dataclass that contains metadata information related to a specific job run. This object is designed
    to represent essential information about a job run in a compact and serializable format. By using this object
    instead of a full `JobRun` snapshot, you can reduce the amount of data transmitted when sending information
    across a network or between different parts of a system.
    TODO Add job_type

    Attributes:
        job_id (str):
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
    job_id: str
    run_id: str
    instance_id: str
    system_parameters: Dict[str, Any]
    user_params: Dict[str, Any]

    def serialize(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "run_id": self.run_id,
            "instance_id": self.instance_id,
            "system_parameters": self.system_parameters,
            "user_params": self.user_params,
        }

    @classmethod
    def deserialize(cls, as_dict):
        return cls(
            as_dict['job_id'],
            as_dict['run_id'],
            as_dict['instance_id'],
            as_dict['system_parameters'],
            as_dict['user_params'],
        )

    def contains_system_parameters(self, *params):
        return all(param in self.system_parameters for param in params)

    def __repr__(self) -> str:
        return f"{self.job_id}@{self.run_id}:{self.instance_id}"


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
    def instance_id(self):
        """
        Returns:
            str: Instance reference/identity identifier. Expected to be a unique value.
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

    @property
    @abc.abstractmethod
    def phases(self):
        """
        Returns:
            List[PhaseInfo]: Phases in the order as defined in the instance
        """

    @abc.abstractmethod
    def get_phase_control(self, phase_id: str, phase_type: str = None):
        """
        TODO
        """

    @abc.abstractmethod
    def snapshot(self):
        """
        Creates a consistent, thread-safe snapshot of the job instance's current state.

        Returns:
            JobRun: A snapshot representing the current state of the job instance.
        """

    @property
    @abc.abstractmethod
    def output(self):
        pass

    @abc.abstractmethod
    def run(self):
        """
        Run the job.

        This method is not expected to raise any errors. In case of any failure the error details can be retrieved
        by calling `exec_error` method.
        """

    @abc.abstractmethod
    def stop(self):
        """
        Attempts to cancel a scheduled job or stop a job that is already executing.

        Note:
            The way the stop request is handled can vary based on the implementation or the specific job.
            It's possible that not all instances will respond successfully to the stop request.
        """

    @abc.abstractmethod
    def interrupted(self):
        """
        TODO: Notify about keyboard interruption signal
        """

    @abc.abstractmethod
    def wait_for_transition(self, phase_name=None, run_state=RunState.NONE, *, timeout=None):
        """
        TODO
        """

    @abc.abstractmethod
    def add_observer_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
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
            notify_on_register (bool, optional):
                If True, immediately notifies the observer about the last known instance state change upon registration.
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
class JobFaults:
    transition_observer_faults: Tuple[Fault, ...]
    output_observer_faults: Tuple[Fault, ...]

    def serialize(self) -> Dict[str, Any]:
        return {
            "transition_observer_faults": [fault.serialize() for fault in self.transition_observer_faults],
            "output_observer_faults": [fault.serialize() for fault in self.output_observer_faults]
        }

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'JobFaults':
        return cls(
            transition_observer_faults=tuple(
                Fault.deserialize(fault) for fault in as_dict.get("transition_observer_faults", [])
            ),
            output_observer_faults=tuple(
                Fault.deserialize(fault) for fault in as_dict.get("output_observer_faults", [])
            )
        )


@dataclass(frozen=True)
class JobRun:
    """
    Immutable snapshot of job instance
    """
    metadata: JobInstanceMetadata
    _run: Run  # private field
    faults: Optional[JobFaults]
    status: Optional[Status] = None

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'JobRun':
        return cls(
            metadata=JobInstanceMetadata.deserialize(as_dict['metadata']),
            _run=Run.deserialize(as_dict['run']),
            faults=JobFaults.deserialize(as_dict['faults']) if as_dict.get('faults') else None,
            status=Status.deserialize(as_dict['task']) if as_dict.get('task') else None,
        )

    def serialize(self) -> Dict[str, Any]:
        d = {
            "metadata": self.metadata.serialize(),
            "run": self._run.serialize(),
            "task": self.status.serialize() if self.status else None,
        }
        if self.faults:
            d["faults"] = self.faults.serialize()
        return d

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

    @property
    def instance_id(self) -> str:
        """
        Returns:
            str: Instance part of the instance full identifier.
        """
        return self.metadata.instance_id

    @property
    def phases(self) -> Tuple[PhaseInfo, ...]:
        return self._run.phases

    @property
    def current_phase(self) -> Optional[PhaseInfo]:
        return self._run.current_phase

    def find_phase(self, criterion):
        return self._run.find_phase(criterion)

    def phase_after(self, phase: PhaseInfo):
        return self._run.phase_after(phase)

    @property
    def lifecycle(self) -> Lifecycle:
        return self._run.lifecycle

    @property
    def termination(self) -> Optional[TerminationInfo]:
        return self._run.termination


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

    @property
    def scheduled(self):
        return self.in_state(RunState.CREATED)

    @property
    def pending(self):
        return self.in_state(RunState.PENDING)

    @property
    def queued(self):
        return self.in_state(RunState.IN_QUEUE)

    @property
    def executing(self):
        return self.in_state(RunState.EXECUTING)

    @property
    def terminal(self):
        return self.in_state(RunState.ENDED)

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        return {"runs": [run.serialize(include_empty=include_empty) for run in self]}


# TODO PhaseTransitionObserver
class InstanceTransitionObserver(abc.ABC):

    @abc.abstractmethod
    def new_instance_phase(self, job_run: JobRun, previous_phase: PhaseRun, new_phase: PhaseRun, ordinal: int):
        """
        Called when the instance transitions to a new phase.

        The notification can optionally happen also when this observer is registered with the instance
        to make the observer aware about the current phase of the instance.

        Args:
            job_run (JobInstSnapshot): A snapshot of the job instance that transitioned to a new phase.
            previous_phase (TerminationStatus): The previous phase of the job instance.
            new_phase (TerminationStatus): The new/current phase state of the job instance.
            ordinal (int): The number of the current phase.
        """


class InstanceOutputObserver(abc.ABC):

    @abc.abstractmethod
    def new_instance_output(self, instance_meta: JobInstanceMetadata, output_line: OutputLine):
        """TODO"""


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


class JobInstanceObservable:

    def __init__(self):
        self._transition_notification = ObservableNotification[InstanceTransitionObserver]()
        self._output_notification = ObservableNotification[InstanceOutputObserver]()

    def add_observer_transition(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        self._transition_notification.add_observer(observer, priority)

    def remove_observer_transition(self, observer):
        self._transition_notification.remove_observer(observer)

    def add_observer_output(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        self._output_notification.add_observer(observer, priority)

    def remove_observer_output(self, observer):
        self._output_notification.remove_observer(observer)
