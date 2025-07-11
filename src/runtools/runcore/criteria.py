"""
This module provides various criteria objects used to match job instances or their parts.

TODO: Remove immutable properties
"""

from abc import abstractmethod, ABC
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any, Optional, TypeVar, Generic, Iterable

from runtools.runcore.job import JobInstanceMetadata, JobRun, InstanceID
from runtools.runcore.run import Outcome, TerminationInfo, \
    PhaseDetail, TerminationStatus, RunLifecycle, Stage
from runtools.runcore.util import MatchingStrategy, to_list, DateTimeRange, TimeRange

T = TypeVar('T')


class MatchCriteria(ABC, Generic[T]):

    @abstractmethod
    def matches(self, tested: T) -> bool:
        """
        Check if the provided tested item matches the criteria.

        :param tested: The item to check against the criteria.
        :return: True if the item matches the criteria, False otherwise.
        """
        pass


def negate_id(id_value: str) -> str:
    """
    Creates a negated ID pattern for matching.

    Args:
        id_value: The ID to negate

    Returns:
        A pattern that will match anything except the given ID
    """
    return f"!{id_value}" if id_value else id_value


@dataclass
class MetadataCriterion(MatchCriteria[JobInstanceMetadata]):
    """
    Specifies criteria for matching instance metadata.
    If all fields are empty, the matching strategy defaults to `MatchingStrategy.ALWAYS_TRUE`.

    Attributes:
        job_id (str): The pattern for job ID matching. If empty, the field is ignored.
        run_id (str): The pattern for run ID matching. If empty, the field is ignored.
        match_any_field (bool): If True, matches if any provided field matches. If False, all fields must match.
        strategy (MatchingStrategy): The strategy to use for matching. Default is `MatchingStrategy.EXACT`.
    """
    job_id: str = ''
    run_id: str = ''
    match_any_field: bool = False
    strategy: MatchingStrategy = MatchingStrategy.EXACT

    @classmethod
    def all_match(cls) -> 'MetadataCriterion':
        """Creates a criterion that matches all instances."""
        return cls(strategy=MatchingStrategy.ALWAYS_TRUE)

    @classmethod
    def none_match(cls) -> 'MetadataCriterion':
        """Creates a criterion that matches no instances."""
        return cls(strategy=MatchingStrategy.ALWAYS_FALSE)

    @classmethod
    def all_except(cls, instance_id: InstanceID) -> 'MetadataCriterion':
        """
        Creates a criterion that matches any job run except the specified one.

        Args:
            instance_id: The specific job instance to exclude from matching.

        Returns:
            MetadataCriterion: A criteria object that will match any job instance except the given one.
        """
        return MetadataCriterion(
            job_id=negate_id(instance_id.job_id),
            run_id=negate_id(instance_id.run_id),
            match_any_field=True,
        )

    @staticmethod
    def exact_match(instance_id: InstanceID) -> 'MetadataCriterion':
        """
        Creates a criterion that matches a specific instance.

        Args:
            instance_id: The specific instance to create a match for.

        Returns:
            MetadataCriterion: A criteria object that will match the given job instance.
        """
        return MetadataCriterion(instance_id.job_id, instance_id.run_id)

    @classmethod
    def parse(cls, pattern: str,
              strategy: MatchingStrategy = MatchingStrategy.EXACT) -> 'MetadataCriterion':
        """
        Parses the provided pattern and returns the corresponding metadata criterion.

        The pattern can be in one of these formats:
        - "job_id@run_id" - Match specific job ID and run ID combination
        - Any other text - Match against job_id and run_id with match_any_field=True

        Args:
            pattern: The pattern to parse. Can be empty, a job/run combo with '@', or plain text.
            strategy: The strategy to use for matching. Default is `MatchingStrategy.EXACT`

        Returns:
            A new criteria object configured according to the pattern format.
        """
        if not pattern:
            return cls.all_match()

        # Handle job@run pattern
        if '@' in pattern:
            job_id, run_id = pattern.split('@', 1)
            return cls(job_id, run_id, False, strategy)

        # Handle plain text (match against job_id or run_id)
        return cls(pattern, pattern, True, strategy)

    @classmethod
    def parse_strict(cls, id_string):
        instance_id = InstanceID.parse(id_string)
        return cls(instance_id.job_id, instance_id.run_id, match_any_field=False, strategy=MatchingStrategy.EXACT)

    def _matches_id(self, actual: str, criteria: str) -> bool:
        """
        Internal method to match a single ID against its criteria.

        Args:
            actual: The actual ID value to check
            criteria: The criteria pattern to match against

        Returns:
            True if the ID matches the criteria, False otherwise
        """
        if not criteria:
            return True
        if criteria.startswith('!'):
            # Remove '!' and negate the result for negative matching
            return not self.strategy(actual, criteria[1:])
        return self.strategy(actual, criteria)

    def __call__(self, metadata: JobInstanceMetadata) -> bool:
        """Makes the criterion callable, delegating to matches()."""
        return self.matches(metadata)

    def matches(self, metadata: JobInstanceMetadata) -> bool:
        """
        Check if the provided metadata matches this object.

        Args:
            metadata: A metadata to match

        Returns:
            Whether the provided metadata matches this criteria
        """
        job_id_match = self._matches_id(metadata.job_id, self.job_id)
        run_id_match = self._matches_id(metadata.run_id, self.run_id)

        if self.match_any_field:
            return (not self.job_id or job_id_match) or (not self.run_id or run_id_match)
        else:
            return (not self.job_id or job_id_match) and (not self.run_id or run_id_match)

    def serialize(self) -> Dict[str, Any]:
        """Serializes the criterion to a dictionary."""
        return {
            'job_id': self.job_id,
            'run_id': self.run_id,
            'match_any_field': self.match_any_field,
            'strategy': self.strategy.name.lower(),
        }

    @classmethod
    def deserialize(cls, as_dict: Dict[str, Any]) -> 'MetadataCriterion':
        """
        Deserializes a criterion from a dictionary.

        Args:
            as_dict: Dictionary containing the serialized criterion

        Returns:
            The deserialized criterion
        """
        # Handle backward compatibility with match_any_id
        match_any = as_dict.get('match_any_field')
        if match_any is None:
            match_any = as_dict.get('match_any_id', False)

        return cls(
            as_dict['job_id'],
            as_dict['run_id'],
            match_any,
            MatchingStrategy[as_dict['strategy'].upper()],
        )

    def __str__(self) -> str:
        fields = []
        if self.job_id:
            fields.append(f"job_id='{self.job_id}'")
        if self.run_id:
            fields.append(f"run_id='{self.run_id}'")
        if self.match_any_field:
            fields.append("match_any_field=True")
        if self.strategy != MatchingStrategy.EXACT:
            fields.append(f"strategy={self.strategy.name}")
        return f"<{', '.join(fields)}>" if fields else ""


def compound_instance_filter(metadata_criteria):
    def match(metadata):
        return not metadata_criteria or any(criteria(metadata) for criteria in metadata_criteria)

    return match


@dataclass
class TerminationCriterion(MatchCriteria[TerminationInfo]):
    """
    Criteria for matching termination information.
    """
    status: Optional[TerminationStatus] = None
    outcome: Outcome = Outcome.ANY
    ended_range: DateTimeRange = None

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> 'TerminationCriterion':
        return cls(
            status=TerminationStatus[data['status']] if data.get('status') else None,
            outcome=Outcome[data.get('outcome', Outcome.ANY.name)],
            ended_range=DateTimeRange.deserialize(data['ended_range']) if data.get('ended_range') else None
        )

    def serialize(self) -> Dict[str, Any]:
        return {
            'status': self.status.name if self.status else None,
            'outcome': self.outcome.name,
            'ended_range': self.ended_range.serialize() if self.ended_range else None,
        }

    def matches(self, term_info: TerminationInfo) -> bool:
        if self.status is not None and term_info.status != self.status:
            return False

        if self.outcome != Outcome.ANY and not term_info.status.is_outcome(self.outcome):
            return False

        if self.ended_range and not self.ended_range(term_info.terminated_at):
            return False

        return True

    def __call__(self, term_info: TerminationInfo) -> bool:
        return self.matches(term_info)

    def __bool__(self) -> bool:
        return (self.status is not None or
                self.outcome != Outcome.ANY or
                self.ended_range)

    def __str__(self) -> str:
        fields = []
        if self.status is not None:
            fields.append(f"status={self.status.name}")
        if self.outcome != Outcome.ANY:
            fields.append(f"outcome={self.outcome.name}")
        if self.ended_range:
            fields.append(f"ended{self.ended_range}")
        return f"<{', '.join(fields)}>" if fields else ""


class PhaseMatch(Enum):
    """
    Controls how phases are matched in the criteria.

    Attributes:
        ROOT: Match only the root phase, ignoring descendants
        ALL: Match any phase in the hierarchy (root or descendants)
        DESCENDANTS_ONLY: Match only non-root phases
    """
    ROOT = "ROOT"
    ALL = "ALL"
    DESCENDANTS_ONLY = "DESCENDANTS_ONLY"


@dataclass
class LifecycleCriterion(MatchCriteria[RunLifecycle]):
    """
    Criteria for matching run lifecycle information.
    """
    stage: Optional[Stage] = None
    created: Optional[DateTimeRange] = None
    started: Optional[DateTimeRange] = None
    ended: Optional[DateTimeRange] = None
    total_run_time: Optional[TimeRange] = None
    termination: Optional[TerminationCriterion] = None

    def serialize(self) -> Dict[str, Any]:
        """Serialize to a dictionary."""
        data = {}
        if self.stage:
            data['stage'] = self.stage.name
        if self.created:
            data['created_range'] = self.created.serialize()
        if self.started:
            data['started_range'] = self.started.serialize()
        if self.ended:
            data['ended_range'] = self.ended.serialize()
        if self.total_run_time:
            data['exec_range'] = self.total_run_time.serialize()
        if self.termination:
            data['termination'] = self.termination.serialize()
        return data

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> 'LifecycleCriterion':
        """Deserialize from a dictionary."""
        return cls(
            stage=Stage[data['stage']] if data.get('stage') else None,
            created=DateTimeRange.deserialize(data['created_range']) if data.get('created_range') else None,
            started=DateTimeRange.deserialize(data['started_range']) if data.get('started_range') else None,
            ended=DateTimeRange.deserialize(data['ended_range']) if data.get('ended_range') else None,
            total_run_time=TimeRange.deserialize(data['exec_range']) if data.get('exec_range') else None,
            termination=TerminationCriterion.deserialize(data['termination']) if data.get('termination') else None
        )

    def __call__(self, lifecycle: RunLifecycle):
        return self.matches(lifecycle)

    def matches(self, lifecycle: RunLifecycle) -> bool:
        """Check if the lifecycle matches all specified criteria."""
        if self.stage and lifecycle.stage != self.stage:
            return False

        if self.created and not self.created(lifecycle.created_at):
            return False

        if self.started and (not lifecycle.started_at or not self.started(lifecycle.started_at)):
            return False

        if self.total_run_time:
            if not lifecycle.total_run_time or not self.total_run_time(lifecycle.total_run_time):
                return False

        if not lifecycle.termination:
            return not (self.ended or self.termination)

        if self.ended and not self.ended(lifecycle.termination.terminated_at):
            return False

        if self.termination and not self.termination(lifecycle.termination):
            return False

        return True

    def set_created(self, since=None, until=None, until_incl=False):
        self.created = DateTimeRange(since, until, until_incl)
        return self

    def set_started(self, since=None, until=None, until_incl=False):
        self.started = DateTimeRange(since, until, until_incl)
        return self

    def set_ended(self, since=None, until=None, until_incl=False):
        self.ended = DateTimeRange(since, until, until_incl)
        return self

    def set_date_range(self, date_range: DateTimeRange, for_stage: Stage) -> 'LifecycleCriterion':
        """
        Sets the appropriate date range based on the filter type.

        Args:
            date_range: The date range to apply
            for_stage: Which stage timestamp field to filter on

        Returns:
            Self for method chaining
        """
        match for_stage:
            case Stage.CREATED:
                self.created = date_range
            case Stage.RUNNING:
                self.started = date_range
            case Stage.ENDED:
                self.ended = date_range
            case _:
                raise ValueError(f"Unknown stage field: {for_stage}")
        return self

    def reached_stage(self, stage: Stage) -> 'LifecycleCriterion':
        """
        Configure criterion to match runs that have reached or passed the specified stage.

        Args:
            stage: The stage that must have been reached

        Returns:
            Self for method chaining

        Examples:
            # Find all runs that have started (reached RUNNING or ENDED)
            criteria.reached_stage(Stage.RUNNING)

            # Find all runs that have completed (reached ENDED)
            criteria.reached_stage(Stage.ENDED)
        """
        match stage:
            case Stage.CREATED:
                self.created = DateTimeRange.unbounded()
            case Stage.RUNNING:
                self.started = DateTimeRange.unbounded()  # Must have a started timestamp
            case Stage.ENDED:
                self.ended = DateTimeRange.unbounded()  # Must have an ended timestamp (termination)

        return self

    def __bool__(self) -> bool:
        """Check if any criteria are set."""
        return bool(self.stage or self.created or self.started or self.ended or self.total_run_time or self.termination)

    def __str__(self) -> str:
        """String representation showing non-None criteria."""
        fields = []
        if self.stage:
            fields.append(f"stage={self.stage.name}")
        if self.created:
            fields.append(f"created={self.created}")
        if self.started:
            fields.append(f"started={self.started}")
        if self.ended:
            fields.append(f"ended={self.ended}")
        if self.total_run_time:
            fields.append(f"exec={self.total_run_time}")
        if self.termination:
            fields.append(f"termination={self.termination}")
        return f"<{', '.join(fields)}>" if fields else ""


@dataclass
class PhaseCriterion(MatchCriteria[PhaseDetail]):
    """
    Criteria for matching phase details, incorporating phase-specific and lifecycle criteria.

    For criteria that target only the root phase (e.g., lifecycle information),
    ensure match_type=PhaseMatch.ROOT.
    When using PhaseMatch.ALL or DESCENDANTS_ONLY, only non-root fields will be considered.

    Attributes:
        phase_type: Phase type to match
        phase_id: Phase ID to match
        idle: Idle phase match
        phase_name: Phase name to match
        attributes: Dictionary of attributes to match. None = no attribute matching
        lifecycle: Criteria for matching lifecycle information. For root phase only.
        match_type: How phases are matched. Defaults to ROOT.
    """
    phase_type: Optional[str] = None
    phase_id: Optional[str] = None
    idle: Optional[bool] = None
    phase_name: Optional[str] = None
    attributes: Optional[Dict[str, Any]] = None
    lifecycle: Optional[LifecycleCriterion] = None
    match_type: PhaseMatch = PhaseMatch.ROOT

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> 'PhaseCriterion':
        """Deserialize a dictionary into a PhaseCriterion instance."""
        return cls(
            phase_type=data.get('phase_type'),
            phase_id=data.get('phase_id'),
            idle=data.get('idle'),
            phase_name=data.get('phase_name'),
            attributes=data.get('attributes'),
            lifecycle=LifecycleCriterion.deserialize(data.get('lifecycle')) if data.get('lifecycle') else None,
            match_type=PhaseMatch[data.get('match_type', PhaseMatch.ROOT.name)]
        )

    def serialize(self) -> Dict[str, Any]:
        """Serialize this criterion into a dictionary."""
        return {
            'phase_type': self.phase_type,
            'phase_id': self.phase_id,
            'idle': self.idle,
            'phase_name': self.phase_name,
            'attributes': self.attributes,
            'lifecycle': self.lifecycle.serialize() if self.lifecycle else None,
            'match_type': self.match_type.name
        }

    def matches_phase(self, phase_detail: PhaseDetail) -> bool:
        """
        Check if a single phase matches this criterion.

        """
        # Check basic fields for all match types
        if self.phase_type and phase_detail.phase_type != self.phase_type:
            return False

        if self.phase_id and phase_detail.phase_id != self.phase_id:
            return False

        if self.idle is not None and phase_detail.is_idle != self.idle:
            return False

        if self.phase_name and phase_detail.phase_name != self.phase_name:
            return False

        if self.attributes:
            if not phase_detail.attributes:
                return False
            for key, value in self.attributes.items():
                if phase_detail.attributes.get(key) != value:
                    return False

        if self.lifecycle and not self.lifecycle.matches(phase_detail.lifecycle):
            return False

        return True

    def __call__(self, phase: PhaseDetail):
        return self.matches(phase)

    def matches(self, phase: PhaseDetail) -> bool:
        """Check if phase or its children match based on match_type."""
        match self.match_type:
            case PhaseMatch.ROOT:
                return self.matches_phase(phase)
            case PhaseMatch.ALL:
                return self.matches_phase(phase) or any(self.matches_phase(c) for c in phase.children)
            case PhaseMatch.DESCENDANTS_ONLY:
                return any(self.matches_phase(c) for c in phase.children)

    def __bool__(self) -> bool:
        """Check if any criteria are set."""
        return bool(self.phase_type or self.phase_id or self.idle or
                    self.phase_name or self.attributes or self.lifecycle)

    def __str__(self) -> str:
        """Create a string representation showing non-None criteria."""
        fields = []
        if self.phase_type:
            fields.append(f"type='{self.phase_type}'")
        if self.phase_id:
            fields.append(f"id='{self.phase_id}'")
        if self.idle:
            fields.append(f"idle={self.idle}")
        if self.phase_name:
            fields.append(f"name='{self.phase_name}'")
        if self.attributes:
            fields.append(f"attrs={self.attributes}")
        if self.lifecycle:
            fields.append(f"lifecycle{self.lifecycle}")
        if self.match_type != PhaseMatch.ROOT:
            fields.append(f"match_type={self.match_type.name}")
        return f"<{', '.join(fields)}>" if fields else ""


class JobRunCriteria(MatchCriteria[JobRun]):
    """
    Criteria for querying and matching job instances.
    """

    def __init__(self, *, metadata_criteria=None, lifecycle_criteria=None, phase_criteria=None):
        self.metadata_criteria = to_list(metadata_criteria)
        self.lifecycle_criteria = to_list(lifecycle_criteria)
        self.phase_criteria = to_list(phase_criteria)

    @classmethod
    def all(cls):
        return cls(metadata_criteria=MetadataCriterion.all_match())

    @classmethod
    def deserialize(cls, as_dict):
        new = cls()
        new.metadata_criteria = [MetadataCriterion.deserialize(c) for c in as_dict.get('metadata_criteria', ())]
        new.lifecycle_criteria = [LifecycleCriterion.deserialize(c) for c in as_dict.get('lifecycle_criteria', ())]
        new.phase_criteria = [PhaseCriterion.deserialize(c) for c in as_dict.get('phase_criteria', ())]
        return new

    def serialize(self):
        return {
            'metadata_criteria': [c.serialize() for c in self.metadata_criteria],
            'lifecycle_criteria': [c.serialize() for c in self.lifecycle_criteria],
            'phase_criteria': [c.serialize() for c in self.phase_criteria],
        }

    @classmethod
    def parse(cls, pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT) -> "JobRunCriteria":
        new = cls()
        new += MetadataCriterion.parse(pattern, strategy)
        return new

    @classmethod
    def parse_all(cls, patterns: Iterable[str], strategy: MatchingStrategy = MatchingStrategy.EXACT) \
            -> "JobRunCriteria":
        new = cls()
        for p in patterns:
            new += MetadataCriterion.parse(p, strategy)
        return new

    @classmethod
    def parse_strict(cls, id_string) -> "JobRunCriteria":
        new = cls()
        new += MetadataCriterion.parse_strict(id_string)
        return new

    @classmethod
    def job_match(cls, job_id, strategy: MatchingStrategy = MatchingStrategy.EXACT):
        new = cls()
        new += MetadataCriterion(job_id=job_id, strategy=strategy)
        return new

    @classmethod
    def exact_match(cls, job_run):
        new = cls()
        new += MetadataCriterion.exact_match(job_run.instance_id)
        return new

    @classmethod
    def all_except(cls, job_run):
        new = cls()
        new += MetadataCriterion.all_except(job_run.instance_id)
        return new

    @classmethod
    def instance_match(cls, instance_id):
        new = cls()
        new += MetadataCriterion.exact_match(instance_id)
        return new

    def __iadd__(self, criterion):
        return self.add(criterion)

    def add(self, criterion):
        match criterion:
            case MetadataCriterion():
                self.metadata_criteria.append(criterion)
            case LifecycleCriterion():
                self.lifecycle_criteria.append(criterion)
            case PhaseCriterion():
                self.phase_criteria.append(criterion)
            case _:
                raise ValueError(f"Invalid criterion type: {type(criterion)}")
        return self

    def matches_metadata(self, job_run):
        return not self.metadata_criteria or any(c(job_run.metadata) for c in self.metadata_criteria)

    def matches_lifecycle(self, job_run):
        return not self.lifecycle_criteria or any(c(job_run.lifecycle) for c in self.lifecycle_criteria)

    def match_phases(self, job_run):
        """Check if any phase in the job run matches any of the phase criteria."""
        if not self.phase_criteria:
            return True

        return any(
            any(c.matches(phase) for c in self.phase_criteria)
            for phase in job_run.phases
        )

    def __call__(self, job_run):
        return self.matches(job_run)

    def matches(self, job_run):
        """Check if a job run matches all criteria."""
        return (self.matches_metadata(job_run) and
                self.matches_lifecycle(job_run) and
                self.match_phases(job_run))

    def __bool__(self):
        return bool(self.metadata_criteria or
                    self.lifecycle_criteria or
                    self.phase_criteria)

    def __str__(self) -> str:
        parts = []
        if self.metadata_criteria:
            if criteria_strs := [str(c) for c in self.metadata_criteria if bool(c)]:
                parts.append(f"metadata={''.join(criteria_strs)}")
        if self.lifecycle_criteria:
            if criteria_strs := [str(c) for c in self.lifecycle_criteria if bool(c)]:
                parts.append(f"lifecycle={''.join(criteria_strs)}")
        if self.phase_criteria:
            if criteria_strs := [str(c) for c in self.phase_criteria if bool(c)]:
                parts.append(f"phase={''.join(criteria_strs)}")
        return f"{' '.join(parts)}" if parts else ""

    def _last_lc_criterion(self):
        if not self.lifecycle_criteria:
            self.lifecycle_criteria = [LifecycleCriterion()]
        return self.lifecycle_criteria[-1]

    def created(self, since=None, until=None, until_incl=False):
        self._last_lc_criterion().created = DateTimeRange(since, until, until_incl)
        return self

    def started(self, since=None, until=None, until_incl=False):
        self._last_lc_criterion().started = DateTimeRange(since, until, until_incl)
        return self

    def ended(self, since=None, until=None, until_incl=False):
        self._last_lc_criterion().ended = DateTimeRange(since, until, until_incl)
        return self


class SortOption(str, Enum):
    """
    Options for sorting job run records.

    Attributes:
        CREATED: Sort by job instance creation timestamp
        STARTED: Sort by job instance start timestamp
        ENDED: Sort by job instance completion timestamp
        TIME: Sort by total execution duration
        JOB_ID: Sort alphabetically by job identifier
        RUN_ID: Sort alphabetically by run identifier
    """
    CREATED = "created"
    STARTED = "started"
    ENDED = "ended"
    TIME = "time"
    JOB_ID = "job"
    RUN_ID = "run"

    def key_func(self):
        """
        Returns a key function for sorting JobRun objects.

        Returns:
            Callable: Function that extracts the sort key from a JobRun
        """
        match self:
            case SortOption.CREATED:
                return lambda job_run: job_run.lifecycle.created_at
            case SortOption.STARTED:
                return lambda job_run: job_run.lifecycle.started_at or job_run.lifecycle.created_at
            case SortOption.ENDED:
                return lambda job_run: (job_run.lifecycle.termination.terminated_at
                                        if job_run.lifecycle.termination else job_run.lifecycle.created_at)
            case SortOption.TIME:
                return lambda job_run: job_run.lifecycle.elapsed.total_seconds() if job_run.lifecycle.elapsed else 0
            case SortOption.JOB_ID:
                return lambda job_run: job_run.job_id
            case SortOption.RUN_ID:
                return lambda job_run: job_run.run_id
            case _:
                raise AssertionError(f"Programmer error - unimplemented key for sort option: {self}")

    def sort_runs(self, job_runs, *, reverse=False):
        """
        Sorts a list of JobRun objects using this sort option.

        Args:
            job_runs: List of JobRun objects to sort
            reverse: If True, sort in descending order

        Returns:
            List[JobRun]: Sorted list of job runs
        """
        return sorted(job_runs, key=self.key_func(), reverse=reverse)
