"""
This module provides various criteria objects used to match job instances or their parts.

Matching rules:
    - Within a single criterion: all set fields are AND'd.
    - Within a ``JobRunCriteria`` list: criteria of the same type are OR'd.
    - Between ``JobRunCriteria`` lists: the three lists (metadata, lifecycle, phase) are AND'd.

Use ``criteria()`` or ``JobRunCriteria.builder()`` to incrementally compose criteria via a fluent API.
"""

from __future__ import annotations

from abc import abstractmethod, ABC
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from collections.abc import Mapping
from typing import Any, Self, TypeVar, Generic, Iterable

from runtools.runcore.job import JobInstanceMetadata, JobRun, InstanceID
from runtools.runcore.run import Outcome, TerminationInfo, \
    PhaseRun, TerminationStatus, RunLifecycle, Stage
from runtools.runcore.util import MatchingStrategy, DateTimeRange, TimeRange

T = TypeVar('T')


class MatchCriteria(ABC, Generic[T]):

    @abstractmethod
    def matches(self, tested: T) -> bool:
        """
        Check if the provided tested item matches the criteria.

        :param tested: The item to check against the criteria.
        :return: True if the item matches the criteria, False otherwise.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class MetadataCriterion(MatchCriteria[JobInstanceMetadata]):
    """Specifies criteria for matching instance metadata.

    If all fields are empty, the matching strategy defaults to ``MatchingStrategy.ALWAYS_TRUE``.

    Attributes:
        job_id (str): Pattern for job ID matching. Empty means ignored.
        run_id (str): Pattern for run ID matching. Empty means ignored.
        ordinal (int | None): Exact ordinal to match. ``None`` means any ordinal.
        exclude (InstanceID | None): Concrete execution to exclude from results.
        match_any_field (bool): If True, matches if any provided field matches. If False, all must match.
        strategy (MatchingStrategy): The strategy to use for matching. Default is ``EXACT``.
    """
    job_id: str = ''
    run_id: str = ''
    ordinal: int | None = None
    exclude: InstanceID | None = None
    match_any_field: bool = False
    strategy: MatchingStrategy = MatchingStrategy.EXACT

    @classmethod
    def all_match(cls) -> MetadataCriterion:
        """Creates a criterion that matches all instances."""
        return cls(strategy=MatchingStrategy.ALWAYS_TRUE)

    @classmethod
    def all_except(cls, instance_id: InstanceID) -> MetadataCriterion:
        """Creates a criterion that matches any job run except the specified concrete execution.

        Args:
            instance_id (InstanceID): The specific instance to exclude (including ordinal).

        Returns:
            MetadataCriterion: A criteria object matching any instance except the given one.
        """
        return MetadataCriterion(exclude=instance_id, strategy=MatchingStrategy.ALWAYS_TRUE)

    @classmethod
    def exact_match(cls, instance_id: InstanceID) -> MetadataCriterion:
        """Creates a criterion that matches a specific concrete execution.

        Args:
            instance_id (InstanceID): The specific instance to match (including ordinal).

        Returns:
            MetadataCriterion: A criteria object matching the given instance.
        """
        return cls(instance_id.job_id, instance_id.run_id, instance_id.ordinal)

    @classmethod
    def parse(cls, pattern: str,
              strategy: MatchingStrategy = MatchingStrategy.EXACT) -> MetadataCriterion:
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

        # Handle job@run or job@run:ordinal pattern
        if '@' in pattern:
            job_id, rest = pattern.split('@', 1)
            if ':' in rest:
                run_id, ordinal_str = rest.rsplit(':', 1)
                ordinal = int(ordinal_str) if ordinal_str else None
            else:
                run_id, ordinal = rest, None
            return cls(job_id, run_id, ordinal=ordinal, match_any_field=False, strategy=strategy)

        # Handle plain text (match against job_id or run_id)
        return cls(pattern, pattern, match_any_field=True, strategy=strategy)

    @classmethod
    def parse_strict(cls, id_string: str) -> MetadataCriterion:
        """Parses an instance ID string and returns a criterion for exact matching.

        Unlike ``parse()``, this requires a valid instance ID format and uses exact matching.
        Supports ``'job_id@run_id'`` and ``'job_id@run_id:ordinal'``.

        Args:
            id_string (str): Instance ID string.

        Returns:
            MetadataCriterion: Configured for exact matching of the specified instance.
        """
        instance_id = InstanceID.parse(id_string)
        return cls.exact_match(instance_id)

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

    def __call__(self, metadata: JobInstanceMetadata | None) -> bool:
        """Makes the criterion callable, delegating to matches()."""
        return self.matches(metadata)

    def matches(self, metadata: JobInstanceMetadata | None) -> bool:
        """
        Check if the provided metadata matches this object.

        Args:
            metadata: A metadata to match

        Returns:
            Whether the provided metadata matches this criteria
        """
        if metadata is None:
            return False
        if self.exclude is not None and metadata.instance_id == self.exclude:
            return False

        job_id_match = self._matches_id(metadata.job_id, self.job_id)
        run_id_match = self._matches_id(metadata.run_id, self.run_id)

        if self.match_any_field:
            id_match = (not self.job_id or job_id_match) or (not self.run_id or run_id_match)
        else:
            id_match = (not self.job_id or job_id_match) and (not self.run_id or run_id_match)

        if not id_match:
            return False
        if self.ordinal is not None and metadata.ordinal != self.ordinal:
            return False
        return True

    def serialize(self) -> dict[str, Any]:
        """Serializes the criterion to a dictionary."""
        d: dict[str, Any] = {
            'job_id': self.job_id,
            'run_id': self.run_id,
            'match_any_field': self.match_any_field,
            'strategy': self.strategy.name.lower(),
        }
        if self.ordinal is not None:
            d['ordinal'] = self.ordinal
        if self.exclude is not None:
            d['exclude'] = self.exclude.serialize()
        return d

    @classmethod
    def deserialize(cls, as_dict: dict[str, Any]) -> MetadataCriterion:
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

        exclude_data = as_dict.get('exclude')
        return cls(
            job_id=as_dict.get('job_id', ''),
            run_id=as_dict.get('run_id', ''),
            ordinal=as_dict.get('ordinal'),
            exclude=InstanceID.deserialize(exclude_data) if exclude_data else None,
            match_any_field=match_any,
            strategy=MatchingStrategy[as_dict.get('strategy', 'exact').upper()],
        )

    def __str__(self) -> str:
        fields = []
        if self.job_id:
            fields.append(f"job_id='{self.job_id}'")
        if self.run_id:
            fields.append(f"run_id='{self.run_id}'")
        if self.ordinal is not None:
            fields.append(f"ordinal={self.ordinal}")
        if self.exclude is not None:
            fields.append(f"exclude={self.exclude}")
        if self.match_any_field:
            fields.append("match_any_field=True")
        if self.strategy != MatchingStrategy.EXACT:
            fields.append(f"strategy={self.strategy.name}")
        return f"<{', '.join(fields)}>" if fields else ""


@dataclass(frozen=True)
class TerminationCriterion(MatchCriteria[TerminationInfo]):
    """
    Criteria for matching termination information.

    Attributes:
        status: Match specific termination status
        outcome: Match specific outcome category
        success: Match by success/non-success (True = success, False = non-success, None = no filter)
        ended_range: Match by termination timestamp range
    """
    status: TerminationStatus | None = None
    outcome: Outcome | None = None
    success: bool | None = None
    ended_range: DateTimeRange | None = None

    @classmethod
    def deserialize(cls, as_dict: dict[str, Any]) -> TerminationCriterion:
        return cls(
            status=TerminationStatus[as_dict['status']] if as_dict.get('status') else None,
            outcome=Outcome[as_dict['outcome']] if as_dict.get('outcome') else None,
            success=as_dict.get('success'),
            ended_range=DateTimeRange.deserialize(as_dict['ended_range']) if as_dict.get('ended_range') else None
        )

    def serialize(self) -> dict[str, Any]:
        return {
            'status': self.status.name if self.status else None,
            'outcome': self.outcome.name if self.outcome else None,
            'success': self.success,
            'ended_range': self.ended_range.serialize() if self.ended_range else None,
        }

    def matches(self, term_info: TerminationInfo | None) -> bool:
        if term_info is None:
            return False

        if self.status is not None and term_info.status != self.status:
            return False

        if self.outcome is not None and term_info.status.outcome != self.outcome:
            return False

        if self.success is not None:
            if term_info.status.outcome.is_success != self.success:
                return False

        if self.ended_range and not self.ended_range(term_info.terminated_at):
            return False

        return True

    def __call__(self, term_info: TerminationInfo | None) -> bool:
        return self.matches(term_info)

    def __bool__(self) -> bool:
        return (self.status is not None or
                self.outcome is not None or
                self.success is not None or
                self.ended_range is not None)

    def __str__(self) -> str:
        fields = []
        if self.status is not None:
            fields.append(f"status={self.status.name}")
        if self.outcome is not None:
            fields.append(f"outcome={self.outcome.name}")
        if self.success is not None:
            fields.append(f"success={self.success}")
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


@dataclass(frozen=True)
class LifecycleCriterion(MatchCriteria[RunLifecycle]):
    """
    Criteria for matching run lifecycle information.
    """
    stage: Stage | None = None
    created: DateTimeRange | None = None
    started: DateTimeRange | None = None
    ended: DateTimeRange | None = None
    total_run_time: TimeRange | None = None
    termination: TerminationCriterion | None = None

    @classmethod
    def reached_stage(cls, stage: Stage) -> LifecycleCriterion:
        """Create a criterion that matches runs which have reached the given stage.

        Args:
            stage: The stage that must have been reached.
        """
        match stage:
            case Stage.CREATED:
                return cls(created=DateTimeRange.unbounded())
            case Stage.RUNNING:
                return cls(started=DateTimeRange.unbounded())
            case Stage.ENDED:
                return cls(ended=DateTimeRange.unbounded())
            case _:
                raise ValueError(f"Unknown stage: {stage}")

    @classmethod
    def for_date_range(cls, date_range: DateTimeRange, for_stage: Stage) -> LifecycleCriterion:
        """Create a criterion with a pre-built DateTimeRange on the specified stage's timestamp.

        Use this when you already have a DateTimeRange (e.g. from ``DateTimeRange.days_range()``).
        For raw datetime bounds, see ``for_period()``.

        Args:
            date_range (DateTimeRange): The date range to apply.
            for_stage (Stage): Which stage timestamp field to filter on.
        """
        match for_stage:
            case Stage.CREATED:
                return cls(created=date_range)
            case Stage.RUNNING:
                return cls(started=date_range)
            case Stage.ENDED:
                return cls(ended=date_range)
            case _:
                raise ValueError(f"Unknown stage: {for_stage}")

    @classmethod
    def for_period(cls, for_stage: Stage, *, since: datetime | None = None, until: datetime | None = None) \
            -> LifecycleCriterion:
        """Create a criterion with explicit datetime bounds on the specified stage's timestamp.

        Convenience alternative to ``for_date_range()`` when you have raw datetime values
        instead of a DateTimeRange object.

        Args:
            for_stage (Stage): Which stage timestamp field to filter on.
            since (datetime | None): Inclusive lower bound. None means unbounded.
            until (datetime | None): Exclusive upper bound. None means unbounded.
        """
        return cls.for_date_range(DateTimeRange(since=since, until=until), for_stage)

    def serialize(self) -> dict[str, Any]:
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
            data['total_run_time_range'] = self.total_run_time.serialize()
        if self.termination:
            data['termination'] = self.termination.serialize()
        return data

    @classmethod
    def deserialize(cls, as_dict: dict[str, Any]) -> LifecycleCriterion:
        """Deserialize from a dictionary."""
        return cls(
            stage=Stage[as_dict['stage']] if as_dict.get('stage') else None,
            created=DateTimeRange.deserialize(as_dict['created_range']) if as_dict.get('created_range') else None,
            started=DateTimeRange.deserialize(as_dict['started_range']) if as_dict.get('started_range') else None,
            ended=DateTimeRange.deserialize(as_dict['ended_range']) if as_dict.get('ended_range') else None,
            total_run_time=TimeRange.deserialize(as_dict['total_run_time_range']) if as_dict.get('total_run_time_range') else None,
            termination=TerminationCriterion.deserialize(as_dict['termination']) if as_dict.get('termination') else None
        )

    def __call__(self, lifecycle: RunLifecycle | None):
        return self.matches(lifecycle)

    def matches(self, lifecycle: RunLifecycle | None) -> bool:
        """Check if the lifecycle matches all specified criteria."""
        if lifecycle is None:
            return False

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


@dataclass(frozen=True)
class PhaseCriterion(MatchCriteria[PhaseRun]):
    """
    Criteria for matching phase details, incorporating phase-specific and lifecycle criteria.

    For criteria that target only the root phase (e.g., lifecycle information),
    ensure match_type=PhaseMatch.ROOT.
    When using PhaseMatch.ALL or DESCENDANTS_ONLY, only non-root fields will be considered.

    Attributes:
        phase_type: Phase type to match
        phase_id: Phase ID to match
        idle: Idle phase match
        attributes: Dictionary of attributes to match. None = no attribute matching
        lifecycle: Criteria for matching lifecycle information. For root phase only.
        match_type: How phases are matched. Defaults to ROOT.
    """
    phase_type: str | None = None
    phase_id: str | None = None
    idle: bool | None = None
    attributes: Mapping[str, Any] | None = None
    lifecycle: LifecycleCriterion | None = None
    match_type: PhaseMatch = PhaseMatch.ROOT

    def __post_init__(self):
        """Wrap mutable dict in MappingProxyType so the frozen dataclass stays truly immutable."""
        if self.attributes is not None and not isinstance(self.attributes, MappingProxyType):
            object.__setattr__(self, 'attributes', MappingProxyType(self.attributes))

    @classmethod
    def deserialize(cls, as_dict: dict[str, Any]) -> PhaseCriterion:
        """Deserialize a dictionary into a PhaseCriterion instance."""
        return cls(
            phase_type=as_dict.get('phase_type'),
            phase_id=as_dict.get('phase_id'),
            idle=as_dict.get('idle'),
            attributes=as_dict.get('attributes'),
            lifecycle=LifecycleCriterion.deserialize(as_dict.get('lifecycle')) if as_dict.get('lifecycle') else None,
            match_type=PhaseMatch[as_dict.get('match_type', PhaseMatch.ROOT.name)]
        )

    def serialize(self) -> dict[str, Any]:
        """Serialize this criterion into a dictionary."""
        return {
            'phase_type': self.phase_type,
            'phase_id': self.phase_id,
            'idle': self.idle,
            'attributes': dict(self.attributes) if self.attributes else None,
            'lifecycle': self.lifecycle.serialize() if self.lifecycle else None,
            'match_type': self.match_type.name
        }

    def _matches_phase(self, phase_run: PhaseRun, check_lifecycle: bool = False) -> bool:
        """Check if a single phase matches this criterion.

        Args:
            phase_run: The phase to check
            check_lifecycle: Whether to check lifecycle criteria (only for root phase)
        """
        if self.phase_type and phase_run.phase_type != self.phase_type:
            return False

        if self.phase_id and phase_run.phase_id != self.phase_id:
            return False

        if self.idle is not None and phase_run.is_idle != self.idle:
            return False

        if self.attributes:
            if not phase_run.attributes:
                return False
            for key, value in self.attributes.items():
                if phase_run.attributes.get(key) != value:
                    return False

        if check_lifecycle and self.lifecycle and not self.lifecycle.matches(phase_run.lifecycle):
            return False

        return True

    def _matches_any_descendant(self, phase: PhaseRun) -> bool:
        """Recursively check if any descendant phase matches (without lifecycle)."""
        for child in phase.children:
            if self._matches_phase(child) or self._matches_any_descendant(child):
                return True
        return False

    def __call__(self, phase: PhaseRun | None):
        return self.matches(phase)

    def matches(self, phase: PhaseRun | None) -> bool:
        """Check if phase or its descendants match based on match_type."""
        if phase is None:
            return False
        match self.match_type:
            case PhaseMatch.ROOT:
                return self._matches_phase(phase, check_lifecycle=True)
            case PhaseMatch.ALL:
                return self._matches_phase(phase, check_lifecycle=True) or self._matches_any_descendant(phase)
            case PhaseMatch.DESCENDANTS_ONLY:
                return self._matches_any_descendant(phase)
            case _:
                raise AssertionError(f"Unknown match type: {self.match_type}")

    def __bool__(self) -> bool:
        """Check if any criteria are set."""
        return bool(self.phase_type or self.phase_id or self.idle or
                    self.attributes or self.lifecycle)

    def __str__(self) -> str:
        """Create a string representation showing non-None criteria."""
        fields = []
        if self.phase_type:
            fields.append(f"type='{self.phase_type}'")
        if self.phase_id:
            fields.append(f"id='{self.phase_id}'")
        if self.idle is not None:
            fields.append(f"idle={self.idle}")
        if self.attributes:
            fields.append(f"attrs={self.attributes}")
        if self.lifecycle:
            fields.append(f"lifecycle{self.lifecycle}")
        if self.match_type != PhaseMatch.ROOT:
            fields.append(f"match_type={self.match_type.name}")
        return f"<{', '.join(fields)}>" if fields else ""


@dataclass(frozen=True)
class JobRunCriteria(MatchCriteria[JobRun]):
    """
    Immutable criteria for querying and matching job runs.

    Matching rules:
        - Within each tuple: criteria are OR'd (any match suffices).
        - Between tuples: the three groups are AND'd (all groups must match).

    Use factory classmethods for one-shot construction, or ``builder()`` for incremental composition.
    """
    metadata_criteria: tuple[MetadataCriterion, ...] = ()
    lifecycle_criteria: tuple[LifecycleCriterion, ...] = ()
    phase_criteria: tuple[PhaseCriterion, ...] = ()

    def __post_init__(self):
        """Coerce any iterables to tuples so callers can pass lists without breaking immutability."""
        if not isinstance(self.metadata_criteria, tuple):
            object.__setattr__(self, 'metadata_criteria', tuple(self.metadata_criteria))
        if not isinstance(self.lifecycle_criteria, tuple):
            object.__setattr__(self, 'lifecycle_criteria', tuple(self.lifecycle_criteria))
        if not isinstance(self.phase_criteria, tuple):
            object.__setattr__(self, 'phase_criteria', tuple(self.phase_criteria))

    @classmethod
    def builder(cls) -> JobRunCriteriaBuilder:
        """Create a new builder for incremental criteria composition."""
        return JobRunCriteriaBuilder()

    @classmethod
    def all(cls) -> JobRunCriteria:
        return cls(metadata_criteria=(MetadataCriterion.all_match(),))

    @classmethod
    def deserialize(cls, as_dict) -> JobRunCriteria:
        return cls(
            metadata_criteria=tuple(MetadataCriterion.deserialize(c) for c in as_dict.get('metadata_criteria', ())),
            lifecycle_criteria=tuple(LifecycleCriterion.deserialize(c) for c in as_dict.get('lifecycle_criteria', ())),
            phase_criteria=tuple(PhaseCriterion.deserialize(c) for c in as_dict.get('phase_criteria', ())),
        )

    def serialize(self):
        return {
            'metadata_criteria': [c.serialize() for c in self.metadata_criteria],
            'lifecycle_criteria': [c.serialize() for c in self.lifecycle_criteria],
            'phase_criteria': [c.serialize() for c in self.phase_criteria],
        }

    @classmethod
    def parse(cls, pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT) -> JobRunCriteria:
        return cls(metadata_criteria=(MetadataCriterion.parse(pattern, strategy),))

    @classmethod
    def parse_all(cls, patterns: Iterable[str], strategy: MatchingStrategy = MatchingStrategy.EXACT) -> JobRunCriteria:
        return cls(metadata_criteria=tuple(MetadataCriterion.parse(p, strategy) for p in patterns))

    @classmethod
    def parse_strict(cls, id_string) -> JobRunCriteria:
        return cls(metadata_criteria=(MetadataCriterion.parse_strict(id_string),))

    @classmethod
    def job_match(cls, job_id, strategy: MatchingStrategy = MatchingStrategy.EXACT) -> JobRunCriteria:
        return cls(metadata_criteria=(MetadataCriterion(job_id=job_id, strategy=strategy),))

    @classmethod
    def exact_match(cls, job_run) -> JobRunCriteria:
        return cls(metadata_criteria=(MetadataCriterion.exact_match(job_run.instance_id),))

    @classmethod
    def all_except(cls, job_run) -> JobRunCriteria:
        return cls(metadata_criteria=(MetadataCriterion.all_except(job_run.instance_id),))

    @classmethod
    def instance_match(cls, instance_id) -> JobRunCriteria:
        return cls(metadata_criteria=(MetadataCriterion.exact_match(instance_id),))

    def matches_metadata(self, job_run):
        return not self.metadata_criteria or any(c(job_run.metadata) for c in self.metadata_criteria)

    def matches_lifecycle(self, job_run):
        return not self.lifecycle_criteria or any(c(job_run.lifecycle) for c in self.lifecycle_criteria)

    def match_phases(self, job_run):
        """Check if any phase in the job run matches any of the phase criteria."""
        if not self.phase_criteria:
            return True

        for phase in job_run.search_phases():
            if any(c.matches(phase) for c in self.phase_criteria):
                return True
        return False

    def __call__(self, job_run: JobRun | None):
        return self.matches(job_run)

    def matches(self, job_run: JobRun | None):
        """Check if a job run matches all criteria."""
        if job_run is None:
            return False

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

    def __repr__(self) -> str:
        return (f"JobRunCriteria(metadata_criteria={self.metadata_criteria!r}, "
                f"lifecycle_criteria={self.lifecycle_criteria!r}, phase_criteria={self.phase_criteria!r})")


class JobRunCriteriaBuilder:
    """Fluent builder for constructing frozen ``JobRunCriteria``.

    Each method call appends one criterion to the appropriate list (OR semantics).
    To AND multiple fields within a single criterion, construct the criterion object directly
    and pass it via the typed core methods.

    Example::

        result = (criteria()
            .pattern("backup*", MatchingStrategy.FN_MATCH)
            .reached_stage(Stage.ENDED)
            .build())
    """

    def __init__(self):
        self._metadata: list[MetadataCriterion] = []
        self._lifecycle: list[LifecycleCriterion] = []
        self._phase: list[PhaseCriterion] = []

    # -- Typed core methods --

    def metadata(self, *criteria: MetadataCriterion) -> Self:
        """Append one or more metadata criteria."""
        self._metadata.extend(criteria)
        return self

    def lifecycle(self, *criteria: LifecycleCriterion) -> Self:
        """Append one or more lifecycle criteria."""
        self._lifecycle.extend(criteria)
        return self

    def phase(self, *criteria: PhaseCriterion) -> Self:
        """Append one or more phase criteria."""
        self._phase.extend(criteria)
        return self

    # -- Convenience: metadata --

    def pattern(self, text: str, strategy: MatchingStrategy = MatchingStrategy.EXACT) -> Self:
        """Append a metadata criterion parsed from a pattern string."""
        self._metadata.append(MetadataCriterion.parse(text, strategy))
        return self

    def patterns_or_all(self, patterns: Iterable[str] | None,
                        strategy: MatchingStrategy = MatchingStrategy.EXACT) -> Self:
        """Append a metadata criterion for each pattern, or match-all if patterns is empty/None."""
        added = False
        for p in (patterns or ()):
            self._metadata.append(MetadataCriterion.parse(p, strategy))
            added = True
        if not added:
            self._metadata.append(MetadataCriterion.all_match())
        return self

    def job(self, job_id: str, strategy: MatchingStrategy = MatchingStrategy.EXACT) -> Self:
        """Append a metadata criterion matching a job ID."""
        self._metadata.append(MetadataCriterion(job_id=job_id, strategy=strategy))
        return self

    def instance(self, instance_id: InstanceID) -> Self:
        """Append a metadata criterion matching a specific instance."""
        self._metadata.append(MetadataCriterion.exact_match(instance_id))
        return self

    def all_except(self, instance_id: InstanceID) -> Self:
        """Append a metadata criterion matching everything except a specific instance."""
        self._metadata.append(MetadataCriterion.all_except(instance_id))
        return self

    # -- Convenience: lifecycle --

    def created(self, since=None, until=None, until_incl=False) -> Self:
        """Append a lifecycle criterion filtering by creation timestamp."""
        self._lifecycle.append(LifecycleCriterion(created=DateTimeRange(since, until, until_incl)))
        return self

    def started(self, since=None, until=None, until_incl=False) -> Self:
        """Append a lifecycle criterion filtering by start timestamp."""
        self._lifecycle.append(LifecycleCriterion(started=DateTimeRange(since, until, until_incl)))
        return self

    def ended(self, since=None, until=None, until_incl=False) -> Self:
        """Append a lifecycle criterion filtering by end timestamp."""
        self._lifecycle.append(LifecycleCriterion(ended=DateTimeRange(since, until, until_incl)))
        return self

    def reached_stage(self, stage: Stage) -> Self:
        """Append a lifecycle criterion matching runs that reached the given stage."""
        self._lifecycle.append(LifecycleCriterion.reached_stage(stage))
        return self

    def termination(self, criterion: TerminationCriterion) -> Self:
        """Append a lifecycle criterion wrapping a termination criterion."""
        self._lifecycle.append(LifecycleCriterion(termination=criterion))
        return self

    def termination_status(self, status: TerminationStatus) -> Self:
        """Append a lifecycle criterion matching a specific termination status."""
        return self.termination(TerminationCriterion(status=status))

    def termination_outcome(self, outcome: Outcome) -> Self:
        """Append a lifecycle criterion matching a specific outcome category."""
        return self.termination(TerminationCriterion(outcome=outcome))

    def success(self) -> Self:
        """Append a lifecycle criterion matching successful runs."""
        return self.termination(TerminationCriterion(success=True))

    def nonsuccess(self) -> Self:
        """Append a lifecycle criterion matching non-successful runs."""
        return self.termination(TerminationCriterion(success=False))

    def during(self, for_stage: Stage, from_date=None, to_date=None,
                     today=False, yesterday=False, week=False, fortnight=False,
                     three_weeks=False, four_weeks=False, month=False, days_back=None) -> Self:
        """Append lifecycle criteria for each specified date filter (OR logic).

        Args:
            for_stage: Which timestamp field to filter on (CREATED, RUNNING, ENDED).
            from_date: Start date string.
            to_date: End date string.
            today: Filter for today.
            yesterday: Filter for yesterday.
            week: Filter for last week.
            fortnight: Filter for last 2 weeks.
            three_weeks: Filter for last 3 weeks.
            four_weeks: Filter for last 4 weeks.
            month: Filter for last month.
            days_back: Filter for N days back.
        """
        date_ranges = []

        if from_date or to_date:
            date_ranges.append(DateTimeRange.parse_to_utc(from_date, to_date))
        if today:
            date_ranges.append(DateTimeRange.today(to_utc=True))
        if yesterday:
            date_ranges.append(DateTimeRange.yesterday(to_utc=True))
        if week:
            date_ranges.append(DateTimeRange.week_back(to_utc=True))
        if fortnight:
            date_ranges.append(DateTimeRange.days_range(-14, to_utc=True))
        if three_weeks:
            date_ranges.append(DateTimeRange.days_range(-21, to_utc=True))
        if four_weeks:
            date_ranges.append(DateTimeRange.days_range(-28, to_utc=True))
        if month:
            date_ranges.append(DateTimeRange.days_range(-31, to_utc=True))
        if days_back is not None:
            date_ranges.append(DateTimeRange.days_range(-days_back, to_utc=True))

        for dr in date_ranges:
            self._lifecycle.append(LifecycleCriterion.for_date_range(dr, for_stage))

        return self

    def build(self) -> JobRunCriteria:
        """Construct the frozen ``JobRunCriteria``."""
        return JobRunCriteria(
            metadata_criteria=tuple(self._metadata),
            lifecycle_criteria=tuple(self._lifecycle),
            phase_criteria=tuple(self._phase),
        )


def criteria() -> JobRunCriteriaBuilder:
    """Shorthand for ``JobRunCriteria.builder()``."""
    return JobRunCriteriaBuilder()


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
