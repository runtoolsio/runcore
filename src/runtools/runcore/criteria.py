"""
This module provides various criteria objects used to match job instances or their parts.

TODO: Remove immutable properties
"""

import datetime
from abc import abstractmethod, ABC
from dataclasses import dataclass, field
from typing import Dict, Any, Set, Optional, TypeVar, Generic

from runtools.runcore import JobRun
from runtools.runcore.job import JobInstanceMetadata
from runtools.runcore.run import Outcome, Lifecycle, TerminationInfo, RunState, \
    PhaseInfo
from runtools.runcore.util import MatchingStrategy, parse, single_day_range, days_range, \
    format_dt_iso, to_list, DateTimeRange, parse_range_to_utc

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


@dataclass
class InstanceMetadataCriterion(MatchCriteria[JobInstanceMetadata]):
    """
    This class specifies criteria for matching instance metadata.
    If all fields are empty, the matching strategy defaults to `MatchingStrategy.ALWAYS_TRUE`.

    Attributes:
        job_id (str): The pattern for job ID matching. If empty, the field is ignored.
        run_id (str): The pattern for run ID matching. If empty, the field is ignored.
        instance_id (str): The pattern for instance ID matching. If empty, the field is ignored.
        match_all_ids (bool): If True, all provided IDs must match. If False, a match with any provided ID is sufficient.
        strategy (MatchingStrategy): The strategy to use for matching. Default is `MatchingStrategy.EXACT`.
    """
    job_id: str = ''
    run_id: str = ''
    instance_id: str = ''
    match_all_ids: bool = True
    strategy: MatchingStrategy = MatchingStrategy.EXACT

    @classmethod
    def all_match(cls):
        return cls('', '', '', False, MatchingStrategy.ALWAYS_TRUE)

    @classmethod
    def none_match(cls):
        return cls('', '', '', True, MatchingStrategy.ALWAYS_FALSE)

    @staticmethod
    def match_run(job_run):
        """
        Creates a MetadataCriterion object that matches the provided job run by its instance ID.

        Args:
            job_run: The specific job run to create a match for.

        Returns:
            InstanceMetadataCriterion: A criteria object that will match the given job instance.
        """
        return InstanceMetadataCriterion(instance_id=job_run.metadata.instance_id)

    @classmethod
    def parse_pattern(cls, pattern: str, strategy=MatchingStrategy.EXACT):
        """
        Parses the provided pattern and returns the corresponding metadata criterion.

        The pattern can be in one of these formats:
        - ":instance_id" - Match instance ID only
        - "job_id@run_id" - Match specific job ID and run ID combination
        - Any other text - Match against all IDs with match_any=True (will match if it matches any of job_id, run_id, or instance_id)

        Args:
            pattern (str): The pattern to parse. Can be empty, an instance ID with ':' prefix, a job/run combo with '@', or plain text.
            strategy (MatchingStrategy, optional): The strategy to use for matching. Default is `MatchingStrategy.EXACT`

        Returns:
            InstanceMetadataCriterion: A new criteria object configured according to the pattern format.
        """
        if not pattern:
            return cls.all_match()

        # Handle instance ID pattern
        if pattern.startswith(':'):
            return cls('', '', pattern[1:], True, strategy)

        # Handle job@run pattern
        if '@' in pattern:
            job_id, run_id = pattern.split('@', 1)
            return cls(job_id, run_id, '', True, strategy)

        # Handle plain text (match against any ID)
        return cls(pattern, pattern, pattern, False, strategy)

    def _matches_id(self, actual: str, criteria: str) -> bool:
        if not criteria:
            return True
        if criteria.startswith('!'):
            return not self.strategy(actual, criteria[1:])  # Remove '!' and negate the result
        return self.strategy(actual, criteria)

    def __call__(self, metadata: JobInstanceMetadata):
        return self.matches(metadata)

    def matches(self, metadata: JobInstanceMetadata) -> bool:
        """
        The matching method. It can be also executed by calling this object.

        Args:
            metadata: A metadata to match

        Returns:
            bool: Whether the provided metadata matches this criteria
        """
        # Check each ID against its criteria
        job_id_match = self._matches_id(metadata.job_id, self.job_id)
        run_id_match = self._matches_id(metadata.run_id, self.run_id)
        instance_id_match = self._matches_id(metadata.instance_id, self.instance_id)

        if self.match_all_ids:
            return (not self.job_id or job_id_match) and \
                (not self.run_id or run_id_match) and \
                (not self.instance_id or instance_id_match)
        else:
            return (not self.job_id or job_id_match) or \
                (not self.run_id or run_id_match) or \
                (not self.instance_id or instance_id_match)

    def serialize(self):
        return {
            'job_id': self.job_id,
            'run_id': self.run_id,
            'instance_id': self.instance_id,
            'match_any': self.match_all_ids,
            'strategy': self.strategy.name.lower(),
        }

    @classmethod
    def deserialize(cls, as_dict):
        return cls(
            as_dict['job_id'],
            as_dict['run_id'],
            as_dict.get('instance_id', ''),  # For backward compatibility
            as_dict.get('match_any', False),  # For backward compatibility
            MatchingStrategy[as_dict['strategy'].upper()]
        )

    def __str__(self):
        ids = []
        if self.job_id:
            ids.append(f"job={self.job_id}")
        if self.run_id:
            ids.append(f"run={self.run_id}")
        if self.instance_id:
            ids.append(f"instance={self.instance_id}")

        op = "&" if self.match_all_ids else "|"
        return f"({op.join(ids)}) {self.strategy.name}"


def compound_instance_filter(metadata_criteria):
    def match(metadata):
        return not metadata_criteria or any(criteria(metadata) for criteria in metadata_criteria)

    return match


@dataclass
class LifecycleCriterion(MatchCriteria[Lifecycle]):
    """
    A class to represent criteria for determining if the first occurrence of a given run state in a lifecycle falls
    within a specified datetime interval. This criterion is used to filter or identify lifecycles based
    on the timing of their first transition to the specified run state.

    Properties:
        from_dt (datetime, optional):
            The start date-time of the interval. Defaults to None.
        to_dt (datetime, optional):
            The end date-time of the interval. Defaults to None.
        include_to (bool, optional):
            Whether to include the end date-time in the interval. Defaults to True.
    """

    created_range: DateTimeRange
    ended_range: DateTimeRange

    def __init__(self,
                 created_from: Optional[datetime] = None,
                 created_to: Optional[datetime] = None,
                 created_to_included: bool = True,
                 ended_from: Optional[datetime] = None,
                 ended_to: Optional[datetime] = None,
                 ended_to_included: bool = True):
        self.created_range = DateTimeRange(created_from, created_to, created_to_included)
        self.ended_range = DateTimeRange(ended_from, ended_to, ended_to_included)

    @classmethod
    def deserialize(cls, data):
        created_from = parse(data.get("created_from", None))
        created_to = parse(data.get("created_to", None))
        created_to_included = data.get("created_to_included", True)

        ended_from = parse(data.get("ended_from", None))
        ended_to = parse(data.get("ended_to", None))
        ended_to_included = data.get("ended_to_included", True)

        return cls(created_from, created_to, created_to_included, ended_from, ended_to, ended_to_included)

    def serialize(self) -> Dict[str, Any]:
        return {
            "created_from": format_dt_iso(self.created_range.start),
            "created_to": format_dt_iso(self.created_range.end),
            "created_to_included": self.created_range.end_included,
            "ended_from": format_dt_iso(self.ended_range.start),
            "ended_to": format_dt_iso(self.ended_range.end),
            "ended_to_included": self.ended_range.end_included,
        }

    @classmethod
    def to_utc(cls, from_val, to_val):
        """
        Creates criteria with provided values converted to the UTC timezone.

        Args:
            from_val (str, datetime, date): The start date-time of the interval.
            to_val (str, datetime, date): The end date-time of the interval.
        """
        return LifecycleCriterion(*parse_range_to_utc(from_val, to_val))

    @classmethod
    def single_day_period(cls, day_offset, *, to_utc=False):
        """
        Creates criteria for a duration of one day.

        Args:
            day_offset (int): A day offset for which the period is created. 0 > today, -1 > yesterday, 1 > tomorrow...
            to_utc (bool): The interval is converted from local zone to UTC when set to true.
        """
        return cls(*single_day_range(day_offset, to_utc=to_utc))

    @classmethod
    def today(cls, *, to_utc=False):
        return cls.single_day_period(0, to_utc=to_utc)

    @classmethod
    def yesterday(cls, *, to_utc=False):
        return cls.single_day_period(-1, to_utc=to_utc)

    @classmethod
    def days_interval(cls, days, *, to_utc=False):
        """
        Creates criteria for an interval extending a specified number of days into the past or future from now.

        Args:
            days (int):
                Duration of the interval in days. Use a negative number for an interval extending into the past,
                and a positive number for an interval extending into the future.
            to_utc (bool):
                If true, the interval is converted from the local time zone to UTC; otherwise, it remains
                in the local time zone.
        """
        return cls(*days_range(days, to_utc=to_utc))

    @classmethod
    def week_back(cls, *, to_utc=False):
        return cls.days_interval(-7, to_utc=to_utc)

    def __call__(self, lifecycle):
        return self.matches(lifecycle)

    def __bool__(self):
        return self.created_range or self.ended_range

    def matches(self, lifecycle):
        return self.created_range(lifecycle.created_at) and not self.ended_range or self.ended_range(lifecycle.ended_at)


@dataclass
class PhaseCriterion(MatchCriteria[PhaseInfo]):
    phase_type: Optional[str] = None
    phase_id: Optional[str] = None
    run_state: Optional[RunState] = None
    phase_name: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> 'PhaseCriterion':
        return cls(
            phase_type=data['phase_type'],
            phase_id=data['phase_id'],
            run_state=RunState[data['run_state']] if data.get('run_state') else None,
            phase_name=data.get('phase_name'),
            attributes=data.get('attributes', {})
        )

    def serialize(self) -> Dict[str, Any]:
        return {
            'phase_type': self.phase_type,
            'phase_id': self.phase_id,
            'run_state': self.run_state.value if self.run_state else RunState.NONE.value,
            'phase_name': self.phase_name,
            'attributes': self.attributes
        }

    def matches(self, phase_info: PhaseInfo) -> bool:
        if self.phase_type and phase_info.phase_type != self.phase_type:
            return False

        if self.phase_id and phase_info.phase_id != self.phase_id:
            return False

        if self.run_state and phase_info.run_state != self.run_state:
            return False

        if self.phase_name and phase_info.phase_name != self.phase_name:
            return False

        if self.attributes:
            if not phase_info.attributes:
                return False
            for key, value in self.attributes.items():
                if phase_info.attributes.get(key) != value:
                    return False

        return True

    def __call__(self, phase_metadata: PhaseInfo) -> bool:
        return self.matches(phase_metadata)

    def __bool__(self):
        return bool(self.phase_name or self.run_state or self.attributes)


@dataclass
class TerminationCriterion(MatchCriteria[TerminationInfo]):
    """
    This class is used to filter termination info instances.

    Attributes:
        outcome (Set[Outcome]): A set of outcomes to match against the termination status.
    """

    outcome: Outcome = Outcome.ANY

    @classmethod
    def deserialize(cls, data):
        outcome = Outcome[data.get('outcome', Outcome.ANY.name)]
        return cls(outcome)

    def serialize(self):
        return {
            "outcome": self.outcome.name,
        }

    def __call__(self, term_info):
        return self.matches(term_info)

    def matches(self, term_info):
        return term_info.status.is_outcome(self.outcome)

    def __bool__(self):
        return self.outcome != Outcome.ANY


def parse_criteria(pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT) -> 'JobRunCriteria':
    return JobRunCriteria.parse(pattern, strategy)


class JobRunCriteria(MatchCriteria[JobRun]):
    """
    This object aggregates various criteria for querying and matching job instances.
    An instance must meet all the provided criteria to be considered a match.

    Properties:
        jobs (List[Job]):
            A list of specific job IDs for matching.
            An instance matches if its job ID is in this list.
        job_run_id_criteria (List[JobRunIdCriterion]):
            A list of criteria for matching based on job run IDs.
            An instance matches if it meets any of the criteria in this list.
        interval_criteria (List[IntervalCriterion]):
            A list of criteria for matching based on time intervals.
            An instance matches if it meets any of the criteria in this list.
        termination_criteria (List[TerminationCriterion]):
            A list of criteria for matching based on termination conditions.
            An instance matches if it meets any of the criteria in this list.

    The class provides methods to check whether a given job instance matches the criteria,
    serialize and deserialize the criteria, and parse criteria from a pattern.
    """

    def __init__(self, *,
                 jobs=None,
                 metadata_criteria=None,
                 interval_criteria=None,
                 phase_criteria=None,
                 termination_criteria=None):
        self.jobs = to_list(jobs) or []
        self.metadata_criteria = to_list(metadata_criteria)
        self.interval_criteria = to_list(interval_criteria)
        self.phase_criteria = to_list(phase_criteria)
        self.termination_criteria = to_list(termination_criteria)

    @classmethod
    def all(cls):
        return cls(metadata_criteria=InstanceMetadataCriterion.all_match())

    @classmethod
    def deserialize(cls, as_dict):
        new = cls()
        new.jobs = as_dict.get('jobs', [])
        new.metadata_criteria = [InstanceMetadataCriterion.deserialize(c) for c in as_dict.get('metadata_criteria', ())]
        new.interval_criteria = [LifecycleCriterion.deserialize(c) for c in as_dict.get('interval_criteria', ())]
        new.phase_criteria = [PhaseCriterion.deserialize(c) for c in as_dict.get('phase_criteria', ())]
        new.termination_criteria = [TerminationCriterion.deserialize(c) for c in
                                    as_dict.get('termination_criteria', ())]
        return new

    def serialize(self):
        return {
            'jobs': self.jobs,
            'metadata_criteria': [c.serialize() for c in self.metadata_criteria],
            'interval_criteria': [c.serialize() for c in self.interval_criteria],
            'phase_criteria': [c.serialize() for c in self.phase_criteria],
            'state_criteria': [c.serialize() for c in self.termination_criteria],
        }

    @classmethod
    def parse(cls, pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT):
        new = cls()
        new += InstanceMetadataCriterion.parse_pattern(pattern, strategy)
        return new

    @classmethod
    def job_id(cls, job_id, strategy: MatchingStrategy = MatchingStrategy.EXACT):
        new = cls()
        new += InstanceMetadataCriterion(job_id=job_id, strategy=strategy)
        return new

    @classmethod
    def match_run(cls, job_run):
        new = cls()
        new += InstanceMetadataCriterion.match_run(job_run)
        return new

    def __iadd__(self, criterion):
        return self.add(criterion)

    def add(self, criterion):
        match criterion:
            case str():
                self.jobs.append(criterion)
            case InstanceMetadataCriterion():
                self.metadata_criteria.append(criterion)
            case LifecycleCriterion():
                self.interval_criteria.append(criterion)
            case PhaseCriterion():
                self.phase_criteria.append(criterion)
            case TerminationCriterion():
                self.termination_criteria.append(criterion)
            case _:
                raise ValueError("Invalid criterion type")

        return self

    def matches_metadata(self, job_run):
        return not self.metadata_criteria or any(c(job_run.metadata) for c in self.metadata_criteria)

    def matches_interval(self, job_run):
        return not self.interval_criteria or any(c(job_run.lifecycle) for c in self.interval_criteria)

    def match_phases(self, job_run):
        return not self.phase_criteria or any(c(p) for c in self.phase_criteria for p in job_run.phases)

    def matches_termination(self, job_run):
        return not self.termination_criteria or any(c(job_run.termination) for c in self.termination_criteria)

    def matches_jobs(self, job_run):
        return not self.jobs or job_run.job_id in self.jobs

    def __call__(self, job_run):
        return self.matches(job_run)

    def matches(self, job_run):
        """
        Args:
            job_run (JobInstance): Job instance to match.
        Returns:
            bool: Whether the provided job instance matches all criteria.
        """
        return self.matches_metadata(job_run) \
            and self.matches_interval(job_run) \
            and self.match_phases(job_run) \
            and self.matches_termination(job_run) \
            and self.matches_jobs(job_run)

    def __bool__(self):
        return (bool(self.metadata_criteria)
                or bool(self.interval_criteria)
                or bool(self.phase_criteria)
                or bool(self.termination_criteria)
                or bool(self.jobs))

    def __repr__(self):
        return (f"{self.__class__.__name__}("
                f"{self.metadata_criteria=}, "
                f"{self.interval_criteria=}, "
                f"{self.phase_criteria=}, "
                f"{self.termination_criteria=}, "
                f"{self.jobs=})")
