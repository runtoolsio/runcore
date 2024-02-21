"""
This module provides various criteria objects used to match job instances or their parts.

TODO: Remove immutable properties
"""

import datetime
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Dict, Any, Set, Optional, TypeVar, Generic

from runtools.runcore.run import Outcome, Lifecycle, TerminationInfo, EntityRun, InstanceMetadata
from runtools.runcore.util import MatchingStrategy, and_, or_, parse, single_day_range, days_range, \
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
class InstanceMetadataCriterion(MatchCriteria[InstanceMetadata]):
    """
    This class specifies criteria for matching instance metadata.
    If all fields are empty, the matching strategy defaults to `MatchingStrategy.ALWAYS_TRUE`.

    Attributes:
        entity_id (str): The pattern for job ID matching. If empty, the field is ignored.
        run_id (str): The pattern for run ID matching. If empty, the field is ignored.
        match_both_ids (bool): If False, a match with either job_id or instance_id is sufficient. Default is True.
        strategy (MatchingStrategy): The strategy to use for matching. Default is `MatchingStrategy.EXACT`.
    """
    entity_id: str
    run_id: str = ''
    match_both_ids: bool = True
    strategy: MatchingStrategy = MatchingStrategy.EXACT

    @classmethod
    def all_match(cls):
        return cls('', '', False, MatchingStrategy.ALWAYS_TRUE)

    @classmethod
    def none_match(cls):
        return cls('', '', True, MatchingStrategy.ALWAYS_FALSE)

    @staticmethod
    def for_run(entity_run):
        """
        Creates a MetadataCriterion object that matches the provided entity run.

        Args:
            entity_run: The specific entity run to create a match for.

        Returns:
            InstanceMetadataCriterion: A criteria object that will match the given job instance.
        """
        return InstanceMetadataCriterion(entity_id=entity_run.metadata.entity_id, run_id=entity_run.metadata.run_id)

    @classmethod
    def parse_pattern(cls, pattern: str, strategy=MatchingStrategy.EXACT):
        """
        Parses the provided pattern and returns the corresponding JobRunIdMatchCriteria object.
        The pattern can contain the `@` token to denote `entity_id` and `instance_id` parts in this format:
        `{entity_id}@{instance_id}`. If the token is not included, then the pattern is matched against both IDs,
        and a match on any fields results in a positive match.

        For matching only `entity_id`, use the format: `{entity_id}@`
        For matching only `instance_id`, use the format: `@{instance_id}`

        Args:
            pattern (str): The pattern to parse. It can contain a job ID and instance ID separated by '@'.
            strategy (MatchingStrategy, optional): The strategy to use for matching. Default is `MatchingStrategy.EXACT`

        Returns:
            InstanceMetadataCriterion: A new IDMatchCriteria object with the parsed entity_id, instance_id, and strategy.
        """
        if "@" in pattern:
            entity_id, instance_id = pattern.split("@")
            match_both = True
        else:
            entity_id = instance_id = pattern
            match_both = False
        return cls(entity_id, instance_id, match_both, strategy)

    @classmethod
    def deserialize(cls, as_dict):
        return cls(as_dict['entity_id'], as_dict['run_id'], as_dict['match_both_ids'],
                   MatchingStrategy[as_dict['strategy'].upper()])

    def serialize(self):
        return {
            'entity_id': self.entity_id,
            'run_id': self.run_id,
            'match_both_ids': self.match_both_ids,
            'strategy': self.strategy.name.lower(),
        }

    def _op(self):
        return and_ if self.match_both_ids else or_

    def __call__(self, id_tuple):
        return self.matches(id_tuple)

    def matches(self, metadata):
        """
        The matching method. It can be also executed by calling this object (`__call__` delegates to this method).

        Args:
            metadata: A metadata to match

        Returns:
            bool: Whether the provided metadata matches this criteria
        """
        op = self._op()
        entity_id = metadata.entity_id
        run_id = metadata.run_id
        return op(not self.entity_id or self.strategy(entity_id, self.entity_id),
                  not self.run_id or self.strategy(run_id, self.run_id))

    def matches_run(self, entity_run):
        """
        Args:
            entity_run: Job instance to be matched

        Returns:
            bool: Whether the provided job instance matches this criteria
        """
        return self.matches(entity_run.metadata)


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

    def matches(self, lifecycle):
        return self.created_range(lifecycle.created_at) and not self.ended_range or self.ended_range(lifecycle.ended_at)


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


def parse_criteria(pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT):
    return EntityRunCriteria.from_instance_pattern(pattern, strategy)


class EntityRunCriteria(MatchCriteria[EntityRun]):
    """
    This object aggregates various criteria for querying and matching job instances.
    An instance must meet all the provided criteria to be considered a match.

    Properties:
        jobs (List[Job]):
            A list of specific job IDs for matching.
            An instance matches if its job ID is in this list.
        job_run_id_criteria (List[EntityRunIdCriterion]):
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

    def __init__(self, *, jobs=None, metadata_criteria=None, interval_criteria=None, termination_criteria=None):
        self.jobs = to_list(jobs) or []
        self.metadata_criteria = to_list(metadata_criteria) or []
        self.interval_criteria = to_list(interval_criteria) or []
        self.termination_criteria = to_list(termination_criteria) or []

    @classmethod
    def all(cls):
        return cls(metadata_criteria=InstanceMetadataCriterion.all_match())

    @classmethod
    def deserialize(cls, as_dict):
        new = cls()
        new.jobs = as_dict.get('jobs', [])
        new.metadata_criteria = [InstanceMetadataCriterion.deserialize(c) for c in as_dict.get('metadata_criteria', ())]
        new.interval_criteria = [LifecycleCriterion.deserialize(c) for c in as_dict.get('interval_criteria', ())]
        new.termination_criteria = [TerminationCriterion.deserialize(c) for c in
                                    as_dict.get('termination_criteria', ())]
        return new

    def serialize(self):
        return {
            'jobs': self.jobs,
            'metadata_criteria': [c.serialize() for c in self.metadata_criteria],
            'interval_criteria': [c.serialize() for c in self.interval_criteria],
            'state_criteria': [c.serialize() for c in self.termination_criteria],
        }

    @classmethod
    def from_instance_pattern(cls, pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT):
        new = cls()
        new += InstanceMetadataCriterion.parse_pattern(pattern, strategy)
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
            case TerminationCriterion():
                self.termination_criteria.append(criterion)
            case _:
                raise ValueError("Invalid criterion type")

        return self

    def matches_metadata(self, entity_run):
        return not self.metadata_criteria or any(c(entity_run.metadata) for c in self.metadata_criteria)

    def matches_interval(self, entity_run):
        return not self.interval_criteria or any(c(entity_run.run.lifecycle) for c in self.interval_criteria)

    def matches_termination(self, entity_run):
        return not self.termination_criteria or any(c(entity_run.run.termination) for c in self.termination_criteria)

    def matches_jobs(self, job_run):
        return not self.jobs or job_run.entity_id in self.jobs

    def __call__(self, entity_run):
        return self.matches(entity_run)

    def matches(self, entity_run):
        """
        Args:
            entity_run (JobInstance): Job instance to match.
        Returns:
            bool: Whether the provided job instance matches all criteria.
        """
        return self.matches_metadata(entity_run) \
            and self.matches_interval(entity_run) \
            and self.matches_termination(entity_run) \
            and self.matches_jobs(entity_run)

    def __bool__(self):
        return (bool(self.metadata_criteria)
                or bool(self.interval_criteria)
                or bool(self.termination_criteria)
                or bool(self.jobs))

    def __repr__(self):
        return (f"{self.__class__.__name__}("
                f"{self.metadata_criteria=}, "
                f"{self.interval_criteria=}, "
                f"{self.termination_criteria=}, "
                f"{self.jobs=})")
