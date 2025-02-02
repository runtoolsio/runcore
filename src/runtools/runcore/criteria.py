"""
This module provides various criteria objects used to match job instances or their parts.

TODO: Remove immutable properties
"""

from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Dict, Any, Optional, TypeVar, Generic

from runtools.runcore.job import JobInstanceMetadata, JobRun
from runtools.runcore.run import Outcome, TerminationInfo, RunState, \
    PhaseDetail, TerminationStatus
from runtools.runcore.util import MatchingStrategy, to_list, DateTimeRange

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
        instance_id (str): The pattern for instance ID matching. If empty, the field is ignored.
        match_any_id (bool): If True, matches if any provided ID matches. If False, all provided IDs must match.
        strategy (MatchingStrategy): The strategy to use for matching. Default is `MatchingStrategy.EXACT`.
    """
    job_id: str = ''
    run_id: str = ''
    instance_id: str = ''
    match_any_id: bool = False
    strategy: MatchingStrategy = MatchingStrategy.EXACT

    @classmethod
    def all_match(cls) -> 'MetadataCriterion':
        """Creates a criterion that matches all instances."""
        return cls('', '', '', True, MatchingStrategy.ALWAYS_TRUE)

    @classmethod
    def none_match(cls) -> 'MetadataCriterion':
        """Creates a criterion that matches no instances."""
        return cls('', '', '', False, MatchingStrategy.ALWAYS_FALSE)

    @staticmethod
    def exact_match(job_run: 'JobRun') -> 'MetadataCriterion':
        """
        Creates a criterion that matches a specific job run by its instance ID.

        Args:
            job_run: The specific job run to create a match for.

        Returns:
            MetadataCriterion: A criteria object that will match the given job instance.
        """
        return MetadataCriterion.instance_match(job_run.metadata.instance_id)

    @classmethod
    def all_except(cls, job_run: 'JobRun') -> 'MetadataCriterion':
        """
        Creates a criterion that matches any job run except the specified one.

        Args:
            job_run: The specific job run to exclude from matching.

        Returns:
            MetadataCriterion: A criteria object that will match any job instance
            except the given one.
        """
        return MetadataCriterion.instance_match(negate_id(job_run.metadata.instance_id))

    @staticmethod
    def instance_match(instance_id: str) -> 'MetadataCriterion':
        """
        Creates a criterion that matches a specific instance.

        Args:
            instance_id: The specific instance to create a match for.

        Returns:
            MetadataCriterion: A criteria object that will match the given job instance.
        """
        return MetadataCriterion(instance_id=instance_id)

    @classmethod
    def parse_pattern(cls, pattern: str,
                      strategy: MatchingStrategy = MatchingStrategy.EXACT) -> 'MetadataCriterion':
        """
        Parses the provided pattern and returns the corresponding metadata criterion.

        The pattern can be in one of these formats:
        - ":instance_id" - Match instance ID only
        - "job_id@run_id" - Match specific job ID and run ID combination
        - Any other text - Match against all IDs with match_any_id=True

        Args:
            pattern: The pattern to parse. Can be empty, an instance ID with ':' prefix,
                    a job/run combo with '@', or plain text.
            strategy: The strategy to use for matching. Default is `MatchingStrategy.EXACT`

        Returns:
            A new criteria object configured according to the pattern format.
        """
        if not pattern:
            return cls.all_match()

        # Handle instance ID pattern
        if pattern.startswith(':'):
            return cls('', '', pattern[1:], False, strategy)

        # Handle job@run pattern
        if '@' in pattern:
            job_id, run_id = pattern.split('@', 1)
            return cls(job_id, run_id, '', False, strategy)

        # Handle plain text (match against any ID)
        return cls(pattern, pattern, pattern, True, strategy)

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
        instance_id_match = self._matches_id(metadata.instance_id, self.instance_id)

        if self.match_any_id:
            # Match if any of the provided IDs match (OR condition)
            return (not self.job_id or job_id_match) or \
                (not self.run_id or run_id_match) or \
                (not self.instance_id or instance_id_match)
        else:
            # Match only if all provided IDs match (AND condition)
            return (not self.job_id or job_id_match) and \
                (not self.run_id or run_id_match) and \
                (not self.instance_id or instance_id_match)

    def serialize(self) -> Dict[str, Any]:
        """Serializes the criterion to a dictionary."""
        return {
            'job_id': self.job_id,
            'run_id': self.run_id,
            'instance_id': self.instance_id,
            'match_any_id': self.match_any_id,
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
        return cls(
            as_dict['job_id'],
            as_dict['run_id'],
            as_dict.get('instance_id', ''),  # For backward compatibility
            as_dict.get('match_any_id', False),  # For backward compatibility
            MatchingStrategy[as_dict['strategy'].upper()]
        )

    def __str__(self) -> str:
        fields = []
        if self.job_id:
            fields.append(f"job_id='{self.job_id}'")
        if self.run_id:
            fields.append(f"run_id='{self.run_id}'")
        if self.instance_id:
            fields.append(f"instance_id='{self.instance_id}'")
        if self.match_any_id:
            fields.append("match_any_id=True")
        if self.strategy != MatchingStrategy.EXACT:
            fields.append(f"strategy={self.strategy.name}")
        return f"[{', '.join(fields)}]" if fields else "[]"


def compound_instance_filter(metadata_criteria):
    def match(metadata):
        return not metadata_criteria or any(criteria(metadata) for criteria in metadata_criteria)

    return match


@dataclass
class TerminationCriteria(MatchCriteria[TerminationInfo]):
    """
    Criteria for matching termination information.
    """
    status: Optional[TerminationStatus] = None
    outcome: Outcome = Outcome.ANY
    ended_range: DateTimeRange = None

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> 'TerminationCriteria':
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
        return f"[{', '.join(fields)}]" if fields else "[]"


@dataclass
class PhaseCriterion(MatchCriteria[PhaseDetail]):
    """
    Criteria for matching phase details, incorporating phase-specific and temporal criteria.
    """
    phase_type: Optional[str] = None
    phase_id: Optional[str] = None
    run_state: Optional[RunState] = None
    phase_name: Optional[str] = None
    attributes: Optional[Dict[str, Any]] = None
    created_range: Optional[DateTimeRange] = None
    started_range: Optional[DateTimeRange] = None
    ended_range: Optional[DateTimeRange] = None
    termination: Optional[TerminationCriteria] = None

    def __post_init__(self):
        if self.attributes is None:
            self.attributes = {}

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> 'PhaseCriterion':
        return cls(
            phase_type=data.get('phase_type'),
            phase_id=data.get('phase_id'),
            run_state=RunState[data['run_state']] if data.get('run_state') else None,
            phase_name=data.get('phase_name'),
            attributes=data.get('attributes'),
            created_range=DateTimeRange.deserialize(data['created_range']) if data.get('created_range') else None,
            started_range=DateTimeRange.deserialize(data['started_range']) if data.get('started_range') else None,
            ended_range=DateTimeRange.deserialize(data['ended_range']) if data.get('ended_range') else None,
            termination=TerminationCriteria.deserialize(data['termination']) if data.get('termination') else None
        )

    def serialize(self) -> Dict[str, Any]:
        data = {
            'phase_type': self.phase_type,
            'phase_id': self.phase_id,
            'run_state': self.run_state.name if self.run_state else None,
            'phase_name': self.phase_name,
            'attributes': self.attributes
        }
        if self.created_range:
            data['created_range'] = self.created_range.serialize()
        if self.started_range:
            data['started_range'] = self.started_range.serialize()
        if self.ended_range:
            data['ended_range'] = self.ended_range.serialize()
        if self.termination:
            data['termination'] = self.termination.serialize()
        return data

    def matches_phase(self, phase_detail: PhaseDetail) -> bool:
        """Check if a single phase matches this criterion."""
        if self.phase_type and phase_detail.phase_type != self.phase_type:
            return False

        if self.phase_id and phase_detail.phase_id != self.phase_id:
            return False

        if self.run_state and phase_detail.run_state != self.run_state:
            return False

        if self.phase_name and phase_detail.phase_name != self.phase_name:
            return False

        if self.attributes:
            if not phase_detail.attributes:
                return False
            for key, value in self.attributes.items():
                if phase_detail.attributes.get(key) != value:
                    return False

        if self.created_range and not self.created_range(phase_detail.created_at):
            return False

        if self.started_range and (not phase_detail.started_at or not self.started_range(phase_detail.started_at)):
            return False

        if not phase_detail.termination:
            return not (self.ended_range or self.termination)

        if self.ended_range and not self.ended_range(phase_detail.termination.terminated_at):
            return False

        if self.termination and not self.termination(phase_detail.termination):
            return False

        return True

    def matches(self, phase_detail: PhaseDetail) -> bool:
        """Check if phase or any of its children match this criterion."""
        if self.matches_phase(phase_detail):
            return True

        return any(self.matches(child) for child in phase_detail.children)

    def __bool__(self) -> bool:
        return bool(self.phase_type or self.phase_id or self.run_state or
                    self.phase_name or self.attributes or self.created_range or
                    self.started_range or self.ended_range or self.termination)

    def __str__(self) -> str:
        fields = []
        if self.phase_type:
            fields.append(f"type='{self.phase_type}'")
        if self.phase_id:
            fields.append(f"id='{self.phase_id}'")
        if self.run_state:
            fields.append(f"state={self.run_state.name}")
        if self.phase_name:
            fields.append(f"name='{self.phase_name}'")
        if self.attributes:
            fields.append(f"attrs={self.attributes}")
        if self.created_range:
            fields.append(f"created{self.created_range}")
        if self.started_range:
            fields.append(f"started{self.started_range}")
        if self.ended_range:
            fields.append(f"ended{self.ended_range}")
        if self.termination:
            fields.append(f"termination{self.termination}")
        return f"[{', '.join(fields)}]" if fields else "[]"


def parse_criteria(pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT) -> 'JobRunCriteria':
    return JobRunCriteria.parse(pattern, strategy)


class JobRunCriteria(MatchCriteria[JobRun]):
    """
    Criteria for querying and matching job instances.
    """

    def __init__(self, *, jobs=None, metadata_criteria=None, phase_criteria=None, termination_criteria=None):
        self.jobs = to_list(jobs) or []
        self.metadata_criteria = to_list(metadata_criteria)
        self.phase_criteria = to_list(phase_criteria)
        self.termination_criteria = to_list(termination_criteria)

    @classmethod
    def all(cls):
        return cls(metadata_criteria=MetadataCriterion.all_match())

    @classmethod
    def deserialize(cls, as_dict):
        new = cls()
        new.jobs = as_dict.get('jobs', [])
        new.metadata_criteria = [MetadataCriterion.deserialize(c) for c in as_dict.get('metadata_criteria', ())]
        new.phase_criteria = [PhaseCriterion.deserialize(c) for c in as_dict.get('phase_criteria', ())]
        new.termination_criteria = [TerminationCriteria.deserialize(c) for c in as_dict.get('termination_criteria', ())]
        return new

    def serialize(self):
        return {
            'jobs': self.jobs,
            'metadata_criteria': [c.serialize() for c in self.metadata_criteria],
            'phase_criteria': [c.serialize() for c in self.phase_criteria],
            'termination_criteria': [c.serialize() for c in self.termination_criteria],
        }

    @classmethod
    def parse(cls, pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT):
        new = cls()
        new += MetadataCriterion.parse_pattern(pattern, strategy)
        return new

    @classmethod
    def job_match(cls, job_id, strategy: MatchingStrategy = MatchingStrategy.EXACT):
        new = cls()
        new += MetadataCriterion(job_id=job_id, strategy=strategy)
        return new

    @classmethod
    def exact_match(cls, job_run):
        new = cls()
        new += MetadataCriterion.exact_match(job_run)
        return new

    @classmethod
    def all_except(cls, job_run):
        new = cls()
        new += MetadataCriterion.all_except(job_run)
        return new

    @classmethod
    def instance_match(cls, instance_id):
        new = cls()
        new += MetadataCriterion.instance_match(instance_id)
        return new

    def __iadd__(self, criterion):
        return self.add(criterion)

    def add(self, criterion):
        match criterion:
            case str():
                self.jobs.append(criterion)
            case MetadataCriterion():
                self.metadata_criteria.append(criterion)
            case PhaseCriterion():
                self.phase_criteria.append(criterion)
            case TerminationCriteria():
                self.termination_criteria.append(criterion)
            case _:
                raise ValueError(f"Invalid criterion type: {type(criterion)}")
        return self

    def matches_metadata(self, job_run):
        return not self.metadata_criteria or any(c(job_run.metadata) for c in self.metadata_criteria)

    def match_phases(self, job_run):
        """Check if any phase in the job run matches any of the phase criteria."""
        return not self.phase_criteria or any(c.matches(job_run.root_phase) for c in self.phase_criteria)

    def matches_termination(self, job_run):
        """Check root phase termination against termination criteria."""
        return not self.termination_criteria or any(
            c(job_run.root_phase.termination) for c in self.termination_criteria)

    def matches_jobs(self, job_run):
        return not self.jobs or job_run.job_id in self.jobs

    def __call__(self, job_run):
        return self.matches(job_run)

    def matches(self, job_run):
        """Check if a job run matches all criteria."""
        return (self.matches_metadata(job_run) and
                self.match_phases(job_run) and
                self.matches_termination(job_run) and
                self.matches_jobs(job_run))

    def __bool__(self):
        return bool(self.metadata_criteria or
                    self.phase_criteria or
                    self.termination_criteria or
                    self.jobs)

    def __str__(self) -> str:
        parts = []
        if self.metadata_criteria:
            criteria_strs = [str(c) for c in self.metadata_criteria if bool(c)]
            if criteria_strs:
                parts.append(f"metadata={''.join(criteria_strs)}")
        if self.phase_criteria:
            criteria_strs = [str(c) for c in self.phase_criteria if bool(c)]
            if criteria_strs:
                parts.append(f"phase={''.join(criteria_strs)}")
        if self.termination_criteria:
            criteria_strs = [str(c) for c in self.termination_criteria if bool(c)]
            if criteria_strs:
                parts.append(f"termination={''.join(criteria_strs)}")
        if self.jobs:
            parts.append(f"jobs={self.jobs}")
        return f"{' | '.join(parts)}" if parts else ""
