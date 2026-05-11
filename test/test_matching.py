from runtools.runcore.job import InstanceID, JobInstanceMetadata
from runtools.runcore.matching import criteria, MetadataCriterion, TerminationCriterion
from runtools.runcore.run import TerminationStatus, TerminationInfo, Outcome

from runtools.runcore.util import utc_now, DateTimeRange


def _metadata(*tags):
    return JobInstanceMetadata(InstanceID('j', 'r', 1), tags=tags)


def test_metadata_criterion_tags_all_matches_when_all_present():
    c = MetadataCriterion(tags_all=('a', 'b'))
    assert c(_metadata('a', 'b', 'c'))
    assert c(_metadata('a', 'b'))


def test_metadata_criterion_tags_all_rejects_when_any_missing():
    c = MetadataCriterion(tags_all=('a', 'b'))
    assert not c(_metadata('a'))
    assert not c(_metadata('b', 'c'))
    assert not c(_metadata())


def test_metadata_criterion_tags_any_matches_when_one_present():
    c = MetadataCriterion(tags_any=('a', 'b'))
    assert c(_metadata('a'))
    assert c(_metadata('b'))
    assert c(_metadata('a', 'b'))


def test_metadata_criterion_tags_any_rejects_when_none_present():
    c = MetadataCriterion(tags_any=('a', 'b'))
    assert not c(_metadata('c'))
    assert not c(_metadata())


def test_metadata_criterion_combines_tags_all_and_tags_any():
    """tags_all AND tags_any — both rules apply within a single criterion."""
    c = MetadataCriterion(tags_all=('prod',), tags_any=('east', 'west'))
    assert c(_metadata('prod', 'east'))
    assert c(_metadata('prod', 'west', 'extra'))
    assert not c(_metadata('prod'))        # tags_any not satisfied
    assert not c(_metadata('east', 'west'))  # tags_all not satisfied


def test_metadata_criterion_tags_normalized_on_construction():
    """Raw user input is normalized at construction (lowercase, # stripped)."""
    c = MetadataCriterion(tags_all=('#Prod',), tags_any=('EAST',))
    assert c.tags_all == ('prod',)
    assert c.tags_any == ('east',)


def test_metadata_criterion_tags_serialize_round_trip():
    c = MetadataCriterion(tags_all=('a', 'b'), tags_any=('c',))
    restored = MetadataCriterion.deserialize(c.serialize())
    assert restored.tags_all == ('a', 'b')
    assert restored.tags_any == ('c',)


def test_metadata_criterion_no_tags_serialize_omits_keys():
    c = MetadataCriterion(job_id='j')
    d = c.serialize()
    assert 'tags_all' not in d
    assert 'tags_any' not in d


def test_interval_utc_conversion():
    c = criteria().created(*DateTimeRange.parse_to_utc(from_val='2023-11-10T09:00+02:00', to_val=None)).build()
    assert c.lifecycle_criteria[-1].created.since.hour == 7


def test_termination_criteria_by_outcome():
    matching = TerminationCriterion(outcome=Outcome.REJECTED)
    not_matching = TerminationCriterion(outcome=Outcome.ABORTED)

    tested_info = TerminationInfo(TerminationStatus.OVERLAP, utc_now())
    assert matching(tested_info)
    assert not not_matching(tested_info)


def test_termination_criteria_by_success():
    success_criterion = TerminationCriterion(success=True)
    nonsuccess_criterion = TerminationCriterion(success=False)

    completed_info = TerminationInfo(TerminationStatus.COMPLETED, utc_now())
    failed_info = TerminationInfo(TerminationStatus.FAILED, utc_now())
    aborted_info = TerminationInfo(TerminationStatus.STOPPED, utc_now())

    assert success_criterion(completed_info)
    assert not success_criterion(failed_info)
    assert not success_criterion(aborted_info)

    assert not nonsuccess_criterion(completed_info)
    assert nonsuccess_criterion(failed_info)
    assert nonsuccess_criterion(aborted_info)
