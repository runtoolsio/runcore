from operator import eq

from runtools.runcore.criteria import MetadataCriterion
from runtools.runcore.job import JobInstanceMetadata, InstanceID
from runtools.runcore.util import MatchingStrategy


def meta(job_id, run_id, user_params=None):
    instance_id = InstanceID(job_id, run_id)
    return JobInstanceMetadata(instance_id, user_params or {})


def test_job_run_pattern_match():
    pattern = 'job_id@run_id'
    sut = MetadataCriterion.parse_pattern(pattern, strategy=eq)

    assert sut.matches(meta('job_id', 'run_id'))
    assert not sut.matches(meta('job_id', 'run'))
    assert not sut.matches(meta('job', 'run_id'))


def test_single_value_pattern_match():
    pattern = 'identifier'
    sut = MetadataCriterion.parse_pattern(pattern, strategy=eq)

    assert sut.matches(meta('identifier', 'any_run_id'))
    assert sut.matches(meta('any_job_id', 'identifier'))
    assert not sut.matches(meta('job', 'any_run_id'))
    assert not sut.matches(meta('any_job_id', 'run'))


def test_job_or_run_pattern_match():
    job_id_pattern = 'job_id@'
    run_id_pattern = '@run_id'

    sut_job_id = MetadataCriterion.parse_pattern(job_id_pattern, strategy=eq)
    sut_run_id = MetadataCriterion.parse_pattern(run_id_pattern, strategy=eq)

    assert sut_job_id.matches(meta('job_id', 'any_run_id'))
    assert sut_run_id.matches(meta('any_job_id', 'run_id'))
    assert not sut_job_id.matches(meta('any_job_id', 'job_id'))
    assert not sut_run_id.matches(meta('run_id', 'any_run_id'))


def test_negate():
    sut = MetadataCriterion.parse_pattern('job_id@!run_id', strategy=MatchingStrategy.FN_MATCH)
    assert not sut.matches(meta('job_id', 'run_id'))
    assert sut.matches(meta('job_id', 'r_id'))

    sut = MetadataCriterion.parse_pattern('!job_id@!run_id', strategy=MatchingStrategy.FN_MATCH)
    assert not sut.matches(meta('job_id', 'run_id'))
    assert not sut.matches(meta('job_id', 'r_id'))
    assert not sut.matches(meta('j_id', 'run_id'))
    assert sut.matches(meta('j_id', 'r_id'))


def test_all_except():
    """Test that all_except correctly matches everything except the specified instance."""
    # Create a criterion that excludes job1@run1
    instance_id = InstanceID("job1", "run1")
    sut = MetadataCriterion.all_except(instance_id)

    # Test that the excluded instance doesn't match
    assert not sut.matches(meta("job1", "run1"))

    # Test that different instances match
    assert sut.matches(meta("job1", "run2"))  # Same job_id, different run_id
    assert sut.matches(meta("job2", "run1"))  # Different job_id, same run_id
    assert sut.matches(meta("job2", "run2"))  # Both different

    # Verify that the negation syntax is used correctly
    assert sut.job_id.startswith("!")
    assert sut.run_id.startswith("!")

    # Verify match_any_field is false, requiring both conditions to match
    assert sut.match_any_field
