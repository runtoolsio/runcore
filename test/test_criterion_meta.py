from operator import eq

from runtools.runcore.criteria import InstanceMetadataCriterion
from runtools.runcore.job import JobInstanceMetadata
from runtools.runcore.util import MatchingStrategy


def meta(job_id, run_id, instance_id=''):
    return JobInstanceMetadata(job_id, run_id, instance_id, {}, {})


def test_job_run_pattern_match():
    pattern = 'job_id@run_id'
    sut = InstanceMetadataCriterion.parse_pattern(pattern, strategy=eq)

    assert sut.matches(meta('job_id', 'run_id'))
    assert not sut.matches(meta('job_id', 'run'))
    assert not sut.matches(meta('job', 'run_id'))


def test_single_value_pattern_match():
    pattern = 'identifier'
    sut = InstanceMetadataCriterion.parse_pattern(pattern, strategy=eq)

    assert sut.matches(meta('identifier', 'any_run_id'))
    assert sut.matches(meta('any_job_id', 'identifier'))
    assert sut.matches(meta('any_job_id', 'any_run_id', 'identifier'))
    assert not sut.matches(meta('job', 'any_run_id'))
    assert not sut.matches(meta('any_job_id', 'run'))


def test_job_or_run_pattern_match():
    job_id_pattern = 'job_id@'
    instance_id_pattern = '@run_id'

    sut_job_id = InstanceMetadataCriterion.parse_pattern(job_id_pattern, strategy=eq)
    sut_instance_id = InstanceMetadataCriterion.parse_pattern(instance_id_pattern, strategy=eq)

    assert sut_job_id.matches(meta('job_id', 'any_run_id'))
    assert sut_instance_id.matches(meta('any_job_id', 'run_id'))
    assert not sut_job_id.matches(meta('any_job_id', 'job_id'))
    assert not sut_instance_id.matches(meta('run_id', 'any_run_id'))


def test_only_instance_id():
    sut = InstanceMetadataCriterion.parse_pattern(':inst_id', strategy=eq)

    assert sut.matches(meta('job_id', 'run_id', 'inst_id'))
    assert not sut.matches(meta('inst_id', 'inst_id', 'different_id'))


def test_negate():
    sut = InstanceMetadataCriterion.parse_pattern(':!inst_id', strategy=MatchingStrategy.FN_MATCH)

    assert not sut.matches(meta('job_id', 'run_id', 'inst_id'))
    assert sut.matches(meta('job_id', 'run_id', 'any_other'))

    sut = InstanceMetadataCriterion.parse_pattern('job_id@!run_id', strategy=MatchingStrategy.FN_MATCH)
    assert not sut.matches(meta('job_id', 'run_id'))
    assert sut.matches(meta('job_id', 'r_id'))

    sut = InstanceMetadataCriterion.parse_pattern('!job_id@!run_id', strategy=MatchingStrategy.FN_MATCH)
    assert not sut.matches(meta('job_id', 'run_id'))
    assert not sut.matches(meta('job_id', 'r_id'))
    assert not sut.matches(meta('j_id', 'run_id'))
    assert sut.matches(meta('j_id', 'r_id'))
