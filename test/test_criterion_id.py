from operator import eq

from runtoolsio.runcore.criteria import InstanceMetadataCriterion
from runtoolsio.runcore.run import JobInstanceMetadata


def job_run_id(job_id, run_id):
    return JobInstanceMetadata(job_id, run_id, '', {}, {})


def test_full_match():
    pattern = 'job_id@instance_id'
    sut = InstanceMetadataCriterion.parse_pattern(pattern, strategy=eq)

    assert sut.matches(job_run_id('job_id', 'instance_id'))
    assert not sut.matches(job_run_id('job_id', 'instance'))
    assert not sut.matches(job_run_id('job', 'instance_id'))


def test_match():
    pattern = 'identifier'
    sut = InstanceMetadataCriterion.parse_pattern(pattern, strategy=eq)

    assert sut.matches(job_run_id('identifier', 'any_instance_id'))
    assert sut.matches(job_run_id('any_job_id', 'identifier'))
    assert not sut.matches(job_run_id('job', 'any_instance_id'))
    assert not sut.matches(job_run_id('any_job_id', 'instance'))


def test_individual_id_match():
    job_id_pattern = 'job_id@'
    instance_id_pattern = '@instance_id'

    sut_job_id = InstanceMetadataCriterion.parse_pattern(job_id_pattern, strategy=eq)
    sut_instance_id = InstanceMetadataCriterion.parse_pattern(instance_id_pattern, strategy=eq)

    assert sut_job_id.matches(job_run_id('job_id', 'any_instance_id'))
    assert sut_instance_id.matches(job_run_id('any_job_id', 'instance_id'))
    assert not sut_job_id.matches(job_run_id('any_job_id', 'job_id'))
    assert not sut_instance_id.matches(job_run_id('instance_id', 'any_instance_id'))
