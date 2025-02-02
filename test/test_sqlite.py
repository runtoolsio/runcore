from datetime import datetime as dt

import pytest

from runtools.runcore.criteria import LifecycleCriterion, JobRunCriteria, \
    parse_criteria
from runtools.runcore.db import sqlite
from runtools.runcore.run import TerminationStatus
from runtools.runcore.test.job import job_run
from runtools.runcore.util import parse_iso8601_duration, MatchingStrategy


@pytest.fixture
def sut():
    with sqlite.create(':memory:') as db:
        yield db


def test_store_and_fetch(sut):
    test_run = job_run('j1', term_status=TerminationStatus.FAILED)
    sut.store_job_runs(test_run)
    jobs = sut.read_history_runs()

    assert test_run == jobs[0]


def test_last(sut):
    sut.store_job_runs(
        job_run('j1', 'r1-1', offset_min=1),
        job_run('j2', 'r2-1', offset_min=2),
        job_run('j1', 'r1-2', offset_min=3),
        job_run('j3', 'r3-1', offset_min=4),
        job_run('j2', 'r2-2', offset_min=5))

    jobs = sut.read_history_runs(last=True)
    assert len(jobs) == 3
    assert [job.run_id for job in jobs] == ['r1-2', 'r3-1', 'r2-2']


def test_sort(sut):
    sut.store_job_runs(job_run('j1'), job_run('j2', offset_min=1), job_run('j3', offset_min=-1))

    jobs = sut.read_history_runs()
    assert jobs.job_ids == ['j3', 'j1', 'j2']

    jobs = sut.read_history_runs(asc=False)
    assert jobs.job_ids == ['j2', 'j1', 'j3']


def test_limit(sut):
    sut.store_job_runs(job_run('1'), job_run('2', offset_min=1), job_run('3', offset_min=-1))

    jobs = sut.read_history_runs(limit=1)
    assert len(jobs) == 1
    assert jobs[0].job_id == '3'


def test_offset(sut):
    sut.store_job_runs(job_run('1'), job_run('2', offset_min=1), job_run('3', offset_min=-1))

    jobs = sut.read_history_runs(offset=2)
    assert len(jobs) == 1
    assert jobs[0].job_id == '2'


def test_job_id_match(sut):
    sut.store_job_runs(job_run('j1', 'i1'), job_run('j12', 'i12'), job_run('j11', 'i11'), job_run('j111', 'i111'), job_run('j121', 'i121'))

    assert len(sut.read_history_runs(parse_criteria('j1'))) == 1
    assert len(sut.read_history_runs(parse_criteria('j1@'))) == 1
    assert len(sut.read_history_runs(parse_criteria('j1@i1'))) == 1
    assert len(sut.read_history_runs(parse_criteria('@i1'))) == 1
    assert len(sut.read_history_runs(parse_criteria('i1'))) == 1

    assert len(sut.read_history_runs(parse_criteria('j1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_history_runs(parse_criteria('j1@', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_history_runs(parse_criteria('j1@i1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_history_runs(parse_criteria('@i1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_history_runs(parse_criteria('i1', MatchingStrategy.PARTIAL))) == 5

    assert len(sut.read_history_runs(parse_criteria('j1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_history_runs(parse_criteria('j1?1@', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_history_runs(parse_criteria('j1?1@i1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_history_runs(parse_criteria('@i1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_history_runs(parse_criteria('i1?1', MatchingStrategy.FN_MATCH))) == 2


def test_cleanup(sut):
    sut.store_job_runs(job_run('j1', offset_min=-120), job_run('j2'), job_run('j3', offset_min=-240), job_run('j4', offset_min=-10),
                       job_run('j5', offset_min=-60))

    sut.clean_up(1, parse_iso8601_duration('PT50S'))
    jobs = sut.read_history_runs()
    assert len(jobs) == 1
    assert jobs[0].job_id == 'j2'


def test_interval(sut):
    sut.store_job_runs(job_run('j1', created=dt(2023, 4, 23), completed=dt(2023, 4, 23)))
    sut.store_job_runs(job_run('j2', created=dt(2023, 4, 22), completed=dt(2023, 4, 22, 23, 59, 59)))
    sut.store_job_runs(job_run('j3', created=dt(2023, 4, 22), completed=dt(2023, 4, 22, 23, 59, 58)))

    ic = LifecycleCriterion(ended_from=dt(2023, 4, 23))
    jobs = sut.read_history_runs(JobRunCriteria(interval_criteria=ic))
    assert jobs.job_ids == ['j1']

    ic = LifecycleCriterion(ended_to=dt(2023, 4, 22, 23, 59, 59))
    jobs = sut.read_history_runs(JobRunCriteria(interval_criteria=ic))
    assert sorted(jobs.job_ids) == ['j2', 'j3']

    ic = LifecycleCriterion(ended_to=dt(2023, 4, 22, 23, 59, 59), ended_to_included=False)
    jobs = sut.read_history_runs(JobRunCriteria(interval_criteria=ic))
    assert jobs.job_ids == ['j3']

    ic = LifecycleCriterion(ended_from=dt(2023, 4, 22, 23, 59, 59), created_to=dt(2023, 4, 23))
    jobs = sut.read_history_runs(JobRunCriteria(interval_criteria=ic))
    assert sorted(jobs.job_ids) == ['j1', 'j2']
