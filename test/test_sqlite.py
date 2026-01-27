from datetime import datetime as dt

import pytest

from datetime import timedelta

from runtools.runcore.criteria import JobRunCriteria, LifecycleCriterion, TerminationCriterion
from runtools.runcore.db import sqlite
from runtools.runcore.run import TerminationStatus, Outcome
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.util import parse_iso8601_duration, MatchingStrategy
from runtools.runcore.util.dt import TimeRange

parse = JobRunCriteria.parse

@pytest.fixture
def sut():
    with sqlite.create('test_env', database=':memory:') as db:
        yield db


def test_store_and_fetch(sut):
    test_run = fake_job_run('j1', term_status=TerminationStatus.FAILED)
    sut.store_job_runs(test_run)
    jobs = sut.read_history_runs()

    assert test_run == jobs[0]


def test_last(sut):
    sut.store_job_runs(
        fake_job_run('j1', 'r1-1', offset_min=1),
        fake_job_run('j2', 'r2-1', offset_min=2),
        fake_job_run('j1', 'r1-2', offset_min=3),
        fake_job_run('j3', 'r3-1', offset_min=4),
        fake_job_run('j2', 'r2-2', offset_min=5))

    jobs = sut.read_history_runs(last=True)
    assert len(jobs) == 3
    assert [job.run_id for job in jobs] == ['r1-2', 'r3-1', 'r2-2']


def test_sort(sut):
    sut.store_job_runs(fake_job_run('j1'), fake_job_run('j2', offset_min=1), fake_job_run('j3', offset_min=-1))

    jobs = sut.read_history_runs()
    assert [j.job_id for j in jobs] == ['j3', 'j1', 'j2']

    jobs = sut.read_history_runs(asc=False)
    assert [j.job_id for j in jobs] == ['j2', 'j1', 'j3']


def test_limit(sut):
    sut.store_job_runs(fake_job_run('1'), fake_job_run('2', offset_min=1), fake_job_run('3', offset_min=-1))

    jobs = sut.read_history_runs(limit=1)
    assert len(jobs) == 1
    assert jobs[0].job_id == '3'


def test_offset(sut):
    sut.store_job_runs(fake_job_run('1'), fake_job_run('2', offset_min=1), fake_job_run('3', offset_min=-1))

    jobs = sut.read_history_runs(offset=2)
    assert len(jobs) == 1
    assert jobs[0].job_id == '2'


def test_job_id_match(sut):
    sut.store_job_runs(fake_job_run('j1', 'i1'), fake_job_run('j12', 'i12'), fake_job_run('j11', 'i11'),
                       fake_job_run('j111', 'i111'), fake_job_run('j121', 'i121'))

    assert len(sut.read_history_runs(parse('j1'))) == 1
    assert len(sut.read_history_runs(parse('j1@'))) == 1
    assert len(sut.read_history_runs(parse('j1@i1'))) == 1
    assert len(sut.read_history_runs(parse('@i1'))) == 1
    assert len(sut.read_history_runs(parse('i1'))) == 1

    assert len(sut.read_history_runs(parse('j1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_history_runs(parse('j1@', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_history_runs(parse('j1@i1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_history_runs(parse('@i1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_history_runs(parse('i1', MatchingStrategy.PARTIAL))) == 5

    assert len(sut.read_history_runs(parse('j1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_history_runs(parse('j1?1@', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_history_runs(parse('j1?1@i1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_history_runs(parse('@i1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_history_runs(parse('i1?1', MatchingStrategy.FN_MATCH))) == 2


def test_cleanup(sut):
    sut.store_job_runs(fake_job_run('j1', offset_min=-120), fake_job_run('j2'), fake_job_run('j3', offset_min=-240),
                       fake_job_run('j4', offset_min=-10),
                       fake_job_run('j5', offset_min=-60))

    sut.clean_up(1, parse_iso8601_duration('PT50S'))
    jobs = sut.read_history_runs()
    assert len(jobs) == 1
    assert jobs[0].job_id == 'j2'


def test_interval(sut):
    sut.store_job_runs(fake_job_run('j1', created_at=dt(2023, 4, 23), ended_at=dt(2023, 4, 23)))
    sut.store_job_runs(fake_job_run('j2', created_at=dt(2023, 4, 22), ended_at=dt(2023, 4, 22, 23, 59, 59)))
    sut.store_job_runs(fake_job_run('j3', created_at=dt(2023, 4, 22), ended_at=dt(2023, 4, 22, 23, 59, 58)))

    # Test ended_from
    jobs = sut.read_history_runs(JobRunCriteria().created(since=dt(2023, 4, 23)))
    assert [j.job_id for j in jobs] == ['j1']

    # Test ended_to inclusive
    jobs = sut.read_history_runs(JobRunCriteria().ended(until=dt(2023, 4, 22, 23, 59, 59), until_incl=True))
    assert sorted([j.job_id for j in jobs]) == ['j2', 'j3']

    # Test ended_to exclusive
    jobs = sut.read_history_runs(JobRunCriteria().ended(until=dt(2023, 4, 22, 23, 59, 59)))
    assert [j.job_id for j in jobs] == ['j3']

    # Test combined ended_from (incl) and created_to
    jobs = sut.read_history_runs(JobRunCriteria()
                                 .created(until=dt(2023, 4, 23), until_incl=True)
                                 .ended(since=dt(2023, 4, 22, 23, 59, 59)))
    assert sorted([j.job_id for j in jobs]) == ['j1', 'j2']


def test_termination_status(sut):
    sut.store_job_runs(
        fake_job_run('j1', term_status=TerminationStatus.COMPLETED),
        fake_job_run('j2', term_status=TerminationStatus.FAILED),
        fake_job_run('j3', term_status=TerminationStatus.COMPLETED))

    criteria = JobRunCriteria().add(LifecycleCriterion(termination=TerminationCriterion(status=TerminationStatus.FAILED)))
    jobs = sut.read_history_runs(criteria)
    assert [j.job_id for j in jobs] == ['j2']


def test_termination_outcome(sut):
    sut.store_job_runs(
        fake_job_run('j1', term_status=TerminationStatus.COMPLETED),
        fake_job_run('j2', term_status=TerminationStatus.FAILED),
        fake_job_run('j3', term_status=TerminationStatus.STOPPED))

    criteria = JobRunCriteria().add(LifecycleCriterion(termination=TerminationCriterion(outcome=Outcome.FAULT)))
    jobs = sut.read_history_runs(criteria)
    assert [j.job_id for j in jobs] == ['j2']


def test_total_run_time(sut):
    sut.store_job_runs(
        fake_job_run('j1', created_at=dt(2023, 1, 1, 12, 0), ended_at=dt(2023, 1, 1, 12, 1)),   # 1 min
        fake_job_run('j2', created_at=dt(2023, 1, 1, 12, 0), ended_at=dt(2023, 1, 1, 12, 10)),  # 10 min
        fake_job_run('j3', created_at=dt(2023, 1, 1, 12, 0), ended_at=dt(2023, 1, 1, 12, 30)))  # 30 min

    # Filter runs between 5 and 15 minutes
    criteria = JobRunCriteria().add(LifecycleCriterion(total_run_time=TimeRange(min=timedelta(minutes=5), max=timedelta(minutes=15))))
    jobs = sut.read_history_runs(criteria)
    assert [j.job_id for j in jobs] == ['j2']
