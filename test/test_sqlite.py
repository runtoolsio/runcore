from datetime import datetime as dt
from datetime import timedelta

import pytest

from runtools.runcore.db import sqlite, IncompatibleSchemaError
from runtools.runcore.db.sqlite import SCHEMA_VERSION
from runtools.runcore.job import DuplicateInstanceError, InstanceID
from runtools.runcore.matching import criteria, JobRunCriteria, LifecycleCriterion
from runtools.runcore.retention import RetentionPolicy
from runtools.runcore.run import TerminationStatus, Outcome
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.util import MatchingStrategy
from runtools.runcore.util.dt import TimeRange, DateTimeRange

parse = JobRunCriteria.parse


def _init_and_store(db, *job_runs):
    """Initialize and store job runs (two-phase persistence)."""
    for run in job_runs:
        iid = run.metadata.instance_id
        db.init_run(iid.job_id, iid.run_id, run.metadata.user_params)
    db.store_runs(*job_runs)

@pytest.fixture
def sut():
    with sqlite.create_memory('test_env') as db:
        yield db


def test_schema_version_set_on_creation():
    import sqlite3
    conn = sqlite3.connect(':memory:')
    db = sqlite.SQLite(lambda: conn)
    db.open()
    version = conn.execute('PRAGMA user_version').fetchone()[0]
    assert version == SCHEMA_VERSION
    db.close()


def test_schema_version_mismatch_raises():
    import sqlite3
    conn = sqlite3.connect(':memory:')
    conn.execute('PRAGMA user_version = 999')
    conn.execute('CREATE TABLE runs (job_id text, run_id text, ordinal integer NOT NULL DEFAULT 1, '
                 'user_params text, created timestamp, started timestamp, ended timestamp, exec_time real, '
                 'root_phase text, output_locations text, termination_status int, '
                 'faults text, status text, warnings int, misc text)')
    conn.commit()
    db = sqlite.SQLite(lambda: conn)
    with pytest.raises(IncompatibleSchemaError):
        db.open()


def test_store_and_fetch(sut):
    test_run = fake_job_run('j1', term_status=TerminationStatus.FAILED)
    _init_and_store(sut, test_run)
    jobs = sut.read_runs()

    assert test_run == jobs[0]


def test_last(sut):
    _init_and_store(sut,
        fake_job_run('j1', 'r1-1', offset_min=1),
        fake_job_run('j2', 'r2-1', offset_min=2),
        fake_job_run('j1', 'r1-2', offset_min=3),
        fake_job_run('j3', 'r3-1', offset_min=4),
        fake_job_run('j2', 'r2-2', offset_min=5))

    jobs = sut.read_runs(last=True)
    assert len(jobs) == 3
    assert [job.run_id for job in jobs] == ['r1-2', 'r3-1', 'r2-2']


def test_sort(sut):
    _init_and_store(sut, fake_job_run('j1'), fake_job_run('j2', offset_min=1), fake_job_run('j3', offset_min=-1))

    jobs = sut.read_runs()
    assert [j.job_id for j in jobs] == ['j3', 'j1', 'j2']

    jobs = sut.read_runs(asc=False)
    assert [j.job_id for j in jobs] == ['j2', 'j1', 'j3']


def test_limit(sut):
    _init_and_store(sut, fake_job_run('1'), fake_job_run('2', offset_min=1), fake_job_run('3', offset_min=-1))

    jobs = sut.read_runs(limit=1)
    assert len(jobs) == 1
    assert jobs[0].job_id == '3'


def test_offset(sut):
    _init_and_store(sut, fake_job_run('1'), fake_job_run('2', offset_min=1), fake_job_run('3', offset_min=-1))

    jobs = sut.read_runs(offset=2)
    assert len(jobs) == 1
    assert jobs[0].job_id == '2'


def test_job_id_match(sut):
    _init_and_store(sut, fake_job_run('j1', 'i1'), fake_job_run('j12', 'i12'), fake_job_run('j11', 'i11'),
                       fake_job_run('j111', 'i111'), fake_job_run('j121', 'i121'))

    assert len(sut.read_runs(parse('j1'))) == 1
    assert len(sut.read_runs(parse('j1@'))) == 1
    assert len(sut.read_runs(parse('j1@i1'))) == 1
    assert len(sut.read_runs(parse('@i1'))) == 1
    assert len(sut.read_runs(parse('i1'))) == 1

    assert len(sut.read_runs(parse('j1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_runs(parse('j1@', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_runs(parse('j1@i1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_runs(parse('@i1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_runs(parse('i1', MatchingStrategy.PARTIAL))) == 5

    assert len(sut.read_runs(parse('j1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_runs(parse('j1?1@', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_runs(parse('j1?1@i1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_runs(parse('@i1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_runs(parse('i1?1', MatchingStrategy.FN_MATCH))) == 2


def test_enforce_retention_per_job(sut):
    _init_and_store(sut,
        fake_job_run('j1', 'r1', offset_min=1),
        fake_job_run('j1', 'r2', offset_min=2),
        fake_job_run('j1', 'r3', offset_min=3),
        fake_job_run('j2', 'r4', offset_min=4))

    sut.enforce_retention('j1', RetentionPolicy(max_runs_per_job=2, max_runs_per_env=-1))
    jobs = sut.read_runs()
    assert len(jobs) == 3
    assert {j.run_id for j in jobs} == {'r2', 'r3', 'r4'}


def test_enforce_retention_per_env(sut):
    _init_and_store(sut,
        fake_job_run('j1', 'r1', offset_min=1),
        fake_job_run('j2', 'r2', offset_min=2),
        fake_job_run('j3', 'r3', offset_min=3),
        fake_job_run('j4', 'r4', offset_min=4),
        fake_job_run('j5', 'r5', offset_min=5))

    sut.enforce_retention('j5', RetentionPolicy(max_runs_per_job=-1, max_runs_per_env=2))
    jobs = sut.read_runs()
    assert len(jobs) == 2
    assert {j.run_id for j in jobs} == {'r4', 'r5'}


def test_enforce_retention_combined(sut):
    _init_and_store(sut,
        fake_job_run('j1', 'r1', offset_min=1),
        fake_job_run('j1', 'r2', offset_min=2),
        fake_job_run('j1', 'r3', offset_min=3),
        fake_job_run('j2', 'r4', offset_min=4))

    sut.enforce_retention('j1', RetentionPolicy(max_runs_per_job=1, max_runs_per_env=3))
    jobs = sut.read_runs()
    # Per-job prunes r1, r2 (keep r3 for j1); per-env keeps 3 most recent → r2, r3, r4
    # But r2 already deleted by per-job, so final: r3, r4
    assert len(jobs) == 2
    assert {j.run_id for j in jobs} == {'r3', 'r4'}


def test_interval(sut):
    _init_and_store(sut, fake_job_run('j1', created_at=dt(2023, 4, 23), ended_at=dt(2023, 4, 23)))
    _init_and_store(sut, fake_job_run('j2', created_at=dt(2023, 4, 22), ended_at=dt(2023, 4, 22, 23, 59, 59)))
    _init_and_store(sut, fake_job_run('j3', created_at=dt(2023, 4, 22), ended_at=dt(2023, 4, 22, 23, 59, 58)))

    # Test ended_from
    jobs = sut.read_runs(criteria().created(since=dt(2023, 4, 23)).build())
    assert [j.job_id for j in jobs] == ['j1']

    # Test ended_to inclusive
    jobs = sut.read_runs(criteria().ended(until=dt(2023, 4, 22, 23, 59, 59), until_incl=True).build())
    assert sorted([j.job_id for j in jobs]) == ['j2', 'j3']

    # Test ended_to exclusive
    jobs = sut.read_runs(criteria().ended(until=dt(2023, 4, 22, 23, 59, 59)).build())
    assert [j.job_id for j in jobs] == ['j3']

    # Test combined ended_from (incl) and created_to
    jobs = sut.read_runs(JobRunCriteria(lifecycle_criteria=(
        LifecycleCriterion(
            created=DateTimeRange(until=dt(2023, 4, 23), until_included=True),
            ended=DateTimeRange(since=dt(2023, 4, 22, 23, 59, 59)),
        ),
    )))
    assert sorted([j.job_id for j in jobs]) == ['j1', 'j2']


def test_termination_status(sut):
    _init_and_store(sut,
        fake_job_run('j1', term_status=TerminationStatus.COMPLETED),
        fake_job_run('j2', term_status=TerminationStatus.FAILED),
        fake_job_run('j3', term_status=TerminationStatus.COMPLETED))

    run_match = criteria().termination_status(TerminationStatus.FAILED).build()
    jobs = sut.read_runs(run_match)
    assert [j.job_id for j in jobs] == ['j2']


def test_termination_outcome(sut):
    _init_and_store(sut,
        fake_job_run('j1', term_status=TerminationStatus.COMPLETED),
        fake_job_run('j2', term_status=TerminationStatus.FAILED),
        fake_job_run('j3', term_status=TerminationStatus.STOPPED))

    run_match = criteria().termination_outcome(Outcome.FAULT).build()
    jobs = sut.read_runs(run_match)
    assert [j.job_id for j in jobs] == ['j2']


def test_total_run_time(sut):
    _init_and_store(sut,
        fake_job_run('j1', created_at=dt(2023, 1, 1, 12, 0), ended_at=dt(2023, 1, 1, 12, 1)),   # 1 min
        fake_job_run('j2', created_at=dt(2023, 1, 1, 12, 0), ended_at=dt(2023, 1, 1, 12, 10)),  # 10 min
        fake_job_run('j3', created_at=dt(2023, 1, 1, 12, 0), ended_at=dt(2023, 1, 1, 12, 30)))  # 30 min

    # Filter runs between 5 and 15 minutes
    run_match = JobRunCriteria(lifecycle_criteria=(LifecycleCriterion(total_run_time=TimeRange(min=timedelta(minutes=5), max=timedelta(minutes=15))),))
    jobs = sut.read_runs(run_match)
    assert [j.job_id for j in jobs] == ['j2']


# --- Two-phase persistence tests ---

def test_init_and_store(sut):
    run = fake_job_run('j1', 'r1')
    sut.init_run('j1', 'r1', {'name': 'value'})
    sut.store_runs(run)

    jobs = sut.read_runs()
    assert len(jobs) == 1
    assert run == jobs[0]


def test_init_duplicate_raises(sut):
    sut.init_run('j1', 'r1')

    with pytest.raises(DuplicateInstanceError):
        sut.init_run('j1', 'r1')


def test_init_only_not_in_history(sut):
    sut.init_run('j1', 'r1')

    assert sut.read_runs() == []
    assert sut.read_run_stats() == []


def test_store_without_init(sut):
    run = fake_job_run('j1', 'r1')
    sut.store_runs(run)

    assert sut.read_runs() == []


def test_last_with_init_only_row(sut):
    """last=True returns the latest completed run even when a newer init-only row exists."""
    _init_and_store(sut, fake_job_run('j1', 'r1', offset_min=1))
    sut.init_run('j1', 'r2')  # newer init-only row

    jobs = sut.read_runs(last=True)
    assert len(jobs) == 1
    assert jobs[0].run_id == 'r1'


# --- Ordinal tests ---

def test_different_ordinals_are_not_duplicates(sut):
    """Same logical run with different ordinals should coexist."""
    sut.init_run('j1', 'r1')
    sut.init_run('j1', 'r1', auto_increment=True)

    # Both init rows exist (not yet completed, so not in history)
    assert sut.read_runs() == []


def test_same_ordinal_is_duplicate(sut):
    sut.init_run('j1', 'r1')

    with pytest.raises(DuplicateInstanceError):
        sut.init_run('j1', 'r1')


def test_ordinal_round_trips_through_store(sut):
    """Auto-incremented ordinals survive the init → store → read cycle."""
    sut.init_run('j1', 'r1')  # ordinal 1
    sut.init_run('j1', 'r1', auto_increment=True)  # ordinal 2
    actual_id = sut.init_run('j1', 'r1', auto_increment=True)  # ordinal 3
    assert actual_id.ordinal == 3

    run = fake_job_run('j1', 'r1', ordinal=3)
    sut.store_runs(run)

    jobs = sut.read_runs()
    assert len(jobs) == 1
    assert jobs[0].metadata.ordinal == 3
    assert jobs[0].metadata.instance_id == InstanceID('j1', 'r1', 3)


# --- next_ordinal tests ---

def test_auto_increment_no_conflict(sut):
    """auto_increment with no existing row inserts ordinal 1."""
    actual = sut.init_run('j1', 'r1', auto_increment=True)
    assert actual == InstanceID('j1', 'r1', 1)


def test_auto_increment_on_conflict(sut):
    """auto_increment assigns next ordinal when duplicate exists."""
    sut.init_run('j1', 'r1')
    actual = sut.init_run('j1', 'r1', auto_increment=True)
    assert actual.ordinal == 2
    assert actual == InstanceID('j1', 'r1', 2)


def test_auto_increment_multiple(sut):
    """Repeated auto_increment produces sequential ordinals."""
    sut.init_run('j1', 'r1')
    second = sut.init_run('j1', 'r1', auto_increment=True)
    assert second.ordinal == 2
    third = sut.init_run('j1', 'r1', auto_increment=True)
    assert third.ordinal == 3


def test_auto_increment_different_runs(sut):
    """auto_increment is scoped to (job_id, run_id)."""
    sut.init_run('j1', 'r1')
    actual = sut.init_run('j1', 'r2', auto_increment=True)
    assert actual == InstanceID('j1', 'r2', 1)  # No conflict, ordinal 1


# --- Duplicate run in read_runs tests ---

def test_duplicate_run_with_no_start(sut):
    """A duplicate run (CREATED → ENDED, never started) appears in read_runs."""
    run = fake_job_run('j1', 'r1', term_status=TerminationStatus.DUPLICATE)
    _init_and_store(sut, run)

    jobs = sut.read_runs()
    assert len(jobs) == 1
    assert jobs[0].lifecycle.termination.status == TerminationStatus.DUPLICATE


def test_suppressed_run(sut):
    run = fake_job_run('j1', 'r1', term_status=TerminationStatus.SUPPRESSED)
    _init_and_store(sut, run)

    jobs = sut.read_runs()
    assert len(jobs) == 1
    assert jobs[0].lifecycle.termination.status == TerminationStatus.SUPPRESSED
