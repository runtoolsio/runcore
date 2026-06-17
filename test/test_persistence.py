import pytest

from runtools.runcore import db
from runtools.runcore.db.persister import RunStatePersister
from runtools.runcore.job import (
    InstanceLifecycleEvent, InstanceObservableNotifications, InstancePhaseEvent, InstanceStatusEvent,
)
from runtools.runcore.run import Stage, TerminationStatus
from runtools.runcore.test.job import fake_job_run


def test_load_sqlite():
    assert db.load_database_module('sqlite')


class FakeDB:
    def __init__(self):
        self.stored = []

    def store_active_runs(self, *job_runs):
        self.stored.extend(job_runs)


@pytest.fixture
def fake_db():
    return FakeDB()


@pytest.fixture
def persister(fake_db):
    return RunStatePersister(fake_db)


def attach(persister):
    """Attach the persister to a fresh instance hub and return that hub."""
    notifications = InstanceObservableNotifications()
    persister.attach(notifications)
    return notifications


def fire_phase(notifications, job_run):
    notifications.phase_notification.observer_proxy.instance_phase_update(
        InstancePhaseEvent(job_run, True, 'p1', job_run.lifecycle.stage, job_run.last_updated))


def fire_status(notifications, job_run):
    notifications.status_notification.observer_proxy.instance_status_update(
        InstanceStatusEvent(job_run, job_run.last_updated))


def fire_lifecycle(notifications, job_run, stage):
    notifications.lifecycle_notification.observer_proxy.instance_lifecycle_update(
        InstanceLifecycleEvent(job_run, stage, job_run.last_updated))


def test_events_buffer_until_flush(persister, fake_db):
    notifications = attach(persister)
    run = fake_job_run('j1', term_status=None)

    fire_phase(notifications, run)
    assert fake_db.stored == []  # Buffered, not written yet

    persister.flush()
    assert fake_db.stored == [run]


def test_flush_keeps_only_latest_snapshot(persister, fake_db):
    notifications = attach(persister)
    first = fake_job_run('j1', run_id='r1', term_status=None)
    second = fake_job_run('j1', run_id='r1', term_status=None)

    fire_status(notifications, first)
    fire_phase(notifications, second)  # Later event for the same run supersedes the earlier
    persister.flush()

    assert fake_db.stored == [second]  # One write, latest snapshot


def test_ended_event_does_not_write(persister, fake_db):
    notifications = attach(persister)
    ended = fake_job_run('j1', term_status=TerminationStatus.COMPLETED)

    fire_lifecycle(notifications, ended, Stage.ENDED)
    persister.flush()

    assert fake_db.stored == []  # Terminal write is _finalize_run's job, not the persister's


def test_ended_clears_pending_snapshot(persister, fake_db):
    notifications = attach(persister)
    active = fake_job_run('j1', run_id='r1', term_status=None)
    ended = fake_job_run('j1', run_id='r1', term_status=TerminationStatus.COMPLETED)

    fire_status(notifications, active)                 # Buffered
    fire_lifecycle(notifications, ended, Stage.ENDED)  # Drops it from the dirty set
    persister.flush()

    assert fake_db.stored == []  # Pending active snapshot must not land after the run ended


def test_reused_instance_id_persists_again_after_end(persister, fake_db):
    notifications = attach(persister)
    ended = fake_job_run('j1', run_id='r1', term_status=TerminationStatus.COMPLETED)
    reused = fake_job_run('j1', run_id='r1', term_status=None)  # Same id reused (e.g. after history removal)

    fire_lifecycle(notifications, ended, Stage.ENDED)  # Old run ends — dropped, not written
    fire_phase(notifications, reused)                   # New run with the reused id must still persist
    persister.flush()

    assert fake_db.stored == [reused]


def test_close_flushes_remaining_snapshots(persister, fake_db):
    notifications = attach(persister)
    run = fake_job_run('j1', term_status=None)

    fire_status(notifications, run)
    persister.close()

    assert fake_db.stored == [run]


def test_events_ignored_after_close(persister, fake_db):
    notifications = attach(persister)
    persister.close()

    fire_phase(notifications, fake_job_run('j1', term_status=None))
    persister.flush()

    assert fake_db.stored == []
