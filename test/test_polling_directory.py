"""Behaviour of PollingInstanceDirectory — the snapshot/polling instance directory.

Driven by calling ``reconcile()`` directly (the public on-demand poll) rather than the background
thread, so polls are deterministic. Backed by a real in-memory SQLite store.
"""
from datetime import timedelta

import pytest

from runtools.runcore.db import sqlite
from runtools.runcore.matching import JobRunCriteria
from runtools.runcore.proxy import SnapshotJobInstanceProxy
from runtools.runcore.run import Stage, TerminationStatus
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.transport.db import PollingInstanceDirectory
from runtools.runcore.util import utc_now

BASE = utc_now().replace(microsecond=0)


@pytest.fixture
def db():
    with sqlite.create_memory('test_env') as database:
        yield database


@pytest.fixture
def directory(db):
    sut = PollingInstanceDirectory(db, lambda run: SnapshotJobInstanceProxy(run, db, db))
    yield sut
    sut.close()  # idempotent; the poll thread is never started in these tests


def _seed_active(db, run):
    iid = run.metadata.instance_id
    db.init_run(iid.job_id, iid.run_id, created_at=run.lifecycle.created_at)
    db.store_active_runs(run)


class _Recorder:
    def __init__(self):
        self.events = []

    def instance_lifecycle_update(self, event):
        self.events.append(event)

    def instance_phase_update(self, event):
        self.events.append(event)

    def instance_status_update(self, event):
        self.events.append(event)


def _watch_all(directory):
    recorder = _Recorder()
    directory.notifications.add_observer_lifecycle(recorder)
    directory.notifications.add_observer_phase(recorder)
    directory.notifications.add_observer_status(recorder)
    return recorder


def test_reconcile_admits_active_runs(directory, db):
    _seed_active(db, fake_job_run('j1', created_at=BASE, term_status=None))

    directory.reconcile()

    assert [i.id.job_id for i in directory.get_instances()] == ['j1']


def test_first_admission_is_silent(directory, db):
    _seed_active(db, fake_job_run('j1', created_at=BASE, term_status=None))
    recorder = _watch_all(directory)

    directory.reconcile()

    assert recorder.events == []  # seeding admits without replaying history


def test_changed_snapshot_updates_proxy_without_spurious_events(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    directory.reconcile()
    proxy = directory.get_instance(run.metadata.instance_id)
    recorder = _watch_all(directory)

    db.store_active_runs(fake_job_run('j1', created_at=BASE, offset_min=5, term_status=None))  # later -> new cursor
    directory.reconcile()

    assert directory.get_instance(run.metadata.instance_id) is proxy  # same proxy, not re-admitted
    assert proxy.snap().last_updated > run.last_updated               # snapshot advanced via update_from_snapshot
    assert recorder.events == []                                      # no stage/status change -> no events


def test_unchanged_snapshot_is_a_noop(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    directory.reconcile()
    proxy = directory.get_instance(run.metadata.instance_id)
    recorder = _watch_all(directory)

    directory.reconcile()  # nothing changed in the store

    assert directory.get_instance(run.metadata.instance_id) is proxy
    assert recorder.events == []


def test_ended_run_is_evicted_and_emits_ended(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    directory.reconcile()
    recorder = _watch_all(directory)

    db.store_runs(fake_job_run('j1', created_at=BASE, term_status=TerminationStatus.COMPLETED))  # terminal write
    directory.reconcile()

    assert directory.get_instance(run.metadata.instance_id) is None          # evicted from the view
    assert Stage.ENDED in [e.new_stage for e in recorder.events]             # ENDED reached directory observers


def test_vanished_run_is_evicted_silently(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    directory.reconcile()
    recorder = _watch_all(directory)

    db.remove_runs(JobRunCriteria.instance_match(run.metadata.instance_id))  # gone, no terminal snapshot
    directory.reconcile()

    assert directory.get_instance(run.metadata.instance_id) is None
    assert recorder.events == []  # nothing to replay


def test_close_does_not_join_the_poll_thread_from_itself(directory):
    import threading
    # An observer can call close() while handling a synthesized event — that callback runs on the
    # poll thread, so close() must not try to join the current thread (which would RuntimeError).
    directory._poll_thread = threading.current_thread()  # simulate close() invoked on the poll thread

    directory.close()  # must return cleanly, not raise

    assert directory._stop.is_set()  # shutdown was still signalled; the loop exits on its own


def test_open_seeds_then_close_stops(directory, db):
    _seed_active(db, fake_job_run('j1', created_at=BASE, term_status=None))

    directory.open()  # first poll seeds synchronously, then the poll thread starts
    try:
        assert [i.id.job_id for i in directory.get_instances()] == ['j1']
    finally:
        directory.close()


def _backdate_heartbeat(db, minutes):
    db._conn.execute("UPDATE runs SET heartbeat_at = ?",
                     ((utc_now() - timedelta(minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S.%f'),))
    db._conn.commit()


def test_stale_heartbeat_marks_instance_lost(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    _backdate_heartbeat(db, minutes=5)  # a node that died without finalizing stops touching

    directory.reconcile()

    proxy = directory.get_instance(run.metadata.instance_id)
    assert proxy.is_lost
    assert proxy.heartbeat_age > 60


def test_fresh_heartbeat_keeps_instance_alive(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)  # init_run baselines the heartbeat at creation

    directory.reconcile()

    proxy = directory.get_instance(run.metadata.instance_id)
    assert not proxy.is_lost
    assert proxy.heartbeat_age is not None


def test_lost_instance_stays_in_view(directory, db):
    # Interpret, never mutate: a lost run remains visible and attributable, not evicted
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    _backdate_heartbeat(db, minutes=5)

    directory.reconcile()
    directory.reconcile()

    assert [i.id.job_id for i in directory.get_instances()] == ['j1']


def test_control_request_synthesizes_control_event(directory, db):
    control_events = []

    class ControlRecorder:
        def instance_control_update(self, event):
            control_events.append(event)

    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    directory.reconcile()  # admit silently
    directory.notifications.add_observer_control(ControlRecorder())

    from dataclasses import replace
    from runtools.runcore.job import ControlRequest
    request = ControlRequest('approve', BASE + timedelta(minutes=10), phase_id='gate')
    db.store_active_runs(replace(run, control_requests=(request,)))  # applied_at drives last_updated
    directory.reconcile()

    [event] = control_events
    assert event.request == request
