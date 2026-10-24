"""Behaviour of PollingInstanceDirectory — the snapshot/polling instance directory.

Driven by calling ``reconcile()`` directly (the public on-demand poll) rather than the background
thread, so polls are deterministic. Backed by a real in-memory SQLite store.
"""
from datetime import timedelta

import pytest

from runtools.runcore.db import sqlite
from runtools.runcore.job import InstanceLifecycleEvent, InstanceOutputEvent
from runtools.runcore.matching import JobRunCriteria
from runtools.runcore.output import OutputLine
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


def test_instance_admitted_after_seed_is_announced_at_current_stage(directory, db):
    directory.reconcile()  # empty seed — still the seed
    recorder = _watch_all(directory)
    run = fake_job_run('j1', created_at=BASE, term_status=None)  # first seen mid-life
    _seed_active(db, run)

    directory.reconcile()
    directory.reconcile()  # announced once, not on every poll

    [event] = recorder.events
    assert event.new_stage == Stage.RUNNING
    assert event.timestamp == run.lifecycle.started_at
    assert event.job_run.instance_id == run.instance_id


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
    assert proxy.snap().is_lost()


def test_fresh_heartbeat_keeps_instance_alive(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)  # init_run baselines the heartbeat at creation

    directory.reconcile()

    proxy = directory.get_instance(run.metadata.instance_id)
    assert proxy.snap().heartbeat_at is not None
    assert not proxy.snap().is_lost()


def test_unchanged_heartbeat_going_stale_is_logged_once(directory, db, caplog, monkeypatch):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    directory.reconcile()  # admitted live

    later = utc_now() + timedelta(minutes=5)  # the stored heartbeat stays put; time passes
    monkeypatch.setattr('runtools.runcore.transport.db.utc_now', lambda: later)
    directory.reconcile()
    directory.reconcile()

    assert caplog.text.count("Instance lost") == 1


def test_heartbeat_touch_revives_lost_instance_without_events(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    _backdate_heartbeat(db, minutes=5)
    directory.reconcile()
    recorder = _watch_all(directory)

    db.touch_heartbeats([run.metadata.instance_id])  # the owner regains contact; the cursor stays put
    directory.reconcile()

    assert not directory.get_instance(run.metadata.instance_id).snap().is_lost()
    assert recorder.events == []


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


def _append_lines(db, run, *messages, start=1):
    iid = run.metadata.instance_id
    db.append_output([(iid, OutputLine(message, start + i)) for i, message in enumerate(messages)])


class _CountingTailReader:
    """OutputTailReader decorator proving tail reads happen only under demand."""

    def __init__(self, db):
        self._db = db
        self.reads = 0

    def read_output_tail(self, instance_id, max_lines, *, after_ordinal=None):
        self.reads += 1
        return self._db.read_output_tail(instance_id, max_lines, after_ordinal=after_ordinal)


def test_no_output_reads_without_demand(db):
    reader = _CountingTailReader(db)
    directory = PollingInstanceDirectory(db, lambda run: SnapshotJobInstanceProxy(run, db, reader))
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    _append_lines(db, run, 'one', 'two')

    directory.reconcile()
    directory.reconcile()  # the directory's own aggregate relay must not count as demand

    assert reader.reads == 0


def test_first_output_sync_replays_retained_lines_once(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    _append_lines(db, run, 'old-one', 'old-two')
    received = []
    directory.notifications.add_observer_output(received.append)

    directory.reconcile()  # admits and replays the retained tail
    directory.reconcile()

    assert [e.output_line.message for e in received] == ['old-one', 'old-two']


def test_new_lines_are_emitted_in_order(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    received = []
    directory.notifications.add_observer_output(received.append)
    directory.reconcile()

    _append_lines(db, run, 'one', 'two')
    directory.reconcile()
    directory.reconcile()  # no new lines -> no duplicates

    assert [e.output_line.message for e in received] == ['one', 'two']
    assert received[0].instance == run.metadata


def test_proxy_level_observer_is_output_demand(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    directory.reconcile()
    proxy = directory.get_instance(run.metadata.instance_id)
    received = []
    proxy.notifications.add_observer_output(received.append)
    directory.reconcile()

    _append_lines(db, run, 'one')
    directory.reconcile()

    assert [e.output_line.message for e in received] == ['one']


def test_demand_lapse_catches_up_without_repeating_delivered_lines(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    received = []
    directory.notifications.add_observer_output(received.append)
    directory.reconcile()

    _append_lines(db, run, 'already delivered')
    directory.reconcile()
    directory.notifications.remove_observer_output(received.append)
    _append_lines(db, run, 'unwatched', start=2)
    directory.reconcile()  # no demand: keep the cursor without reading
    directory.notifications.add_observer_output(received.append)
    directory.reconcile()  # catch up across the demand pause
    _append_lines(db, run, 'watched', start=3)
    directory.reconcile()

    assert [e.output_line.message for e in received] == ['already delivered', 'unwatched', 'watched']


def test_initial_pull_does_not_skip_retained_or_subsequent_output(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    _append_lines(db, run, 'pulled')
    directory.reconcile()  # admit without output demand
    proxy = directory.get_instance(run.metadata.instance_id)
    received = []
    directory.notifications.add_observer_output(received.append)

    assert [l.message for l in proxy.output.tail()] == ['pulled']
    _append_lines(db, run, 'streamed', start=2)
    directory.reconcile()

    assert [e.output_line.message for e in received] == ['pulled', 'streamed']


def test_empty_initial_pull_does_not_skip_first_output(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    directory.reconcile()
    proxy = directory.get_instance(run.metadata.instance_id)
    received = []
    directory.notifications.add_observer_output(received.append)

    assert proxy.output.tail() == []  # nothing produced yet at pull time
    _append_lines(db, run, 'first')
    directory.reconcile()

    assert [e.output_line.message for e in received] == ['first']


def test_pull_does_not_move_the_stream_cursor(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    received = []
    directory.notifications.add_observer_output(received.append)
    directory.reconcile()
    _append_lines(db, run, 'one')
    directory.reconcile()

    proxy = directory.get_instance(run.metadata.instance_id)
    proxy.output.tail()  # covers the emitted line; must neither replay nor skip
    _append_lines(db, run, 'two', start=2)
    directory.reconcile()

    assert [e.output_line.message for e in received] == ['one', 'two']


def test_eviction_emits_final_output_before_ended(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    events = []
    directory.notifications.add_observer_output(events.append)
    directory.notifications.add_observer_lifecycle(events.append)
    directory.reconcile()

    _append_lines(db, run, 'last words')
    db.store_runs(fake_job_run('j1', created_at=BASE, term_status=TerminationStatus.COMPLETED))
    directory.reconcile()  # eviction: final output increment, then the terminal replay

    assert [e.output_line.message for e in events if isinstance(e, InstanceOutputEvent)] == ['last words']
    assert isinstance(events[0], InstanceOutputEvent)  # the final increment precedes ENDED
    assert any(isinstance(e, InstanceLifecycleEvent) and e.new_stage == Stage.ENDED for e in events)


def test_subscribe_and_pull_after_no_demand_check_does_not_lose_output(directory, db, monkeypatch):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    directory.reconcile()
    received = []
    original = directory._has_output_demand

    def subscribe_after_check(proxy):
        demand = original(proxy)
        proxy.notifications.add_observer_output(received.append)
        assert proxy.output.tail() == []
        _append_lines(db, run, 'arrived after pull')
        return demand

    with monkeypatch.context() as patch:
        patch.setattr(directory, '_has_output_demand', subscribe_after_check)
        directory.reconcile()
    directory.reconcile()

    assert [e.output_line.message for e in received] == ['arrived after pull']


def test_resuming_output_tolerates_pruned_ordinals(directory, db):
    run = fake_job_run('j1', created_at=BASE, term_status=None)
    _seed_active(db, run)
    received = []
    directory.notifications.add_observer_output(received.append)
    _append_lines(db, run, 'one')
    directory.reconcile()

    directory.notifications.remove_observer_output(received.append)
    _append_lines(db, run, 'two', 'three', 'four', start=2)
    db.prune_output_tail(run.instance_id, 2)
    directory.reconcile()
    directory.notifications.add_observer_output(received.append)
    directory.reconcile()

    assert [e.output_line.ordinal for e in received] == [1, 3, 4]
