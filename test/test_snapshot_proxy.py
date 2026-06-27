"""Behaviour of ``SnapshotJobInstanceProxy`` — the snapshot-pushed proxy for polling transports."""
import pytest

from runtools.runcore.output import Mode
from runtools.runcore.run import Stage, TerminationStatus
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.proxy import SnapshotJobInstanceProxy
from runtools.runcore.util import utc_now

BASE = utc_now().replace(microsecond=0)


def _running():
    return fake_job_run('j1', created_at=BASE, term_status=None)


def _ended():
    return fake_job_run('j1', created_at=BASE, term_status=TerminationStatus.COMPLETED)


class _Recorder:
    def __init__(self):
        self.events = []

    def instance_lifecycle_update(self, event):
        self.events.append(event)

    def instance_phase_update(self, event):
        self.events.append(event)

    def instance_status_update(self, event):
        self.events.append(event)


def test_update_applies_newer_snapshot():
    proxy = SnapshotJobInstanceProxy(_running())
    ended = _ended()

    proxy.update_from_snapshot(ended)

    assert proxy.snap() is ended


def test_update_ignores_snapshot_that_is_not_newer():
    newer = fake_job_run('j1', created_at=BASE, offset_min=5, term_status=None)
    older = fake_job_run('j1', created_at=BASE, offset_min=1, term_status=None)
    proxy = SnapshotJobInstanceProxy(newer)

    proxy.update_from_snapshot(older)

    assert proxy.snap() is newer


def test_ended_proxy_is_not_resurrected_by_active_snapshot():
    ended = _ended()
    proxy = SnapshotJobInstanceProxy(ended)

    proxy.update_from_snapshot(_running())  # active snapshot of an already-ended instance

    assert proxy.snap() is ended


def test_update_emits_synthesized_state_events():
    proxy = SnapshotJobInstanceProxy(_running())
    recorder = _Recorder()
    proxy.notifications.add_observer_lifecycle(recorder)

    proxy.update_from_snapshot(_ended())

    assert Stage.ENDED in [e.new_stage for e in recorder.events]


def test_observer_sees_new_state_within_callback():
    proxy = SnapshotJobInstanceProxy(_running())
    seen = []
    proxy.notifications.add_observer_lifecycle(type('Obs', (), {
        'instance_lifecycle_update': lambda self, event: seen.append(proxy.snap())})())

    ended = _ended()
    proxy.update_from_snapshot(ended)

    assert seen == [ended]  # cache is replaced before events are emitted


def test_no_change_emits_nothing():
    run = _running()
    proxy = SnapshotJobInstanceProxy(run)
    recorder = _Recorder()
    proxy.notifications.add_observer_lifecycle(recorder)
    proxy.notifications.add_observer_phase(recorder)
    proxy.notifications.add_observer_status(recorder)

    proxy.update_from_snapshot(run)  # same snapshot — passes the guard but yields no diff

    assert recorder.events == []


def test_control_and_output_are_not_supported_yet():
    proxy = SnapshotJobInstanceProxy(_running())

    with pytest.raises(NotImplementedError):
        proxy.stop()
    with pytest.raises(NotImplementedError):
        proxy._exec_phase_op('phase', 'op')
    with pytest.raises(NotImplementedError):
        proxy._fetch_output_tail(Mode.TAIL, 0)
