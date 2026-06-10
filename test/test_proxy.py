from dataclasses import replace
from datetime import timedelta

import pytest

from runtools.runcore.job import (
    InstanceLifecycleEvent, InstanceObservableNotifications, InstanceStatusEvent,
)
from runtools.runcore.output import Output
from runtools.runcore.proxy import JobInstanceProxyBase
from runtools.runcore.run import Stage, StopReason, TerminationStatus
from runtools.runcore.status import Event, Status
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.util import utc_now


class FakeProxy(JobInstanceProxyBase):
    """Minimal concrete proxy exercising the transport-neutral state half."""

    def __init__(self, initial, notifications):
        super().__init__(initial, notifications)
        self.executed_ops = []
        self.stopped_with = None

    @property
    def output(self) -> Output:
        raise NotImplementedError

    def stop(self, stop_reason=StopReason.STOPPED):
        self.stopped_with = stop_reason

    def _exec_phase_op(self, phase_id, op_name, *op_args):
        self.executed_ops.append((phase_id, op_name, op_args))
        return 'done'


@pytest.fixture
def base_ts():
    return utc_now().replace(microsecond=0)


@pytest.fixture
def hub():
    return InstanceObservableNotifications()


def lifecycle_event(job_run, new_stage):
    return InstanceLifecycleEvent(job_run, new_stage, job_run.last_updated)


def fire_lifecycle(hub, event):
    hub.lifecycle_notification.observer_proxy.instance_lifecycle_update(event)


def fire_status(hub, event):
    hub.status_notification.observer_proxy.instance_status_update(event)


def test_last_updated_is_max_phase_timestamp(base_ts):
    running = fake_job_run('j1', created_at=base_ts, term_status=None)
    ended = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=10))

    assert ended.last_updated == base_ts + timedelta(minutes=10)
    assert ended.last_updated > running.last_updated


def test_last_updated_includes_status_timestamps(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    status_ts = run.last_updated + timedelta(minutes=3)
    with_status = replace(run, status=Status(Event('processing', status_ts), [], [], None))

    assert with_status.last_updated == status_ts


def test_is_newer_than(base_ts):
    older = fake_job_run('j1', created_at=base_ts, term_status=None)
    newer = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=10))

    assert newer.is_newer_than(older)
    assert not older.is_newer_than(newer)


def test_is_newer_than_applies_on_equal_timestamps(base_ts):
    first = fake_job_run('j1', created_at=base_ts, term_status=None)
    second = fake_job_run('j1', created_at=base_ts, term_status=None)

    assert first.is_newer_than(second)
    assert second.is_newer_than(first)


def test_is_newer_than_never_replaces_ended_with_non_ended(base_ts):
    ended = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=5))
    running = fake_job_run('j1', created_at=base_ts + timedelta(minutes=10), term_status=None)

    assert not running.is_newer_than(ended)


def test_proxy_updates_state_from_events(base_ts, hub):
    initial = fake_job_run('j1', created_at=base_ts, term_status=None)
    proxy = FakeProxy(initial, hub)

    updated = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=10),
                           term_status=TerminationStatus.FAILED)
    fire_lifecycle(hub, lifecycle_event(updated, Stage.ENDED))

    assert proxy.snap() == updated


def test_proxy_ignores_stale_events(base_ts, hub):
    initial = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=10))
    proxy = FakeProxy(initial, hub)

    stale = fake_job_run('j1', created_at=base_ts, term_status=None)
    fire_status(hub, InstanceStatusEvent(stale, stale.last_updated))

    assert proxy.snap() == initial


def test_proxy_ignores_events_of_other_instances(base_ts, hub):
    initial = fake_job_run('j1', created_at=base_ts, term_status=None)
    proxy = FakeProxy(initial, hub)

    other = fake_job_run('j2', created_at=base_ts, ended_at=base_ts + timedelta(minutes=10))
    fire_lifecycle(hub, lifecycle_event(other, Stage.ENDED))

    assert proxy.snap() == initial


def test_proxy_unbinds_from_hub_when_instance_ends(base_ts, hub):
    initial = fake_job_run('j1', created_at=base_ts, term_status=None)
    proxy = FakeProxy(initial, hub)

    ended = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=10))
    fire_lifecycle(hub, lifecycle_event(ended, Stage.ENDED))
    # Newer event for the same (ended) instance must not reach the unbound proxy
    later = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=20))
    fire_lifecycle(hub, lifecycle_event(later, Stage.ENDED))

    assert proxy.snap() == ended


def test_phase_control_delegates_to_exec_phase_op(base_ts, hub):
    initial = fake_job_run('j1', created_at=base_ts, term_status=None)
    proxy = FakeProxy(initial, hub)

    control = proxy.find_phase_control_by_id('program')
    assert control.pause('now') == 'done'
    assert proxy.executed_ops == [('program', 'pause', ('now',))]


def test_proxy_run_is_not_supported(base_ts, hub):
    proxy = FakeProxy(fake_job_run('j1', created_at=base_ts, term_status=None), hub)

    with pytest.raises(NotImplementedError):
        proxy.run()
