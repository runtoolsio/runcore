from dataclasses import replace
from datetime import timedelta

import pytest

from runtools.runcore.job import (
    HEARTBEAT_STALE_AFTER, InstanceLifecycleEvent, InstanceObservableNotifications, InstanceOutputEvent,
    InstanceStatusEvent, JobInstance,
)
from runtools.runcore.output import OutputLine
from runtools.runcore.proxy import JobInstanceProxyBase
from runtools.runcore.run import Outcome, RunLifecycle, Stage, StopReason, TerminationStatus
from runtools.runcore.status import Event, Status
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.util import utc_now


class FakeProxy(JobInstanceProxyBase):
    """Minimal concrete proxy exercising the transport-neutral state half."""

    def __init__(self, initial, notifications):
        super().__init__(initial)
        self.binding = self.bind_to(notifications)
        self.executed_ops = []
        self.stopped_with = None
        self.fetched_lines = []
        self.fetch_count = 0
        self.fetched_max_lines = None

    def stop(self, stop_reason=StopReason.STOPPED):
        self.stopped_with = stop_reason

    def _exec_phase_op(self, phase_id, op_name, *op_args):
        self.executed_ops.append((phase_id, op_name, op_args))
        return 'done'

    def _fetch_output_tail(self, max_lines):
        self.fetch_count += 1
        self.fetched_max_lines = max_lines
        if max_lines > 0:
            return self.fetched_lines[-max_lines:]
        return self.fetched_lines


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


def fire_output(hub, job_run, line):
    event = InstanceOutputEvent(job_run.metadata, line, utc_now())
    hub.output_notification.observer_proxy.instance_output_update(event)


def test_state_observer_excludes_output(base_ts, hub):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    received = []
    hub.add_observer_state_events(received.append)

    fire_output(hub, run, OutputLine('line', 1))
    fire_lifecycle(hub, lifecycle_event(run, Stage.RUNNING))

    assert len(received) == 1
    assert isinstance(received[0], InstanceLifecycleEvent)


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


def test_is_newer_than_orders_ended_after_non_ended_whatever_the_timestamps(base_ts):
    """Lifecycle is monotonic — an estimated termination (a run marked lost) may predate the last
    active snapshot and must still replace it."""
    ended = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=5))
    running = fake_job_run('j1', created_at=base_ts + timedelta(minutes=10), term_status=None)

    assert ended.is_newer_than(running)
    assert not running.is_newer_than(ended)


def test_run_with_stale_heartbeat_is_lost(base_ts):
    run = replace(fake_job_run('j1', created_at=base_ts, term_status=None), heartbeat_at=base_ts)

    assert run.is_lost(now=base_ts + timedelta(seconds=HEARTBEAT_STALE_AFTER + 1))
    assert not run.is_lost(now=base_ts + timedelta(seconds=HEARTBEAT_STALE_AFTER))


def test_ended_run_is_never_lost(base_ts):
    run = replace(fake_job_run('j1', created_at=base_ts), heartbeat_at=base_ts)

    assert not run.is_lost(now=base_ts + timedelta(hours=1))


def test_run_without_known_heartbeat_is_not_lost(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)  # e.g. a live RPC snapshot

    assert not run.is_lost(now=base_ts + timedelta(hours=1))


def test_lost_run_ends_at_last_heartbeat_with_every_unfinished_phase(base_ts):
    heartbeat = base_ts + timedelta(minutes=30)
    run = replace(fake_job_run('j1', created_at=base_ts, term_status=None), heartbeat_at=heartbeat)

    lost = run.as_lost(now=base_ts + timedelta(hours=1))

    assert lost.lifecycle.termination.status == TerminationStatus.LOST
    assert lost.lifecycle.termination.status.outcome == Outcome.FAULT
    assert lost.lifecycle.termination.terminated_at == heartbeat
    assert all(phase.lifecycle.is_ended for phase in lost.search_phases())
    finished_before = [p.lifecycle.termination for p in run.search_phases() if p.lifecycle.is_ended]
    assert finished_before and all(t in [p.lifecycle.termination for p in lost.search_phases()]
                                   for t in finished_before)  # already ended phases keep their outcome


def test_lost_run_never_ends_before_its_start(base_ts):
    run = replace(fake_job_run('j1', created_at=base_ts, term_status=None), heartbeat_at=base_ts - timedelta(minutes=5))

    lost = run.as_lost(now=base_ts + timedelta(hours=1))

    assert lost.lifecycle.termination.terminated_at == run.lifecycle.started_at


def test_lost_run_never_ends_in_the_storage_clock_future(base_ts):
    run = replace(fake_job_run('j1', created_at=base_ts + timedelta(minutes=10), term_status=None),  # node ahead
                  heartbeat_at=base_ts - timedelta(minutes=5))

    lost = run.as_lost(now=base_ts)

    assert lost.lifecycle.termination.terminated_at == base_ts


def test_never_started_lost_run_ends_at_last_heartbeat_without_start(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    heartbeat = base_ts + timedelta(minutes=5)
    created = replace(run, heartbeat_at=heartbeat,
                      root_phase=replace(run.root_phase, lifecycle=RunLifecycle(base_ts), children=()))

    lost = created.as_lost(now=base_ts + timedelta(hours=1))

    assert lost.lifecycle.started_at is None
    assert lost.lifecycle.termination.terminated_at == heartbeat


def test_only_an_active_run_with_a_known_heartbeat_can_be_lost(base_ts):
    with pytest.raises(ValueError):
        fake_job_run('j1', created_at=base_ts, term_status=None).as_lost(now=base_ts)  # no heartbeat
    with pytest.raises(ValueError):
        replace(fake_job_run('j1', created_at=base_ts), heartbeat_at=base_ts).as_lost(now=base_ts)  # ended


def test_heartbeat_is_not_job_state(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    touched = replace(run, heartbeat_at=base_ts + timedelta(minutes=1))

    assert touched == run
    assert 'heartbeat_at' not in touched.serialize()
    assert run.is_newer_than(touched) and touched.is_newer_than(run)


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


def test_phase_control_delegates_to_exec_phase_op(base_ts, hub):
    initial = fake_job_run('j1', created_at=base_ts, term_status=None)
    proxy = FakeProxy(initial, hub)

    control = proxy.find_phase_control_by_id('program')
    assert control.pause('now') == 'done'
    assert proxy.executed_ops == [('program', 'pause', ('now',))]


def test_proxy_implements_the_whole_consumer_contract_without_run(base_ts, hub):
    """Running is producer-side (runjob); a proxy has no member it cannot honour."""
    proxy = FakeProxy(fake_job_run('j1', created_at=base_ts, term_status=None), hub)

    assert isinstance(proxy, JobInstance)
    assert not hasattr(proxy, 'run')


def test_output_fetches_default_tail(base_ts, hub):
    proxy = FakeProxy(fake_job_run('j1', created_at=base_ts, term_status=None), hub)
    proxy.fetched_lines = [OutputLine('l1', 1), OutputLine('l2', 2)]

    assert proxy.output.tail() == proxy.fetched_lines
    assert proxy.output.tail() == proxy.fetched_lines
    assert proxy.fetch_count == 2
    assert proxy.fetched_max_lines == 0


def test_output_tail_fetches_even_after_output_events(base_ts, hub):
    """Proxies cache no output — pull always goes to the transport's authoritative tail;
    events are a separate streaming lane for subscribers."""
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    proxy = FakeProxy(run, hub)
    proxy.fetched_lines = [OutputLine(f'l{o}', o) for o in range(1, 8)]

    output = proxy.output
    for ordinal in (5, 6, 7):
        fire_output(hub, run, OutputLine(f'l{ordinal}', ordinal))

    assert [line.ordinal for line in output.tail(max_lines=3)] == [5, 6, 7]
    assert proxy.fetch_count == 1
    assert proxy.fetched_max_lines == 3


def test_output_fetch_results_are_not_cached(base_ts, hub):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    proxy = FakeProxy(run, hub)
    proxy.fetched_lines = [OutputLine('l1', 1)]

    output = proxy.output
    output.tail()
    output.tail()

    assert proxy.fetch_count == 2


def test_output_events_of_other_instances_are_filtered(base_ts, hub):
    """The proxy's instance filter drops foreign output events before they reach subscribers."""
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    proxy = FakeProxy(run, hub)
    received = []
    proxy.notifications.add_observer_output(
        type('R', (), {'instance_output_update': lambda self, e: received.append(e.output_line.message)})())

    other = fake_job_run('j2', created_at=base_ts, term_status=None)
    fire_output(hub, other, OutputLine('other', 1))
    fire_output(hub, run, OutputLine('mine', 1))

    assert received == ['mine']


def test_binding_unbind_is_idempotent(base_ts, hub):
    proxy = FakeProxy(fake_job_run('j1', created_at=base_ts, term_status=None), hub)

    proxy.binding.unbind()
    proxy.binding.unbind()  # Second unbind must be a no-op, not a double observer removal


