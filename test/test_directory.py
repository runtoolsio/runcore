from datetime import timedelta

import pytest

from runtools.runcore.job import InstanceLifecycleEvent
from runtools.runcore.matching import JobRunCriteria, LifecycleCriterion
from runtools.runcore.proxy import JobInstanceProxyBase
from runtools.runcore.run import Stage, StopReason
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.transport import DiscoveredRuns, EventDrivenInstanceDirectoryBase
from runtools.runcore.util import utc_now


class _Proxy(JobInstanceProxyBase):

    def stop(self, stop_reason=StopReason.STOPPED):
        pass

    def _exec_phase_op(self, phase_id, op_name, *op_args):
        pass

    def _fetch_output_tail(self, mode, max_lines):
        return []


class FakeDiscovery:

    def __init__(self, *runs):
        self.runs = list(runs)
        self.sweeps = 0
        self.complete = True

    def discover_active_runs(self, run_match=None):
        self.sweeps += 1
        runs = [r for r in self.runs if run_match(r)] if run_match else list(self.runs)
        return DiscoveredRuns(runs, self.complete)


class FakeDirectory(EventDrivenInstanceDirectoryBase):

    def __init__(self, discovery):
        super().__init__(discovery)
        self.wire_handler = None
        self.closed = False
        self.proxies_created = 0

    def _create_proxy(self, initial):
        self.proxies_created += 1
        return _Proxy(initial)

    def _start_receiving(self, handler):
        self.wire_handler = handler

    def _close_resources(self):
        self.closed = True


@pytest.fixture
def base_ts():
    return utc_now().replace(microsecond=0)


def wire_lifecycle(directory, job_run, stage):
    """Deliver a lifecycle event through the wire handler, serialized as on the real wire."""
    event = InstanceLifecycleEvent(job_run, stage, job_run.last_updated)
    directory.wire_handler(event.EVENT_TYPE, event.instance, event.serialize()["event"])


def test_open_admits_discovered_instances(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    directory = FakeDirectory(FakeDiscovery(run))

    directory.open()

    instances = directory.get_instances()
    assert [i.id for i in instances] == [run.instance_id]


def test_directory_returns_same_proxy_per_instance(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    directory = FakeDirectory(FakeDiscovery(run))
    directory.open()

    first = directory.get_instance(run.instance_id)
    second = directory.get_instance(run.instance_id)
    swept = directory.get_instances()[0]

    assert first is second is swept


def test_events_during_discovery_replay_after_proxies_exist(base_ts):
    initial = fake_job_run('j1', created_at=base_ts, term_status=None)
    ended = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=10))

    class RacingDiscovery(FakeDiscovery):
        """Delivers an ENDED wire event mid-discovery — must be buffered, not lost."""

        def discover_active_runs(self, run_match=None):
            if self.sweeps == 0:
                wire_lifecycle(directory, ended, Stage.ENDED)
            return super().discover_active_runs(run_match)

    discovery = RacingDiscovery(initial)
    directory = FakeDirectory(discovery)
    directory.open()

    proxy = directory.get_instance(initial.instance_id)
    assert proxy is None or proxy.snap().lifecycle.is_ended


def test_events_admit_unknown_instances(base_ts):
    discovery = FakeDiscovery()
    directory = FakeDirectory(discovery)
    directory.open()
    sweeps_after_open = discovery.sweeps

    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    wire_lifecycle(directory, run, Stage.RUNNING)

    # Admitted into the identity map straight from the event — no sweep involved
    assert directory.get_instance(run.instance_id) is not None
    assert discovery.sweeps == sweeps_after_open


def test_ended_instances_are_evicted(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    discovery = FakeDiscovery(run)
    directory = FakeDirectory(discovery)
    directory.open()

    discovery.runs.clear()  # The node no longer reports the instance
    ended = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=10))
    wire_lifecycle(directory, ended, Stage.ENDED)

    assert directory.get_instances() == []
    assert directory.get_instance(run.instance_id) is None


def test_live_events_update_proxy_state(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    directory = FakeDirectory(FakeDiscovery(run))
    directory.open()
    proxy = directory.get_instance(run.instance_id)

    updated = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=10))
    wire_lifecycle(directory, updated, Stage.ENDED)

    assert proxy.snap().lifecycle.is_ended


def test_get_instances_filters_by_run_match(base_ts):
    run1 = fake_job_run('j1', created_at=base_ts, term_status=None)
    run2 = fake_job_run('j2', created_at=base_ts, term_status=None)
    directory = FakeDirectory(FakeDiscovery(run1, run2))
    directory.open()

    matched = directory.get_instances(JobRunCriteria.instance_match(run1.instance_id))
    assert [i.id for i in matched] == [run1.instance_id]


def test_get_instance_hits_identity_map_without_sweep(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    discovery = FakeDiscovery(run)
    directory = FakeDirectory(discovery)
    directory.open()
    sweeps_after_open = discovery.sweeps

    directory.get_instance(run.instance_id)
    assert discovery.sweeps == sweeps_after_open


def test_external_observers_receive_replayed_events(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    received = []

    class Observer:
        def instance_lifecycle_update(self, event):
            received.append(event)

    class RacingDiscovery(FakeDiscovery):
        def discover_active_runs(self, run_match=None):
            if self.sweeps == 0:
                wire_lifecycle(directory, run, Stage.RUNNING)
            return super().discover_active_runs(run_match)

    directory = FakeDirectory(RacingDiscovery())
    directory.notifications.add_observer_lifecycle(Observer())
    directory.open()

    assert len(received) == 1


def test_close_releases_resources(base_ts):
    directory = FakeDirectory(FakeDiscovery())
    directory.open()
    directory.close()
    assert directory.closed


def test_get_instances_never_queries_transport(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    discovery = FakeDiscovery(run)
    directory = FakeDirectory(discovery)
    directory.open()
    sweeps_after_open = discovery.sweeps

    directory.get_instances()
    directory.get_instances(JobRunCriteria.instance_match(run.instance_id))
    directory.get_instances(JobRunCriteria.all())

    assert discovery.sweeps == sweeps_after_open


def test_get_instances_filters_by_live_proxy_state(base_ts):
    running = fake_job_run('j1', created_at=base_ts, term_status=None)
    directory = FakeDirectory(FakeDiscovery(running))
    directory.open()

    ended_match = JobRunCriteria(lifecycle_criteria=(LifecycleCriterion(stage=Stage.ENDED),))
    assert directory.get_instances(ended_match) == []




def test_get_instance_miss_means_not_live(base_ts):
    discovery = FakeDiscovery()
    directory = FakeDirectory(discovery)
    directory.open()
    sweeps_after_open = discovery.sweeps

    unknown = fake_job_run('j1', created_at=base_ts, term_status=None)
    assert directory.get_instance(unknown.instance_id) is None
    assert discovery.sweeps == sweeps_after_open  # A miss never queries the transport


def test_ended_event_for_unknown_instance_creates_no_proxy(base_ts):
    directory = FakeDirectory(FakeDiscovery())
    directory.open()

    ended = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=5))
    wire_lifecycle(directory, ended, Stage.ENDED)

    assert directory.proxies_created == 0  # No wasted construct-bind-detach cycle
    assert directory.get_instances() == []


def test_removed_instance_stops_receiving_events(base_ts):
    run = fake_job_run('j1', created_at=base_ts, term_status=None)
    directory = FakeDirectory(FakeDiscovery(run))
    directory.open()
    proxy = directory.get_instance(run.instance_id)

    ended = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=10))
    wire_lifecycle(directory, ended, Stage.ENDED)
    # The directory unbound the proxy on removal — a newer event must not reach it
    later = fake_job_run('j1', created_at=base_ts, ended_at=base_ts + timedelta(minutes=20))
    wire_lifecycle(directory, later, Stage.ENDED)

    assert proxy.snap() == ended
