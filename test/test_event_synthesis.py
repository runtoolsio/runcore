"""Behaviour of the snapshot-transport event synthesizer (``proxy._synthesize_events``).

The synthesizer reconstructs the lifecycle/phase/status events a producing node would have emitted
from a pair of run snapshots. Snapshots are built directly here (rather than via ``fake_job_run``)
so each phase's stage and per-stage timestamps can be set precisely.
"""
from datetime import timedelta

from runtools.runcore.job import (
    InstanceID, InstanceLifecycleEvent, InstancePhaseEvent, InstanceStatusEvent, JobInstanceMetadata, JobRun,
)
from runtools.runcore.proxy import _synthesize_events
from runtools.runcore.run import PhaseRun, RunLifecycle, Stage, TerminationInfo, TerminationStatus
from runtools.runcore.status import Event, Status
from runtools.runcore.util import utc_now

BASE = utc_now().replace(microsecond=0)


def _ts(minutes):
    return BASE + timedelta(minutes=minutes)


def _lifecycle(*, created=0, started=None, ended=None, status=TerminationStatus.COMPLETED):
    termination = TerminationInfo(status, _ts(ended)) if ended is not None else None
    return RunLifecycle(
        created_at=_ts(created),
        started_at=_ts(started) if started is not None else None,
        termination=termination,
    )


def _phase(phase_id, lifecycle, children=()):
    return PhaseRun(phase_id, 'test', False, None, None, lifecycle, tuple(children))


def _run(root, status=None):
    return JobRun(JobInstanceMetadata(InstanceID('j1', 'r1', 1), {}), root, status=status)


def _status(message, at):
    return Status(last_event=Event(message, _ts(at)), operations=[], warnings=[], result=None)


def test_no_change_yields_no_events():
    run = _run(_phase('root', _lifecycle(created=0, started=1)))

    assert _synthesize_events(run, run) == []


def test_root_running_to_ended_yields_lifecycle_then_phase():
    prev = _run(_phase('root', _lifecycle(created=0, started=1)))
    curr = _run(_phase('root', _lifecycle(created=0, started=1, ended=5)))

    events = _synthesize_events(prev, curr)

    # Producer emits the lifecycle event before the phase event at the same transition
    assert [type(e) for e in events] == [InstanceLifecycleEvent, InstancePhaseEvent]
    assert all(e.new_stage == Stage.ENDED and e.timestamp == _ts(5) for e in events)
    assert all(e.job_run is curr for e in events)  # events carry the newer snapshot


def test_coalesced_multi_stage_jump_is_replayed_with_real_timestamps():
    prev = _run(_phase('root', _lifecycle(created=0)))                       # CREATED
    curr = _run(_phase('root', _lifecycle(created=0, started=1, ended=5)))   # ENDED, RUNNING crossed in the gap

    lifecycle = [e for e in _synthesize_events(prev, curr) if isinstance(e, InstanceLifecycleEvent)]

    assert [(e.new_stage, e.timestamp) for e in lifecycle] == [(Stage.RUNNING, _ts(1)), (Stage.ENDED, _ts(5))]


def test_stopped_before_start_skips_running():
    # A phase stopped before it starts jumps CREATED -> ENDED with started_at unset, so RUNNING has
    # no timestamp and must be skipped (else the timestamp sort would compare None to datetimes).
    prev = _run(_phase('root', _lifecycle(created=0)))            # CREATED
    curr = _run(_phase('root', _lifecycle(created=0, ended=5)))   # ENDED, never started

    events = _synthesize_events(prev, curr)

    assert [(type(e), e.new_stage) for e in events] == [
        (InstanceLifecycleEvent, Stage.ENDED), (InstancePhaseEvent, Stage.ENDED)]
    assert all(e.timestamp == _ts(5) for e in events)  # no None timestamp reached the sort


def test_created_root_yields_lifecycle_only_no_phase_event():
    # The producer never emits a CREATED phase transition — CREATED is the initial state.
    curr = _run(_phase('root', _lifecycle(created=0)))  # CREATED

    events = _synthesize_events(None, curr)

    assert [type(e) for e in events] == [InstanceLifecycleEvent]
    assert events[0].new_stage == Stage.CREATED


def test_created_child_emits_nothing():
    prev = _run(_phase('root', _lifecycle(created=0, started=1)))
    curr = _run(_phase('root', _lifecycle(created=0, started=1),
                       children=[_phase('child', _lifecycle(created=2))]))  # child newly appears, CREATED

    assert _synthesize_events(prev, curr) == []  # root unchanged; a CREATED child is neither phase nor lifecycle


def test_first_sighting_reports_net_stage_not_history():
    curr = _run(_phase('root', _lifecycle(created=0, started=1, ended=5)))

    lifecycle = [e for e in _synthesize_events(None, curr) if isinstance(e, InstanceLifecycleEvent)]

    assert [e.new_stage for e in lifecycle] == [Stage.ENDED]  # net stage only, no replayed CREATED/RUNNING


def test_child_phase_transition_yields_phase_event_only():
    root_lc = _lifecycle(created=0, started=1)
    prev = _run(_phase('root', root_lc, children=[_phase('child', _lifecycle(created=2))]))
    curr = _run(_phase('root', root_lc, children=[_phase('child', _lifecycle(created=2, started=3))]))

    events = _synthesize_events(prev, curr)

    assert not any(isinstance(e, InstanceLifecycleEvent) for e in events)  # child is not the root
    [event] = events
    assert (event.phase_id, event.is_root_phase, event.new_stage) == ('child', False, Stage.RUNNING)


def test_status_change_yields_status_event_with_status_timestamp():
    root = _phase('root', _lifecycle(created=0, started=1))
    prev = _run(root)
    curr = _run(root, status=_status('working', at=3))

    [event] = _synthesize_events(prev, curr)

    assert isinstance(event, InstanceStatusEvent)
    assert event.timestamp == _ts(3)


def test_unchanged_status_yields_no_event():
    root = _phase('root', _lifecycle(created=0, started=1))
    prev = _run(root, status=_status('working', at=3))
    curr = _run(root, status=_status('working', at=3))  # equal by value, distinct object

    assert _synthesize_events(prev, curr) == []


def test_events_are_ordered_by_timestamp():
    prev = _run(_phase('root', _lifecycle(created=0)))
    curr = _run(_phase('root', _lifecycle(created=0, started=1, ended=5)), status=_status('working', at=3))

    timestamps = [e.timestamp for e in _synthesize_events(prev, curr)]

    assert timestamps == sorted(timestamps)
    assert _ts(3) in timestamps  # the status event interleaves between the RUNNING and ENDED transitions
