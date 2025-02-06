import datetime
import pytest

from runtools.runcore.run import RunLifecycle, Stage, TerminationInfo, TerminationStatus
from runtools.runcore.util import utc_now


@pytest.fixture
def sut() -> RunLifecycle:
    base = datetime.datetime(2023, 1, 1)

    return RunLifecycle(
        created_at=base,
        started_at=base + datetime.timedelta(minutes=20),
        termination=TerminationInfo(
            status=TerminationStatus.COMPLETED,
            terminated_at=base + datetime.timedelta(minutes=50)
        )
    )


def test_stages(sut):
    created_only = RunLifecycle(created_at=utc_now())
    assert created_only.stage == Stage.CREATED

    running = RunLifecycle(
        created_at=utc_now(),
        started_at=utc_now()
    )
    assert running.stage == Stage.RUNNING

    assert sut.stage == Stage.ENDED


def test_transitions(sut):
    assert sut.created_at == datetime.datetime(2023, 1, 1, 0, 0)
    assert sut.started_at == datetime.datetime(2023, 1, 1, 0, 20)
    assert sut.termination.terminated_at == datetime.datetime(2023, 1, 1, 0, 50)
    assert sut.last_transition_at == datetime.datetime(2023, 1, 1, 0, 50)


def test_run_time(sut):
    # Total run time should be from started_at to terminated_at (30 minutes)
    assert sut.total_run_time == datetime.timedelta(minutes=30)

    # Lifecycle that hasn't started should have no run time
    created_only = RunLifecycle(created_at=utc_now())
    assert created_only.total_run_time is None

    # Running but not terminated should have no run time yet
    running = RunLifecycle(created_at=utc_now(), started_at=utc_now())
    assert running.total_run_time is None


def test_state_checks(sut):
    assert not sut.is_running()
    assert sut.is_ended()
    assert sut.completed_successfully()

    running = RunLifecycle(created_at=utc_now(), started_at=utc_now())
    assert running.is_running()
    assert not running.is_ended()
    assert not running.completed_successfully()

    failed = RunLifecycle(
        created_at=utc_now(),
        started_at=utc_now(),
        termination=TerminationInfo(
            status=TerminationStatus.FAILED,
            terminated_at=utc_now()
        )
    )
    assert not failed.is_running()
    assert failed.is_ended()
    assert not failed.completed_successfully()


def test_serialization(sut):
    serialized = sut.serialize()
    deserialized = RunLifecycle.deserialize(serialized)

    assert deserialized.created_at == sut.created_at
    assert deserialized.started_at == sut.started_at
    assert deserialized.termination.status == sut.termination.status
    assert deserialized.termination.terminated_at == sut.termination.terminated_at

    # Test partial serialization (only created_at)
    created_only = RunLifecycle(created_at=utc_now())
    serialized = created_only.serialize()
    assert 'started_at' not in serialized
    assert 'termination' not in serialized
    deserialized = RunLifecycle.deserialize(serialized)
    assert deserialized.created_at == created_only.created_at
    assert deserialized.started_at is None
    assert deserialized.termination is None
