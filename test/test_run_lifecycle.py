import datetime

import pytest

from runtoolsio.runcore.run import PhaseRun, PhaseNames, RunState, Lifecycle
from runtoolsio.runcore.util import utc_now

PENDING = "PENDING"
EXECUTING = "EXECUTING"


@pytest.fixture
def sut() -> Lifecycle:
    # Initial transition
    base = datetime.datetime(2023, 1, 1)
    init_transition = PhaseRun(PhaseNames.INIT, RunState.CREATED, base)
    lifecycle = Lifecycle(init_transition)

    # 10 minutes after initialization, it goes to PENDING state
    lifecycle.add_phase_run(PhaseRun(PENDING, RunState.PENDING, base + datetime.timedelta(minutes=10)))
    # 20 minutes after initialization, it goes to EXECUTING state
    lifecycle.add_phase_run(PhaseRun(EXECUTING, RunState.EXECUTING, base + datetime.timedelta(minutes=20)))
    # 50 minutes after initialization, it terminates
    lifecycle.add_phase_run(PhaseRun(PhaseNames.TERMINAL, RunState.ENDED, base + datetime.timedelta(minutes=50)))

    return lifecycle


def test_phases(sut):
    assert sut.phases == [
        PhaseNames.INIT,
        PENDING,
        EXECUTING,
        PhaseNames.TERMINAL
    ]
    assert sut.current_phase_name == PhaseNames.TERMINAL
    assert sut.phase_count == 4


def test_ordinal(sut):
    assert sut.get_ordinal(PENDING) == 2


def test_transitions(sut):
    assert sut.phase_started_at(EXECUTING) == datetime.datetime(2023, 1, 1, 0, 20)
    assert sut.last_transition_at == datetime.datetime(2023, 1, 1, 0, 50)


def test_states(sut):
    assert sut.state_first_transition_at(RunState.EXECUTING) == datetime.datetime(2023, 1, 1, 0, 20)
    assert sut.state_last_transition_at(RunState.ENDED) == datetime.datetime(2023, 1, 1, 0, 50)
    assert sut.contains_state(RunState.CREATED)
    assert not sut.contains_state(RunState.IN_QUEUE)
    assert sut.created_at == datetime.datetime(2023, 1, 1, 0, 0)
    assert sut.executed_at == datetime.datetime(2023, 1, 1, 0, 20)
    assert sut.ended_at == datetime.datetime(2023, 1, 1, 0, 50)


def test_current_and_previous_phase(sut):
    assert sut.current_phase_name == PhaseNames.TERMINAL
    assert sut.previous_phase_name == EXECUTING


def test_phase_run(sut):
    init_phase_run = sut.phase_run(PhaseNames.INIT)
    assert init_phase_run.started_at == datetime.datetime(2023, 1, 1)
    assert init_phase_run.ended_at == datetime.datetime(2023, 1, 1, 0, 10)
    assert init_phase_run.run_time == datetime.timedelta(minutes=10)


def test_termination(sut):
    assert sut.is_ended
    assert not Lifecycle(PhaseRun(PhaseNames.INIT, RunState.CREATED, utc_now())).is_ended


def test_run_time(sut):
    # 50min - 20min based on create_sut()
    assert sut.total_executing_time == datetime.timedelta(minutes=30)


def test_phases_between(sut):
    assert sut.phases_between(PENDING, EXECUTING) == [PENDING, EXECUTING]
    assert (sut.phases_between(PENDING, PhaseNames.TERMINAL)
            == [PENDING, EXECUTING, PhaseNames.TERMINAL])
    assert sut.phases_between(PENDING, PENDING) == [PENDING]
    assert sut.phases_between(EXECUTING, PENDING) == []
    assert sut.phases_between(PENDING, 'Not contained') == []
    assert sut.phases_between('Not contained', PENDING) == []
