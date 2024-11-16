from threading import Condition
from typing import Iterable, Optional, Callable

from runtools.runcore import util
from runtools.runcore.common import InvalidStateError
from runtools.runcore.run import Phase, PhaseRun, TerminationInfo, Run, RunState, \
    TerminationStatus, PhaseInfo
from runtools.runcore.util import utc_now


class TestPhase(Phase):

    def __init__(self, phase_id, run_state):
        self._phase_id = phase_id
        self._run_state = run_state
        self.approved = False
        self.ran = False
        self.stopped = False

    @property
    def id(self):
        return self._phase_id

    @property
    def type(self) -> str:
        return "TEST"

    @property
    def run_state(self) -> RunState:
        return self._run_state

    @property
    def name(self):
        return self._phase_id

    def approve(self):
        self.approved = True

    @property
    def stop_status(self):
        return TerminationStatus.STOPPED

    def run(self, run_ctx):
        self.ran = True

    def stop(self):
        self.stopped = True


class FakePhaser:

    def __init__(self, phases: Iterable[Phase], lifecycle, *, timestamp_generator=util.utc_now):
        self.phases = list(phases)
        self._timestamp_generator = timestamp_generator
        self.lifecycle = lifecycle
        self.transition_hook: Optional[Callable[[PhaseRun, PhaseRun, int], None]] = None
        self.output_hook: Optional[Callable[[PhaseInfo, str, bool], None]] = None

        self.termination: Optional[TerminationInfo] = None
        self._current_phase_index = -1
        self._condition = Condition()

    def get_phase(self, phase_id, phase_type: str = None):
        """
        Retrieve a phase by its ID and optionally verify its type.

        Args:
            phase_id: The ID of the phase to find
            phase_type: Optional type to verify against the found phase

        Returns:
            The matching Phase object

        Raises:
            ValueError: If no phase with the given ID exists or if type verification fails
        """
        for phase in self.phases:
            if phase.id == phase_id:
                if phase_type and phase.type != phase_type:
                    raise ValueError(f"Phase '{phase_id}' has type '{phase.type}' but '{phase_type}' was expected")
                return phase

        raise KeyError(f"Phase '{phase_id}' not found")

    def run_info(self) -> Run:
        phases = tuple(p.info() for p in self.phases)
        return Run(phases, self.lifecycle, self.termination)

    def prime(self):
        if self._current_phase_index != -1:
            raise InvalidStateError("Primed already")
        self._next_phase(TestPhase('init', RunState.CREATED))

    def next_phase(self):
        self._current_phase_index += 1
        if self._current_phase_index >= len(self.phases):
            self._next_phase(TestPhase('term', RunState.ENDED))
            self.termination = TerminationInfo(TerminationStatus.COMPLETED, utc_now())
        else:
            self._next_phase(self.phases[self._current_phase_index])

    def _next_phase(self, phase):
        """
        Impl note: The execution must be guarded by the phase lock (except terminal phase)
        """
        self.lifecycle.add_phase_run(PhaseRun(phase.id, phase.run_state, self._timestamp_generator()))
        if self.transition_hook:
            self.execute_transition_hook_safely(self.transition_hook)
        with self._condition:
            self._condition.notify_all()

    def execute_transition_hook_safely(self, transition_hook: Optional[Callable[[PhaseRun, PhaseRun, int], None]]):
        transition_hook(self.lifecycle.previous_run, self.lifecycle.current_run, self.lifecycle.phase_count)

    def wait_for_transition(self, phase_name=None, run_state=RunState.NONE, *, timeout=None):
        with self._condition:
            while True:
                for run in self.lifecycle.phase_runs:
                    if run.id == phase_name or run.run_state == run_state:
                        return True

                if not self._condition.wait(timeout):
                    return False
                if not phase_name and not run_state:
                    return True

    def run(self):
        pass

    def stop(self):
        self._next_phase(TestPhase('term', RunState.ENDED))
        self.termination = TerminationInfo(TerminationStatus.STOPPED, utc_now())
