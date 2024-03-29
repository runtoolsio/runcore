from threading import Condition
from typing import Iterable, Optional, Callable

from runtools.runcore import util
from runtools.runcore.common import InvalidStateError
from runtools.runcore.run import Phase, PhaseRun, AbstractPhaser, TerminationInfo, Run, RunState, InitPhase, \
    TerminalPhase, TerminationStatus
from runtools.runcore.util import utc_now


class FakePhaser(AbstractPhaser):

    def __init__(self, phases: Iterable[Phase], lifecycle, *, timestamp_generator=util.utc_now):
        super().__init__(phases, timestamp_generator=timestamp_generator)
        self.phases_list = list(phases)
        self.lifecycle = lifecycle
        self.termination: Optional[TerminationInfo] = None
        self._current_phase_index = -1
        self._condition = Condition()

    def run_info(self) -> Run:
        phases = tuple(p.info() for p in self._key_to_phase.values())
        return Run(phases, self.lifecycle, self.termination)

    def prime(self):
        if self._current_phase_index != -1:
            raise InvalidStateError("Primed already")
        self._next_phase(InitPhase())

    def next_phase(self):
        self._current_phase_index += 1
        if self._current_phase_index >= len(self.phases_list):
            self._next_phase(TerminalPhase())
            self.termination = TerminationInfo(TerminationStatus.COMPLETED, utc_now())
        else:
            self._next_phase(self.phases_list[self._current_phase_index])

    def _next_phase(self, phase):
        """
        Impl note: The execution must be guarded by the phase lock (except terminal phase)
        """
        self.lifecycle.add_phase_run(PhaseRun(phase.key, phase.run_state, self._timestamp_generator()))
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
        self._next_phase(TerminalPhase())
        self.termination = TerminationInfo(TerminationStatus.STOPPED, utc_now())
