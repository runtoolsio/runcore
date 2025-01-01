from datetime import datetime, timedelta, UTC
from threading import Condition
from typing import Iterable, Optional

from runtools.runcore import util
from runtools.runcore.common import InvalidStateError
from runtools.runcore.job import JobInstance, JobRun, InstanceTransitionObserver, \
    InstanceOutputObserver, JobInstanceMetadata
from runtools.runcore.output import Output, Mode
from runtools.runcore.run import Phase, PhaseRun, TerminationInfo, Run, RunState, \
    TerminationStatus, PhaseInfo, Fault, Lifecycle, control_api
from runtools.runcore.util import utc_now
from runtools.runcore.util.observer import ObservableNotification, DEFAULT_OBSERVER_PRIORITY

INIT = 'init'
APPROVAL = 'approval'
PROGRAM = 'program'
TERM = 'term'


class FakePhase(Phase):

    def __init__(self, phase_id, run_state):
        self._phase_id = phase_id
        self._run_state = run_state
        self._approved = False
        self.ran = False
        self.stopped = False

    @property
    def id(self):
        return self._phase_id

    @property
    def type(self) -> str:
        return "FAKE"

    @property
    def run_state(self) -> RunState:
        return self._run_state

    @property
    def name(self):
        return self._phase_id

    @control_api
    def approve(self):
        self._approved = True

    @control_api
    @property
    def approved(self):
        return self._approved

    @property
    def stop_status(self):
        return TerminationStatus.STOPPED

    def run(self, env, run_ctx):
        self.ran = True

    def stop(self):
        self.stopped = True


class BasicOutput(Output):

    def __init__(self):
        self.lines = []

    def add_line(self, output_line):
        self.lines.append(output_line)

    def tail(self, mode: Mode = Mode.TAIL, max_lines: int = 0):
        return self.lines


class FakeJobInstance(JobInstance):

    def __init__(self, job_id, phases: Iterable[Phase], lifecycle, *,
                 run_id=None, instance_id_gen=util.unique_timestamp_hex,
                 timestamp_generator=util.utc_now, **user_params):
        inst_id = instance_id_gen()
        parameters = {}  # TODO
        self._metadata = JobInstanceMetadata(job_id, run_id or inst_id, inst_id, parameters, user_params)

        self._phases = list(phases)
        self._timestamp_generator = timestamp_generator
        self.lifecycle = lifecycle
        self.termination: Optional[TerminationInfo] = None
        self._current_phase_index = -1
        self._condition = Condition()

        self._output = BasicOutput()
        self._status_tracker = None
        self.transition_notification = ObservableNotification[InstanceTransitionObserver]()
        self.output_notification = ObservableNotification[InstanceOutputObserver]()

    @property
    def instance_id(self):
        return self._metadata.instance_id

    @property
    def metadata(self):
        return self._metadata

    @property
    def status_tracker(self):
        return self._status_tracker

    @property
    def current_phase(self):
        if 0 <= self._current_phase_index < len(self.phases):
            return self.phases[self._current_phase_index].id
        return None

    @property
    def phases(self):
        return self._phases

    @property
    def output(self):
        return self._output

    def get_phase(self, phase_id: str, phase_type: str = None) -> Phase:
        """
        Retrieve a phase by its ID and optionally verify its type.
        """
        for phase in self.phases:
            if phase.id == phase_id:
                if phase_type and phase.type != phase_type:
                    raise ValueError(f"Phase '{phase_id}' has type '{phase.type}' but '{phase_type}' was expected")
                return phase
        raise KeyError(f"Phase '{phase_id}' not found")

    def get_phase_control(self, phase_id: str, phase_type: str = None):
        return self.get_phase(phase_id, phase_type).control

    def snapshot(self) -> JobRun:
        return JobRun(self.metadata, self._create_run_info(), None,
                      self._status_tracker.to_status() if self._status_tracker else None)

    def _create_run_info(self) -> Run:
        phases = tuple(p.info for p in self.phases)
        return Run(phases, self.lifecycle, self.termination)

    def prime(self):
        if self._current_phase_index != -1:
            raise InvalidStateError("Primed already")
        self._next_phase(FakePhase('init', RunState.CREATED))

    def next_phase(self):
        self._current_phase_index += 1
        if self._current_phase_index >= len(self.phases):
            self._next_phase(FakePhase('term', RunState.ENDED))
            self.termination = TerminationInfo(TerminationStatus.COMPLETED, utc_now())
        else:
            self._next_phase(self.phases[self._current_phase_index])

    def _next_phase(self, phase):
        """
        Impl note: The execution must be guarded by the phase lock (except terminal phase)
        """
        phase_run = PhaseRun(phase.id, phase.run_state, self._timestamp_generator())
        self.lifecycle.add_phase_run(phase_run)

        # Call transition hook through the notification system
        old_phase = self.lifecycle.previous_run
        new_phase = self.lifecycle.current_run
        job_run = JobRun(self.metadata, self._create_run_info(), None,
                         self._status_tracker.to_status() if self._status_tracker else None)
        self.transition_notification.observer_proxy.new_instance_phase(
            job_run, old_phase, new_phase, self.lifecycle.phase_count)

        with self._condition:
            self._condition.notify_all()

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
        self._next_phase(FakePhase('term', RunState.ENDED))
        self.termination = TerminationInfo(TerminationStatus.STOPPED, utc_now())

    def interrupted(self):
        """
        Cancel not yet started execution or interrupt started execution.
        """
        self.stop()  # TODO Interrupt

    def add_observer_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
        self.transition_notification.add_observer(observer, priority)

    def remove_observer_transition(self, callback):
        self.transition_notification.remove_observer(callback)

    def add_observer_output(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self.output_notification.add_observer(observer, priority)

    def remove_observer_output(self, observer):
        self.output_notification.remove_observer(observer)

    @property
    def prioritized_transition_observers(self):
        return self.transition_notification.prioritized_observers


class FakeJobInstanceBuilder:

    def __init__(self, job_id='j1', run_id=None, system_params=None, user_params=None):
        instance_id = util.unique_timestamp_hex()
        run_id = run_id or instance_id
        self.metadata = JobInstanceMetadata(job_id, run_id, instance_id, system_params or {}, user_params or {})
        self.phases = []

    def add_phase(self, phase_id, run_state):
        self.phases.append(FakePhase(phase_id, run_state))
        return self

    def build(self) -> FakeJobInstance:
        lifecycle = Lifecycle()
        return FakeJobInstance(
            self.metadata.job_id,
            self.phases,
            lifecycle,
            run_id=self.metadata.run_id,
            **self.metadata.user_params
        )


class TestJobRunBuilder:

    def __init__(self, job_id='j1', run_id=None, system_params=None, user_params=None):
        instance_id = util.unique_timestamp_hex()
        run_id = run_id or instance_id
        self.metadata = JobInstanceMetadata(job_id, run_id, instance_id, system_params or {}, user_params or {})
        self.phase_runs = []
        self.tracker = None
        self.termination_info = None
        self.current_ts = datetime.now(UTC).replace(microsecond=0)

    def add_phase(self, phase_id, state, start=None, end=None):
        if not start:
            start = self.current_ts
            end = start + timedelta(minutes=1)

        if phase_id != INIT and not self.phase_runs:
            self.add_phase(INIT, RunState.CREATED, start - timedelta(minutes=2), start - timedelta(minutes=1))

        phase_run = PhaseRun(phase_id, state, start, end)
        self.phase_runs.append(phase_run)
        return self

    def with_termination_info(self, status, time, failure=None):
        self.termination_info = TerminationInfo(status, time, failure)
        return self

    def build(self):
        meta = [PhaseInfo(p.phase_id, 'TEST', p.run_state) for p in self.phase_runs]
        lifecycle = Lifecycle(*self.phase_runs)
        run = Run(tuple(meta), lifecycle, self.termination_info)
        return JobRun(self.metadata, run, None, self.tracker.to_status() if self.tracker else None)


def ended_run(job_id, run_id='r1', *, offset_min=0, term_status=TerminationStatus.COMPLETED, created=None,
              completed=None) -> JobRun:
    start_time = datetime.now().replace(microsecond=0) + timedelta(minutes=offset_min)

    builder = TestJobRunBuilder(job_id, run_id, user_params={'name': 'value'})

    builder.add_phase(INIT, RunState.CREATED, created or start_time,
                      start_time + timedelta(minutes=1))
    builder.add_phase(APPROVAL, RunState.EXECUTING, start_time + timedelta(minutes=1),
                      start_time + timedelta(minutes=2))
    builder.add_phase(PROGRAM, RunState.EXECUTING, start_time + timedelta(minutes=2),
                      start_time + timedelta(minutes=3))
    builder.add_phase(TERM, RunState.ENDED, completed or start_time + timedelta(minutes=3), None)

    failure = Fault('err1', 'reason') if term_status == TerminationStatus.FAILED else None
    builder.with_termination_info(term_status, start_time + timedelta(minutes=3), failure)

    return builder.build()
