from datetime import datetime, timedelta
from typing import Type, Optional

from runtools.runcore import util
from runtools.runcore.job import JobInstance, JobRun, InstanceTransitionObserver, \
    InstanceOutputObserver
from runtools.runcore.output import InMemoryOutput, Mode
from runtools.runcore.run import PhaseRun, TerminationInfo, Lifecycle, RunState, PhaseInfo, Run, \
    TerminationStatus, RunFailure, Phase, P, JobInstanceMetadata, PhaseKey
from runtools.runcore.test.run import FakePhaser
from runtools.runcore.track import TaskTrackerMem
from runtools.runcore.util.observer import ObservableNotification, DEFAULT_OBSERVER_PRIORITY

INIT = PhaseKey('init', 'id')
APPROVAL = PhaseKey('approval', 'id')
PROGRAM = PhaseKey('program', 'id')
TERM = PhaseKey('term', 'id')


class FakePhase(Phase):

    def __init__(self, phase_key, run_state):
        super().__init__(phase_key.type, phase_key.id, run_state)
        self.approved = False
        self.ran = False
        self.stopped = False

    def approve(self):
        self.approved = True

    @property
    def stop_status(self):
        return TerminationStatus.STOPPED

    def run(self, run_ctx):
        self.ran = True

    def stop(self):
        self.stopped = True


class AbstractBuilder:
    current_ts = datetime.utcnow().replace(microsecond=0)

    def __init__(self, job_id, run_id=None, system_params=None, user_params=None):
        instance_id = util.unique_timestamp_hex()
        run_id = run_id or instance_id
        self.metadata = JobInstanceMetadata(job_id, run_id, instance_id, system_params or {}, user_params or {})
        self.termination_info = None


class FakeJobInstance(JobInstance):

    def __init__(self, job_id, phaser, lifecycle, *,
                 run_id=None, instance_id_gen=util.unique_timestamp_hex, **user_params):
        inst_id = instance_id_gen()
        parameters = {}  # TODO
        self._metadata = JobInstanceMetadata(job_id, run_id or inst_id, inst_id, parameters, user_params)
        self.phaser = phaser
        self.lifecycle = lifecycle
        self.output = InMemoryOutput()
        self._task_tracker = None or TaskTrackerMem()
        self.transition_notification = ObservableNotification[InstanceTransitionObserver]()
        self.output_notification = ObservableNotification[InstanceOutputObserver]()

        phaser.transition_hook = self._transition_hook

    @property
    def instance_id(self):
        return self._metadata.instance_id

    @property
    def metadata(self):
        return self._metadata

    @property
    def task_tracker(self):
        return self._task_tracker

    @property
    def current_phase(self):
        return self.phaser.current_phase

    @property
    def phases(self):
        return self.phaser.phases

    def get_phase(self, phase_type: Type[P], phase_name: str) -> Optional[P]:
        return self.phaser.get_phase(phase_type, phase_name)

    def job_run_info(self) -> JobRun:
        return JobRun(self.metadata, self.phaser.run_info(), self._task_tracker.tracked_task)

    def fetch_output(self, mode=Mode.HEAD, *, lines=0):
        return self.output.fetch(mode, lines=lines)

    def run(self):
        pass

    def stop(self):
        """
        Cancel not yet started execution or stop started execution.
        Due to synchronous design there is a small window when an execution can be stopped before it is started.
        All execution implementations must cope with such scenario.
        """
        self.phaser.stop()

    def interrupted(self):
        """
        Cancel not yet started execution or interrupt started execution.
        Due to synchronous design there is a small window when an execution can be interrupted before it is started.
        All execution implementations must cope with such scenario.
        """
        self.phaser.stop()  # TODO Interrupt

    def wait_for_transition(self, phase_name=None, run_state=RunState.NONE, *, timeout=None):
        return self.phaser.wait_for_transition(phase_name, run_state, timeout=timeout)

    def add_observer_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
        self.transition_notification.add_observer(observer, priority)

    def remove_observer_transition(self, callback):
        self.transition_notification.remove_observer(callback)

    def _transition_hook(self, old_phase: PhaseRun, new_phase: PhaseRun, ordinal):
        job_run = JobRun(self.metadata, self.phaser.run_info(), self._task_tracker.tracked_task)
        self.transition_notification.observer_proxy.new_instance_phase(job_run, old_phase, new_phase, ordinal)

    def add_observer_output(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self.output_notification.add_observer(observer, priority)

    def remove_observer_output(self, observer):
        self.output_notification.remove_observer(observer)

    @property
    def prioritized_transition_observers(self):
        return self.transition_notification.prioritized_observers


class FakeJobInstanceBuilder(AbstractBuilder):

    def __init__(self, job_id='j1', run_id=None, system_params=None, user_params=None):
        super().__init__(job_id, run_id, system_params, user_params)
        self.phases = []

    def add_phase(self, key, run_state):
        self.phases.append(FakePhase(key, run_state))
        return self

    def build(self) -> FakeJobInstance:
        lifecycle = Lifecycle()
        phaser = FakePhaser(self.phases, lifecycle)
        return FakeJobInstance(self.metadata.entity_id, phaser, lifecycle, run_id=self.metadata.run_id,
                               **self.metadata.user_params)


class TestJobRunBuilder(AbstractBuilder):

    def __init__(self, job_id='j1', run_id=None, system_params=None, user_params=None):
        super().__init__(job_id, run_id, system_params, user_params)
        self.phases = []
        self.tracker = TaskTrackerMem()

    def add_phase(self, key, state, start=None, end=None):
        if not start:
            start = super().current_ts
            end = start + timedelta(minutes=1)

        if key != INIT and not self.phases:
            self.add_phase(INIT, RunState.CREATED, start - timedelta(minutes=2), start - timedelta(minutes=1))

        phase_run = PhaseRun(key, state, start, end)
        self.phases.append(phase_run)
        return self

    def with_termination_info(self, status, time, failure=None):
        self.termination_info = TerminationInfo(status, time, failure)
        return self

    def build(self):
        meta = (PhaseInfo('exec', 'p1', RunState.EXECUTING),)
        lifecycle = Lifecycle(*self.phases)
        run = Run(meta, lifecycle, self.termination_info)
        return JobRun(self.metadata, run, self.tracker.tracked_task)


def ended_run(job_id, run_id='r1', *, offset_min=0, term_status=TerminationStatus.COMPLETED, created=None,
              completed=None):
    start_time = datetime.utcnow().replace(microsecond=0) + timedelta(minutes=offset_min)

    builder = TestJobRunBuilder(job_id, run_id, user_params={'name': 'value'})

    builder.add_phase(INIT, RunState.CREATED, created or start_time,
                      start_time + timedelta(minutes=1))
    builder.add_phase(APPROVAL, RunState.EXECUTING, start_time + timedelta(minutes=1),
                      start_time + timedelta(minutes=2))
    builder.add_phase(PROGRAM, RunState.EXECUTING, start_time + timedelta(minutes=2),
                      start_time + timedelta(minutes=3))
    builder.add_phase(TERM, RunState.ENDED, completed or start_time + timedelta(minutes=3), None)

    failure = RunFailure('err1', 'reason') if term_status == TerminationStatus.FAILED else None
    builder.with_termination_info(term_status, start_time + timedelta(minutes=3), failure)

    return builder.build()
