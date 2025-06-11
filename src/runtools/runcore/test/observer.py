"""
:class:`InstanceStateObserver` implementation for testing purposes.

Observed notifications are stored as events in indexed sequence.
The wait_for_* methods allow to wait for a specific event to be observed. This can be used for synchronization
between job executing thread and testing thread.
"""

import logging
from queue import Queue
from threading import Condition
from typing import List, Callable

from runtools.runcore.job import JobRun, InstanceOutputObserver, InstanceOutputEvent, \
    InstanceLifecycleObserver, InstanceLifecycleEvent
from runtools.runcore.run import Stage

log = logging.getLogger(__name__)

type_id = 'test'


class GenericObserver:

    def __init__(self):
        self.updates = Queue()

    def __getattr__(self, name):
        def method(*args):
            self.updates.put_nowait((name, args))

        return method

    def __call__(self, *args):
        self.updates.put_nowait(("__call__", args))


class TestLifecycleObserver(InstanceLifecycleObserver):
    __test__ = False  # To tell pytest it isn't a test class

    def __init__(self):
        self.events: List[InstanceLifecycleEvent] = []
        self.completion_lock = Condition()

    def instance_lifecycle_update(self, event: InstanceLifecycleEvent):
        self.events.append(event)
        self._release_state_waiter()

    @property
    def job_runs(self) -> List[JobRun]:
        """
        :return: job of the last event
        """
        return [e.job_run for e in self.events]

    @property
    def stages(self) -> List[Stage]:
        return [e.new_stage for e in self.events]

    @property
    def last_state(self):
        return self.events[-1][2].run_state

    def last_job_state(self, job_id):
        """
        :return: last state of the specified job
        """
        return next(e[2].run_state for e in reversed(self.events) if e[0].job_id == job_id)

    def _release_state_waiter(self):
        with self.completion_lock:
            self.completion_lock.notify()  # Support only one-to-one thread sync to keep things simple

    def wait_for_state(self, exec_state, timeout: float = 1) -> bool:
        """
        Wait for receiving notification with the specified state

        :param exec_state: Waits for the state specified by this parameter
        :param timeout: Waiting interval in seconds
        :return: True when specified state received False when timed out
        """
        return self._wait_for_state_condition(lambda: exec_state in (e[2].run_state for e in self.events), timeout)

    def wait_for_ended_stage(self, timeout: float = 1) -> bool:
        """
        Wait for receiving notification with a terminal state

        :param timeout: Waiting interval in seconds
        :return: True when terminal state received False when timed out
        """
        terminal_condition = lambda: any((e for e in self.events if e.new_stage == Stage.ENDED))
        return self._wait_for_state_condition(terminal_condition, timeout)

    def _wait_for_state_condition(self, state_condition: Callable[[], bool], timeout: float):
        with self.completion_lock:
            return self.completion_lock.wait_for(state_condition, timeout)


class TestOutputObserver(InstanceOutputObserver):
    __test__ = False  # To tell pytest it isn't a test class

    def __init__(self):
        self.outputs = []

    def new_instance_output(self, event: InstanceOutputEvent):
        self.outputs.append(event)

    @property
    def last_text(self):
        return self.outputs[-1].output_line.text
