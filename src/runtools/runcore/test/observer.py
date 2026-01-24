"""
Observer implementations for testing purposes.

Observed notifications are stored as events in indexed sequence.
"""

import logging
from queue import Queue
from typing import List

from runtools.runcore.job import JobRun, InstanceOutputObserver, InstanceOutputEvent, \
    InstanceLifecycleObserver, InstanceLifecycleEvent
from runtools.runcore.run import Stage

log = logging.getLogger(__name__)


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

    def instance_lifecycle_update(self, event: InstanceLifecycleEvent):
        self.events.append(event)

    @property
    def job_runs(self) -> List[JobRun]:
        return [e.job_run for e in self.events]

    @property
    def stages(self) -> List[Stage]:
        return [e.new_stage for e in self.events]


class TestOutputObserver(InstanceOutputObserver):
    __test__ = False  # To tell pytest it isn't a test class

    def __init__(self):
        self.outputs = []

    def instance_output_update(self, event: InstanceOutputEvent):
        self.outputs.append(event)

    @property
    def last_message(self):
        return self.outputs[-1].output_line.message
