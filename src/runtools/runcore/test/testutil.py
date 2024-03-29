from multiprocessing import Queue
from pathlib import Path

import tomli_w

from runtools.runcore import paths
from runtools.runcore.job import JobRun, InstanceTransitionObserver


def create_test_config(config):
    create_custom_test_config(paths.CONFIG_FILE, config)


def create_custom_test_config(filename, config):
    path = _custom_test_config_path(filename)
    with open(path, 'wb') as outfile:
        tomli_w.dump(config, outfile)
    return path


def remove_test_config():
    remove_custom_test_config(paths.CONFIG_FILE)


def remove_custom_test_config(filename):
    config = _custom_test_config_path(filename)
    if config.exists():
        config.unlink()


def _test_config_path() -> Path:
    return _custom_test_config_path(paths.CONFIG_FILE)


def _custom_test_config_path(filename) -> Path:
    return Path.cwd() / filename


class StateWaiter:
    """
    This class is used for waiting for execution states of job executed in different process.

    See :class:`PutStateToQueueObserver`

    Attributes:
        state_queue The process must put execution states into this queue
    """

    def __init__(self):
        self.state_queue = Queue()

    def wait_for_state(self, state, timeout=1):
        while True:
            if state == self.state_queue.get(timeout=timeout):
                return


class PutPhaseToQueueObserver(InstanceTransitionObserver):
    """
    This observer puts execution states into the provided queue. With multiprocessing queue this can be used for sending
    execution states into the parent process.

    See :class:`StateWaiter`
    """

    def __init__(self, queue):
        self.queue = queue

    def new_instance_phase(self, job_run: JobRun, previous_phase, new_phase, changed):
        self.queue.put_nowait(new_phase)
