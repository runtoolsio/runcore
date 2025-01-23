"""
This module provides the logic required for the locking mechanisms used by specific parts of the library.
TODO: Move to runjob?
"""

import logging
import random
import time

import portalocker

from runtools.runcore import paths
from runtools.runcore.common import InvalidStateError

log = logging.getLogger(__name__)


class FileLock:
    """
    A file-based lock implementation using Portalocker.
    The lock can be reused within the same thread but cannot be shared between threads.
    """

    def __init__(self, lock_file, *, timeout=10, max_check_time=0.05):
        self.lock_file = lock_file
        self.timeout = timeout
        self.max_check_time = max_check_time
        self._file_lock = None
        self._start_time = None

    def _check_interval(self):
        """
        Determines the interval between lock acquisition attempts. Using a constant interval could lead
        to lock starvation when multiple instances try to acquire the lock at the same time.

        Returns:
             int: A random interval (in seconds) between 10 milliseconds and the max check time.
        """
        # Convert to integers for randint by rounding max time to milliseconds
        return random.randint(10, int(self.max_check_time * 1000)) / 1000

    def acquire(self):
        """
        Manually acquire the lock.

        Raises:
            RuntimeError: If the lock has already been used or acquired
        """
        if self._file_lock:
            raise InvalidStateError("Lock is already acquired")

        self._file_lock = portalocker.Lock(self.lock_file, timeout=self.timeout, check_interval=self._check_interval())

        self._start_time = time.time()
        self._file_lock.acquire()
        log.debug(f'event=[file_lock_acquired] wait=[{(time.time() - self._start_time) * 1000 :.2f} ms]')

    def release(self):
        """
        Manually release the lock.

        Raises:
            RuntimeError: If the lock hasn't been acquired
        """
        if not self._file_lock:
            raise InvalidStateError("Lock is not acquired")

        self._file_lock.release()
        self._file_lock = None

        lock_time_ms = (time.time() - self._start_time) * 1000
        log.debug(f'event=[lock_released] locked=[{lock_time_ms:.2f} ms]')

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


def default_queue_lock():
    return FileLock(paths.lock_path('state0.lock', True))


def default_file_lock_factory(*, timeout=10, max_check_time=0.05):
    def factory(lock_file):
        return FileLock(lock_file, timeout=timeout, max_check_time=max_check_time)

    return factory
