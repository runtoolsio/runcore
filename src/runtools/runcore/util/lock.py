"""
This module provides the logic required for the locking mechanisms used by specific parts of the library.
TODO: Move to runjob?
"""

import logging
import random
import re
import time
import weakref
from threading import RLock
from typing import Protocol, runtime_checkable

import portalocker

from runtools.runcore import paths
from runtools.runcore.err import InvalidStateError

log = logging.getLogger(__name__)


@runtime_checkable
class Lock(Protocol):
    """A named exclusive mutex. Non-reentrant; not shareable between threads."""

    def acquire(self): ...

    def release(self): ...

    def __enter__(self): ...

    def __exit__(self, exc_type, exc_val, exc_tb): ...


@runtime_checkable
class LockProvider(Protocol):
    """Provides named exclusive locks for job coordination within one environment.

    Guarantees:
     - Env scope: the same ``lock_id`` names the same lock for every process attached to
       the environment; different environments never contend.
     - IDs are arbitrary strings; encoding them to the backing medium (file name,
       advisory key) is the implementation's responsibility.
     - Crash release: a lock held by a dead process is released by the medium
       (flock: kernel, advisory: session end) without any cleanup sweep.
    """

    def lock(self, lock_id: str) -> Lock: ...


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
        wait_time_ms = (time.time() - self._start_time) * 1000
        log.debug("Lock acquired", extra={"file": str(self.lock_file), "wait_ms": round(wait_time_ms, 2)})

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
        log.debug("Lock released", extra={"file": str(self.lock_file), "locked_ms": round(lock_time_ms, 2)})

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class FileLockProvider:
    """File-based LockProvider: one lock file per lock ID under an env-scoped directory.

    Lock IDs are sanitized to safe file names; a sanitization collision over-locks
    (false contention), never under-locks.
    """

    def __init__(self, env_id, *, timeout=10, max_check_time=0.05):
        self._lock_dir = paths.lock_dir(create=True) / env_id
        self._lock_dir.mkdir(exist_ok=True)
        self._timeout = timeout
        self._max_check_time = max_check_time

    def lock(self, lock_id: str) -> FileLock:
        safe_id = re.sub(r'[^\w.-]', '_', lock_id)
        return FileLock(self._lock_dir / f"{safe_id}.lock",
                        timeout=self._timeout, max_check_time=self._max_check_time)


class MemoryLockProvider:
    """In-memory LockProvider: reentrant locks shared by ID within the process.

    Locks are cleaned up when no longer referenced. Process scope satisfies the env-scope
    guarantee only for in-process environments, which is their sole use.
    """

    def __init__(self):
        self._locks = weakref.WeakValueDictionary()
        self._dict_lock = RLock()

    def lock(self, lock_id: str) -> RLock:
        with self._dict_lock:
            # First try to get the lock - keep a strong reference
            lock = self._locks.get(lock_id)
            if lock is None:
                lock = RLock()
                self._locks[lock_id] = lock
            return lock
