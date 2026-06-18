"""Node-side persistence of active run state — transport-neutral.

:class:`RunStatePersister` observes job-instance events and records each run's latest
*active* (non-ended) snapshot, which :meth:`flush` writes to the environment database, so
discovery and recovery have an up-to-date view without contacting the producing node.

Model:
    - every lifecycle / phase / status event records the latest snapshot (coalesced);
    - an ended snapshot drops the run from the dirty set and is never written — the
      authoritative terminal write stays with the node's ``_finalize_run`` (it runs after
      the output router closes, so it captures the final output locations);
    - :meth:`flush` is the only writer and must not be called concurrently by multiple
      threads — the caller owns scheduling, DB-thread affinity, and flush-error handling
      (a failed write keeps its snapshots staged for the next flush).

Single writer + latest-snapshot-per-run means the newest state always wins, with no DB I/O
under the lock; the lock guards only the in-memory dirty map against concurrent event
threads. ``store_active_runs`` additionally no-ops once a row is terminal, so a stale
snapshot can never resurrect an ended run.
"""

import logging
from threading import Lock
from typing import Dict

from runtools.runcore.db import EnvironmentDatabase
from runtools.runcore.job import InstanceID, InstanceNotifications, JobRun

log = logging.getLogger(__name__)


class RunStatePersister:
    """Coalesces job-instance events and persists active run snapshots on flush."""

    def __init__(self, db: EnvironmentDatabase):
        self._db = db
        self._dirty: Dict[InstanceID, JobRun] = {}
        self._lock = Lock()  # Guards _dirty only — never held across DB I/O
        self._closed = False

    def attach(self, notifications: InstanceNotifications) -> None:
        """Begin recording the instance's active state."""
        notifications.add_observer_lifecycle(self._mark_dirty)
        notifications.add_observer_phase(self._mark_dirty)
        notifications.add_observer_status(self._mark_dirty)

    def detach(self, notifications: InstanceNotifications) -> None:
        """Stop observing the instance (its state is no longer tracked)."""
        notifications.remove_observer_lifecycle(self._mark_dirty)
        notifications.remove_observer_phase(self._mark_dirty)
        notifications.remove_observer_status(self._mark_dirty)

    def flush(self) -> None:
        """Write the coalesced active snapshots, clearing each only after it lands.

        Single-threaded by contract. Propagates DB errors after retaining the unwritten
        snapshots, so the caller logs and retries on the next flush — failed writes are
        never silently dropped, and a newer snapshot staged during the write is kept.
        """
        with self._lock:
            snapshots = dict(self._dirty)
        if not snapshots:
            return
        self._db.store_active_runs(*snapshots.values())  # Outside the lock; no-ops on ended rows
        with self._lock:
            for instance_id, job_run in snapshots.items():
                if self._dirty.get(instance_id) is job_run:  # untouched since the copy → safe to drop
                    del self._dirty[instance_id]

    def close(self) -> None:
        """Stop accepting events and make a best-effort final flush.

        Unlike the periodic flush, a failure here cannot be retried (the flush loop has
        stopped and no further events are accepted), so it is logged and swallowed rather
        than propagated — shutdown must not fail on a last-snapshot write error.
        """
        with self._lock:
            self._closed = True
        try:
            self.flush()
        except Exception:
            log.warning("Final persister flush failed; retained active snapshots dropped on shutdown",
                        exc_info=True)

    def _mark_dirty(self, event) -> None:
        job_run = event.job_run
        with self._lock:
            if self._closed:
                return
            if job_run.lifecycle.is_ended:
                self._dirty.pop(job_run.instance_id, None)  # Terminal write is _finalize_run's
            else:
                self._dirty[job_run.instance_id] = job_run
