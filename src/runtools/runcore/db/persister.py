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
      threads — the caller owns scheduling and DB-thread affinity.

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
        """Write the coalesced active snapshots. Single-threaded by contract."""
        with self._lock:
            snapshots = list(self._dirty.values())
            self._dirty.clear()
        for job_run in snapshots:  # Outside the lock; store_active_runs no-ops on ended rows
            self._persist(job_run)

    def close(self) -> None:
        """Stop accepting events and flush remaining snapshots."""
        with self._lock:
            self._closed = True
        self.flush()

    def _mark_dirty(self, event) -> None:
        job_run = event.job_run
        with self._lock:
            if self._closed:
                return
            if job_run.lifecycle.is_ended:
                self._dirty.pop(job_run.instance_id, None)  # Terminal write is _finalize_run's
            else:
                self._dirty[job_run.instance_id] = job_run

    def _persist(self, job_run: JobRun) -> None:
        """Best-effort write; isolate DB failures so they don't kill the caller's flush."""
        try:
            self._db.store_active_runs(job_run)
        except Exception:
            log.warning("Failed to persist run state instance=%s", job_run.instance_id,
                        extra={"instance": str(job_run.instance_id)}, exc_info=True)
