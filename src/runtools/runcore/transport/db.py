"""Database-backed instance transport — transport-neutral.

Learns an environment's live runs by querying the environment database (the rows kept current by
the node-side run-state persister) instead of contacting the producing nodes — the transport for
deployments without a live presence channel (e.g. a shared database). Socket transports use RPC
discovery, which is live and authoritative, and should not use this.

:class:`DbInstanceDiscovery` is a one-shot sweep; :class:`PollingInstanceDirectory` keeps a live
view current by polling the store and synthesizing events through snapshot-fed proxies.
"""

import logging
from threading import Event, Thread, current_thread
from typing import Optional, cast

from runtools.runcore.db import RunStorage
from runtools.runcore.job import InstanceNotifications, InstanceObservableNotifications, JobRun, NotificationBinding
from runtools.runcore.matching import JobRunCriteria
from runtools.runcore.proxy import JobInstanceProxyBase, SnapshotJobInstanceProxy
from runtools.runcore.transport import DiscoveredRuns, InstanceDirectoryBase

log = logging.getLogger(__name__)

DEFAULT_ACTIVE_POLL_INTERVAL = 0.25  # Seconds between polls while instances are active
DEFAULT_IDLE_POLL_INTERVAL = 2.0     # Seconds between polls while the view is empty


class DbInstanceDiscovery:
    """Discover active runs from the environment database.

    A database sweep has a single authoritative source, so the result is always
    ``complete``. Snapshots lag the persister's flush interval and, after a producer
    crash, may name runs that are no longer running — expiring those is a reconciler's
    concern, not discovery's.
    """

    def __init__(self, db: RunStorage):
        self._db = db

    def discover_active_runs(self, run_match=None) -> DiscoveredRuns:
        """Run one discovery sweep against the database."""
        return DiscoveredRuns(self._db.read_active_runs(run_match), complete=True)


class PollingInstanceDirectory(InstanceDirectoryBase):
    """Instance directory for snapshot transports: keeps the view current by polling a ``RunStorage``.

    Each :meth:`reconcile` is one poll. A cheap version scan (``active_run_versions``) selects the
    instances whose stored snapshot changed; those are deep-read and pushed to their
    :class:`SnapshotJobInstanceProxy` (admitting new ones), and instances that have left the active
    set are evicted. The poll *is* the reconciler — there is no inbound event stream, so the proxies
    are the event source and the directory aggregate fans in from them.

    Polling cadence adapts: fast while instances are active, slow while the view is empty.
    """

    def __init__(self, db: RunStorage, *,
                 active_interval: float = DEFAULT_ACTIVE_POLL_INTERVAL,
                 idle_interval: float = DEFAULT_IDLE_POLL_INTERVAL):
        super().__init__()
        self._db = db
        self._notifications = InstanceObservableNotifications()
        self._active_interval = active_interval
        self._idle_interval = idle_interval
        self._seen_versions: dict = {}
        self._stop = Event()
        self._poll_thread = None

    @property
    def notifications(self) -> InstanceNotifications:
        return self._notifications

    def open(self) -> None:
        self.reconcile()  # Seed the view with the first poll
        self._poll_thread = Thread(target=self._poll_loop, name="instance-poll", daemon=True)
        self._poll_thread.start()

    def reconcile(self) -> None:
        """Run one poll and reconcile the view. Driven solely by the poll loop (single-threaded); a
        future doorbell should wake the loop, not call this concurrently."""
        versions = dict(self._db.active_run_versions())
        changed = [iid for iid, cursor in versions.items() if self._seen_versions.get(iid) != cursor]
        for run in self._deep_read(changed):
            self._apply(run)
        for instance_id in self._seen_versions.keys() - versions.keys():
            self._evict_absent(instance_id)
        self._seen_versions = versions

    def _poll_loop(self) -> None:
        while not self._stop.wait(self._poll_interval()):
            try:
                self.reconcile()
            except Exception:
                log.warning("Instance poll reconcile failed; retrying next interval", exc_info=True)

    def _deep_read(self, instance_ids) -> list:
        """Full snapshots for the changed instances — the deep read the cheap version scan defers."""
        if not instance_ids:
            return []  # empty criteria would match everything — never turn no-changes into a full sweep
        return self._db.read_active_runs(JobRunCriteria.instances_match(instance_ids))

    def _apply(self, run: JobRun) -> None:
        """Admit a newly seen instance (silently) or push the snapshot to its existing proxy."""
        proxy = self._snapshot_proxy(run.instance_id)
        if proxy is None:
            self._admit(run)  # Creates the proxy from the snapshot; no synthesized events on first sight
        else:
            proxy.update_from_snapshot(run)  # Outside the lock — fans out to observers

    def _evict_absent(self, instance_id) -> None:
        """Handle an instance that has left the active scan: replay its terminal snapshot (if any) so
        observers see ENDED, then drop it.

        An instance only leaves the active scan by ending or being removed — its row stays in the
        scan while active — so a missing terminal row means it vanished without finalizing (e.g. a
        producer crash): nothing to emit, just evict.
        """
        ended = self._db.read_runs(JobRunCriteria.instance_match(instance_id), asc=False, limit=1, offset=0)
        proxy = self._snapshot_proxy(instance_id)
        if proxy is not None and ended:
            proxy.update_from_snapshot(ended[0])  # Emits ENDED before the proxy is unbound
        with self._proxies_lock:
            self._ensure_removed(instance_id)

    def _snapshot_proxy(self, instance_id) -> Optional[SnapshotJobInstanceProxy]:
        """The admitted proxy for ``instance_id`` — always a SnapshotJobInstanceProxy here, since this
        directory is the only thing that creates them — or None if it is not in the view."""
        with self._proxies_lock:
            admitted = self._admitted.get(instance_id)
        # _admitted is typed to the base proxy (shared core); this directory only ever admits the
        # snapshot subtype via _create_proxy, so narrowing here is sound.
        return cast(SnapshotJobInstanceProxy, admitted.proxy) if admitted else None

    def _poll_interval(self) -> float:
        with self._proxies_lock:
            return self._active_interval if self._admitted else self._idle_interval

    def _wire_proxy(self, proxy: JobInstanceProxyBase) -> NotificationBinding:
        return self._notifications.bind_to(proxy.notifications)  # aggregate <- proxy: the proxy is the source

    def _create_proxy(self, initial: JobRun) -> SnapshotJobInstanceProxy:
        return SnapshotJobInstanceProxy(initial)

    def _close_resources(self) -> None:
        self._stop.set()  # the loop exits after its current pass even without a join
        # close() may run on the poll thread itself — an observer can close the directory while
        # handling a synthesized event (reconcile -> update_from_snapshot -> emit -> observer) — and
        # a thread cannot join itself, so skip the join in that case.
        if self._poll_thread is not None and self._poll_thread is not current_thread():
            self._poll_thread.join()
