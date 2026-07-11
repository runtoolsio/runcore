"""
Environment connector module providing interfaces for interacting with job environments.

This module defines connector components that enable communication with job environments
and access to both live and historical job data. The connectors serve as clients to environment
nodes, allowing monitoring and control of job instances.

Key components:
- EnvironmentConnector: Abstract base class defining the connector interface
- connect(): factory entry point that dispatches by environment kind

The concrete implementation (``_Connector``) is module-private; callers build one through
``compose()`` (internal framework plumbing) or the public ``connect()``. Concrete transport
bundles live under ``runtools.runcore.transport``.

The module provides factory methods for quickly creating commonly used connector configurations:
    with connect() as conn:
        # Get snapshots of active job instances
        active_runs = conn.get_active_runs()
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import Event, Lock
from typing import Iterable, Optional, assert_never, override

from runtools.runcore import output
from runtools.runcore.env import EnvironmentKind, LocalEnvironmentConfig, PostgresEnvironmentConfig, \
    EnvironmentEntry, EnvironmentNotFoundError, resolve_env_ref, ensure_environment, open_configured_db
from runtools.runcore.err import run_isolated_collect_exceptions
from runtools.runcore.job import InstanceNotifications, JobInstance, InstanceLifecycleObserver, InstanceLifecycleEvent, \
    JobRun, InstanceID
from runtools.runcore.matching import JobRunCriteria, SortOption
from runtools.runcore.proxy import SnapshotJobInstanceProxy
from runtools.runcore.transport import InstanceDirectory
from runtools.runcore.transport.db import PollingInstanceDirectory


def wait_for_interrupt(env, *, reraise=True):
    try:
        Event().wait()
    except KeyboardInterrupt:
        env.close()
    finally:
        if reraise:
            raise KeyboardInterrupt


@dataclass
class WatchedRun:
    """A single criterion–run pair tracked by a Watcher."""
    criteria: JobRunCriteria
    matched_run: Optional[JobRun] = None


class EnvironmentConnector(ABC):
    """
    An abstract base class defining the interface for connecting to and interacting with an environment.

    Environment connectors provide access to job instances and their historical data.
    They allow monitoring and controlling jobs and retrieving information about their state and execution history.
    Connectors cannot create or run new job instances - that functionality is provided by environment nodes.
    """

    @property
    @abstractmethod
    def env_id(self):
        pass

    def __enter__(self):
        """Open the connector."""
        self.open()
        return self

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def get_active_runs(self, run_match=None):
        pass

    def get_run(self, instance_id: InstanceID) -> Optional[JobRun]:
        """Fetch a single JobRun by instance ID — tries active runs, then history."""
        criteria = JobRunCriteria.instance_match(instance_id)
        runs = self.get_active_runs(criteria)
        if runs:
            return runs[0]
        runs = self.read_runs(criteria, asc=False, limit=1, offset=0)
        return runs[0] if runs else None

    @abstractmethod
    def read_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        pass

    @abstractmethod
    def iter_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        pass

    def get_instance(self, instance_id) -> JobInstance:
        instances = self.get_instances(JobRunCriteria.instance_match(instance_id))
        return next(iter(instances), None)

    @abstractmethod
    def get_instances(self, run_match=None) -> Iterable[JobInstance]:
        pass

    @abstractmethod
    def read_run_stats(self, run_match=None):
        pass

    @abstractmethod
    def remove_history_runs(self, run_match: JobRunCriteria) -> list[InstanceID]:
        """Remove finished runs matching the criteria from persistence and delete their output.

        Args:
            run_match: Criteria selecting which history runs to remove.

        Returns:
            List of instance IDs that were removed.

        Raises:
            ValueError: If any matching run is still active.
        """

    @property
    def output_backends(self):
        """Output backends available for this environment, in config order."""
        return []

    @property
    @abstractmethod
    def notifications(self) -> InstanceNotifications:
        """Register observers here to receive events from all instances in this environment."""

    # noinspection PyProtectedMember
    def watcher(self, *criteria: JobRunCriteria, search_past: bool):
        """Create a watcher that waits until each criterion is satisfied by a matching run.

        Criteria are evaluated in order — earlier criteria get first claim on matching runs.
        Each run can satisfy at most one criterion (one-to-one assignment).

        Args:
            *criteria (JobRunCriteria): One or more criteria, each must be matched by a distinct run.
            search_past (bool): If True, check history and active runs before waiting for live events.

        Returns:
            Watcher with wait()/cancel() interface. Bool-true when all criteria are satisfied.
        """
        connector = self

        class Watcher(InstanceLifecycleObserver):

            def __init__(self):
                self._entries = [WatchedRun(c) for c in criteria]
                # Guarded by _lock:
                # - _claimed_ids
                # - WatchedRun.matched_run on entries in _entries
                self._claimed_ids: set[InstanceID] = set()
                self._event = Event()
                self._lock = Lock()
                self._timed_out = False
                self._cancelled = False

            def __bool__(self):
                return self._is_complete

            @property
            def _is_complete(self) -> bool:
                return all(entry.matched_run is not None for entry in self._entries)

            @property
            def watched_runs(self) -> list[WatchedRun]:
                return self._entries.copy()

            @property
            def matched_runs(self) -> list[JobRun]:
                return [e.matched_run for e in self._entries if e.matched_run is not None]

            @property
            def is_timed_out(self):
                return self._timed_out

            @property
            def is_cancelled(self):
                return self._cancelled

            def _try_close(self, force=False):
                with self._lock:
                    if (not force and not bool(self)) or self._event.is_set():
                        return
                    self._event.set()
                connector.notifications.remove_observer_lifecycle(self)

            def _unmatched_entries(self) -> list[WatchedRun]:
                return [entry for entry in self._entries if entry.matched_run is None]

            def _claim(self, entry: WatchedRun, runs: Iterable[JobRun]):
                """Try to satisfy one entry with the oldest-created unclaimed matching run."""
                with self._lock:
                    if entry.matched_run is not None:
                        return
                    candidates = sorted(
                        (r for r in runs if r.instance_id not in self._claimed_ids),
                        key=lambda r: r.lifecycle.created_at,
                    )
                    if candidates:
                        match = candidates[0]
                        entry.matched_run = match
                        self._claimed_ids.add(match.instance_id)

            def _bootstrap_from(self, source):
                for entry in self._unmatched_entries():
                    self._claim(entry, source(entry.criteria))
                self._try_close()

            def instance_lifecycle_update(self, event: InstanceLifecycleEvent):
                for entry in self._unmatched_entries():
                    if entry.criteria(event.job_run):
                        self._claim(entry, [event.job_run])
                        self._try_close()
                        break

            def wait(self, *, timeout=None):
                """Block until all criteria are satisfied, timeout expires, or watcher is cancelled.

                Returns:
                    bool: True if all criteria were satisfied, False on timeout or cancellation.
                """
                try:
                    self._event.wait(timeout)
                    if self._cancelled:
                        return False
                    if self._is_complete:
                        return True
                    self._timed_out = True
                    return False
                finally:
                    connector.notifications.remove_observer_lifecycle(self)

            def cancel(self):
                if self._is_complete:
                    return
                self._cancelled = True
                self._try_close(force=True)

        watcher = Watcher()
        self.notifications.add_observer_lifecycle(watcher)
        if search_past:
            watcher._bootstrap_from(lambda crit: connector.read_runs(crit, sort=SortOption.CREATED, asc=True))
            if not watcher:
                watcher._bootstrap_from(connector.get_active_runs)

        return watcher

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def close(self):
        pass


class _Connector(EnvironmentConnector):
    """Connector that combines live instance access with persisted run history.

    Live instances come from the instance directory. Historical runs, stats, and
    deletion come from the environment database. Output backends are kept here
    because they are used for historical output reads and cleanup.
    """

    def __init__(self, env_id, env_db, directory: InstanceDirectory, output_backends=()):
        self._env_id = env_id
        self._db = env_db
        self._directory = directory
        self._output_backends = tuple(output_backends)

    @property
    def env_id(self):
        return self._env_id

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._directory.notifications

    @property
    @override
    def output_backends(self):
        return self._output_backends

    def open(self):
        self._directory.open()

    def get_active_runs(self, run_match=None):
        """Snapshots of the directory's live instances — event-maintained, no per-call RPC."""
        return [instance.snap() for instance in self._directory.get_instances(run_match)]

    def get_instances(self, run_match=None):
        return self._directory.get_instances(run_match)

    @override
    def get_instance(self, instance_id):
        return self._directory.get_instance(instance_id)

    def read_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._db.read_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def iter_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._db.iter_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_run_stats(self, run_match=None):
        return self._db.read_run_stats(run_match)

    def remove_history_runs(self, run_match):
        active = self.get_active_runs(run_match)
        if active:
            raise ValueError(f"Cannot remove active runs: {', '.join(str(r.instance_id) for r in active)}")
        removed_ids = self._db.remove_runs(run_match)
        for backend in self._output_backends:
            backend.delete_output(*removed_ids)
        return removed_ids

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing environment connector",
            self._directory.close,
            self._db.close,
            *(b.close for b in self._output_backends),
        )


def compose(env_id, env_db, directory: InstanceDirectory, output_backends) -> EnvironmentConnector:
    """Construct an :class:`EnvironmentConnector` from its runtime parts.

    This is internal assembly code used by environment factories. User code should
    call :func:`connect` instead.
    """
    return _Connector(env_id, env_db, directory, output_backends)


def _connect_local(entry: EnvironmentEntry) -> EnvironmentConnector:
    """Connector for a ``local`` environment: sqlite + unix-socket directory."""
    from runtools.runcore.db import sqlite
    from runtools.runcore.transport import unix_socket
    # Check before open: opening a missing sqlite file would silently provision a fresh schema
    if not sqlite.exists(entry):
        raise EnvironmentNotFoundError(f"Database for environment '{entry.id}' not found", {entry.id})
    env_db = sqlite.create(entry)
    with open_configured_db(env_db, entry.id, LocalEnvironmentConfig) as config:
        # Build cheap, in-memory output backends first; the directory allocates a component
        # dir + flock and would leak if output construction failed after directory setup.
        output_backends = output.create_backends(entry.id, config.output.storages)
        directory = unix_socket.create_instance_directory(entry.id, config.root_dir)
        return compose(entry.id, env_db, directory, output_backends)


def _connect_postgres(entry: EnvironmentEntry) -> EnvironmentConnector:
    """Connector for a ``postgres`` environment: postgres + polling directory."""
    from runtools.runcore.db import postgres  # lazy — psycopg stays an optional dep
    env_db = postgres.create(entry)
    with open_configured_db(env_db, entry.id, PostgresEnvironmentConfig) as config:
        output_backends = output.create_backends(entry.id, config.output.storages)
        directory = PollingInstanceDirectory(env_db, lambda run: SnapshotJobInstanceProxy(run, env_db))
        return compose(entry.id, env_db, directory, output_backends)


def connect(env_ref: EnvironmentEntry | str | None = None) -> EnvironmentConnector:
    """Connect to an environment. Opens DB, reads config, creates connector.

    The builtin local environment is auto-provisioned if its backing store doesn't exist yet.
    Named environments must be created explicitly — connecting to a missing one raises EnvironmentNotFoundError.

    Args:
        env_ref: Environment entry, env_id string, or None for built-in local.
    """
    entry = resolve_env_ref(env_ref)
    ensure_environment(entry)
    match entry.kind:
        case EnvironmentKind.LOCAL:
            return _connect_local(entry)
        case EnvironmentKind.POSTGRES:
            return _connect_postgres(entry)
        case _:
            assert_never(entry.kind)  # new kind added but not wired here
