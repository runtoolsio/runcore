"""
Environment connector module providing interfaces for interacting with job environments.

This module defines connector components that enable communication with local job environments
and access to both live and historical job data. The connectors serve as clients to environment
nodes, allowing monitoring and control of job instances.

Key components:
- EnvironmentConnector: Abstract base class defining the connector interface
- LocalConnector: Implementation for local (same host) environment connections using socket communication
- LocalConnectorLayout: Filesystem structure definitions for connector components

The module provides factory methods for quickly creating commonly used connector configurations:
    with local() as connector:
        # Get snapshots of active job instances
        active_runs = connector.get_active_runs()
"""

import fcntl
import logging
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from threading import Event, Lock
from dataclasses import dataclass
from typing import Callable, Optional, Iterable, List, override

from runtools.runcore import paths, util, db, output
from runtools.runcore.client import LocalInstanceClient
from runtools.runcore.criteria import JobRunCriteria, SortOption
from runtools.runcore.db import PersistenceDisabledError, sqlite
from runtools.runcore.env import LocalEnvironmentConfig, \
    EnvironmentConfigUnion, EnvironmentNotFoundError, DEFAULT_LOCAL_ENVIRONMENT, get_env_config
from runtools.runcore.err import run_isolated_collect_exceptions
from runtools.runcore.job import InstanceNotifications, JobInstance, InstanceLifecycleObserver, InstanceLifecycleEvent, \
    JobRun, InstanceID
from runtools.runcore.listening import EventReceiver, InstanceEventReceiver
from runtools.runcore.paths import ConfigFileNotFoundError
from runtools.runcore.proxy import JobInstanceProxy

log = logging.getLogger(__name__)


def wait_for_interrupt(env, *, reraise=True):
    try:
        Event().wait()
    except KeyboardInterrupt:
        env.close()
    finally:
        if reraise:
            raise KeyboardInterrupt


class LocalConnectorLayout(ABC):
    """
    Abstract base class defining the filesystem structure for a local environment connector.

    Connector layouts manage the directories and UNIX socket files used for RPC communication
    between a connector and environment nodes. Implementations must provide:

      - Base directories for environment and connector components.
      - Socket file names and full paths for event listening.
      - A provider for locating RPC server socket paths of environment nodes.
      - A cleanup mechanism for removing created resources when connector is closed.
    """

    @property
    @abstractmethod
    def env_dir(self) -> Path:
        """
        Returns:
            Path: Directory containing the environment components.
        """
        pass

    @property
    @abstractmethod
    def listener_events_socket_path(self) -> Path:
        """
        Returns:
            Path: Full filesystem path to the events listener socket file.
        """
        pass

    @property
    @abstractmethod
    def server_sockets_provider(self) -> Callable[[str], Path]:
        """
        Returns:
            Callable[[str], Path]: Provider function that generates paths
                                   to the RPC server socket files of environment nodes.
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """
        Performs cleanup of resources allocated by this layout.

        This typically includes:
          - Deleting socket files created under `component_dir`.
          - Removing temporary directories.
        """
        pass


class StandardLocalConnectorLayout(LocalConnectorLayout):
    """
    Standard implementation of a local connector layout.

    Defines the filesystem layout for environment connectors, including where socket files are located
    and how they're named.

    Example structure:
    /tmp/runtools/env/{env_id}/                      # Environment directory (env_dir)
    │
    └── connector_789xyz/                            # Component directory (component_dir)
        ├── .lock                                    # Exclusive flock held while alive
        ├── listener-events.sock                     # Connector's events listener socket
        └── ...                                      # Other connector-specific sockets
    """

    def __init__(self, env_dir: Path, component_name: str):
        """
        Initializes the connector layout with environment directory and component name.

        Acquires an exclusive flock on a `.lock` file inside the component directory.
        The lock is held for the lifetime of this layout and released on cleanup.

        Args:
            env_dir: Directory containing the environment components.
            component_name: Name of the component subdirectory (created if needed).
        """
        self._env_dir = env_dir
        self._component_dir = paths.ensure_dirs(env_dir / component_name)
        self._server_socket_name = "server-rpc.sock"
        self._listener_events_socket_name = "listener-events.sock"

        lock_path = self._component_dir / ".lock"
        self._lock_fd = lock_path.open("a")
        fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    @classmethod
    def create(cls, env_id: str, root_dir: Optional[Path] = None, component_prefix: str = "connector_"):
        """
        Creates a layout for a connector with a unique component directory.

        Args:
            env_id: Identifier for the environment of the connector.
            root_dir: Root directory containing environments or uses the default one.
            component_prefix: Prefix for component directories.

        Returns (StandardLocalConnectorLayout): Layout instance for a connector.
        """
        return cls(*ensure_component_dir(env_id, component_prefix, root_dir))

    @classmethod
    def from_config(cls, env_config, component_prefix: str = "connector_"):
        return cls.create(env_config.id, env_config.layout.root_dir, component_prefix)

    @property
    def env_dir(self) -> Path:
        return self._env_dir

    @property
    def listener_events_socket_path(self) -> Path:
        """
        Returns:
            Path: Full path of domain socket used for receiving all events
        """
        return self._component_dir / self._listener_events_socket_name

    @property
    def server_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to the RPC server socket files
                      of each environment node within the environment directory.
        """
        return paths.files_in_subdir_provider(self._env_dir, self._server_socket_name)

    def cleanup(self):
        """
        Releases the flock and removes the component directory. Idempotent.
        """
        if not self._lock_fd.closed:
            self._lock_fd.close()
        shutil.rmtree(self._component_dir, ignore_errors=True)


def resolve_env_dir(env_id: str, root_dir: Optional[Path] = None) -> Path:
    """Resolve the environment directory path for a given environment ID.

    Args:
        env_id: Environment identifier.
        root_dir: Optional root directory override. Uses the default runtime dir if None.

    Returns:
        Path to the environment directory (may not exist yet).
    """
    if root_dir:
        return root_dir / env_id
    return paths.runtime_env_dir() / env_id


def ensure_component_dir(env_id, component_prefix, root_dir=None):
    """
    Ensures the environment directory exists and generates a unique component name.

    Args:
        env_id: Identifier for the target environment.
        component_prefix: Prefix for the unique component directory name.
        root_dir: Optional root path for environment directories. Uses default if None.

    Returns:
        A tuple of (env_dir, component_name). The component directory itself is created
        by the layout constructor (which also acquires the flock).
    """
    env_dir = paths.ensure_dirs(resolve_env_dir(env_id, root_dir))
    component_name = component_prefix + util.unique_timestamp_hex()
    return env_dir, component_name


def clean_stale_component_dirs(env_dir: Path) -> List[Path]:
    """Remove component directories whose owner process is dead.

    Each live component holds an exclusive flock on ``{component_dir}/.lock``.
    If the flock can be acquired (non-blocking), the owner is dead and the directory is removed.

    Args:
        env_dir: The environment directory containing component subdirectories.

    Returns:
        List of removed directory paths.
    """
    if not env_dir.is_dir():
        return []

    removed = []
    for entry in env_dir.iterdir():
        if not entry.is_dir():
            continue
        lock_file = entry / ".lock"
        if not lock_file.exists():
            continue
        try:
            fd = lock_file.open("r")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fd.close()
                continue
            except OSError:
                log.warning(f"Unexpected error probing lock on {entry}", exc_info=True)
                fd.close()
                continue
            # Lock acquired — owner is dead
            fd.close()
            shutil.rmtree(entry, ignore_errors=True)
            removed.append(entry)
            log.info(f"Removed stale component directory: {entry}")
        except OSError as e:
            log.debug(f"Skipping {entry}: {e}")

    return removed


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

    @property
    @abstractmethod
    def persistence_enabled(self) -> bool:
        pass

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
        try:
            runs = self.read_runs(criteria, asc=False, limit=1, offset=0)
        except PersistenceDisabledError:
            return None
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


def create(env_config: EnvironmentConfigUnion) -> EnvironmentConnector:
    """Create a connector from an environment configuration.

    Args:
        env_config (EnvironmentConfigUnion): Environment configuration that determines the connector type and settings.

    Returns:
        EnvironmentConnector: Configured connector for the environment.
    """
    if isinstance(env_config, LocalEnvironmentConfig):
        persistence = db.create_persistence(env_config.id, env_config.persistence)
        output_backends = output.create_backends(env_config.id, env_config.output.storages)
        layout = StandardLocalConnectorLayout.from_config(env_config)
        return _local(env_config.id, persistence, layout, output_backends)

    raise AssertionError(f"Unsupported environment type: {env_config.type}. This is a programming error.")


def _local(env_id, persistence, connector_layout, output_backends=()) -> EnvironmentConnector:
    clean_stale_component_dirs(connector_layout.env_dir)
    client = LocalInstanceClient(connector_layout.server_sockets_provider)
    event_receiver = EventReceiver(connector_layout.listener_events_socket_path)
    return LocalConnector(env_id, connector_layout, persistence, client, event_receiver, output_backends)


def connect(env_id: Optional[str] = None) -> EnvironmentConnector:
    """Connect to an environment by ID, falling back to default local config if no config exists."""
    try:
        return create(get_env_config(env_id))
    except (EnvironmentNotFoundError, ConfigFileNotFoundError):
        env_id = env_id or DEFAULT_LOCAL_ENVIRONMENT
        log.debug(f"No config entry for environment '{env_id}', using default local config")
        return create(LocalEnvironmentConfig(id=env_id))


class LocalConnector(EnvironmentConnector):
    """
    Concrete implementation of the EnvironmentConnector for interacting with local environments.

    Local environments are those running within the same operating system. LocalConnector uses Unix domain sockets
    to communicate with environment nodes, enabling management of job instances and collection of their status
    and history. It handles both live job data via RPC calls and historical job data through persistence.
    """

    def __init__(self, env_id, connector_layout, persistence, client, event_receiver, output_backends=()):
        self._notifications = InstanceEventReceiver()
        self._env_id = env_id
        self._layout = connector_layout
        self._persistence = persistence
        self._client = client
        self._event_receiver = event_receiver
        self._output_backends = tuple(output_backends)
        self._event_receiver.register_handler(self._notifications)

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._notifications

    @property
    def env_id(self):
        return self._env_id

    @property
    def persistence_enabled(self) -> bool:
        return self._persistence.enabled

    @property
    @override
    def output_backends(self):
        return self._output_backends

    def open(self):
        self._persistence.open()
        self._event_receiver.start()

    def get_active_runs(self, run_match=None):
        """Retrieve active job runs from all available servers.

        Args:
            run_match: Optional criteria for filtering job runs
                      (default: None, which matches all runs)

        Returns:
            List of JobRun objects from all responding servers

        Note:
            Communication errors are logged but not surfaced to callers, which may result
            in incomplete data. Future options to consider:
              1. Return a result dataclass with both runs and errors
              2. Raise an exception containing partial results
              3. Add an optional on_error callback parameter
              4. Add a separate get_active_runs_with_errors() method
        """
        run_results = self._client.collect_active_runs(run_match)
        active_runs = []

        for result in run_results:
            if result.error:
                log.warning(f"[instance_call_error] op=[collect_active_runs] server=[{result.server_address}]",
                            exc_info=result.error)
                continue
            active_runs.extend(result.retval)

        return active_runs

    def get_instances(self, run_match=None):
        # Same error handling consideration as get_active_runs()
        run_results = self._client.collect_active_runs(run_match)
        instances = []

        for result in run_results:
            if result.error:
                log.warning(f"event=[instance_call_error] op=[get_instances] server=[{result.server_address}]",
                            exc_info=result.error)
                continue

            server_address = result.server_address
            for job_run in result.retval:
                instances.append(JobInstanceProxy(
                    self._client, server_address, job_run.instance_id, self._notifications))

        return instances

    def read_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._persistence.read_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def iter_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._persistence.iter_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_run_stats(self, run_match=None):
        return self._persistence.read_run_stats(run_match)

    def remove_history_runs(self, run_match):
        active = self.get_active_runs(run_match)
        if active:
            raise ValueError(f"Cannot remove active runs: {', '.join(str(r.instance_id) for r in active)}")
        removed_ids = self._persistence.remove_runs(run_match)
        for backend in self._output_backends:
            backend.delete_output(*removed_ids)
        return removed_ids

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing local environment",
            self._event_receiver.close,
            self._client.close,
            self._persistence.close,
            *(b.close for b in self._output_backends),
            self._layout.cleanup,
        )
