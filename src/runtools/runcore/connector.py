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

import logging
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from threading import Event, Lock
from typing import Callable, Optional, Iterable, List, override

from runtools.runcore import paths, util, db
from runtools.runcore.client import LocalInstanceClient
from runtools.runcore.criteria import JobRunCriteria, SortOption
from runtools.runcore.db import NullPersistence, sqlite
from runtools.runcore.env import LocalEnvironmentConfig, \
    EnvironmentConfigUnion, DEFAULT_LOCAL_ENVIRONMENT
from runtools.runcore.err import run_isolated_collect_exceptions
from runtools.runcore.job import InstanceNotifications, JobInstance, InstanceLifecycleObserver, InstanceLifecycleEvent, \
    JobRun
from runtools.runcore.listening import EventReceiver, InstanceEventReceiver
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
    /tmp/runtools/env/{env_id}/                      # Directory for the specific environment (env_path)
    │
    └── connector_789xyz/                            # Connector directory (connector_path)
        ├── listener-events.sock                     # Connector's events listener socket
        └── ...                                      # Other connector-specific sockets
    """

    def __init__(self, env_path: Path, connector_path: Path):
        """
        Initializes the connector layout with environment and component directories.

        Args:
            env_path (Path): Directory containing the environment components
            connector_path (Path): Directory specific to this connector instance

        """
        self._env_path = env_path
        self._component_path = connector_path
        self._server_socket_name = "server-rpc.sock"
        self._listener_events_socket_name = "listener-events.sock"

    @classmethod
    def create(cls, env_id: str, root_dir: Optional[Path] = None, connector_dir_prefix: str = "connector_"):
        """
        Creates a layout for a connector and ensures that the directory for the connector is created.

        Args:
            env_id (str): Identifier for the environment of the connector
            root_dir (Path, optional): Root directory containing environments or uses the default one.
            connector_dir_prefix (str): Prefix for connector directories

        Returns (StandardLocalConnectorLayout): Layout instance for a connector
        """
        return cls(*ensure_component_dir(env_id, connector_dir_prefix, root_dir))

    @classmethod
    def from_config(cls, env_config, connector_dir_prefix: str = "connector_"):
        return cls.create(env_config.id, env_config.layout.root_dir, connector_dir_prefix)

    @property
    def listener_events_socket_path(self) -> Path:
        """
        Returns:
            Path: Full path of domain socket used for receiving all events
        """
        return self._component_path / self._listener_events_socket_name

    @property
    def server_sockets_provider(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to the RPC server socket files
                      of each environment node within the environment directory.
        """
        return paths.files_in_subdir_provider(self._env_path, self._server_socket_name)

    def cleanup(self):
        """
        Removes the connector directory and all its contents from the filesystem.
        """
        shutil.rmtree(self._component_path)


def ensure_component_dir(env_id, component_prefix, root_dir=None):
    """
    Creates a component directory within a specified environment directory.

    Ensures the environment directory exists and creates a unique subdirectory
    within it for a component instance (e.g., node or connector).

    Args:
        env_id: Identifier for the target environment.
        component_prefix: Prefix for the unique component directory name.
        root_dir: Optional root path for environment directories. Uses default if None.

    Returns:
        A tuple containing the environment directory path and the unique component directory path.
    """
    if root_dir:
        env_dir = paths.ensure_dirs(root_dir / env_id)
    else:
        env_dir = paths.ensure_dirs(paths.runtime_env_dir() / env_id)

    comp_id = component_prefix + util.unique_timestamp_hex()
    component_dir = paths.ensure_dirs(env_dir / comp_id)

    return env_dir, component_dir


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

    @abstractmethod
    def iter_history_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        """
        Iterate over ended job instances based on specified criteria.

        This method provides memory-efficient access to job history by yielding
        results one at a time rather than loading all records into memory.
        For large result sets, this is preferred over read_history_runs().

        Args:
            run_match: Criteria to match specific job instances
            sort: Field by which records are sorted (default: SortCriteria.ENDED)
            asc: Sort order - True for ascending, False for descending (default: True)
            limit: Maximum number of records to yield, -1 for unlimited (default: -1)
            offset: Number of records to skip before yielding (default: 0)
            last: If True, only yield the last record for each job (default: False)

        Yields:
            JobRun: Individual job instances matching the criteria

        Returns:
            Iterator[JobRun]: An iterator over JobRun instances

        Raises:
            PersistenceDisabledError: If persistence is not enabled for this environment
        """
        pass

    def get_instance(self, instance_id) -> JobInstance:
        instances = self.get_instances(JobRunCriteria.instance_match(instance_id))
        return next(iter(instances), None)

    @abstractmethod
    def get_instances(self, run_match=None) -> Iterable[JobInstance]:
        pass

    @abstractmethod
    def read_history_runs(self, run_match, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        pass

    @abstractmethod
    def read_history_stats(self, run_match=None):
        pass

    @property
    @abstractmethod
    def notifications(self) -> InstanceNotifications:
        """Register observers here to receive events from all instances in this environment."""

    # noinspection PyProtectedMember
    def watcher(self, run_match, *, search_past, stop_count=1):
        connector = self

        class Watcher(InstanceLifecycleObserver):

            def __init__(self):
                self._matched_runs: List[JobRun] = []
                self._matched_ids: set = set()
                self._event = Event()
                self._watch_lock = Lock()
                self._timed_out = False
                self._cancelled = False

            def __bool__(self):
                return self.remaining_count == 0

            @property
            def run_match(self):
                return run_match

            @property
            def matched_runs(self):
                return self._matched_runs.copy()

            @property
            def remaining_count(self):
                return stop_count - len(self._matched_runs)

            @property
            def is_timed_out(self):
                return self._timed_out

            @property
            def is_cancelled(self):
                return self._cancelled

            def _close(self):
                connector.notifications.remove_observer_lifecycle(self)
                self._event.set()

            def _add_matched(self, matched):
                if self.remaining_count == 0:
                    return
                for run in matched:
                    if run.instance_id in self._matched_ids:
                        continue
                    self._matched_ids.add(run.instance_id)
                    self._matched_runs.append(run)
                    if self.remaining_count == 0:
                        self._close()
                        return

            def _watch_history(self):
                runs = connector.read_history_runs(run_match, limit=stop_count)
                with self._watch_lock:
                    self._add_matched(runs)

            def _watch_active(self):
                runs = connector.get_active_runs(run_match)
                with self._watch_lock:
                    self._add_matched(runs)

            def instance_lifecycle_update(self, event: InstanceLifecycleEvent):
                if run_match(event.job_run):
                    with self._watch_lock:
                        self._add_matched([event.job_run])

            def wait(self, *, timeout=None):
                try:
                    completed = self._event.wait(timeout)
                    self._timed_out = not completed
                    return False if self._cancelled else completed
                finally:
                    connector.notifications.remove_observer_lifecycle(self)

            def cancel(self):
                self._cancelled = True
                self._close()

        watcher = Watcher()
        self.notifications.add_observer_lifecycle(watcher)
        if search_past:
            watcher._watch_history()
            if watcher.remaining_count:
                watcher._watch_active()

        return watcher

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def close(self):
        pass


def create(env_config: EnvironmentConfigUnion):
    if isinstance(env_config, LocalEnvironmentConfig):
        if env_config.persistence:
            persistence = db.create_persistence(env_config.id, env_config.persistence)
        else:
            persistence = NullPersistence()
        layout = StandardLocalConnectorLayout.from_config(env_config)
        return local(env_config.id, persistence, layout)

    raise AssertionError(f"Unsupported environment type: {env_config.type}. This is a programming error.")


def local(env_id=DEFAULT_LOCAL_ENVIRONMENT, persistence=None, connector_layout=None) -> EnvironmentConnector:
    """
    Factory function to create a connector for the given local environment using standard components.
    This provides a convenient way to get a ready-to-use connector for local environment interaction.

    A local connector provides access to job instances and their history within a local environment,
    allowing monitoring and controlling of jobs belonging to the environment.

    Args:
        env_id (str): The identifier for the local environment.
        persistence (Persistence, optional): A specific persistence implementation.
            If None, a default SQLite backend is used for the environment.
        connector_layout (LocalConnectorLayout): Optional custom connector layout.
            Defaults to standard layout for local environments
    Returns:
        EnvironmentConnector: Configured connector to the local environment
    """
    layout = connector_layout or StandardLocalConnectorLayout.create(env_id)
    persistence = persistence or sqlite.create(str(paths.sqlite_db_path(env_id, create=True)))
    client = LocalInstanceClient(layout.server_sockets_provider)
    event_receiver = EventReceiver(layout.listener_events_socket_path)
    return LocalConnector(env_id, layout, persistence, client, event_receiver)


class LocalConnector(EnvironmentConnector):
    """
    Concrete implementation of the EnvironmentConnector for interacting with local environments.

    Local environments are those running within the same operating system. LocalConnector uses Unix domain sockets
    to communicate with environment nodes, enabling management of job instances and collection of their status
    and history. It handles both live job data via RPC calls and historical job data through persistence.
    """

    def __init__(self, env_id, connector_layout, persistence, client, event_receiver):
        self._notifications = InstanceEventReceiver()
        self._env_id = env_id
        self._layout = connector_layout
        self._persistence = persistence
        self._client = client
        self._event_receiver = event_receiver
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

    def read_history_runs(self, run_match, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._persistence.read_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def iter_history_runs(self, run_match=None, sort=SortOption.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._persistence.iter_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_history_stats(self, run_match=None):
        return self._persistence.read_history_stats(run_match)

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing local environment",
            self._event_receiver.close,
            self._client.close,
            self._persistence.close,
            self._layout.cleanup,
        )
