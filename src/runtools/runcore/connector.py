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
from threading import Event
from typing import Callable, Optional

from runtools.runcore import paths, util
from runtools.runcore.client import RemoteCallClient
from runtools.runcore.constants import DEFAULT_ENVIRONMENT
from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.db import SortCriteria, sqlite
from runtools.runcore.err import run_isolated_collect_exceptions
from runtools.runcore.job import JobInstanceObservable
from runtools.runcore.layout import LocalEnvironmentLayoutConfig, LayoutDefaults
from runtools.runcore.listening import EventReceiver, InstanceEventReceiver
from runtools.runcore.remote import JobInstanceRemote
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY

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
      - Socket file names and full paths for client RPC and event listening.
      - A provider for locating RPC server socket paths of environment nodes.
      - A cleanup mechanism for removing created resources when connector is closed.
    """

    @property
    @abstractmethod
    def socket_path_client_rpc(self) -> Path:
        """
        Returns:
            Path: Path to socket file used by connector for receiving RPC responses.
        """
        pass

    @property
    @abstractmethod
    def socket_path_listener_events(self) -> Path:
        """
        Returns:
            Path: Full filesystem path to the events listener socket file.
        """
        pass

    @property
    @abstractmethod
    def provider_sockets_server_rpc(self) -> Callable[[str], Path]:
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
    /tmp/runtools/env/{env_id}/                      # Directory for the specific environment (env_dir)
    │
    └── connector_789xyz/                            # Connector directory (connector_dir)
        ├── client-rpc.sock                          # Connector's RPC client socket
        ├── listener-events.sock                     # Connector's events listener socket
        └── ...                                      # Other connector-specific sockets
    """

    def __init__(
            self,
            env_dir: Path,
            component_dir: Path,
            node_dir_prefix: str = LayoutDefaults.NODE_DIR_PREFIX,
            connector_dir_prefix: str = LayoutDefaults.CONNECTOR_DIR_PREFIX,
            socket_name_client_rpc: str = LayoutDefaults.SOCKET_NAME_CLIENT_RPC,
            socket_name_server_rpc: str = LayoutDefaults.SOCKET_NAME_SERVER_RPC,
            socket_name_listener_events: str = LayoutDefaults.SOCKET_NAME_LISTENER_EVENTS
    ):
        """
        Initializes the connector layout with environment and component directories.

        Args:
            env_dir (Path): Directory containing the environment components
            component_dir (Path): Directory specific to this connector instance
            node_dir_prefix (str): Prefix for node directories
            connector_dir_prefix (str): Prefix for connector directories
            socket_name_client_rpc (str): Filename for client RPC socket
            socket_name_server_rpc (str): Filename for server RPC socket
            socket_name_listener_events (str): Filename for event listener socket
        """
        self._env_dir = env_dir
        self._component_dir = component_dir
        self._node_dir_prefix = node_dir_prefix
        self._connector_dir_prefix = connector_dir_prefix
        self._socket_name_client_rpc = socket_name_client_rpc
        self._socket_name_server_rpc = socket_name_server_rpc
        self._socket_name_listener_events = socket_name_listener_events

    @classmethod
    def create(cls, env_id: str, root_dir: Optional[Path] = None,
               connector_dir_prefix: str = LayoutDefaults.CONNECTOR_DIR_PREFIX):
        """
        Creates a layout for a new connector together with a new unique directory for the connector.

        Args:
            env_id (str): Identifier for the environment
            root_dir (Path, optional): Root directory containing environments or uses the default one.
            connector_dir_prefix (str): Prefix for connector directories

        Returns (StandardLocalConnectorLayout): Layout instance for a connector
        """
        env_dir, component_dir = ensure_component_dirs(env_id, connector_dir_prefix, root_dir)
        return cls(
            env_dir=env_dir,
            component_dir=component_dir,
            connector_dir_prefix=connector_dir_prefix
        )

    @classmethod
    def from_config(cls, config: LocalEnvironmentLayoutConfig):
        """
        Factory method to create a StandardLocalConnectorLayout from a configuration object.

        Args:
            config (LocalEnvironmentLayoutConfig): Configuration object

        Returns:
            StandardLocalConnectorLayout: A new connector layout configured according to the provided config
        """
        return cls(
            env_dir=config.env_dir,
            component_dir=config.component_dir,
            node_dir_prefix=config.node_dir_prefix,
            connector_dir_prefix=config.connector_dir_prefix,
            socket_name_client_rpc=config.socket_name_client_rpc,
            socket_name_server_rpc=config.socket_name_server_rpc,
            socket_name_listener_events=config.socket_name_listener_events
        )

    @property
    def env_dir(self) -> Path:
        """
        Returns the environment directory containing all components.

        Returns:
            Path: Directory containing the environment
        """
        return self._env_dir

    @property
    def component_dir(self) -> Path:
        """
        Returns the directory for this connector.

        Returns:
            Path: Directory containing this connector
        """
        return self._component_dir

    @property
    def socket_name_client_rpc(self) -> str:
        """
        Returns:
            str: File name of domain socket used for receiving responses from RPC server
        """
        return self._socket_name_client_rpc

    @property
    def socket_path_client_rpc(self) -> Path:
        """
        Returns:
            Path: Full path of RPC client domain socket used for receiving responses from RPC server
        """
        return self._component_dir / self.socket_name_client_rpc

    @property
    def socket_name_listener_events(self) -> str:
        """
        Returns:
            str: File name of domain socket used for receiving all events
        """
        return self._socket_name_listener_events

    @property
    def socket_path_listener_events(self) -> Path:
        """
        Returns:
            Path: Full path of domain socket used for receiving all events
        """
        return self._component_dir / self.socket_name_listener_events

    @property
    def provider_sockets_server_rpc(self) -> Callable:
        """
        Returns:
            Callable: A provider function that generates paths to the RPC server socket files
                      of each environment node within the environment directory.
        """
        return paths.files_in_subdir_provider(
            self.env_dir,
            self._socket_name_server_rpc,
            subdir_pattern=f"^{self._node_dir_prefix}"
        )

    def cleanup(self):
        """
        Removes the connector directory and all its contents from the filesystem.
        """
        shutil.rmtree(self.component_dir)


def ensure_component_dirs(env_id, component_prefix, root_dir=None):
    """
    Creates a unique component directory within a specified environment directory.

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
        env_dir = root_dir / env_id
    else:
        env_dir = paths.ensure_dirs(paths.runtime_env_dir() / env_id)

    comp_id = component_prefix + util.unique_timestamp_hex()
    component_dir = paths.ensure_dirs(env_dir / comp_id)

    return env_dir, component_dir


class EnvironmentConnector(JobInstanceObservable, ABC):
    """
    An abstract base class defining the interface for connecting to and interacting with an environment.

    Environment connectors provide access to job instances and their historical data.
    They allow monitoring and controlling jobs and retrieving information about their state and execution history.
    Connectors cannot create or run new job instances - that functionality is provided by environment nodes.
    """

    def __enter__(self):
        """
        Open the environment node.
        """
        self.open()
        return self

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def get_active_runs(self, run_match=None):
        pass

    def get_instance(self, instance_id):
        inst = self.get_instances(JobRunCriteria.instance_match(instance_id))
        return inst[0] if inst else None

    @abstractmethod
    def get_instances(self, run_match=None):
        pass

    @abstractmethod
    def read_history_runs(self, run_match, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        pass

    @abstractmethod
    def read_history_stats(self, run_match=None):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def close(self):
        pass


def local(env_id=DEFAULT_ENVIRONMENT, persistence=None, connector_layout=None) -> EnvironmentConnector:
    """
    Factory function to create a connector for the given local environment using standard components.
    This provides a convenient way to get a ready-to-use connector for local environment interaction.

    A local connector provides access to job instances and their history within a local environment,
    allowing monitoring and controlling of jobs belonging to the environment.

    Args:
        env_id (str): The identifier for the local environment. Defaults to `DEF_ENV_ID`.
        persistence (Persistence, optional): A specific persistence implementation.
            If None, a default SQLite backend is used for the environment.
        connector_layout (LocalConnectorLayout): Optional custom connector layout.
            Defaults to standard layout for local environments
    Returns:
        EnvironmentConnector: Configured connector to the local environment
    """
    layout = connector_layout or StandardLocalConnectorLayout.create(env_id)
    persistence = persistence or sqlite.create(str(paths.sqlite_db_path(env_id, create=True)))
    client = RemoteCallClient(layout.provider_sockets_server_rpc, layout.socket_path_client_rpc)
    event_receiver = EventReceiver(layout.socket_path_listener_events)
    return LocalConnector(env_id, layout, persistence, client, event_receiver)


class LocalConnector(EnvironmentConnector):
    """
    Concrete implementation of the EnvironmentConnector for interacting with local environments.

    Local environments are those running within the same operating system.
    LocalConnector uses socket-based communication to connect to environment nodes,
    enabling remote management of job instances and collection of their status and history.
    It handles both live job data via RPC calls and historical job data through persistence.
    """

    def __init__(self, env_id, connector_layout, persistence, client, event_receiver):
        self.env_id = env_id
        self._layout = connector_layout
        self._persistence = persistence
        self._client = client
        self._event_receiver = event_receiver
        self._instance_event_receiver = InstanceEventReceiver()
        self._event_receiver.register_handler(self._instance_event_receiver)

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
                log.warning(f"[remote_call_error] op=[collect_active_runs] server=[{result.server_address}]",
                            exc_info=result.error)
                continue
            active_runs.extend(result.retval)

        return active_runs

    def get_instances(self, run_match=None):
        run_results = self._client.collect_active_runs(run_match)
        instances = []

        for result in run_results:
            if result.error:
                # TODO Skip servers with errors
                continue

            server_address = result.server_address
            for job_run in result.retval:
                instances.append(JobInstanceRemote(self._client, server_address, job_run.instance_id))

        return instances

    def read_history_runs(self, run_match, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._persistence.read_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_history_stats(self, run_match=None):
        return self._persistence.read_history_stats(run_match)

    def add_observer_all_events(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._instance_event_receiver.add_observer_all_events(observer, priority)

    def remove_observer_all_events(self, observer):
        self._instance_event_receiver.remove_observer_all_events(observer)

    def add_observer_stage(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._instance_event_receiver.add_observer_stage(observer, priority)

    def remove_observer_stage(self, observer):
        self._instance_event_receiver.remove_observer_stage(observer)

    def add_observer_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._instance_event_receiver.add_observer_transition(observer, priority)

    def remove_observer_transition(self, observer):
        self._instance_event_receiver.remove_observer_transition(observer)

    def add_observer_output(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._instance_event_receiver.add_observer_output(observer, priority)

    def remove_observer_output(self, observer):
        self._instance_event_receiver.remove_observer_output(observer)

    def close(self):
        run_isolated_collect_exceptions(
            "Errors during closing local environment",
            self._event_receiver.close,
            self._client.close,
            self._persistence.close,
            self._layout.cleanup,
        )
