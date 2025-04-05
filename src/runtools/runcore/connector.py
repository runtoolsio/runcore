import logging
import shutil
from abc import ABC, abstractmethod
from threading import Event

from runtools.runcore import paths, util
from runtools.runcore.client import RemoteCallClient
from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.db import SortCriteria, sqlite
from runtools.runcore.job import JobInstanceObservable
from runtools.runcore.listening import EventReceiver, InstanceEventReceiver
from runtools.runcore.remote import JobInstanceRemote
from runtools.runcore.err import run_isolated_collect_exceptions
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY

log = logging.getLogger(__name__)

DEF_ENV_ID = 'default'


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
    Abstract class defining the filesystem structure for a local environment connector.

    Implementations of this class provide paths to socket files required by a connector to communicate
    with environment nodes as well as the paths where the connector should place its own listening sockets.
    """

    @property
    @abstractmethod
    def socket_path_client_rpc(self):
        """
        Returns:
            Path: Path to socket file used by the connector for receiving RPC responses.
        """
        pass

    @property
    @abstractmethod
    def socket_path_listener_events(self):
        """
        Returns:
            Path: Path to socket file used by the connector for receiving events.
        """
        pass

    @property
    @abstractmethod
    def provider_sockets_server_rpc(self):
        """
        Returns:
            Callable: Provider function that generates paths to the RPC server socket files of environment nodes.
        """
        pass

    @abstractmethod
    def cleanup(self):
        """
        Cleans up resources used by this connector layout.

        This typically involves removing temporary files and directories created for socket communication.
        """
        pass


class StandardLocalConnectorLayout(LocalConnectorLayout):
    """
    This class manages the filesystem layout for environment connectors, defining where socket files are located
    and how they're named.

    Example structure:
    /tmp/runtools/env/{env_id}/                      # Directory for the specific environment (env_dir)
    │
    ├── node_abc123/                                 # Environment node directory (NODE_DIR_PREFIX + id)
    │   ├── server-rpc.sock                          # Node's RPC server socket
    │   ├── client-rpc.sock                          # Node's RPC client socket
    │   ├── listener-events.sock                     # Node's events listener socket
    │   └── ...                                      # Other node-specific sockets
    │
    └── connector_789xyz/                            # Connector directory (connector_dir)
        ├── client-rpc.sock                          # Connector's RPC client socket
        ├── listener-events.sock                     # Connector's events listener socket
        └── ...                                      # Other connector-specific sockets

    Attributes:
        NODE_DIR_PREFIX (str): Prefix for node directories within the environment directory.
        CONNECTOR_DIR_PREFIX (str): Prefix for connector directories within the environment directory.
        _connector_dir (Path): Path to this connector's specific directory.
    """
    NODE_DIR_PREFIX = "node_"
    CONNECTOR_DIR_PREFIX = "connector_"

    @classmethod
    def create(cls, env_id, root_dir=None):
        """
        Creates a layout for a new connector together with a new unique directory for the connector in the environment
        directory. Connector's socket paths are all defined relative to this dedicated connector directory,
        ensuring that when sockets are created, they will be properly isolated from other connectors
        and nodes in the same environment.

        Args:
            env_id (str): Identifier for the environment
            root_dir (Path, optional): Root directory containing environments or uses the default one.
        Returns (StandardLocalConnectorLayout): Layout instance for connector
        """
        return cls(*ensure_component_dirs(env_id, cls.CONNECTOR_DIR_PREFIX, root_dir))

    def __init__(self, env_dir, connector_dir):
        """
        Initializes the connector layout with environment and connector directories.

        The connector directory is where all connector's socket files will be placed.
        Environment node sockets will be searched for within subdirectories of the environment directory.

        Args:
            env_dir (Path): Directory containing the environment structure
            connector_dir (Path): Directory specific to this connector instance
        """
        self._env_dir = env_dir
        self._connector_dir = connector_dir

    @property
    def env_dir(self):
        """
        Returns the environment directory containing all nodes and connectors.

        This directory serves as the root for the entire environment structure,
        with individual node and connector directories as subdirectories.

        Returns:
            Path: Directory containing individual environments
        """
        return self._env_dir

    @property
    def socket_name_client_rpc(self):
        """
        Returns:
            str: File name of domain socket used for receiving responses from RPC server
        """
        return 'client-rpc.sock'

    @property
    def socket_path_client_rpc(self):
        """
        Returns:
            Path: Full path of RPC client domain socket used for receiving responses from RPC server
        """
        return self._connector_dir / self.socket_name_client_rpc

    @property
    def socket_name_listener_events(self):
        """
        Returns:
            str: File name of domain socket used for receiving all events from the connected environment
        """
        return 'listener-events.sock'

    @property
    def socket_path_listener_events(self):
        """
        Returns:
            Path: Full path of domain socket used for receiving all events from the connected environment
        """
        return self._connector_dir / self.socket_name_listener_events

    @property
    def socket_name_server_rpc(self):
        """
        Returns:
            str: File name of server domain socket used for sending requests to RPC servers
        """
        return 'server-rpc.sock'

    @property
    def provider_sockets_server_rpc(self):
        """
        Returns:
            Callable: A provider function that generates paths to the RPC server socket files of
            each environment node within the environment directory.
        """
        return paths.files_in_subdir_provider(self.env_dir, self.socket_name_server_rpc,
                                              subdir_pattern=f"^{self.NODE_DIR_PREFIX}")

    def cleanup(self):
        """
        Removes the connector directory and all its contents from the filesystem.
        """
        shutil.rmtree(self._connector_dir)


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


def local(env_id=DEF_ENV_ID, persistence=None, connector_layout=None) -> EnvironmentConnector:
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
