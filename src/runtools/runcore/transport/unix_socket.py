"""Unix socket transport: filesystem-based layout + JSON-RPC over Unix domain sockets.

Connector-side runtime for the ``unix_socket`` transport. Nodes and connectors live on the
same host, exchange RPC over Unix sockets, broadcast events via datagram listeners, and
mark liveness with ``fcntl.flock`` on per-component ``.lock`` files.
"""

import fcntl
import json
import logging
import shutil
from abc import ABC, abstractmethod
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Callable, List, Optional

from runtools.runcore import paths, util
from runtools.runcore.client import (
    InstanceCallResult,
    TargetNotFoundError,
    _convert_result,
    _job_runs_retval_mapper,
    _no_retval_mapper,
)
from runtools.runcore.err import run_isolated_collect_exceptions
from runtools.runcore.job import InstanceID, JobInstance, JobInstanceMetadata, JobRun
from runtools.runcore.matching import JobRunCriteria
from runtools.runcore.output import OutputLine
from runtools.runcore.proxy import JobInstanceProxyBase
from runtools.runcore.run import StopReason
from runtools.runcore.transport import DiscoveredRuns, EventDrivenInstanceDirectoryBase
from runtools.runcore.util.socket import DatagramSocketServer, SocketRequestResult, StreamSocketClient

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

class UnixSocketConnectorLayout(ABC):
    """
    Abstract base class defining the filesystem structure for a unix_socket transport connector.

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


class StandardUnixSocketConnectorLayout(UnixSocketConnectorLayout):
    """
    Standard implementation of a unix_socket connector layout.

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

        Returns (StandardUnixSocketConnectorLayout): Layout instance for a connector.
        """
        return cls(*ensure_component_dir(env_id, component_prefix, root_dir))

    @property
    def env_dir(self) -> Path:
        return self._env_dir

    @property
    def listener_events_socket_path(self) -> Path:
        """
        Returns:
            Path: Full path of domain socket used for receiving all events
        """
        # TODO Only the combined events socket is exposed. The node dispatcher already routes
        #      per event type, so per-type connector listener sockets (+ receiver binding) could
        #      be added here to let a connector subscribe to a subset of event types.
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
                log.warning("Unexpected error probing lock", extra={"entry": str(entry)}, exc_info=True)
                fd.close()
                continue
            # Lock acquired — owner is dead
            fd.close()
            shutil.rmtree(entry, ignore_errors=True)
            removed.append(entry)
            log.debug("Removed stale component dir", extra={"dir": str(entry)})
        except OSError as e:
            log.debug("Skipping stale check", extra={"entry": str(entry), "reason": str(e)})

    return removed


# ---------------------------------------------------------------------------
# RPC client
# ---------------------------------------------------------------------------

class UnixSocketRpcClient(StreamSocketClient):
    """Client for communicating with job instances over the unix_socket transport.

    Uses JSON-RPC 2.0 over Unix domain sockets to query and control job instances
    running on the same machine. Supports both single-target operations and
    broadcasting to multiple instance servers.

    The client implements context manager protocol for proper resource cleanup.
    """

    def __init__(self, server_sockets_provider):
        """Initialize the client with server socket provider."""
        super().__init__(server_sockets_provider)
        self._request_id = 0

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit ensuring client is closed."""
        self.close()

    def _next_request_id(self) -> int:
        """Generate next sequential request ID."""
        self._request_id += 1
        return self._request_id

    def broadcast_method(self, method: str, *params: Any, retval_mapper: Callable[[Any], Any] = _no_retval_mapper) \
            -> List[InstanceCallResult]:
        """Send a method call to all reachable servers.

        Args:
            method: Method name to call
            params: Method parameters
            retval_mapper: Optional function to transform return values

        Returns:
            List of InstanceCallResult containing responses from each server
        """
        request_results: List[SocketRequestResult] = self._send_requests(method, *params)
        return [_convert_result(req_res, retval_mapper) for req_res in request_results]

    def _send_requests(self, method: str, *params: Any) -> List[SocketRequestResult]:
        """Broadcast a JSON-RPC request to all reachable servers.

        Args:
            method: Method name
            params: Method parameters

        Returns:
            List of SocketRequestResult with responses
        """
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._next_request_id()
        }
        return self.communicate(json.dumps(request), ())

    def collect_active_runs(self, run_match) -> List[InstanceCallResult]:
        """Retrieves information about all active job instances from all available servers.

        Args:
            run_match: Filter criteria for matching specific instances

        Returns:
            List of InstanceCallResult containing JobRun objects for matching instances
            from each responding server
        """
        if not run_match:
            run_match = JobRunCriteria.all()
        return self.broadcast_method("get_active_runs", run_match.serialize(), retval_mapper=_job_runs_retval_mapper)

    def _broadcast_instance_command(self, method: str, instance_id: InstanceID, *params,
                                    retval_mapper: Callable[[Any], Any] = _no_retval_mapper) -> Any:
        """Broadcast an instance-keyed command — only the owning node acts.

        Returns the owning node's return value. Raises TargetNotFoundError when no
        node owns the instance; any other node-side error is raised as-is.
        """
        results = self.broadcast_method(method, instance_id.serialize(), *params, retval_mapper=retval_mapper)
        for result in results:
            if result.error is None:
                return result.retval
        for result in results:
            if result.error is not None and not isinstance(result.error, TargetNotFoundError):
                raise result.error
        raise TargetNotFoundError(str(instance_id))

    def stop_instance(self, instance_id: InstanceID, stop_reason: StopReason = StopReason.STOPPED) -> None:
        """Stops a job instance, wherever in the environment it runs.

        Args:
            instance_id: ID of the instance to stop
            stop_reason: Reason for stopping the instance

        Raises:
            ValueError: If instance_id is not provided
            TargetNotFoundError: If no node owns the instance
        """
        if not instance_id:
            raise ValueError('Instance ID is mandatory for the stop operation')

        self._broadcast_instance_command("stop_instance", instance_id, stop_reason.name)

    def get_output_tail(self, instance_id: InstanceID, max_lines: int = 100) -> List[OutputLine]:
        """Retrieves recent output lines from a job instance, wherever it runs.

        Args:
            instance_id: ID of the instance to read output from
            max_lines: Maximum number of lines to retrieve (default: 100)

        Returns:
            List of OutputLine objects containing the instance output

        Raises:
            TargetNotFoundError: If no node owns the instance
        """

        def resp_mapper(retval: Any) -> List[OutputLine]:
            return [OutputLine.deserialize(line) for line in retval]

        return self._broadcast_instance_command("get_output_tail", instance_id, max_lines,
                                                retval_mapper=resp_mapper)

    def exec_phase_op(self, instance_id: InstanceID, phase_id: str, op_name: str, *op_args) -> Any:
        """Executes an operation on a phase of a job instance, wherever it runs.

        Args:
            instance_id: ID of the target instance
            phase_id: ID of the phase to operate on
            op_name: Name of the operation to execute
            op_args: Optional arguments for the operation

        Returns:
            Operation-specific return value

        Raises:
            ValueError: If phase_id or op_name is not provided
            TargetNotFoundError: If no node owns the instance
            PhaseNotFoundError: If the specified phase is not found
        """
        if not phase_id:
            raise ValueError('Phase ID is required for control operation')
        if not op_name:
            raise ValueError('Operation name is required for control operation')

        return self._broadcast_instance_command("exec_phase_op", instance_id, phase_id, op_name, op_args)


# ---------------------------------------------------------------------------
# Event receiver
# ---------------------------------------------------------------------------

def _missing_field_txt(obj, missing):
    return f"event=[invalid_event] object=[{obj}] reason=[missing field: {missing}]"


def _read_event_metadata(req_body_json):
    event_metadata = req_body_json.get('event_metadata')
    if not event_metadata:
        raise ValueError(_missing_field_txt('root', 'event_metadata'))

    event_type = event_metadata.get('event_type')
    if not event_type:
        raise ValueError(_missing_field_txt('event_metadata', 'event_type'))

    instance_metadata = event_metadata.get('instance')
    if not instance_metadata:
        raise ValueError(_missing_field_txt('event_metadata', 'instance'))

    return event_type, JobInstanceMetadata.deserialize(instance_metadata)


class UnixSocketEventReceiver(DatagramSocketServer):
    """Datagram-socket receiver for node-broadcast events.

    Listens on a Unix datagram socket for serialized events emitted by node-side
    :class:`UnixSocketEventDispatcher` instances and routes the parsed payload to registered
    handlers.
    """

    def __init__(self, socket_path, instance_match=None):
        """
        Initialize the event receiver.

        Args:
            socket_path: Path to the socket file
            instance_match: Optional filter for instances
        """
        super().__init__(socket_path, allow_ping=True)
        self.instance_match = instance_match
        self._event_handlers = {}
        self._default_handlers = []

    def register_handler(self, handler, *event_types: str):
        """
        Register a handler for a specific event type.

        Args:
            event_types: Event type identifier
            handler: Handler function to process events of this type
        """
        if event_types:
            for event_type in event_types:
                self._event_handlers[event_type] = handler
        else:
            self._default_handlers.append(handler)

        return self

    def handle(self, req_body):
        """Process received event data."""
        try:
            req_body_json = json.loads(req_body)
        except JSONDecodeError:
            log.warning("Invalid JSON event received", extra={"length": len(req_body)})
            return

        try:
            event_type, instance_meta = _read_event_metadata(req_body_json)
        except ValueError as e:
            log.warning("Event deserialization error", extra={"detail": str(e)})
            return

        # Check if this event type should be handled by this receiver
        if self.instance_match and not self.instance_match(instance_meta):
            return

        handlers = list(self._default_handlers)
        if event_type in self._event_handlers:
            handlers.append(self._event_handlers[event_type])
        for handler in handlers:
            try:
                handler(event_type, instance_meta, req_body_json.get("event"))
            except Exception:
                log.exception("Event handler failed event_type=%s", event_type,
                              extra={"event_type": event_type, "instance": str(instance_meta)})


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

class UnixSocketInstanceDiscovery:
    """Find active runs by broadcasting to Unix socket RPC servers.

    Each discovery sweep asks all reachable node servers for their active runs. The
    returned :class:`DiscoveredRuns` is incomplete if any server fails to answer.
    """

    def __init__(self, rpc_client: UnixSocketRpcClient):
        self._client = rpc_client

    def discover_active_runs(self, run_match=None) -> DiscoveredRuns:
        """Run one broadcast discovery sweep."""
        runs = []
        complete = True
        for result in self._client.collect_active_runs(run_match):
            if result.error:
                log.warning("Instance call error op=discover_active_runs",
                            extra={"op": "discover_active_runs", "server": str(result.server_address)},
                            exc_info=result.error)
                complete = False
                continue
            runs.extend(result.retval)
        return DiscoveredRuns(runs, complete)


# ---------------------------------------------------------------------------
# Instance proxy
# ---------------------------------------------------------------------------

class UnixSocketJobInstanceProxy(JobInstanceProxyBase):
    """Proxy to a job instance reachable over the unix_socket transport.

    Commands and output reads are instance-keyed broadcasts: only the owning node
    acts, so the proxy needs no routing — wherever the instance runs, it is reached.
    """

    def __init__(self, initial: JobRun, client: UnixSocketRpcClient):
        super().__init__(initial)
        self._client = client

    def stop(self, stop_reason=StopReason.STOPPED):
        self._client.stop_instance(self.id, stop_reason)

    def _exec_phase_op(self, phase_id: str, op_name: str, *op_args):
        return self._client.exec_phase_op(self.id, phase_id, op_name, *op_args)

    def _fetch_output_tail(self, max_lines: int) -> List[OutputLine]:
        return self._client.get_output_tail(self.id, max_lines)


# ---------------------------------------------------------------------------
# Instance directory
# ---------------------------------------------------------------------------

class UnixSocketInstanceDirectory(EventDrivenInstanceDirectoryBase):
    """Instance directory over unix domain sockets.

    Uses a local component directory for its sockets and liveness lock, a Unix
    socket RPC client for commands and output reads, and a datagram receiver for
    instance events.
    """

    def __init__(self,
                 layout: UnixSocketConnectorLayout,
                 rpc_client: UnixSocketRpcClient,
                 discovery: UnixSocketInstanceDiscovery,
                 event_receiver: UnixSocketEventReceiver):
        super().__init__(discovery)
        self._layout = layout
        self._rpc_client = rpc_client
        self._event_receiver = event_receiver

    def _create_proxy(self, initial: JobRun) -> JobInstance:
        return UnixSocketJobInstanceProxy(initial, self._rpc_client)

    def _start_receiving(self, handler) -> None:
        self._event_receiver.register_handler(handler)
        self._event_receiver.start()

    def _close_resources(self) -> None:
        run_isolated_collect_exceptions(
            "Errors during closing unix_socket instance directory",
            self._event_receiver.close,
            self._rpc_client.close,
            self._layout.cleanup,
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_instance_directory(env_id: str, root_dir: Optional[Path] = None) -> UnixSocketInstanceDirectory:
    """Create the Unix socket directory for an environment."""
    # Sweep stale components before allocating our own — no need to scan past our live lock.
    clean_stale_component_dirs(resolve_env_dir(env_id, root_dir))
    layout = StandardUnixSocketConnectorLayout.create(env_id, root_dir)
    rpc_client = UnixSocketRpcClient(layout.server_sockets_provider)
    discovery = UnixSocketInstanceDiscovery(rpc_client)
    event_receiver = UnixSocketEventReceiver(layout.listener_events_socket_path)
    return UnixSocketInstanceDirectory(layout, rpc_client, discovery, event_receiver)
