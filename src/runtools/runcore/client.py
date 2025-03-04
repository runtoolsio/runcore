"""
This module provides a simple JSON-RPC 2.0 client interface for communicating with job instances.

The RemoteCallClient allows sending RPC commands to running job instances to:
- Query active job runs
- Stop running instances
- Get output logs
- Execute phase-specific operations

Example usage:
    with RemoteCallClient() as client:
        # Get all active runs
        results = client.collect_active_runs()

        # Stop a specific instance
        client.stop_instance(server_addr, instance_id)

        # Get output lines
        output_lines = client.get_output_tail(server_addr, instance_id, max_lines=100)

The client handles JSON-RPC protocol details and error handling while providing
a simple API for common job instance operations. It supports both single-target
and broadcast operations across multiple servers.
"""

import json
import logging
from dataclasses import dataclass
from json import JSONDecodeError
from typing import List, Any, Optional, TypeVar, Generic, Callable, Iterable

from runtools.runcore import paths
from runtools.runcore.common import RuntoolsException
from runtools.runcore.job import JobRun
from runtools.runcore.output import OutputLine
from runtools.runcore.util.json import JsonRpcResponse, JsonRpcParseError, ErrorType, ErrorCode
from runtools.runcore.util.socket import SocketClient, SocketRequestResult

log = logging.getLogger(__name__)

T = TypeVar('T')


class RemoteCallError(RuntoolsException):
    """Base exception class for remote call errors.

    Args:
        server_id: Identifier of the server where error occurred
        message: Optional error message details
    """
    def __init__(self, server_id: str, message: Optional[str] = None):
        self.server_id = server_id
        super().__init__(f"Server '{server_id}' {message or ''}")


class RemoteCallClientError(RemoteCallError):
    """Exception raised when client-side error occurs during remote call.
    This exception mean that the client implementation is probably incorrect
    or expects different version of the server.

    Args:
        server_address: Address of the target server
        message: Optional error message details
    """
    def __init__(self, server_address: str, message: Optional[str] = None):
        super().__init__(server_address, message)


class RemoteCallServerError(RemoteCallError):
    """Exception raised when server-side error occurs during remote call.

    Args:
        server_address: Address of the target server
        message: Optional error message details
    """
    def __init__(self, server_address: str, message: Optional[str] = None):
        super().__init__(server_address, message)


class TargetNotFoundError(RemoteCallError):
    """Exception raised when specified target server or method target is not found.

    Args:
        server_address: Optional address of the target server
    """
    def __init__(self, server_address: str = None):
        super().__init__(server_address)


class PhaseNotFoundError(RemoteCallError):
    """Exception raised when specified phase is not found on target server.

    Args:
        server_address: Address of the target server
    """
    def __init__(self, server_address: str):
        super().__init__(server_address)


R = TypeVar("R")


@dataclass
class RemoteCallResult(Generic[R]):
    """Contains result of a remote call operation.

    Args:
        server_address: Address of the server that handled the request
        retval: Return value from the remote call if successful
        error: Error details if the call failed
    """
    server_address: str
    retval: Optional[R]
    error: Optional[RemoteCallError] = None


def _parse_retval_or_raise_error(resp: SocketRequestResult) -> Any:
    """Parse and validate JSON-RPC response, raising appropriate errors if needed.

    Args:
        resp: Socket request result containing the response

    Returns:
        Parsed return value from the response

    Raises:
        RemoteCallServerError: For server-side errors
        RemoteCallClientError: For client-side errors
        TargetNotFoundError: When target doesn't exist
        PhaseNotFoundError: When phase doesn't exist
    """
    sid = resp.server_address
    if resp.error:
        raise RemoteCallServerError(sid, 'Socket based error occurred') from resp.error

    try:
        response_body = json.loads(resp.response)
    except JSONDecodeError as e:
        raise RemoteCallServerError(sid, 'Cannot parse API response JSON payload') from e

    try:
        json_rpc_resp = JsonRpcResponse.deserialize(response_body)
    except JsonRpcParseError as e:
        raise RemoteCallServerError(sid, 'Invalid JSON-RPC 2.0 response') from e

    if err := json_rpc_resp.error:
        if err.code.type == ErrorType.CLIENT:
            raise RemoteCallClientError(sid, str(err))
        elif err.code.type == ErrorType.SERVER:
            raise RemoteCallServerError(sid, str(err))
        elif err.code.type == ErrorType.SIGNAL:
            if err.code == ErrorCode.TARGET_NOT_FOUND:
                raise TargetNotFoundError(sid)
            elif err.code == ErrorCode.PHASE_NOT_FOUND:
                raise PhaseNotFoundError(sid)
            else:
                raise RemoteCallClientError(sid, f"Unknown signaling error: {err.code}")
        else:
            raise RemoteCallClientError(sid, f"Unknown error type: {err.code.type}")

    if "retval" not in json_rpc_resp.result:
        raise RemoteCallServerError(sid, f"Retval is missing in JSON-RPC result: {json_rpc_resp.result}")
    return json_rpc_resp.result["retval"]


def _convert_result(resp: SocketRequestResult, retval_mapper: Callable[[Any], R]) -> RemoteCallResult:
    """Parse JSON-RPC response into RemoteCallResult without raising exceptions.

    Args:
        resp: Socket request result to parse
        retval_mapper: Function to transform the return value

    Returns:
        RemoteCallResult containing either the mapped return value or error details
    """
    try:
        retval = _parse_retval_or_raise_error(resp)
        return RemoteCallResult(resp.server_address, retval_mapper(retval))
    except RemoteCallError as e:
        return RemoteCallResult(resp.server_address, None, e)


def _no_retval_mapper(retval: Any) -> Any:
    """Identity mapper that returns input value unchanged."""
    return retval


def _job_runs_retval_mapper(retval: Any) -> List[JobRun]:
    """Maps JSON job run data to JobRun objects.

    Args:
        retval: JSON data representing job runs

    Returns:
        List of JobRun objects
    """
    return [JobRun.deserialize(job_run) for job_run in retval]


class RemoteCallClient(SocketClient):
    """Client for making JSON-RPC 2.0 calls to job instances.

    Provides methods for querying and controlling job instances through remote procedure calls.
    Supports both single-target operations and broadcasting to multiple servers.

    The client implements context manager protocol for proper resource cleanup.
    """

    def __init__(self, socket_files_provider):
        """Initialize the client with default socket configuration."""
        super().__init__(socket_files_provider, client_address=str(paths.socket_path_client(True)))  # TODO
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

    def call_method(
            self,
            server_address: str,
            method: str,
            *params: Any,
            retval_mapper: Callable[[Any], R] = _no_retval_mapper) -> R:
        """Call a method on a specific server.

        Args:
            server_address: Address of target server
            method: Method name to call
            params: Method parameters
            retval_mapper: Optional function to transform return value

        Returns:
            Method return value (transformed by mapper if provided)

        Raises:
            RemoteCallServerError: For server-side errors
            RemoteCallClientError: For client-side errors
            TargetNotFoundError: When target doesn't exist
            PhaseNotFoundError: When phase doesn't exist
        """
        request_results = self._send_requests(method, *params, server_addresses=[server_address])
        if not request_results:
            raise TargetNotFoundError

        json_rpc_res = _parse_retval_or_raise_error(request_results[0])
        return retval_mapper(json_rpc_res)

    def broadcast_method(self, method: str, *params: Any, retval_mapper: Callable[[Any], R] = _no_retval_mapper) \
            -> List[RemoteCallResult[R]]:
        """Send a method call to all available servers.

        Args:
            method: Method name to call
            params: Method parameters
            retval_mapper: Optional function to transform return values

        Returns:
            List of RemoteCallResult containing responses from each server
        """
        request_results: List[SocketRequestResult] = self._send_requests(method, *params)
        return [_convert_result(req_res, retval_mapper) for req_res in request_results]

    def _send_requests(
            self,
            method: str,
            *params: Any,
            server_addresses: Iterable[str] = ()) -> List[SocketRequestResult]:
        """Send JSON-RPC requests to specified servers.

        Args:
            method: Method name
            params: Method parameters
            server_addresses: Optional list of target servers

        Returns:
            List of SocketRequestResult with responses
        """
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._next_request_id()
        }
        return self.communicate(json.dumps(request), server_addresses)

    def collect_active_runs(self, run_match) -> List[RemoteCallResult[List[JobRun]]]:
        """Retrieves information about all active job instances from all available servers.

        Args:
            run_match: Filter criteria for matching specific instances

        Returns:
            List of RemoteCallResult containing JobRun objects for matching instances
            from each responding server
        """
        return self.broadcast_method("get_active_runs", run_match.serialize(), retval_mapper=_job_runs_retval_mapper)

    def get_active_runs(self, server_address: str, run_match) -> List[JobRun]:
        """Retrieves information about active job instances from a specific server.

        Args:
            server_address: Address of the target server
            run_match: Filter criteria for matching specific instances

        Returns:
            List of JobRun objects for matching instances

        Raises:
            RemoteCallServerError: For server-side errors
            RemoteCallClientError: For client-side errors
            TargetNotFoundError: When target doesn't exist
        """
        return self.call_method(
            server_address, "get_active_runs", run_match.serialize(), retval_mapper=_job_runs_retval_mapper)

    def stop_instance(self, server_address: str, instance_id: str) -> None:
        """Stops a specific job instance.

        Args:
            server_address: Address of the target server
            instance_id: ID of the instance to stop

        Raises:
            ValueError: If instance_id is not provided
            TargetNotFoundError: If the specified server or the target instance is not found
        """
        if not instance_id:
            raise ValueError('Instance ID is mandatory for the stop operation')

        self.call_method(server_address, "stop_instance", instance_id)

    def get_output_tail(self, server_address: str, instance_id: str, max_lines: int = 100) -> List[OutputLine]:
        """Retrieves recent output lines from a specific job instance.

        Args:
            server_address: Address of the target server
            instance_id: ID of the instance to read output from
            max_lines: Maximum number of lines to retrieve (default: 100)

        Returns:
            List of OutputLine objects containing the instance output

        Raises:
            TargetNotFoundError: If the specified server or the target instance is not found
        """

        def resp_mapper(retval: Any) -> List[OutputLine]:
            return [OutputLine.deserialize(line) for line in retval]

        return self.call_method(server_address, "get_output_tail", instance_id, max_lines,
                                retval_mapper=resp_mapper)

    def exec_phase_op(self, server_address: str, instance_id: str, phase_id: str, op_name: str, *op_args) -> Any:
        """Executes an operation on a specific phase of a job instance.

        Args:
            server_address: Address of the server hosting the instance
            instance_id: ID of the target instance
            phase_id: ID of the phase to operate on
            op_name: Name of the operation to execute
            op_args: Optional arguments for the operation

        Returns:
            Operation-specific return value

        Raises:
            ValueError: If phase_id or op_name is not provided
            TargetNotFoundError: If the specified server or the target instance is not found
            PhaseNotFoundError: If the specified phase is not found
        """
        if not phase_id:
            raise ValueError('Phase ID is required for control operation')
        if not op_name:
            raise ValueError('Operation name is required for control operation')

        return self.call_method(
            server_address, "exec_phase_op", instance_id, phase_id, op_name, op_args)