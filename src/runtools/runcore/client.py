"""
This module provides classes and functions for communicating with active job instances using JSON-RPC 2.0.
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

RPC_FILE_EXTENSION = '.rpc'

T = TypeVar('T')


class RemoteCallError(RuntoolsException):
    def __init__(self, server_id: str, message: Optional[str] = None):
        self.server_id = server_id
        super().__init__(f"Server '{server_id}' {message or ''}")


class RemoteCallClientError(RemoteCallError):

    def __init__(self, server_address: str, message: Optional[str] = None):
        super().__init__(server_address, message)


class RemoteCallServerError(RemoteCallError):

    def __init__(self, server_address: str, message: Optional[str] = None):
        super().__init__(server_address, message)


class TargetNotFoundError(RemoteCallError):
    def __init__(self, server_address: str = None):
        super().__init__(server_address)


class PhaseNotFoundError(RemoteCallError):
    def __init__(self, server_address: str):
        super().__init__(server_address)


R = TypeVar("R")


@dataclass
class RemoteCallResult(Generic[R]):
    server_address: str
    retval: Optional[R]
    error: Optional[RemoteCallError] = None


def _parse_retval_or_raise_error(resp: SocketRequestResult) -> Any:
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
    """Parse JSON-RPC response into RemoteCallResult without raising exceptions"""
    try:
        retval = _parse_retval_or_raise_error(resp)
        return RemoteCallResult(resp.server_address, retval_mapper(retval))
    except RemoteCallError as e:
        return RemoteCallResult(resp.server_address, None, e)


def _no_retval_mapper(retval: Any) -> Any:
    return retval


def _job_runs_retval_mapper(retval: Any) -> List[JobRun]:
    return [JobRun.deserialize(job_run) for job_run in retval]


class RemoteCallClient(SocketClient):
    """
    Client for communicating with job instances using JSON-RPC 2.0 protocol.

    This client supports communicating with multiple servers simultaneously and
    collecting their responses. Each method returns both successful responses
    and any errors that occurred during communication.
    """

    def __init__(self):
        super().__init__(
            paths.socket_files_provider(RPC_FILE_EXTENSION),
            client_address=str(paths.socket_path_client(True))
        )
        self._request_id = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _next_request_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def call_method(
            self,
            server_address: str,
            method: str,
            *params: Any,
            retval_mapper: Callable[[Any], R] = _no_retval_mapper) -> R:
        request_results = self._send_requests(method, *params, server_addresses=[server_address])
        if not request_results:
            raise TargetNotFoundError

        json_rpc_res = _parse_retval_or_raise_error(request_results[0])
        return retval_mapper(json_rpc_res)

    def broadcast_method(self, method: str, *params: Any, retval_mapper: Callable[[Any], R] = _no_retval_mapper) \
            -> List[RemoteCallResult[R]]:
        """
        Send a JSON-RPC request to all available servers.

        Args:
            method: The JSON-RPC method name
            params: Optional additional parameters for the request
            retval_mapper: Function to map instance responses to desired type

        Returns:
            CollectedResponses containing successful responses and any errors
        """
        request_results: List[SocketRequestResult] = self._send_requests(method, *params)
        return [_convert_result(req_res, retval_mapper) for req_res in request_results]

    def _send_requests(
            self,
            method: str,
            *params: Any,
            server_addresses: Iterable[str] = ()) -> List[SocketRequestResult]:
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._next_request_id()
        }
        return self.communicate(json.dumps(request), server_addresses)

    def collect_active_runs(self, run_match=None) -> List[RemoteCallResult[List[JobRun]]]:
        """
        Retrieves instance information for all active job instances for the current user.

        Args:
            run_match: Optional filter for instance matching.

        Returns:
            CollectedResponses containing JobRun objects for matching instances.
        """
        return self.broadcast_method("get_active_runs", run_match, retval_mapper=_job_runs_retval_mapper)

    def get_active_runs(self, server_address, run_match) -> List[JobRun]:
        """
        Retrieves instance information for all active job instances for the current user.

        Args:

        Returns:
            CollectedResponses containing JobRun objects for matching instances.
        """
        return self.call_method(
            server_address, "get_active_runs", run_match, retval_mapper=_job_runs_retval_mapper)

    def stop_instance(self, server_address, instance_id) -> None:
        """
        Stops job instances that match the provided criteria.

        Args:
            server_address: Server to send the request to
            instance_id: ID of the instance to stop
        Returns:
            None

        Raises:
            ValueError: If instance_match is not provided
        """
        if not instance_id:
            raise ValueError('Instance ID is mandatory for the stop operation')

        self.call_method(server_address, "stop_instance", instance_id)

    def get_output_tail(self, server_address, instance_id, max_lines=100) -> List[OutputLine]:
        """
        Retrieves the output from job instances that match the provided criteria.

        Args:
            server_address (str): Server to send the request to
            instance_id (str): ID of the instance to read the output
            max_lines (int): Maximum lines to get

        Returns:
            CollectedResponses containing OutputResponse objects for each instance
        """

        def resp_mapper(retval: Any) -> List[OutputLine]:
            return [OutputLine.deserialize(line) for line in retval]

        return self.call_method(server_address, "get_output_tail", instance_id, max_lines,
                                retval_mapper=resp_mapper)

    def exec_phase_op(self, server_address, instance_id, phase_id: str, op_name: str, *op_args) -> Any:
        """
        Executes an operation on a specific phase of matching job instance.

        Args:
            server_address: Address where the instance is located
            instance_id: Criteria for matching instances
            phase_id: ID of the phase to control
            op_name: Name of the phase operation to execute
            op_args: Optional arguments for the operation

        Returns:
            CollectedResponses containing operation results for each instance

        Raises:
            ValueError: If required parameters are missing
        """
        if not phase_id:
            raise ValueError('Phase ID is required for control operation')
        if not op_name:
            raise ValueError('Operation name is required for control operation')

        return self.call_method(
            server_address, "exec_phase_op", instance_id, phase_id, op_name, op_args)
