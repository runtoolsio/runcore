"""
This module provides classes and functions for communicating with active job instances using JSON-RPC 2.0.
"""

import json
import logging
from dataclasses import dataclass
from json import JSONDecodeError
from typing import List, Any, Dict, Optional, TypeVar, Generic, Callable

from runtools.runcore import paths
from runtools.runcore.common import RuntoolsException
from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.job import JobRun, JobInstanceMetadata
from runtools.runcore.output import OutputLine
from runtools.runcore.util.json import JsonRpcResponse, JsonRpcParseError, ErrorType, ErrorCode, JsonRpcError
from runtools.runcore.util.socket import SocketClient, RequestResult

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'

T = TypeVar('T')


class ApiError(RuntoolsException):
    def __init__(self, server_id: str, message: str = None):
        self.server_id = server_id
        super().__init__(f"Server '{server_id}' {message or ''}")


class ApiClientError(ApiError):
    pass


class ApiServerError(ApiError):

    def __init__(self, server_address: str, message: str):
        super().__init__(server_address, message)


class InstanceNotFoundError(ApiError):
    def __init__(self, server_address: str = None):
        super().__init__(server_address)


class PhaseNotFoundError(ApiError):
    def __init__(self, server_address: str):
        super().__init__(server_address)


@dataclass
class InstanceResult:
    """
    Represents data for a single job instance from a JSON-RPC response.

    Attributes:
        instance: Metadata about the job instance.
        result: The JSON body of the response for this instance.
    """
    instance: JobInstanceMetadata
    result: Dict[str, Any]


@dataclass
class APIError:
    server_id: str
    socket_error: Optional[Exception] = None
    response_error: Optional[JsonRpcError] = None
    parse_error: Optional[str] = None


@dataclass
class CollectedResponses(Generic[T]):
    """
    Represents responses and errors collected from multiple API endpoints.

    This class handles responses from multiple API calls by collecting both
    successful responses and any errors into separate lists.

    Attributes:
        successful: A list of responses of type T that completed without error.
        errors: A list of APIError instances representing errors that occurred during the API calls.
    """
    successful: List[T]
    errors: List[APIError]

    def __iter__(self):
        return iter((self.successful, self.errors))


@dataclass
class JobInstanceResponse:
    instance_metadata: JobInstanceMetadata


def process_multi_server_responses(
        server_responses: List[RequestResult],
        resp_mapper: Callable[[InstanceResult], T]
) -> CollectedResponses[T]:
    """
    Process JSON-RPC 2.0 responses from multiple servers.

    Args:
        server_responses: List of responses from different servers
        resp_mapper: Function to map each instance result to the desired type

    Returns:
        CollectedResponses containing successful responses and errors
    """
    successful: List[T] = []
    errors: List[APIError] = []

    for server_id, resp, error in server_responses:
        # Handle socket communication errors
        if error:
            errors.append(APIError(server_id=server_id, socket_error=error))
            continue

        try:
            response = json.loads(resp)
        except JSONDecodeError as e:
            errors.append(APIError(server_id=server_id, parse_error=str(e)))
            continue

        # Validate JSON-RPC 2.0 response structure
        if not isinstance(response, dict) or 'jsonrpc' not in response or response['jsonrpc'] != '2.0':
            errors.append(APIError(
                server_id=server_id,
                parse_error="Invalid JSON-RPC 2.0 response format"
            ))
            continue

        # Handle JSON-RPC errors
        if 'error' in response:
            errors.append(APIError(
                server_id=server_id,
                response_error=JsonRpcError.deserialize(response['error'])
            ))
            continue

        # Process successful responses for each instance
        try:
            for instance_data in response['result']:
                instance_metadata = JobInstanceMetadata.deserialize(instance_data['instance_metadata'])
                instance_result = InstanceResult(instance=instance_metadata, result=instance_data)
                successful.append(resp_mapper(instance_result))
        except Exception as e:
            errors.append(APIError(
                server_id=server_id,
                parse_error=f"Error mapping result: {str(e)}"
            ))

    return CollectedResponses(successful, errors)


def _no_retval_mapper(retval: Any) -> Any:
    return retval


def _parse_retval_or_raise_error(resp: RequestResult) -> Any:
    sid = resp.server_address
    if resp.error:
        raise ApiServerError(sid, 'Socket based error occurred') from resp.error

    try:
        response_body = json.loads(resp.response)
    except JSONDecodeError as e:
        raise ApiServerError(sid, 'Cannot parse API response JSON payload') from e

    try:
        json_rpc_resp = JsonRpcResponse.deserialize(response_body)
    except JsonRpcParseError as e:
        raise ApiServerError(sid, 'Invalid JSON-RPC 2.0 response') from e

    if err := json_rpc_resp.error:
        if err.code.type == ErrorType.CLIENT:
            raise ApiClientError(sid, str(err))
        elif err.code.type == ErrorType.SERVER:
            raise ApiServerError(sid, str(err))
        elif err.code.type == ErrorType.SIGNAL:
            if err.code == ErrorCode.INSTANCE_NOT_FOUND:
                raise InstanceNotFoundError(sid)
            elif err.code == ErrorCode.PHASE_NOT_FOUND:
                raise PhaseNotFoundError(sid)
            else:
                raise ApiClientError(sid, f"Unknown signaling error: {err.code}")
        else:
            raise ApiClientError(sid, f"Unknown error type: {err.code.type}")

    if "retval" not in json_rpc_resp.result:
        raise ApiServerError(sid, f"Retval is missing in JSON-RPC result: {json_rpc_resp.result}")
    return json_rpc_resp.result["retval"]


class APIClient(SocketClient):
    """
    Client for communicating with job instances using JSON-RPC 2.0 protocol.

    This client supports communicating with multiple servers simultaneously and
    collecting their responses. Each method returns both successful responses
    and any errors that occurred during communication.
    """

    def __init__(self):
        super().__init__(
            paths.socket_files_provider(API_FILE_EXTENSION),
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

    def execute_instance_method(
            self,
            server_address: str,
            method: str,
            *params: Any,
            response_mapper: Callable[[Any], T] = _no_retval_mapper):
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._next_request_id()
        }
        request_results: List[RequestResult] = self.communicate(json.dumps(request), [server_address])
        if not request_results:
            raise InstanceNotFoundError

        json_rpc_res = _parse_retval_or_raise_error(request_results[0])
        return response_mapper(json_rpc_res)

    def send_request(
            self,
            method: str,
            run_match=None,
            params: Optional[dict] = None,
            resp_mapper: Callable[[InstanceResult], T] = _no_retval_mapper
    ) -> CollectedResponses[T]:
        """
        Send a JSON-RPC request to all available servers.

        Args:
            method: The JSON-RPC method name
            run_match: Optional criteria for matching job instances
            params: Optional additional parameters for the request
            resp_mapper: Function to map instance responses to desired type

        Returns:
            CollectedResponses containing successful responses and any errors
        """
        if params is None:
            params = {}

        run_match = run_match or JobRunCriteria.all()
        params["run_match"] = run_match.serialize()

        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._next_request_id()
        }

        server_responses = self.communicate(json.dumps(request))
        return process_multi_server_responses(server_responses, resp_mapper)

    def get_active_runs(self, run_match=None) -> CollectedResponses[JobRun]:
        """
        Retrieves instance information for all active job instances for the current user.

        Args:
            run_match: Optional filter for instance matching.

        Returns:
            CollectedResponses containing JobRun objects for matching instances.
        """

        def resp_mapper(inst_resp: InstanceResult) -> JobRun:
            return JobRun.deserialize(inst_resp.result["job_run"])

        return self.send_request("instances.get", run_match, resp_mapper=resp_mapper)

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

        self.execute_instance_method(server_address, "stop_instance", instance_id)

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

        return self.execute_instance_method(server_address, "get_output_tail", instance_id, max_lines,
                                            response_mapper=resp_mapper)

    def exec_phase_op(self, server_address, instance_id, phase_id: str, op_name: str, *op_args) -> \
            CollectedResponses[InstanceResult]:
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

        return self.execute_instance_method(
            server_address, "exec_phase_op", instance_id, phase_id, op_name, op_args)
