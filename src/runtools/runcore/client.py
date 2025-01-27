"""
This module provides classes and functions for communicating with active job instances using JSON-RPC 2.0.
"""

import json
import logging
from dataclasses import dataclass
from enum import Enum, auto
from json import JSONDecodeError
from typing import List, Any, Dict, Optional, TypeVar, Generic, Callable

from runtools.runcore import paths
from runtools.runcore.common import RuntoolsException
from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.job import JobRun, JobInstanceMetadata
from runtools.runcore.output import OutputLine
from runtools.runcore.util.json import JsonRpcResponse, JsonRpcParseError, ErrorType, ErrorCode
from runtools.runcore.util.socket import SocketClient, ServerResponse

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

    def __init__(self, server_id: str, message: str):
        super().__init__(server_id, message)


class InstanceNotFoundError(ApiError):
    def __init__(self, server_id: str = None):
        super().__init__(server_id)


class PhaseNotFoundError(ApiError):
    def __init__(self, server_id: str):
        super().__init__(server_id)


@dataclass
class JsonRpcError:
    code: int
    message: str
    data: Optional[Any] = None

    @classmethod
    def from_dict(cls, error_dict: dict) -> 'JsonRpcError':
        return cls(
            code=error_dict['code'],
            message=error_dict['message'],
            data=error_dict.get('data')
        )


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


class ApprovalResult(Enum):
    APPROVED = auto()
    NOT_APPLICABLE = auto()
    UNKNOWN = auto()


@dataclass
class ApprovalResponse(JobInstanceResponse):
    release_result: ApprovalResult


class StopResult(Enum):
    STOP_INITIATED = auto()
    NOT_APPLICABLE = auto()
    UNKNOWN = auto()


@dataclass
class StopResponse(JobInstanceResponse):
    stop_result: StopResult


@dataclass
class OutputResponse(JobInstanceResponse):
    output: List[OutputLine]


@dataclass
class SignalDispatchResponse(JobInstanceResponse):
    dispatched: bool


def process_multi_server_responses(
        server_responses: List[ServerResponse],
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
                response_error=JsonRpcError.from_dict(response['error'])
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


def _no_resp_mapper(instance_result: InstanceResult) -> InstanceResult:
    return instance_result


def _parse_success_response_or_raise(resp: ServerResponse) -> JsonRpcResponse:
    sid = resp.server_id
    if resp.error:
        raise ApiServerError(sid, 'Socket based error occurred') from resp.error

    try:
        response_body = json.loads(resp.api_response)
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

    return json_rpc_resp


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

    def send_single_instance_request(
            self,
            server_id: str,
            method: str,
            *params: Optional[List],
            response_mapper: Callable[[Any], T] = _no_resp_mapper):
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._next_request_id()
        }
        server_responses: List[ServerResponse] = self.communicate(json.dumps(request), [server_id])
        if not server_responses:
            raise InstanceNotFoundError

        json_rpc_resp: JsonRpcResponse = _parse_success_response_or_raise(server_responses[0])
        return response_mapper(json_rpc_resp.result)

    def send_request(
            self,
            method: str,
            run_match=None,
            params: Optional[dict] = None,
            resp_mapper: Callable[[InstanceResult], T] = _no_resp_mapper
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

    def approve_pending_instances(self, run_match, phase_id=None) -> CollectedResponses[ApprovalResponse]:
        """
        Approves job instances that are pending in the provided phase and match the criteria.

        Args:
            run_match: Criteria for matching instances to approve
            phase_id: Optional ID of the approval phase

        Returns:
            CollectedResponses containing ApprovalResponse objects for each instance

        Raises:
            ValueError: If run_match is not provided
        """
        if run_match is None:
            raise ValueError("Missing run criteria (match)")

        def approve_resp_mapper(inst_resp: InstanceResult) -> ApprovalResponse:
            return ApprovalResponse(
                instance_metadata=inst_resp.instance,
                release_result=ApprovalResult.APPROVED if inst_resp.result[
                    "approved"] else ApprovalResult.NOT_APPLICABLE
            )

        params = {"phase_id": phase_id} if phase_id else {}
        return self.send_request("instances.approve", run_match, params, approve_resp_mapper)

    def stop_instances(self, server_id, instance_id) -> None:
        """
        Stops job instances that match the provided criteria.

        Args:
            server_id: Server to send the request to
            instance_id: Criteria for matching instances to stop

        Returns:
            None

        Raises:
            ValueError: If instance_match is not provided
        """
        if not instance_id:
            raise ValueError('Instance ID is mandatory for the stop operation')

        self.send_single_instance_request(server_id, "stop_instance", instance_id)

    def get_tail(self, instance_match=None) -> CollectedResponses[OutputResponse]:
        """
        Retrieves the output from job instances that match the provided criteria.

        Args:
            instance_match: Optional criteria for matching instances

        Returns:
            CollectedResponses containing OutputResponse objects for each instance
        """

        def resp_mapper(inst_resp: InstanceResult) -> OutputResponse:
            return OutputResponse(inst_resp.instance,
                                  [OutputLine.deserialize(line) for line in inst_resp.result["tail"]])

        return self.send_request("instances.output.tail", instance_match, resp_mapper=resp_mapper)

    def signal_dispatch(self, instance_match, queue_id) -> CollectedResponses[SignalDispatchResponse]:
        """
        Signals dispatch for instances matching the criteria in the specified queue.

        Args:
            instance_match: Criteria for matching instances
            queue_id: ID of the queue to dispatch from

        Returns:
            CollectedResponses containing SignalDispatchResponse objects for each instance

        Raises:
            ValueError: If queue_id is not provided
        """
        if not queue_id:
            raise ValueError('Queue ID is required for dispatch operation')

        def resp_mapper(inst_resp: InstanceResult) -> SignalDispatchResponse:
            return SignalDispatchResponse(inst_resp.instance, inst_resp.result["dispatched"])

        params = {"queue_id": queue_id}
        return self.send_request("instances.dispatch", instance_match, params, resp_mapper=resp_mapper)

    def phase_control(self, instance_match, phase_id: str, op_name: str, op_args: Optional[list] = None) -> \
            CollectedResponses[InstanceResult]:
        """
        Executes a control operation on a specific phase of matching job instances.

        Args:
            instance_match: Criteria for matching instances
            phase_id: ID of the phase to control
            op_name: Name of the operation to execute
            op_args: Optional arguments for the operation (dict for named args, list for positional)

        Returns:
            CollectedResponses containing operation results for each instance

        Raises:
            ValueError: If required parameters are missing
        """
        if not phase_id:
            raise ValueError('Phase ID is required for control operation')
        if not op_name:
            raise ValueError('Operation name is required for control operation')

        params = {
            "phase_id": phase_id,
            "op_name": op_name,
            "op_args": op_args if op_args is not None else []
        }

        return self.send_request("instances.phase.control", instance_match, params)
