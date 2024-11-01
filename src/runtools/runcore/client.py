"""
This module provides classes and functions for communicating with active job instances.
"""

import json
import logging
from dataclasses import dataclass
from enum import Enum, auto
from json import JSONDecodeError
from typing import List, Any, Dict, NamedTuple, Optional, TypeVar, Generic, Callable, Tuple

from runtools.runcore import paths
from runtools.runcore.job import JobRun
from runtools.runcore.run import InstanceMetadata, JobInstanceMetadata
from runtools.runcore.util.socket import SocketClient, ServerResponse, Error

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'


class InstanceResponse(NamedTuple):
    """
    Represents generic data for a single job instance,
    extracted from a successful de-serialized and pre-processed Instances API response.

    Note that the Instances API may manage several job instances and thus may
    return data for multiple instances, with each instance represented as a single instance response.

    Attributes:
        instance_meta: Metadata about the job instance.
        body: The JSON body of the response, as a dictionary.
    """
    instance_meta: InstanceMetadata
    body: Dict[str, Any]


class APIErrorType(Enum):
    """
    This enumeration defines the types of errors that can occur during communication with API.
    """

    SOCKET = auto()  # Errors related to the socket communication
    INVALID_RESPONSE = auto()  # Errors arising when the API's response cannot be processed correctly
    API_SERVER = auto()  # Errors signaled in the standard response indicating a problem on the server side
    API_CLIENT = auto()  # Errors resulting from client-side issues such as invalid request


class ErrorCode(Enum):
    INVALID_REQUEST = 400
    NOT_FOUND = 404
    INVALID_ENTITY = 422
    UNKNOWN = 1


@dataclass
class ResponseError:
    """
    Represents an error returned in the response from an API.

    This class encapsulates details about an error that the API itself has generated,
    either due to server-side issues or client-request related problems.

    Attributes:
        code: An enumeration member representing the type of error, as defined by the ErrorCode enumeration.
        reason: A human-readable string providing more details about the cause of the error.
    """

    code: ErrorCode
    reason: str


@dataclass
class APIError:
    """
    Represents an error that occurred during communication with an API.

    Attributes:
        api_id: Identifier of the API which generated the error.
        error_type: The type of error, as defined by the APIErrorType enumeration.
        socket_error: An optional object, only present in case of `APIErrorType.SOCKET` error.
        response_error: Details of the error returned by the API, only present in case of `APIErrorType.API_*` errors.
    """

    api_id: str
    error_type: APIErrorType
    socket_error: Optional[Error]
    response_error: Optional[ResponseError]


T = TypeVar('T')


@dataclass
class CollectedResponses(Generic[T]):
    """
    Represents responses and errors collected from multiple API endpoints.

    This class handles responses from multiple API calls by collecting both
    successful responses and any errors into separate lists.

    Attributes:
        successful: A list of responses of type T that completed without error.
        errors: A list of APIError instances representing errors that occurred during the API calls.

    Note: For the Instances API, a single endpoint's response may contain multiple instance responses.
          This means the number of collected responses can exceed the number of contacted API endpoints.
    """

    successful: List[T]
    errors: List[APIError]

    def __iter__(self):
        return iter((self.successful, self.errors))


@dataclass
class JobInstanceResponse:
    instance_metadata: InstanceMetadata


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
    output: List[Tuple[str, bool]]


@dataclass
class SignalDispatchResponse(JobInstanceResponse):
    dispatched: bool


def _no_resp_mapper(api_instance_response: InstanceResponse) -> InstanceResponse:
    return api_instance_response


class APIClient(SocketClient):

    def __init__(self):
        super().__init__(paths.socket_files_provider(API_FILE_EXTENSION), bidirectional=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def send_request(self, api: str, run_match=None, req_body=None,
                     resp_mapper: Callable[[InstanceResponse], T] = _no_resp_mapper) -> CollectedResponses[T]:
        if not req_body:
            req_body = {}
        req_body["request_metadata"] = {"api": api}
        if run_match and run_match.metadata_criteria:
            req_body["request_metadata"]["run_match"] = run_match.serialize()

        server_responses: List[ServerResponse] = self.communicate(json.dumps(req_body))
        return _process_responses(server_responses, resp_mapper)

    def get_active_runs(self, run_match=None) -> CollectedResponses[JobRun]:
        """
        Retrieves instance information for all active job instances for the current user.

        Args:
            run_match (JobRunAggregatedCriteria, optional):
                A filter for instance matching. If provided, only instances that match will be included.

        Returns:
            A container holding the :class:`JobInst` objects that represent job instances.
            It also includes any errors that may have happened, each one related to a specific server API.

        Raises:
            PayloadTooLarge: If the payload size exceeds the maximum limit.
        """

        def resp_mapper(inst_resp: InstanceResponse) -> JobRun:
            return JobRun.deserialize(inst_resp.body["job_run"])

        return self.send_request('/instances', run_match, resp_mapper=resp_mapper)

    def approve_pending_instances(self, run_match, phase_id=None) -> CollectedResponses[ApprovalResponse]:
        """
        This function releases job instances that are pending in the provided group
        and optionally match the provided criteria.

        Args:
            run_match (InstanceMatchCriteria, optional):
                The operation will affect only instances matching these criteria or all instances if not provided.
            phase_id (str, mandatory):
                ID of the approval phase.
        Returns:
            A container holding :class:`ReleaseResponse` objects, each representing the result of the release operation
            for a respective job instance.
            It also includes any errors that may have happened, each one related to a specific server API.
        """

        if run_match is None:
            raise ValueError("Missing run criteria (match)")

        def approve_resp_mapper(inst_resp: InstanceResponse) -> ApprovalResponse:
            try:
                release_res = ApprovalResult[inst_resp.body["approval_result"].upper()]
            except KeyError:
                release_res = ApprovalResult.UNKNOWN
            return ApprovalResponse(inst_resp.instance_meta, release_res)

        req_body = {"phase_id": phase_id}
        return self.send_request('/instances/approve', run_match, req_body, approve_resp_mapper)

    def stop_instances(self, instance_match) -> CollectedResponses[StopResponse]:
        """
        This function stops job instances that match the provided criteria.

        Args:
            instance_match (InstanceMatchCriteria, mandatory):
                The operation will affect only instances matching these criteria.

        Returns:
            A container holding :class:`StopResponse` objects, each representing the result of the stop operation
            for a respective job instance.
            It also includes any errors that may have happened, each one related to a specific server API.

        Note:
            The stop operation might not succeed if the instance doesn't correctly handle stop/terminate signals.
        """

        if not instance_match:
            raise ValueError('Id matching criteria is mandatory for the stop operation')

        def resp_mapper(inst_resp: InstanceResponse) -> StopResponse:
            return StopResponse(inst_resp.instance_meta, StopResult[inst_resp.body["stop_result"].upper()])

        return self.send_request('/instances/stop', instance_match, resp_mapper=resp_mapper)

    def fetch_output(self, instance_match=None) -> CollectedResponses[OutputResponse]:
        """
        This function requests the last lines of the output from job instances
        that optionally match the provided criteria.

        Args:
            instance_match (InstanceMatchCriteria, optional):
                The operation will affect only instances matching these criteria.
                If not provided, the tail of all instances is read.

        Returns:
            A container holding :class:`TailResponse` objects, each containing last lines for a respective job instance.
            It also includes any errors that may have happened, each one related to a specific server API.
        """

        def resp_mapper(inst_resp: InstanceResponse) -> OutputResponse:
            return OutputResponse(inst_resp.instance_meta, inst_resp.body["output"])

        return self.send_request('/instances/output', instance_match, resp_mapper=resp_mapper)

    def signal_dispatch(self, instance_match, queue_id) -> CollectedResponses[SignalDispatchResponse]:
        def resp_mapper(inst_resp: InstanceResponse) -> SignalDispatchResponse:
            return SignalDispatchResponse(inst_resp.instance_meta, inst_resp.body["dispatched"])

        req_body = {"queue_id": queue_id}
        return self.send_request('/instances/_signal/dispatch', instance_match, req_body, resp_mapper=resp_mapper)


def _process_responses(server_responses: List[ServerResponse], resp_mapper: Callable[[InstanceResponse], T]) \
        -> CollectedResponses[T]:
    responses: List[T] = []
    errors: List[APIError] = []

    for server_id, resp, error in server_responses:
        if error:
            log.error("event=[api_error] type=[socket] error=[%s]", error)
            errors.append(APIError(server_id, APIErrorType.SOCKET, error, None))
            continue

        try:
            resp_body = json.loads(resp)
        except JSONDecodeError:
            # TODO Mostly when the resp is too long (i.e. server with many instances)
            raise
        resp_metadata = resp_body.get("response_metadata")
        if not resp_metadata:
            log.error("event=[api_error] type=[invalid_response] error=[missing_response_metadata]")
            errors.append(APIError(server_id, APIErrorType.INVALID_RESPONSE, None, None))
            continue
        if "error" in resp_metadata:
            code = resp_metadata.get('code')
            reason = resp_metadata['error'].get('reason')
            if not code or code < 400 or code >= 600:
                log.error("event=[api_error] type=[invalid_response] error=[invalid_response_code] code=[%s]", code)
                errors.append(APIError(server_id, APIErrorType.INVALID_RESPONSE, None, None))
                continue
            if not reason:
                log.error("event=[api_error] type=[invalid_response] error=[missing_error_reason] code=[%s]", code)
                errors.append(APIError(server_id, APIErrorType.INVALID_RESPONSE, None, None))
                continue

            error_type = APIErrorType.API_CLIENT if code < 500 else APIErrorType.API_SERVER
            try:
                err_code = ErrorCode(code)
            except ValueError:
                log.warning("event=[unknown_error_code] code=[%s]", code)
                err_code = ErrorCode.UNKNOWN
            log.error("event=[api_error] type=[%s] code=[%s] reason=[%s]", error_type, err_code, reason)
            errors.append(APIError(server_id, error_type, None, ResponseError(err_code, reason)))
            continue

        for instance_resp in resp_body['instance_responses']:
            instance_metadata = JobInstanceMetadata.deserialize(instance_resp['instance_metadata'])
            api_instance_response = InstanceResponse(instance_metadata, instance_resp)
            try:
                resp = resp_mapper(api_instance_response)
            except (KeyError, ValueError) as e:
                log.error("event=[api_error] type=[%s] reason=[%s]", APIErrorType.INVALID_RESPONSE, e)
                errors.append(APIError(server_id, APIErrorType.INVALID_RESPONSE, None, None))
                break
            responses.append(resp)

    return CollectedResponses(responses, errors)
