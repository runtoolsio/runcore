"""
RPC client primitives for invoking methods on environment nodes.

This module defines transport-agnostic pieces:
- ``InstanceCallError`` and its subclasses — surface RPC failures
- ``InstanceCallResult`` — one server's outcome from a broadcast
- helpers that map a raw JSON-RPC response to a return value or raise the right error

Transport-specific RPC clients (e.g. ``UnixSocketRpcClient``) live under
``runtools.runcore.transport.<transport>`` and consume these primitives.
"""

import json
from dataclasses import dataclass
from json import JSONDecodeError
from typing import List, Any, Optional, TypeVar, Generic, Callable

from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import JobRun
from runtools.runcore.util.json import JsonRpcResponse, JsonRpcParseError, ErrorType, ErrorCode
from runtools.runcore.util.socket import SocketRequestResult

T = TypeVar('T')


class InstanceCallError(RuntoolsException):
    """Base exception class for errors when calling instance methods.

    Args:
        server_id: Identifier of the server where error occurred
        message: Optional error message details
    """

    def __init__(self, server_id: str, message: Optional[str] = None):
        self.server_id = server_id
        super().__init__(f"Server '{server_id}' {message or ''}")


class InstanceCallClientError(InstanceCallError):
    """Exception raised when client-side error occurs during an instance call.
    This exception means the client implementation is probably incorrect or expects a different version of the server.

    Args:
        server_address: Address of the target server
        message: Optional error message details
    """

    def __init__(self, server_address: str, message: Optional[str] = None):
        super().__init__(server_address, message)


class InstanceCallServerError(InstanceCallError):
    """Exception raised when server-side error occurs during an instance call.

    Args:
        server_address: Address of the target server
        message: Optional error message details
    """

    def __init__(self, server_address: str, message: Optional[str] = None):
        super().__init__(server_address, message)


class TargetNotFoundError(InstanceCallError):
    """Exception raised when specified target server or method target is not found.

    Args:
        server_address: Optional address of the target server
    """

    def __init__(self, server_address: Optional[str] = None):
        super().__init__(server_address)


class PhaseNotFoundError(InstanceCallError):
    """Exception raised when specified phase is not found on target server.

    Args:
        server_address: Address of the target server
    """

    def __init__(self, server_address: str):
        super().__init__(server_address)


R = TypeVar("R")


@dataclass
class InstanceCallResult(Generic[R]):
    """Contains result of an instance call operation.

    Args:
        server_address: Address of the server that handled the request
        retval: Return value from the instance call if successful
        error: Error details if the call failed
    """
    server_address: str
    retval: Optional[R]
    error: Optional[InstanceCallError] = None


def _parse_retval_or_raise_error(resp: SocketRequestResult) -> Any:
    """Parse and validate JSON-RPC response, raising appropriate errors if needed.

    Args:
        resp: Socket request result containing the response

    Returns:
        Parsed return value from the response

    Raises:
        InstanceCallServerError: For server-side errors
        InstanceCallClientError: For client-side errors
        TargetNotFoundError: When target doesn't exist
        PhaseNotFoundError: When phase doesn't exist
    """
    sid = resp.server_address
    if resp.error:
        raise InstanceCallServerError(sid, 'Socket based error occurred') from resp.error

    try:
        response_body = json.loads(resp.response)
    except JSONDecodeError as e:
        raise InstanceCallServerError(sid, 'Cannot parse API response JSON payload') from e

    try:
        json_rpc_resp = JsonRpcResponse.deserialize(response_body)
    except JsonRpcParseError as e:
        raise InstanceCallServerError(sid, 'Invalid JSON-RPC 2.0 response') from e

    if err := json_rpc_resp.error:
        if err.code.type == ErrorType.CLIENT:
            raise InstanceCallClientError(sid, str(err))
        elif err.code.type == ErrorType.SERVER:
            raise InstanceCallServerError(sid, str(err))
        elif err.code.type == ErrorType.SIGNAL:
            if err.code == ErrorCode.TARGET_NOT_FOUND:
                raise TargetNotFoundError(sid)
            elif err.code == ErrorCode.PHASE_NOT_FOUND:
                raise PhaseNotFoundError(sid)
            else:
                raise InstanceCallClientError(sid, f"Unknown signaling error: {err.code}")
        else:
            raise InstanceCallClientError(sid, f"Unknown error type: {err.code.type}")

    if "retval" not in json_rpc_resp.result:
        raise InstanceCallServerError(sid, f"Retval is missing in JSON-RPC result: {json_rpc_resp.result}")
    return json_rpc_resp.result["retval"]


def _convert_result(resp: SocketRequestResult, retval_mapper: Callable[[Any], R]) -> InstanceCallResult:
    """Parse JSON-RPC response into InstanceCallResult without raising exceptions.

    Args:
        resp: Socket request result to parse
        retval_mapper: Function to transform the return value

    Returns:
        InstanceCallResult containing either the mapped return value or error details
    """
    try:
        retval = _parse_retval_or_raise_error(resp)
        return InstanceCallResult(resp.server_address, retval_mapper(retval))
    except InstanceCallError as e:
        return InstanceCallResult(resp.server_address, None, e)


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
