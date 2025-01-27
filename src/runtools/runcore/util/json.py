from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Any, Dict


class ErrorType(Enum):
    CLIENT = auto()
    SERVER = auto()
    SIGNAL = auto()


class ErrorCode(Enum):
    # Standard JSON-RPC 2.0 errors
    PARSE_ERROR = (-32700, ErrorType.CLIENT)
    INVALID_REQUEST = (-32600, ErrorType.CLIENT)
    METHOD_NOT_FOUND = (-32601, ErrorType.CLIENT)
    INVALID_PARAMS = (-32602, ErrorType.CLIENT)
    INTERNAL_ERROR = (-32603, ErrorType.SERVER)

    # Custom error codes
    UNKNOWN = (0, ErrorType.CLIENT)
    PHASE_OP_NOT_FOUND = (1, ErrorType.CLIENT)
    PHASE_OP_INVALID_ARGS = (2, ErrorType.CLIENT)
    METHOD_EXECUTION_ERROR = (100, ErrorType.SERVER)
    INSTANCE_NOT_FOUND = (200, ErrorType.SIGNAL)
    PHASE_NOT_FOUND = (201, ErrorType.SIGNAL)

    @property
    def int_code(self) -> int:
        return self.value[0]

    @property
    def type(self) -> ErrorType:
        return self.value[1]

    @classmethod
    def from_code(cls, code: int) -> 'ErrorCode':
        for error in cls:
            if error.int_code == code:
                return error
        return ErrorCode.UNKNOWN


class JsonRpcParseError(Exception):
    pass


@dataclass
class JsonRpcError:
    code: ErrorCode
    message: str
    data: Optional[Any] = None

    @classmethod
    def deserialize(cls, error_dict: Dict) -> 'JsonRpcError':
        if not isinstance(error_dict, dict):
            raise JsonRpcParseError("Error object must be a dictionary")

        required_fields = {'code', 'message'}
        missing_fields = required_fields - set(error_dict.keys())
        if missing_fields:
            raise JsonRpcParseError(f"Missing required fields: {missing_fields}")

        if not isinstance(error_dict['code'], int):
            raise JsonRpcParseError("'code' must be an integer")

        if not isinstance(error_dict['message'], str):
            raise JsonRpcParseError("'message' must be a string")

        return cls(
            code=(ErrorCode.from_code(error_dict['code'])),
            message=error_dict['message'],
            data=error_dict.get('data')
        )

    def __str__(self) -> str:
        result = f"Error {self.code.name}: {self.message}"
        if self.data is not None:
            result += f" - {self.data}"
        return result


from dataclasses import dataclass
from typing import Optional, Any, Dict, Union


@dataclass
class JsonRpcResponse:
    jsonrpc: str = "2.0"
    id: Optional[Union[int, str]] = None
    result: Optional[Any] = None
    error: Optional[JsonRpcError] = None

    def __post_init__(self):
        if self.result is not None and self.error is not None:
            raise ValueError("Response cannot contain both result and error")
        if self.result is None and self.error is None:
            raise ValueError("Response must contain either result or error")

    @classmethod
    def deserialize(cls, response_dict: Dict) -> 'JsonRpcResponse':
        if not isinstance(response_dict, dict):
            raise JsonRpcParseError("Response must be a dictionary")

        if response_dict.get("jsonrpc") != "2.0":
            raise JsonRpcParseError("Invalid JSON-RPC version")

        id_value = response_dict.get("id")
        if id_value is not None and not isinstance(id_value, (int, str)):
            raise JsonRpcParseError("'id' must be int, string or null")

        error = response_dict.get("error")
        result = response_dict.get("result")

        if error is not None and result is not None:
            raise JsonRpcParseError('Response cannot contain both result and error')

        if error is None and result is None:
            raise JsonRpcParseError('Response must contain either result or error')

        return cls(
            id=id_value,
            result=result,
            error=JsonRpcError.deserialize(error) if error is not None else None
        )
