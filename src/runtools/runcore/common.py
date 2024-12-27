class RuntoolsException(Exception):
    pass


class InvalidStateError(RuntoolsException):

    def __init__(self, message: str):
        super().__init__(message)


class MultipleExceptions(Exception):
    """Exception that carries multiple exceptions that occurred during execution"""

    def __init__(self, exceptions: list[Exception]):
        self.exceptions = exceptions
        message = f"Multiple exceptions occurred ({len(exceptions)})"
        super().__init__(message)
