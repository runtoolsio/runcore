class RuntoolsException(Exception):
    pass


class InvalidStateError(RuntoolsException):

    def __init__(self, message: str):
        super().__init__(message)
