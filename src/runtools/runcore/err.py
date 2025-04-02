class RuntoolsException(Exception):
    pass


class InvalidStateError(RuntoolsException):

    def __init__(self, message: str):
        super().__init__(message)


def run_isolated_collect_exceptions(msg, *callbacks, suppress=False) -> None:
    """
    Execute callbacks in order, collecting any exceptions that occur.
    Each callback is executed regardless of whether previous callbacks failed.

    Args:
        msg: Exception group message if exception is raised
        *callbacks: Callables to execute in isolation
        suppress: If True, suppress any exceptions without raising them
    """
    exceptions = []
    keyboard_interrupted = False

    for callback in callbacks:
        try:
            callback()
        except Exception as e:
            exceptions.append(e)
        except KeyboardInterrupt:
            keyboard_interrupted = True

    if keyboard_interrupted:
        raise KeyboardInterrupt

    if exceptions and not suppress:
        raise ExceptionGroup(msg, exceptions)
