def run_isolated_collect_exceptions(*callbacks) -> None:
    """
    Execute callbacks in order, collecting any exceptions that occur.
    Each callback is executed regardless of whether previous callbacks failed.

    Args:
        *callbacks: Callables to execute in isolation

    Raises:
        Exception: The original exception if only one occurred
        MultipleExceptions: If multiple exceptions occurred
    """
    exceptions = []

    for callback in callbacks:
        try:
            callback()
        except MultipleExceptions as me:
            exceptions.extend(me.exceptions)
        except Exception as e:
            exceptions.append(e)

    if exceptions:
        if len(exceptions) == 1:
            raise exceptions[0]
        raise MultipleExceptions(exceptions)


class MultipleExceptions(Exception):
    """Exception that carries multiple exceptions that occurred during execution"""

    def __init__(self, exceptions: list[Exception]):
        self.exceptions = exceptions
        message = f"Multiple exceptions occurred ({len(exceptions)})"
        super().__init__(message)

    def __iter__(self):
        return iter(self.exceptions)
