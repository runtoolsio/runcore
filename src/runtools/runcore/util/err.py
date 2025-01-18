def run_isolated_collect_exceptions(msg, *callbacks) -> None:
    """
    Execute callbacks in order, collecting any exceptions that occur.
    Each callback is executed regardless of whether previous callbacks failed.
    """
    exceptions = []

    for callback in callbacks:
        try:
            callback()
        except Exception as e:
            exceptions.append(e)

    if exceptions:
        raise ExceptionGroup(msg, exceptions)
