class ForwardLogs:

    def __init__(self, logger, target_handlers):
        self.logger = logger
        self.target_handlers = target_handlers

    def __enter__(self):
        for handler in self.target_handlers:
            self.logger.addHandler(handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for handler in self.target_handlers:
            self.logger.removeHandler(handler)
