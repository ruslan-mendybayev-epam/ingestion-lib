import logging

from attr import dataclass


class CustomLogger(logging.Logger):
    """
    A custom logging class that sets up and returns a logger with a particular name,
    logging level, and logging format.
    """

    def __init__(
            self,
            logger_name: str,
            logging_level: int = logging.INFO,
            logging_format: str = "%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    ):
        """
        Initializes the CustomLogger class with the provided configurations.
        Sets up a console handler and formats for the logger.

        :param logger_name: The name of the logger.
        :param logging_level: The level at which the logger should log.
        :param logging_format: The format of the logging message.
        """
        super().__init__(logger_name, logging_level)
        # create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging_level)
        # create formatter
        formatter = logging.Formatter(logging_format)
        # add formatter to console handler
        console_handler.setFormatter(formatter)
        # add console handler  to logger
        self.addHandler(console_handler)
        self.operation = {}

    def log_success(self, row_count: int = 0, operation: dict = {}, status: str = "Success"):
        merged_operation = {**operation, **self.operation}
        self.info(f"Operation successfully completed with status '{status}' and row count {row_count}. Operation:\n{merged_operation}")

    def log_error(self, error_msg: str, operation: dict = {}, status: str = "Failure"):
        merged_operation = {**operation, **self.operation}
        self.info(f"Operation failed with message '{error_msg}' and status '{status}'. Operation:\n{merged_operation}")
