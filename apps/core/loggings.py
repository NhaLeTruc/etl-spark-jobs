"""
Logging
"""

import inspect

from core.utils import get_or_create_spark_session


class LoggerProvider:
    def get_logger(self):
        spark = get_or_create_spark_session()
        logger = spark._jvm.org.apache.log4j
        return logger.LogManager.getLogger("etl_logger")


class LoggerMethods(LoggerProvider):
    """
    Class to implement different logging methods
    """
    def __init__(self, custom_message: str | None = None):
        super().__init__()
        self.custom_message = custom_message


    def __get_calling_func_name(self):
        """
        Return the name of the main caller function using the stack.
        """
        return inspect.stack()[3][3]


    def set_message(self, custom_message):
        """
        Set custom message for logger functions.
        """
        caller_func = self.__get_calling_func_name() or ""

        return f"{custom_message} [{caller_func}]" if custom_message else f"[{caller_func}]"


    def info(self, message: str | None = None):
        """
        Fuction used to log info
        """
        format_message = self.set_message(message)
        logger = self.get_logger
        logger.info(format_message)



    def warn(self, message: str | None = None):
        """
        Function used to log a warning
        """
        format_message = self.set_message(message)
        logger = super().get_logger()
        logger.warn(format_message)


    def error(self, message: str | None = None):
        """
        Function used to log a error
        """
        format_message = self.set_message(message)
        logger = super().get_logger()
        logger.error(format_message)
