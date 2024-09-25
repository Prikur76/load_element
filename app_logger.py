import logging


# создаем форматтер
_log_format = f'%(asctime)s - [%(levelname)s] - %(name)s - \
    (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s'


def get_file_handler() -> logging.FileHandler:
    """
    Creates a file handler that logs debug messages.
    """
    file_handler = logging.FileHandler("errors.log")
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(logging.Formatter(_log_format))
    return file_handler


def get_stream_handler() -> logging.StreamHandler:
    """Create a stream handler with a higher log level"""
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(_log_format))
    return stream_handler


def get_logger(name: str) -> logging.Logger:
    """
    Creates a logger and adds the custom file and stream handlers.
    Changing the logging level from INFO to DEBUG will print all logs
    to the console.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(get_file_handler())
    logger.addHandler(get_stream_handler())
    return logger
