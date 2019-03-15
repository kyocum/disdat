"""
Log
===
Package wide logging and logging utilities
"""
import sys
import logging
import contextlib

import disdat


# Disdat global logger
logger = logging.getLogger(disdat.__name__)
logger.addHandler(logging.NullHandler())

# External loggers
luigi_logger = logging.getLogger('luigi-interface')
boto3_logger = logging.getLogger('boto3')
botocore_logger = logging.getLogger('botocore')


@contextlib.contextmanager
def context(level=logging.INFO, stream=sys.stdout):
    """
    A context handler for logging debug information

    Args:
        level (int): The logging package log level to user within the context.
        stream (io.IO): The stream to write log messages to.

    Returns:
        logging.Logger: The logger to debug with
    """
    if level is None:
        yield logger
        return

    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler = logging.StreamHandler(stream)
    handler.setFormatter(fmt)
    prior = logger.getEffectiveLevel()
    logger.addHandler(handler)
    handler.setLevel(level)
    logger.setLevel(level)
    yield logger
    logger.removeHandler(handler)
    logger.setLevel(prior)


def enable(level=logging.INFO, stream=sys.stdout):
    """
    Sets the default package-wide configuration.

    Args:
        level (int): The logging package log level to user within the context.
        stream (io.IO): The stream to write log messages to.
    """
    handler = logging.StreamHandler(stream)
    logger.addHandler(handler)
    logger.setLevel(level)
