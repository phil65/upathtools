"""Logging configuration for upathtools."""

from __future__ import annotations

import logging


LogLevel = int | str


def get_logger(name: str, log_level: LogLevel | None = None) -> logging.Logger:
    """Get a logger for the given name.

    Args:
        name: The name of the logger.
              Will be prefixed with 'upathtools'
        log_level: The logging level to set for the logger

    Returns:
        A logger instance
    """
    logger = logging.getLogger(f"upathtools.{name}")
    if log_level is not None:
        logger.setLevel(log_level)
    return logger
