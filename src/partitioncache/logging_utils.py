"""
Enhanced logging utilities for PartitionCache with thread-aware formatting.

This module provides consistent logging across the PartitionCache application with:
- Thread ID prefixes for all log messages
- Level prefixes (ERROR, WARNING, INFO, DEBUG)
- Improved formatting for multi-threaded applications
"""

import logging
import sys
import threading
from logging import getLogger


class ThreadAwareFormatter(logging.Formatter):
    """
    Custom formatter that includes thread information and consistent level prefixes.

    Format: LEVEL(thread_id): Message
    Example: WARNING(4): This is a warning in Thread 4
    """

    def format(self, record: logging.LogRecord) -> str:
        # Get thread ID - use a short form for readability
        thread_id = threading.get_ident() % 10000  # Keep it to 4 digits max

        # Format: LEVEL(thread_id): message
        level_name = record.levelname
        message = record.getMessage()

        return f"{level_name}({thread_id}): {message}"


class ThreadAwareVerboseFormatter(logging.Formatter):
    """
    Verbose formatter with timestamps, module info, and thread information.

    Format: YYYY-MM-DD HH:MM:SS LEVEL(thread_id) [module:line]: Message
    """

    def format(self, record: logging.LogRecord) -> str:
        # Get thread ID
        thread_id = threading.get_ident() % 10000

        # Format timestamp
        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")

        # Module and line info
        module_info = f"{record.name}:{record.lineno}"

        # Format: timestamp LEVEL(thread_id) [module:line]: message
        level_name = record.levelname
        message = record.getMessage()

        return f"{timestamp} {level_name}({thread_id}) [{module_info}]: {message}"


def configure_enhanced_logging(
    verbose: bool = False,
    quiet: bool = False,
    logger_name: str = "PartitionCache"
) -> logging.Logger:
    """
    Configure enhanced logging with thread-aware formatting.

    Args:
        verbose: Enable debug-level logging with detailed formatting
        quiet: Only show warnings and errors
        logger_name: Name of the logger to configure

    Returns:
        Configured logger instance
    """
    logger = getLogger(logger_name)

    # Remove existing handlers to avoid duplication
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Set log level based on arguments
    if quiet:
        logger.setLevel(logging.WARNING)
    elif verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Create console handler
    handler = logging.StreamHandler(sys.stderr)

    # Choose formatter based on verbosity
    if verbose:
        formatter = ThreadAwareVerboseFormatter()
    else:
        formatter = ThreadAwareFormatter()

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_thread_aware_logger(name: str | None = None) -> logging.Logger:
    """
    Get a logger instance configured for thread-aware logging.

    Args:
        name: Logger name (defaults to "PartitionCache")

    Returns:
        Logger instance
    """
    if name is None:
        name = "PartitionCache"

    return getLogger(name)


def log_thread_info(logger: logging.Logger, message: str, level: int = logging.INFO) -> None:
    """
    Log a message with explicit thread information.

    Args:
        logger: Logger instance
        message: Message to log
        level: Log level (default: INFO)
    """
    thread_name = threading.current_thread().name
    thread_id = threading.get_ident()

    enhanced_message = f"[Thread {thread_name}({thread_id})]: {message}"
    logger.log(level, enhanced_message)


# Convenience functions for common log levels with thread info
def log_info_with_thread(logger: logging.Logger, message: str) -> None:
    """Log info message with thread information."""
    log_thread_info(logger, message, logging.INFO)


def log_warning_with_thread(logger: logging.Logger, message: str) -> None:
    """Log warning message with thread information."""
    log_thread_info(logger, message, logging.WARNING)


def log_error_with_thread(logger: logging.Logger, message: str) -> None:
    """Log error message with thread information."""
    log_thread_info(logger, message, logging.ERROR)


def log_debug_with_thread(logger: logging.Logger, message: str) -> None:
    """Log debug message with thread information."""
    log_thread_info(logger, message, logging.DEBUG)

