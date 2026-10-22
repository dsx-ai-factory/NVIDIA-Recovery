# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import logging
import os
import socket
import sys
import threading
import time
from urllib.parse import urlparse

from cachetools import TTLCache

from recovery_libs.utils.constants import (
    OTLP_ENDPOINT_CHECK_MAX_RETRIES,
    OTLP_ENDPOINT_CHECK_MAX_RETRY_INTERVAL,
    OTLP_ENDPOINT_CHECK_MIN_RETRY_INTERVAL,
    OTLP_ENDPOINT_CHECK_TIMEOUT,
    OTLP_GRPC_DEFAULT_PORT,
    OTLP_HTTP_DEFAULT_PORT,
)
from recovery_libs.utils.lazy_otlp_handler import LazyOTLPHandler
from recovery_libs.utils.otlp_handler import OTLPHandler
from recovery_libs.utils.types import OTLPProtocol

logger = logging.getLogger(__name__)

# Capture handler class names at import time (before any test mocking can occur)
_OTLP_HANDLER_NAMES = (OTLPHandler.__name__, LazyOTLPHandler.__name__)

LOG_LEVEL = logging.getLevelNamesMapping()[os.environ.get("LOG_LEVEL", "INFO").upper()]
HTTP_LOG_LEVEL = logging.getLevelNamesMapping()[os.environ.get("HTTP_LOG_LEVEL", "WARNING").upper()]
NO_SPAM_LOGGER_INTERVAL_SECONDS = float(os.environ.get("NO_SPAM_LOGGER_INTERVAL_SECONDS", "600"))
NO_SPAM_LOGGER_MAX_CACHE_SIZE = float(os.environ.get("NO_SPAM_LOGGER_MAX_CACHE_SIZE", "1_000_000"))

# Lock to prevent race conditions during logger configuration
# This is needed because OTLP endpoint checking takes time and concurrent calls
# could interfere with each other
_logger_locks: dict[str, threading.Lock] = {}
_logger_locks_lock = threading.Lock()  # Lock for accessing _logger_locks dict

# Set of loggers that have been configured
_configured_loggers: set[str] = set()


def _get_logger_lock(name: str) -> threading.Lock:
    """Get or create a lock for a specific logger name."""
    with _logger_locks_lock:
        if name not in _logger_locks:
            _logger_locks[name] = threading.Lock()
        return _logger_locks[name]


# Standard LogRecord attributes to exclude from extra fields
STANDARD_RECORD_ATTRS = frozenset(
    {
        "name",
        "msg",
        "args",
        "created",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "thread",
        "threadName",
        "exc_info",
        "exc_text",
        "stack_info",
        "taskName",
        "asctime",
    }
)
# Count of standard attributes for fast path check
STANDARD_ATTRS_COUNT = len(STANDARD_RECORD_ATTRS)


class ConsoleFormatter(logging.Formatter):
    """Console formatter that supports extra fields."""

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record for console output with extra fields.

        Args:
            record: The LogRecord to format.

        Returns:
            Formatted string with extra fields appended.
        """
        # Format the base message
        base_msg = super().format(record)

        # Fast path: check if there are any extra fields
        record_dict = record.__dict__
        if len(record_dict) <= STANDARD_ATTRS_COUNT:
            # Likely no extra fields, but verify quickly
            has_extra = False
            for key in record_dict:
                if key not in STANDARD_RECORD_ATTRS:
                    has_extra = True
                    break
            if not has_extra:
                return base_msg

        # Build string directly instead of creating intermediate list
        result = base_msg
        first = True
        for key, value in record_dict.items():
            if key not in STANDARD_RECORD_ATTRS:
                if first:
                    result += " | "
                    first = False
                else:
                    result += " "
                result += key
                result += "="
                result += str(value)

        return result


class FastJSONFormatter(logging.Formatter):
    """Optimized JSON formatter for high-performance logging."""

    # Translation table for escaping quotes and newlines in one pass
    _escape_table = str.maketrans({'"': '\\"', "\n": "\\n"})

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON string.

        Args:
            record: The LogRecord to format.

        Returns:
            JSON-formatted string without unnecessary whitespace.
        """
        # Use string concatenation for base JSON structure
        # getMessage() handles lazy evaluation of msg % args
        message = record.getMessage().translate(self._escape_table)
        result = (
            '{"timestamp":"'
            + self.formatTime(record, self.datefmt)
            + '","level":"'
            + record.levelname
            + '","logger":"'
            + record.name
            + '","message":"'
            + message
            + '","module":"'
            + record.module
            + '","function":"'
            + (record.funcName or "")
            + '","line":'
            + str(record.lineno)
        )

        # Fast path: check if there are any extra fields
        record_dict = record.__dict__
        if len(record_dict) <= STANDARD_ATTRS_COUNT:
            # Likely no extra fields, but verify quickly
            has_extra = False
            for key in record_dict:
                if key not in STANDARD_RECORD_ATTRS:
                    has_extra = True
                    break
            if not has_extra:
                return result + "}"

        # Inline extra field formatting
        for key, value in record_dict.items():
            if key not in STANDARD_RECORD_ATTRS:
                # Escape key
                key_escaped = key.translate(self._escape_table)
                result += ',"'
                result += key_escaped
                result += '":'
                # Fast type checking with type() for common cases
                value_type = type(value)
                if value_type is str:
                    result += '"'
                    result += value.translate(self._escape_table)
                    result += '"'
                elif value_type is int or value_type is float:
                    result += str(value)
                elif value_type is bool:
                    result += "true" if value else "false"
                elif value is None:
                    result += "null"
                else:
                    # For complex types, convert to string and escape
                    result += '"'
                    result += str(value).translate(self._escape_table)
                    result += '"'

        return result + "}"


def get_logger(
    name: str,
    level: int = LOG_LEVEL,
    custom_library_level: dict[str, int] | None = None,
    propagate: bool = False,
    enable_otlp: bool = False,
) -> logging.Logger:
    """Create or update a logger with the given name and level.

    This function uses delta-based configuration: it only adds handlers that
    are missing, rather than clearing and rebuilding all handlers. This makes
    it safe to call multiple times with different settings.

    OTLP will be enabled if enable_otlp=True AND all three environment variables are set:
    - OTEL_EXPORTER_OTLP_PROTOCOL: Protocol type ("grpc", "http/protobuf", or "http")
    - OTEL_EXPORTER_OTLP_ENDPOINT: OTLP base endpoint URL (HTTP logs go to its /v1/logs path)
    - OTEL_EXPORTER_OTLP_TOKEN: Authentication token

    NOTE: All OTLP handlers share a singleton LoggerProvider, so multiple loggers
    can safely use enable_otlp=True without creating connection conflicts.

    This function is thread-safe.

    Args:
        name: Logger name.
        level: Logging level (e.g., logging.INFO, logging.DEBUG).
            Defaults to LOG_LEVEL environment variable or INFO.
        custom_library_level: Dictionary mapping library names to their desired
            log levels. Useful for suppressing verbose third-party library logs.
            Example: {"httpx": logging.WARNING, "urllib3": logging.ERROR}
            Defaults to None.
        propagate: Whether to propagate logs to parent loggers.
            Defaults to False to prevent duplicate logs in production.
            Set to True in unit tests to capture logs from child loggers.
        enable_otlp: Whether to enable OTLP handler for this logger.
            Defaults to False. Set to True for service loggers that should
            export logs to an OTLP collector.

    Returns:
        Configured logging.Logger instance with console handler and optional
        OTLP handler (if enable_otlp=True and environment variables are configured).
    """
    # Use a per-logger lock to prevent race conditions during configuration
    logger_lock = _get_logger_lock(name)

    with logger_lock:
        # Get or create logger
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = propagate

        # Delta-based configuration: check what handlers already exist
        has_console = any(isinstance(h, logging.StreamHandler) and not _is_otlp_handler(h) for h in logger.handlers)
        has_otlp = any(_is_otlp_handler(h) for h in logger.handlers)

        # Update levels on existing handlers if not already configured
        if name not in _configured_loggers:
            for handler in logger.handlers:
                handler.setLevel(level)

        # Add console handler if missing
        if not has_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            formatter = ConsoleFormatter("%(asctime)s:%(levelname)s [%(name)s] [%(filename)s:%(lineno)d] %(message)s")
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # Add OTLP handler if requested and not already present
        if enable_otlp and not has_otlp:
            otlp_handler = _create_otlp_handler_if_configured(level, service_name=name)
            if otlp_handler:
                logger.addHandler(otlp_handler)
                logger.info(
                    f"OTLP handler successfully configured: protocol={otlp_handler.protocol.value}, endpoint={otlp_handler.endpoint}"
                )

        # Configure custom library log levels
        if custom_library_level:
            _configure_library_levels(custom_library_level)
            logger.debug(f"Custom library log levels configured: {custom_library_level}")

        _configured_loggers.add(name)
        return logger


def _is_otlp_handler(handler: logging.Handler) -> bool:
    """Check if a handler is an OTLP-related handler.

    Args:
        handler: The handler to check.

    Returns:
        True if the handler is an OTLP handler (OTLPHandler or LazyOTLPHandler).
    """
    handler_type = type(handler).__name__
    return handler_type in _OTLP_HANDLER_NAMES


def _check_endpoint_availability(endpoint: str, protocol: OTLPProtocol) -> bool:
    """Check if an OTLP endpoint is reachable.

    Args:
        endpoint: The OTLP endpoint URL (e.g., "http://localhost:4318" or "localhost:4317").
        protocol: The protocol type (HTTP or gRPC).

    Returns:
        True if endpoint is reachable, False otherwise.
    """
    try:
        # Parse endpoint to extract host and port
        if "://" in endpoint:
            parsed = urlparse(endpoint)
            host = parsed.hostname or "localhost"
            port = parsed.port
        else:
            # Handle bare host:port format (common for gRPC)
            if ":" in endpoint:
                host, port_str = endpoint.rsplit(":", 1)
                port = int(port_str)
            else:
                host = endpoint
                port = None

        # Use default ports if not specified
        if port is None:
            port = OTLP_GRPC_DEFAULT_PORT if protocol == OTLPProtocol.GRPC else OTLP_HTTP_DEFAULT_PORT

        # Attempt to connect to the endpoint
        with socket.create_connection((host, port), timeout=OTLP_ENDPOINT_CHECK_TIMEOUT):
            return True
    except (TimeoutError, ConnectionRefusedError, OSError, ValueError) as e:
        logger.debug(f"Endpoint availability check failed for {endpoint}: {e}")
        return False


def _create_otlp_handler_if_configured(level: int, service_name: str) -> OTLPHandler | None:
    """Create OTLP handler if environment variables are properly configured.

    All three environment variables must be set:
    - OTEL_EXPORTER_OTLP_PROTOCOL
    - OTEL_EXPORTER_OTLP_ENDPOINT
    - OTEL_EXPORTER_OTLP_TOKEN

    Before creating the handler, this function checks if the endpoint is reachable.
    If not, it retries with exponential backoff.

    Args:
        level: Logging level to set on the handler.
        service_name: Name of the service sending logs (used in OTLP resource attributes).

    Returns:
        OTLPHandler instance if configuration is valid and endpoint is reachable, None otherwise.
    """
    protocol_str = os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    token = os.getenv("OTEL_EXPORTER_OTLP_TOKEN")

    # All three variables are mandatory
    if not protocol_str or not endpoint or not token:
        return None

    # Parse protocol string to enum
    try:
        protocol = OTLPProtocol.from_string(protocol_str)
    except ValueError as e:
        logger.warning(f"{e}. OTLP logging disabled.")
        return None

    # Check endpoint availability with retry logic
    retry_interval = OTLP_ENDPOINT_CHECK_MIN_RETRY_INTERVAL
    stop_event = threading.Event()

    for attempt in range(OTLP_ENDPOINT_CHECK_MAX_RETRIES):
        if _check_endpoint_availability(endpoint, protocol):
            # Endpoint is reachable, proceed to create handler
            break

        # Log error on every failed attempt
        logger.error(
            f"OTLP endpoint availability check failed (attempt {attempt + 1}/{OTLP_ENDPOINT_CHECK_MAX_RETRIES}): "
            f"endpoint={endpoint}, protocol={protocol.value}"
        )

        # If this was the last attempt, give up
        if attempt == OTLP_ENDPOINT_CHECK_MAX_RETRIES - 1:
            logger.error(
                f"OTLP endpoint {endpoint} is not reachable after {OTLP_ENDPOINT_CHECK_MAX_RETRIES} attempts. "
                "OTLP logging disabled."
            )
            return None

        # Wait before retrying with exponential backoff (interruptible)
        stop_event.wait(retry_interval)
        retry_interval = min(retry_interval * 2, OTLP_ENDPOINT_CHECK_MAX_RETRY_INTERVAL)

    # Endpoint is reachable, create the handler
    logger.info(f"OTLP endpoint is available: endpoint={endpoint}, protocol={protocol.value}")

    try:
        handler = OTLPHandler(
            endpoint=endpoint,
            protocol=protocol,
            token=token,
            service_name=service_name,
        )
        handler.setLevel(level)
        return handler
    except Exception as e:
        logger.exception("Failed to create OTLP handler. OTLP logging disabled.", extra={"error": str(e)})
        return None


def _configure_library_levels(custom_library_level: dict[str, int]) -> None:
    """Configure log levels for specific libraries.

    Sets custom log levels for third-party libraries to control their
    verbosity independently of the main application logger.

    Args:
        custom_library_level: Dictionary mapping library names to log levels.
            Example: {"httpx": logging.WARNING, "urllib3": logging.ERROR}
    """
    for library_name, log_level in custom_library_level.items():
        library_logger = logging.getLogger(library_name)
        library_logger.setLevel(log_level)
        logging.getLogger("recovery").debug(f"Set log level for '{library_name}' to {logging.getLevelName(log_level)}")


class NoSpamLogger:
    """
    A wrapper for a logger that prevents spamming the same message multiple times within a given interval.
    Suitable for use when logging at info or error level from functions called periodically.

    Messages are cached by level and message in a TTLCache, and will not be logged any longer until the TTL interval expires.
    """

    def __init__(
        self,
        logger,
        max_cache_size: float = NO_SPAM_LOGGER_MAX_CACHE_SIZE,
        min_interval_seconds: float = NO_SPAM_LOGGER_INTERVAL_SECONDS,
    ):
        if min_interval_seconds <= 0.0:
            raise ValueError("min_interval_seconds must be positive")
        if max_cache_size <= 0:
            raise ValueError("max_cache_size must be positive")

        self.logger = logger
        self.last_logged: TTLCache[tuple[int, str], float] = TTLCache(maxsize=max_cache_size, ttl=min_interval_seconds)

    def _should_log(self, level: int, message: str) -> bool:
        return (level, message) not in self.last_logged

    def _log(self, level: int, message: str, *args, **kwargs):
        if self._should_log(level, message):
            self.logger.log(level, message, *args, **kwargs)
            self.last_logged[(level, message)] = time.time()

    def debug(self, message: str, *args, **kwargs):
        self._log(logging.DEBUG, message, *args, **kwargs)

    def info(self, message: str, *args, **kwargs):
        self._log(logging.INFO, message, *args, **kwargs)

    def warning(self, message: str, *args, **kwargs):
        self._log(logging.WARNING, message, *args, **kwargs)

    def error(self, message: str, *args, **kwargs):
        self._log(logging.ERROR, message, *args, **kwargs)

    def critical(self, message: str, *args, **kwargs):
        self._log(logging.CRITICAL, message, *args, **kwargs)

    def exception(self, msg, *args, exc_info=True, **kwargs):
        self.error(msg, *args, exc_info=exc_info, **kwargs)
