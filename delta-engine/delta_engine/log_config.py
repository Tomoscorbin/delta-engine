"""
Logging helpers with ANSI-colored level output.

Provides a `LevelColorFormatter` and `configure_logging` to install a colored
stderr handler on the root logger for consistent, readable logs across the
package.
"""

import logging
import sys

RESET = "\033[0m"
YELLOW = "\033[33m"
RED = "\033[31m"
BRIGHT_RED = "\033[91m"


class LevelColorFormatter(logging.Formatter):
    """Formatter that colorizes log records based on the level."""

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        if record.levelno == logging.WARNING:
            return f"{YELLOW}{text}{RESET}"
        if record.levelno == logging.ERROR:
            return f"{RED}{text}{RESET}"
        if record.levelno == logging.CRITICAL:
            return f"{BRIGHT_RED}{text}{RESET}"
        return text


class SafeStreamHandler(logging.StreamHandler):
    """
    StreamHandler that tolerates interpreter/pytest teardown.

    Swallows ValueError raised when the stream is already closed.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            super().emit(record)
        except ValueError:
            # Stream likely closed during interpreter shutdown / pytest teardown
            pass


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure root logging with a colored formatter to stderr.

    Uses sys.__stderr__ to avoid pytest's captured stream and installs a
    shutdown-safe handler. Also quiets noisy third-party loggers.
    """
    # Don't spew "--- Logging error ---" diagnostics in non-debug runs
    logging.raiseExceptions = False

    root = logging.getLogger()

    # Replace existing handlers (intentional for consistent package logs)
    if root.handlers:
        root.handlers.clear()
    root.setLevel(level)

    # Use the real stderr, not pytest's wrapper (prevents closed-stream errors)
    handler = SafeStreamHandler(stream=sys.__stderr__)
    handler.setFormatter(
        LevelColorFormatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)

    # Turn down chatty libs that log during object __del__/shutdown
    for name in ("py4j", "py4j.clientserver"):
        lg = logging.getLogger(name)
        lg.setLevel(logging.WARNING)
        lg.propagate = False
