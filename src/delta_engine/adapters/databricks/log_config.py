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
        """Apply ANSI color codes to a formatted log message."""
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
        """Emit a log record, swallowing ValueError if the stream is closed."""
        try:
            super().emit(record)
        except ValueError:
            pass


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure root logging with a colored formatter to stderr.

    Uses sys.__stderr__ to avoid pytest's captured stream and installs a
    shutdown-safe handler. Also quiets noisy third-party loggers.
    """
    logging.raiseExceptions = False

    root = logging.getLogger()

    if root.handlers:
        root.handlers.clear()
    root.setLevel(level)

    handler = SafeStreamHandler(stream=sys.__stderr__)
    handler.setFormatter(
        LevelColorFormatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)

    for name in ("py4j", "py4j.clientserver"):
        lg = logging.getLogger(name)
        lg.setLevel(logging.WARNING)
        lg.propagate = False
