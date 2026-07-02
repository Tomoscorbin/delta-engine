"""
Logging helpers with ANSI-colored level output.

Provides a `LevelColorFormatter` and `configure_logging` to install a colored
stderr handler on the root logger for consistent, readable logs across the
package.

"""

import logging
import sys
from typing import TextIO

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


def configure_logging(level: int = logging.INFO, stream: TextIO | None = None) -> None:
    """
    Configure root logging with a colored formatter.

    Installs a single shutdown-safe handler on the root logger and quiets noisy
    third-party loggers.

    Args:
        level: Root log level to set (default ``logging.INFO``).
        stream: Destination for log records. Defaults to ``sys.__stderr__``,
            which avoids pytest's captured stream. Notebooks should pass
            ``sys.stdout`` so logs and ``print`` output share one ordered
            stream — otherwise stdout and stderr flush independently and log
            lines can surface after later prints.

    """
    root = logging.getLogger()

    if root.handlers:
        root.handlers.clear()
    root.setLevel(level)

    handler = SafeStreamHandler(stream=stream if stream is not None else sys.__stderr__)
    handler.setFormatter(
        LevelColorFormatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)

    for name in ("py4j", "py4j.clientserver"):
        py4j_logger = logging.getLogger(name)
        py4j_logger.setLevel(logging.WARNING)
        py4j_logger.propagate = False
