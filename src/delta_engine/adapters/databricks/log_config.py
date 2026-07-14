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

    Skips emission when the stream is already closed.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record unless teardown has already closed the stream."""
        stream = self.stream
        if stream is None or getattr(stream, "closed", False):
            return
        super().emit(record)


def configure_logging(level: int = logging.INFO, stream: TextIO | None = None) -> None:
    """
    Configure root logging with a colored formatter.

    Takes over the root logger: any handlers already installed on it are
    removed and replaced with this package's single shutdown-safe coloured
    handler, and noisy third-party loggers (py4j) are quieted. Intended for
    scripts and notebooks that want ready-made output; embedders who own
    their application's logging should configure it themselves and not call
    this function.

    Args:
        level: Root log level to set (default ``logging.INFO``).
        stream: Destination for log records. Defaults to ``sys.__stderr__``,
            which avoids pytest's captured stream. Notebooks should pass
            ``sys.stdout`` so logs and ``print`` output share one ordered
            stream — otherwise stdout and stderr flush independently and log
            lines can surface after later prints.

    """
    root = logging.getLogger()

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
