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
    """
    Formatter that colorizes log records based on the level.

    Levels map to colors using ANSI escape codes. Non-colored levels are left
    unchanged.
    """

    def format(self, record):
        """
        Return a formatted message, wrapped with a color for the level.

        Args:
            record: The log record to format.

        Returns:
            The formatted and optionally colorized message.

        """
        text = super().format(record)
        if record.levelno == logging.WARNING:
            return f"{YELLOW}{text}{RESET}"
        if record.levelno == logging.ERROR:
            return f"{RED}{text}{RESET}"
        if record.levelno == logging.CRITICAL:
            return f"{BRIGHT_RED}{text}{RESET}"
        return text


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure root logging with a colored formatter to stderr.

    If handlers are already installed, they are cleared before applying the
    new configuration.

    Args:
        level: The root logger level to use (defaults to ``logging.INFO``).

    """
    root = logging.getLogger()
    if root.handlers:
        root.handlers.clear()
    root.setLevel(level)

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(
        LevelColorFormatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
    )
    root.addHandler(handler)
