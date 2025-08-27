import logging
import sys

RESET = "\033[0m"; YELLOW = "\033[33m"; RED = "\033[31m"; BRIGHT_RED = "\033[91m"

class LevelColorFormatter(logging.Formatter):
    def format(self, record):
        text = super().format(record)
        if record.levelno == logging.WARNING:
            return f"{YELLOW}{text}{RESET}"
        if record.levelno == logging.ERROR:
            return f"{RED}{text}{RESET}"
        if record.levelno == logging.CRITICAL:
            return f"{BRIGHT_RED}{text}{RESET}"
        return text

def configure_logging(level: int = logging.INFO) -> None:
    root = logging.getLogger()
    if root.handlers:
        root.handlers.clear()
    root.setLevel(level)

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(LevelColorFormatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    root.addHandler(handler)
