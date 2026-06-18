import logging

from delta_engine.adapters.databricks import build_databricks_engine, configure_logging
from delta_engine.adapters.databricks.log_config import LevelColorFormatter
from delta_engine.application.engine import Engine


class _DummySpark:
    """Stand-in for a SparkSession; the factory only stores it on the adapters."""


def test_build_databricks_engine_returns_an_engine():
    # Given a Spark session
    # When building the engine
    engine = build_databricks_engine(_DummySpark())

    # Then a wired Engine is returned
    assert isinstance(engine, Engine)


def test_build_databricks_engine_does_not_touch_root_logging():
    # Given a caller that has installed its own root log handler
    root = logging.getLogger()
    sentinel = logging.NullHandler()
    root.addHandler(sentinel)
    try:
        # When building the engine
        build_databricks_engine(_DummySpark())

        # Then the caller's handler survives -- the factory has no logging side effect
        assert sentinel in root.handlers
    finally:
        root.removeHandler(sentinel)


def test_configure_logging_installs_the_coloured_handler_at_the_requested_level():
    # Given the root logger's current state (restored afterwards so this opt-in
    # global mutation does not leak into other tests)
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    try:
        # When the caller opts in to the package's logging
        configure_logging(level=logging.DEBUG)

        # Then the root logger carries exactly the package's coloured handler at
        # the requested level -- the escape hatch actually configures logging
        assert root.level == logging.DEBUG
        colour_handlers = [h for h in root.handlers if isinstance(h.formatter, LevelColorFormatter)]
        assert len(colour_handlers) == 1
    finally:
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)
