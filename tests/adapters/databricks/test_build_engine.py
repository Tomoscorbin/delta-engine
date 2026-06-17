import logging

from delta_engine.adapters.databricks import build_databricks_engine, configure_logging
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


def test_configure_logging_is_publicly_available_for_opt_in():
    # Given the opt-in logging escape hatch the factory docstring advertises
    # Then it is reachable from the package's public surface
    assert callable(configure_logging)
