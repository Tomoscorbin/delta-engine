import io
import logging

from delta_engine.adapters.databricks.log_config import (
    LevelColorFormatter,
    SafeStreamHandler,
    configure_logging,
)
from delta_engine.adapters.databricks.spark.factory import build_engine


class _DummySpark:
    """Stand-in for a SparkSession; the factory only stores it on the adapters."""


def test_build_engine_does_not_touch_root_logging():
    # Given a caller that has installed its own root log handler
    root = logging.getLogger()
    sentinel = logging.NullHandler()
    root.addHandler(sentinel)
    try:
        # When building the engine
        build_engine(_DummySpark())

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


def test_configure_logging_routes_records_to_the_given_stream():
    # Given a caller-supplied stream (as a notebook passes sys.stdout)
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    stream = io.StringIO()
    try:
        # When logging is configured to write to that stream
        configure_logging(stream=stream)
        logging.getLogger("delta_engine.test").info("hello from the stream")

        # Then the record lands on the supplied stream, not stderr
        assert "hello from the stream" in stream.getvalue()
    finally:
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)


def test_safe_stream_handler_ignores_a_stream_closed_during_teardown(capsys):
    # Given a handler whose stream has already been closed, as happens when a
    # notebook tears down sys.stdout before the last records flush
    stream = io.StringIO()
    handler = SafeStreamHandler(stream)
    stream.close()
    record = logging.LogRecord(
        name="delta_engine.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=0,
        msg="late log record",
        args=(),
        exc_info=None,
    )

    handler.emit(record)

    # Then the late record is dropped silently instead of printing a
    # "Logging error" traceback to stderr
    assert "Logging error" not in capsys.readouterr().err
