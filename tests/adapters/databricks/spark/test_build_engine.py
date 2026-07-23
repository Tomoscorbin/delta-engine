import io
import logging

import pytest

from delta_engine.adapters.databricks.log_config import (
    LevelColorFormatter,
    SafeStreamHandler,
    configure_logging,
)
from delta_engine.adapters.databricks.spark.runtime import (
    release_compatibility_markdown,
)
from delta_engine.databricks import build_spark_engine


class _VersionResult:
    def __init__(self, version: object) -> None:
        self._version = version

    def first(self):
        return {"dbr_version": self._version}


class _Spark:
    def __init__(
        self,
        version: object = "18.x-aarch64-photon-scala2.13",
        failure: Exception | None = None,
    ) -> None:
        self._version = version
        self._failure = failure
        self.queries: list[str] = []

    def sql(self, statement: str):
        self.queries.append(statement)
        if self._failure is not None:
            raise self._failure
        return _VersionResult(self._version)


@pytest.mark.parametrize(
    "version",
    [
        "16.4.x-scala2.12",
        "17.3.x-scala2.13",
        "18.x-aarch64-photon-scala2.13",
    ],
)
def test_build_spark_engine_accepts_supported_databricks_runtimes(version):
    spark = _Spark(version)

    engine = build_spark_engine(spark)

    assert engine is not None
    assert spark.queries == ["SELECT current_version().dbr_version AS dbr_version"]


@pytest.mark.parametrize("version", ["16.3.x-scala2.12", "19.x-scala2.13"])
def test_build_spark_engine_rejects_unsupported_databricks_runtimes(version):
    with pytest.raises(RuntimeError, match=r"requires DBR >=16\.4,<19"):
        build_spark_engine(_Spark(version))


@pytest.mark.parametrize("version", [None, "not-a-runtime", "18.future"])
def test_build_spark_engine_rejects_an_unknown_databricks_runtime(version):
    with pytest.raises(RuntimeError, match=r"requires DBR >=16\.4,<19"):
        build_spark_engine(_Spark(version))


def test_build_spark_engine_preserves_a_runtime_detection_failure():
    failure = RuntimeError("version query failed")

    with pytest.raises(RuntimeError, match="compatibility cannot be verified") as exc_info:
        build_spark_engine(_Spark(failure=failure))

    assert exc_info.value.__cause__ is failure


def test_release_compatibility_markdown_uses_the_enforced_range():
    assert release_compatibility_markdown() == (
        "### Databricks compatibility\n\n"
        "- Spark backend: Databricks Runtime `>=16.4,<19`\n"
        "- SQL warehouse backend: not governed by a Databricks Runtime version"
    )


def test_build_engine_does_not_touch_root_logging():
    # Given a caller that has installed its own root log handler
    root = logging.getLogger()
    sentinel = logging.NullHandler()
    root.addHandler(sentinel)
    try:
        # When building the engine
        build_spark_engine(_Spark())

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

    assert "Logging error" not in capsys.readouterr().err
