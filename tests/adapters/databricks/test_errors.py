"""
Shared exception rendering (Java class preferred, non-throwing message fallback).

The py4j wrapper is detected by duck-typing on ``java_exception``, so the
gateway tests pin that contract against a real ``Py4JJavaError`` and the
duck-typing itself against a plain exception carrying the attribute.
"""

from types import SimpleNamespace

from py4j.protocol import Py4JJavaError

from delta_engine.adapters.databricks.exception_inspection import (
    exception_message,
    exception_type_name,
    is_missing_relation,
)


def test_names_plain_exceptions_by_python_class():
    # Then an unwrapped exception is named by its Python class
    assert exception_type_name(ValueError("nope")) == "ValueError"


def test_renders_plain_exception_messages():
    # Then an unwrapped exception renders its own message
    assert exception_message(ValueError("nope")) == "nope"


def test_message_rendering_never_raises():
    # Given an exception whose own __str__ raises
    class UnrenderableError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("rendering failed")

    # Then the failure record stays constructible with a stable fallback
    assert exception_message(UnrenderableError()) == "<exception message unavailable>"


def test_names_py4j_errors_by_underlying_java_class():
    # Given a Py4JJavaError whose java_exception reports a known Java class.
    # A SimpleNamespace stands in for the JVM object: rendering a real
    # Py4JJavaError's message (str()) requires a live JVM gateway, which unit
    # tests don't have.
    java_exception = SimpleNamespace(
        _target_id="o1",
        getClass=lambda: SimpleNamespace(getName=lambda: "org.apache.spark.sql.AnalysisException"),
    )
    error = Py4JJavaError("boom", java_exception)

    # When naming the exception type
    name = exception_type_name(error)

    # Then the underlying Java class is named, not the py4j wrapper
    assert name == "org.apache.spark.sql.AnalysisException"


def test_names_any_exception_carrying_a_java_exception_by_its_java_class():
    # Given a wrapper that is not a Py4JJavaError but carries java_exception —
    # the detection is duck-typed so the warehouse backend never imports py4j
    error = Exception("boom")
    error.java_exception = SimpleNamespace(  # type: ignore[attr-defined]
        getClass=lambda: SimpleNamespace(getName=lambda: "org.apache.spark.sql.AnalysisException")
    )

    # Then the Java class still wins over the Python class
    assert exception_type_name(error) == "org.apache.spark.sql.AnalysisException"


def test_falls_back_to_wrapper_class_when_the_gateway_is_unreachable():
    # Given a wrapped JVM exception whose remote calls fail (dead gateway)
    def raise_gateway_error(*_args: object) -> None:
        raise RuntimeError("gateway is down")

    java_exception = SimpleNamespace(
        _target_id="o1",
        _gateway_client=SimpleNamespace(send_command=raise_gateway_error),
        getClass=raise_gateway_error,
    )
    error = Py4JJavaError("boom", java_exception)

    # Then both parts of the failure record remain constructible without the gateway
    assert exception_type_name(error) == "Py4JJavaError"
    assert exception_message(error) == "<exception message unavailable>"


# is_missing_relation tests


class _AnalysisError(Exception):
    def __init__(self, condition: str) -> None:
        self._condition = condition

    def getCondition(self) -> str:
        return self._condition


def test_missing_relation_from_spark_condition() -> None:
    # Then only the missing-table condition reads as a missing relation
    assert is_missing_relation(_AnalysisError("TABLE_OR_VIEW_NOT_FOUND")) is True
    assert is_missing_relation(_AnalysisError("INSUFFICIENT_PERMISSIONS")) is False


def test_missing_relation_from_warehouse_message_prefix() -> None:
    # Then warehouse errors, which carry the condition as a message prefix,
    # read the same way
    assert is_missing_relation(RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] Table … not found")) is True
    assert is_missing_relation(RuntimeError("connection reset")) is False


def test_missing_schema_or_catalog_is_not_a_missing_relation() -> None:
    # Then a missing container never reads as a creatable absence — the
    # engine creates tables, never schemas or catalogs
    assert is_missing_relation(_AnalysisError("SCHEMA_NOT_FOUND")) is False
    assert is_missing_relation(_AnalysisError("CATALOG_NOT_FOUND")) is False
    assert (
        is_missing_relation(RuntimeError("[SCHEMA_NOT_FOUND] The schema cannot be found")) is False
    )
