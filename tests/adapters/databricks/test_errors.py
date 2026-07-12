"""
Shared exception type naming (Java class preferred, Python fallback).

The py4j wrapper is detected by duck-typing on ``java_exception``, so these
tests pin that contract against a real ``Py4JJavaError`` rather than a
stand-in exception class.
"""

from types import SimpleNamespace

from py4j.protocol import Py4JJavaError

from delta_engine.adapters.databricks.errors import exception_type_name


def test_names_plain_exceptions_by_python_class():
    assert exception_type_name(ValueError("nope")) == "ValueError"


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


def test_falls_back_to_wrapper_class_when_the_gateway_is_unreachable():
    # Given a wrapped JVM exception whose remote calls fail (dead gateway)
    def raise_gateway_error() -> None:
        raise RuntimeError("gateway is down")

    java_exception = SimpleNamespace(_target_id="o1", getClass=raise_gateway_error)
    error = Py4JJavaError("boom", java_exception)

    # Then naming still succeeds, using the wrapper's own class
    assert exception_type_name(error) == "Py4JJavaError"
