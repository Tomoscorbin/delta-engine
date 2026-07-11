"""Tests for Spark exception type naming (Java class preferred, Python fallback)."""

from types import SimpleNamespace

from py4j.protocol import Py4JJavaError

from delta_engine.adapters.databricks.spark.errors import exception_type_name


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
