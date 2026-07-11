"""Tests for backend exception translation (type name + bounded message)."""

from types import SimpleNamespace

from py4j.protocol import Py4JJavaError

from delta_engine.adapters.databricks.spark.errors import _exception_type_name, translate_exception


def test_translate_reports_python_class_and_message_for_plain_exception():
    details = translate_exception(ValueError("nope"))

    assert details.type_name == "ValueError"
    assert details.message == "nope"


def test_translate_truncates_message_to_first_five_lines():
    details = translate_exception(Exception("L1\nL2\nL3\nL4\nL5\nL6\nL7"))

    assert details.message == "L1\nL2\nL3\nL4\nL5"


def test_type_name_reports_underlying_java_class_for_py4j_error():
    # Given a Py4JJavaError whose java_exception reports a known Java class.
    # The private helper is targeted directly: rendering a real Py4JJavaError's
    # message (str()) requires a live JVM gateway, which unit tests don't have;
    # message bounding is covered by the plain-exception tests above.
    java_exception = SimpleNamespace(
        _target_id="o1",
        getClass=lambda: SimpleNamespace(getName=lambda: "org.apache.spark.sql.AnalysisException"),
    )
    error = Py4JJavaError("boom", java_exception)

    # When naming the exception type
    name = _exception_type_name(error)

    # Then the underlying Java class is named, not the py4j wrapper
    assert name == "org.apache.spark.sql.AnalysisException"
