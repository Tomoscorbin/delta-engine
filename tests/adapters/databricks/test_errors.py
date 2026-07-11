"""Shared exception-translation core used by both Databricks backends."""

from delta_engine.adapters.databricks.errors import ExceptionDetails, translate_exception


def test_translate_uses_python_class_name_and_message():
    details = translate_exception(ValueError("bad input"))
    assert details == ExceptionDetails(type_name="ValueError", message="bad input")


def test_translate_records_multiline_messages_in_full():
    message = "\n".join(f"line {i}" for i in range(1, 10))
    details = translate_exception(RuntimeError(message))
    assert details.message == message
