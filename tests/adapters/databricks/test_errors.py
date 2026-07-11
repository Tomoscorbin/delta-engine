"""Shared exception-translation core used by both Databricks backends."""

from delta_engine.adapters.databricks.errors import (
    ExceptionDetails,
    bounded_message,
    translate_exception,
)


def test_translate_uses_python_class_name_and_message():
    details = translate_exception(ValueError("bad input"))
    assert details == ExceptionDetails(type_name="ValueError", message="bad input")


def test_bounded_message_keeps_only_the_first_five_lines():
    exception = RuntimeError("\n".join(f"line {i}" for i in range(1, 10)))
    assert bounded_message(exception) == "\n".join(f"line {i}" for i in range(1, 6))


def test_translate_bounds_multiline_messages():
    exception = RuntimeError("\n".join(f"line {i}" for i in range(1, 10)))
    details = translate_exception(exception)
    assert details.message.count("\n") == 4
