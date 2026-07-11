"""Shared exception-summarising core used by both Databricks backends."""

from delta_engine.adapters.databricks.errors import (
    ExceptionSummary,
    bounded_message,
    summarize_exception,
)


def test_summarize_uses_python_class_name_and_message():
    summary = summarize_exception(ValueError("bad input"))
    assert summary == ExceptionSummary(type_name="ValueError", message="bad input")


def test_bounded_message_keeps_only_the_first_five_lines():
    exception = RuntimeError("\n".join(f"line {i}" for i in range(1, 10)))
    assert bounded_message(exception) == "\n".join(f"line {i}" for i in range(1, 6))


def test_summarize_bounds_multiline_messages():
    exception = RuntimeError("\n".join(f"line {i}" for i in range(1, 10)))
    summary = summarize_exception(exception)
    assert summary.message.count("\n") == 4
