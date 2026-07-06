"""
Translate backend exceptions into neutral summaries for typed failures.

Spark raises a heterogeneous set of failures (``Py4JJavaError``,
``AnalysisException``, plain Python errors) that varies across runtime
environments. Both adapter boundaries (reader and executor) reduce any of
them to the same two facts a failure report needs: the most informative type
name and a bounded message.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExceptionSummary:
    """The two facts a typed failure records about a backend exception."""

    type_name: str
    message: str


def summarize_exception(exception: Exception) -> ExceptionSummary:
    """Summarize an exception as its most informative type name and message head."""
    return ExceptionSummary(_exception_type_name(exception), _message_preview(exception))


def _message_preview(exception: Exception) -> str:
    """Return the first lines of an exception message, bounded for reports."""
    message_head = str(exception)
    return "\n".join(message_head.splitlines()[:5])


def _exception_type_name(exception: Exception) -> str:
    """
    Return the most informative exception class name available.

    For Py4JJavaError (the primary failure shape on Databricks, where JVM
    exceptions surface through py4j), the underlying Java class is preferred
    over the py4j wrapper — e.g. 'org.apache.spark.sql.AnalysisException'
    rather than 'Py4JJavaError'. Falls back to the Python class name for all
    other exceptions.
    """
    try:
        from py4j.protocol import Py4JJavaError  # type: ignore[import]

        if isinstance(exception, Py4JJavaError):
            try:
                return exception.java_exception.getClass().getName()
            except (AttributeError, TypeError):
                return "Py4JJavaError"
    except ImportError:
        pass
    return type(exception).__name__
