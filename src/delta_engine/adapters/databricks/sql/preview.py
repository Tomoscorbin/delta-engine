"""Compact previews for SQL statements and exceptions in logs/errors."""

from __future__ import annotations


def sql_preview(sql: str, *, max_chars: int = 240, single_line: bool = True) -> str:
    """
    Return a compact, bounded preview of a SQL statement for logs/errors.

    - Normalizes whitespace to a single line when single_line=True.
    - Truncates with an ellipsis when longer than max_chars.
    """
    s = sql.strip()
    if single_line:
        s = " ".join(s.split())
    return s if len(s) <= max_chars else (s[:max_chars] + "…")


def error_preview(exception: Exception) -> str:
    """Return a short, single-string preview of an exception message body."""
    message_head = str(exception)
    return "\n".join(message_head.splitlines()[:5])


def exception_type_name(exception: Exception) -> str:
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
