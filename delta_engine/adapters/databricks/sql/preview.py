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
