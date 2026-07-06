"""Compact single-line previews of SQL statements for logs and errors."""

from __future__ import annotations


def sql_preview(sql: str, *, max_chars: int = 240) -> str:
    """
    Return a compact, bounded preview of a SQL statement for logs/errors.

    - Normalizes all runs of whitespace to single spaces on one line.
    - Truncates with an ellipsis when longer than max_chars.
    """
    s = " ".join(sql.split())
    return s if len(s) <= max_chars else (s[:max_chars] + "…")
