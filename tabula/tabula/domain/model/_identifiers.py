from __future__ import annotations

"""Identifier normalization helpers."""


def normalize_identifier(value: object) -> str:
    """Normalize an identifier part such as catalog, schema, or table name.

    Args:
        value: Raw identifier value.

    Returns:
        Normalized identifier in lowercase.

    Raises:
        TypeError: If ``value`` is not a string.
        ValueError: If the identifier is empty, contains whitespace or dots,
            or is not ASCII.
    """

    if not isinstance(value, str):
        raise TypeError(f"identifier must be str, got {type(value).__name__}")
    if value == "":
        raise ValueError("identifier must not be empty")
    if value != value.strip():
        raise ValueError(f"identifier must not have leading/trailing whitespace: {value!r}")
    if any(ch.isspace() for ch in value):
        raise ValueError("identifier must not contain whitespace")
    if "." in value:
        raise ValueError("identifier must not contain a dot")
    if not value.isascii():
        raise ValueError("identifier must be ASCII")
    return value.lower()
