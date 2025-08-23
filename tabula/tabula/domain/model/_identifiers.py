from __future__ import annotations


def normalize_identifier(value: object) -> str:
    """
    Normalize an identifier part (catalog/schema/name).

    Policy: ASCII-only. No whitespace, no dots. Case-insensitive via lower().

    Raises:
      TypeError  - if value is not str
      ValueError - if empty, has leading/trailing/internal whitespace, contains '.', or non-ASCII
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
