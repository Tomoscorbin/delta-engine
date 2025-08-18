from __future__ import annotations
import re

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def normalize_identifier(label: str, value: str | None) -> str:
    if value is None:
        raise ValueError(f"{label} cannot be None")

    if not isinstance(value, str):
        raise TypeError(f"{label} must be str, got {type(value).__name__}")

    if value.strip() != value:
        raise ValueError(f"{label} must not have leading/trailing whitespace: {value!r}")

    if not value:
        raise ValueError(f"{label} cannot be empty")

    if any(ch.isspace() for ch in value):
        raise ValueError(f"{label} must not contain whitespace characters")

    if "." in value:
        raise ValueError(f"{label} must not contain '.'")

    # Normalize for case-insensitive semantics
    return value.casefold()