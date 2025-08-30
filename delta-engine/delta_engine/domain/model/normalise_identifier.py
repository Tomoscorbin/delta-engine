"""Identifiers can only contain alphanumeric ASCII characters and underscores."""


def normalize_identifier(value: str) -> str:
    """
    Return a normalized identifier: trimmed, ASCII, lowercase, [A-Za-z0-9_]+ only.

    Args:
        value: Raw identifier value.

    Returns:
        The normalized identifier in casefolded (lowercase) form.

    Raises:
        ValueError: If empty after trim, non-ASCII, or contains chars
                    other than letters, digits, or underscore.

    """
    v = value.strip()
    if not v or not v.isascii() or not all(ch.isalnum() or ch == "_" for ch in v):
        raise ValueError(f"Invalid identifier: {value!r}")
    return v.casefold()
