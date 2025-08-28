def normalize_identifier(value: str) -> str:
    """Return a normalized identifier: trimmed, ASCII, lowercase, no spaces/dots.

    Args:
        value: Raw identifier value.

    Returns:
        The normalized identifier in casefolded (lowercase) form.

    Raises:
        ValueError: If the value is empty, contains spaces or dots, or is non-ASCII.
    """
    v = value.strip()
    if not v or " " in v or "." in v or not v.isascii():
        raise ValueError(f"Invalid identifier: {value!r}")
    return v.casefold()
