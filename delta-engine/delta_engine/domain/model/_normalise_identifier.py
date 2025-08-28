def normalize_identifier(value: str) -> str:
    v = value.strip()
    if not v or " " in v or "." in v or not v.isascii():
        raise ValueError(f"Invalid identifier: {value!r}")
    return v.casefold()
