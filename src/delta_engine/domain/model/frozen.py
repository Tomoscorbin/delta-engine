"""Collection normalization for immutable model values."""

from collections.abc import Sequence


def freeze_strings(values: Sequence[str], *, field_name: str) -> tuple[str, ...]:
    """Copy an ordered string collection while rejecting an ambiguous bare string."""
    if isinstance(values, str):
        raise TypeError(f"{field_name} must be a sequence of strings, not a string")
    return tuple(values)
