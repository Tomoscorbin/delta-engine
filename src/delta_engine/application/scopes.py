"""Resolve public scope names into the domain's table-scope policy."""

from typing import Final, Literal

from delta_engine.domain.model import TableScope

type ScopeName = Literal["full", "metadata", "annotations", "tags"]

_SCOPE_BY_NAME: Final[dict[ScopeName, TableScope]] = {
    "full": TableScope.FULL,
    "metadata": TableScope.METADATA,
    "annotations": TableScope.ANNOTATIONS,
    "tags": TableScope.TAGS,
}


def table_scope_for(scope: ScopeName) -> TableScope:
    """
    Resolve a public scope name.

    Raises:
        ValueError: If an untyped caller supplies an unknown scope name.

    """
    # Keep the runtime check for untyped callers.
    if scope not in _SCOPE_BY_NAME:
        expected = ", ".join(repr(name) for name in _SCOPE_BY_NAME)
        raise ValueError(f"Unknown scope {scope!r}; expected one of: {expected}")

    return _SCOPE_BY_NAME[scope]
