"""
Named ownership scopes for Delta table declarations.

The domain defines the complete ``TableAspect`` vocabulary. This module owns
the supported public combinations and resolves a scope name at the API boundary.
"""

from typing import Final, Literal

from delta_engine.domain.model import ALL_ASPECTS, TableAspect

type ScopeName = Literal["full", "metadata", "annotations", "tags"]

METADATA_ASPECTS: Final[frozenset[TableAspect]] = frozenset(
    {
        TableAspect.TABLE_COMMENT,
        TableAspect.COLUMN_COMMENTS,
        TableAspect.TABLE_TAGS,
        TableAspect.COLUMN_TAGS,
        TableAspect.PRIMARY_KEY,
        TableAspect.FOREIGN_KEYS,
    }
)

ANNOTATION_ASPECTS: Final[frozenset[TableAspect]] = frozenset(
    {
        TableAspect.TABLE_COMMENT,
        TableAspect.COLUMN_COMMENTS,
        TableAspect.TABLE_TAGS,
        TableAspect.COLUMN_TAGS,
    }
)

TAG_ASPECTS: Final[frozenset[TableAspect]] = frozenset(
    {
        TableAspect.TABLE_TAGS,
        TableAspect.COLUMN_TAGS,
    }
)

_ASPECTS_BY_SCOPE: Final[dict[ScopeName, frozenset[TableAspect]]] = {
    "full": ALL_ASPECTS,
    "metadata": METADATA_ASPECTS,
    "annotations": ANNOTATION_ASPECTS,
    "tags": TAG_ASPECTS,
}


def managed_aspects_for(scope: ScopeName) -> frozenset[TableAspect]:
    """
    Return the aspects managed by a public scope name.

    Raises:
        ValueError: If an untyped caller supplies an unknown scope name.

    """
    # Keep the runtime check for untyped callers.
    if scope not in _ASPECTS_BY_SCOPE:
        expected = ", ".join(repr(name) for name in _ASPECTS_BY_SCOPE)
        raise ValueError(f"Unknown scope {scope!r}; expected one of: {expected}")

    return _ASPECTS_BY_SCOPE[scope]
