"""
Identifier identity policy for column-like names.

Databricks resolves column-like identifiers case-insensitively while
preserving their display spelling. The engine stores spelling verbatim and
derives an explicit lowercase identity key wherever two identifiers must be
judged the same column. This module is the only place that canonicalization
lives.
"""

from collections.abc import Callable, Iterable


def identifier_key(name: str) -> str:
    """
    Return the Databricks identity key without changing stored spelling.

    Uses ``str.lower``, deliberately not ``str.casefold``: the live
    object-name pin distinguishes Python lowercasing from casefolding, and
    identifier identity must not silently adopt new Unicode semantics.
    """
    return name.lower()


def index_by_identifier[T](items: Iterable[T], name_of: Callable[[T], str]) -> dict[str, T]:
    """
    Index ``items`` by identifier key, rejecting case-insensitive duplicates.

    A silent duplicate would let the later value win and hide a real
    identity collision, so a collision raises ``ValueError`` naming both
    spellings.
    """
    index: dict[str, T] = {}
    for item in items:
        key = identifier_key(name_of(item))
        if key in index:
            raise ValueError(
                f"Duplicate identifier: {name_of(item)!r} collides with"
                f" {name_of(index[key])!r}"
            )
        index[key] = item
    return index
