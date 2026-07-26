"""
Identifier identity policy for column-like names.

Databricks resolves column-like identifiers case-insensitively while
preserving their display spelling. The engine stores spelling verbatim and
derives an explicit lowercase identity key wherever two identifiers must be
judged the same column. This module is the only place that canonicalization
lives.
"""

from collections.abc import Callable, Iterable


class Identifier(str):
    """
    A column-like identifier: preserved spelling, case-insensitive identity.

    ``Identifier`` is a ``str`` whose equality and hash follow Databricks
    identifier resolution: two spellings differing only in case are the same
    identifier. The construction spelling is preserved, so rendering, SQL
    compilation, and error messages show it verbatim. Comparisons against
    plain strings are case-insensitive in both directions (the subclass's
    reflected operator takes priority).

    Identity uses ``str.lower``, deliberately not ``str.casefold``: the live
    object-name pin distinguishes Python lowercasing from casefolding, and
    identifier identity must not silently adopt new Unicode semantics.
    """

    __slots__ = ()

    def __new__(cls, spelling: str) -> "Identifier":
        """Validate and intern the spelling; blank identifiers are invalid."""
        if not spelling.strip():
            raise ValueError(f"Identifier must not be blank: {spelling!r}")
        return super().__new__(cls, spelling)

    @property
    def key(self) -> str:
        """The lowercase identity key shared by every spelling of this identifier."""
        return str.lower(self)

    @property
    def spelling(self) -> str:
        """The exact spelling as a plain ``str``, for case-sensitive comparison."""
        # Slicing a str subclass returns a plain str copy.
        return self[:]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return str.lower(self) == other.lower()
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        equal = self.__eq__(other)
        if equal is NotImplemented:
            return equal
        return not equal

    def __hash__(self) -> int:
        return hash(str.lower(self))


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
                f"Duplicate identifier: {name_of(item)!r} collides with {name_of(index[key])!r}"
            )
        index[key] = item
    return index
