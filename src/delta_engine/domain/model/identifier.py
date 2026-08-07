"""
The engine stores spelling verbatim; ``Identifier`` carries case-insensitive identity.

This module is the only place that canonicalization lives.
"""

from typing import Self


class Identifier(str):
    """
    A column-like identifier: preserved spelling, case-insensitive identity.

    ``Identifier`` is a ``str`` whose equality and hash follow Databricks
    identifier resolution: two spellings differing only in case are the same
    identifier. The construction spelling is preserved, so rendering, SQL
    compilation, and error messages show it verbatim. Comparisons against
    plain strings are case-insensitive in both directions (the subclass's
    reflected operator takes priority).

    """

    __slots__ = ()

    def __new__(cls, spelling: str) -> Self:
        """Validate and intern the spelling; blank identifiers are invalid."""
        if not spelling.strip():
            raise ValueError(f"Identifier must not be blank: {spelling!r}")
        return super().__new__(cls, spelling)

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
