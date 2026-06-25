"""
In-memory registry of desired table definitions for planning.

Accepts table specifications that convert themselves to domain models, rejects
duplicate names, and iterates in qualified-name order to produce deterministic
planning input for the engine.
"""

from collections.abc import Iterator
from typing import Protocol, runtime_checkable

from delta_engine.domain.model import DesiredTable, QualifiedName


@runtime_checkable
class DesiredTableSource(Protocol):
    """A user-facing table specification that can produce a domain table."""

    def to_desired_table(self) -> DesiredTable:
        """Return the domain :class:`DesiredTable` for this specification."""
        ...


class Registry:
    """
    In-memory registry of desired table definitions.

    Tables are keyed by their :class:`QualifiedName`, which gives duplicate
    detection and uniqueness for free. Iteration yields tables in deterministic
    (name-sorted) order.
    """

    def __init__(self) -> None:
        """Create an empty registry."""
        self._tables: dict[QualifiedName, DesiredTable] = {}

    def register(self, *tables: DesiredTableSource) -> None:
        """
        Register one or more table specifications.

        Args:
            *tables: Table specifications that can convert themselves into a
                domain :class:`DesiredTable` via ``to_desired_table()``.

        Raises:
            ValueError: If the same qualified name is registered twice
                (either within this call or across previous calls).

        """
        # Convert and check every table before mutating, so a duplicate in the
        # batch leaves the registry unchanged rather than half-applied.
        new_tables: dict[QualifiedName, DesiredTable] = {}
        for table in tables:
            desired = table.to_desired_table()
            qualified_name = desired.qualified_name
            if qualified_name in self._tables or qualified_name in new_tables:
                raise ValueError(f"Duplicate table registration: {qualified_name}")
            new_tables[qualified_name] = desired

        self._tables.update(new_tables)

    def __iter__(self) -> Iterator[DesiredTable]:
        """Iterate over desired tables in qualified-name order."""
        yield from (self._tables[name] for name in sorted(self._tables, key=str))

    def __len__(self) -> int:
        return len(self._tables)
