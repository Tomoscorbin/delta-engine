"""
In-memory registry of desired table definitions for planning.

Accepts table specifications that convert themselves to domain models, rejects
duplicate names, and iterates in fully qualified name order to produce
deterministic planning input for the engine.
"""

from typing import Protocol, runtime_checkable

from delta_engine.domain.model import DesiredTable


@runtime_checkable
class DesiredTableSource(Protocol):
    """A user-facing table specification that can produce a domain table."""

    def to_desired_table(self) -> DesiredTable:
        """Return the domain :class:`DesiredTable` for this specification."""
        ...


class Registry:
    """
    In-memory registry of desired table definitions.

    Tables are keyed by fully qualified name, which gives duplicate detection
    and uniqueness for free. Iteration yields tables in deterministic
    (name-sorted) order.
    """

    def __init__(self) -> None:
        """Create an empty registry."""
        self._tables_by_name: dict[str, DesiredTable] = {}

    def register(self, *tables: DesiredTableSource) -> None:
        """
        Register one or more table specifications.

        Args:
            *tables: Table specifications that can convert themselves into a
                domain :class:`DesiredTable` via ``to_desired_table()``.

        Raises:
            ValueError: If the same fully qualified name is registered twice
                (either within this call or across previous calls).

        """
        # Convert and check every table before mutating, so a duplicate in the
        # batch leaves the registry unchanged rather than half-applied.
        new_tables: dict[str, DesiredTable] = {}
        for table in tables:
            desired = table.to_desired_table()
            fqn = str(desired.qualified_name)
            if fqn in self._tables_by_name or fqn in new_tables:
                raise ValueError(f"Duplicate table registration: {fqn}")
            new_tables[fqn] = desired

        self._tables_by_name.update(new_tables)

    def __iter__(self):
        """Iterate over desired tables in fully-qualified-name order."""
        yield from (self._tables_by_name[fqn] for fqn in sorted(self._tables_by_name))

    def __len__(self):
        """Return the number of registered tables."""
        return len(self._tables_by_name)
