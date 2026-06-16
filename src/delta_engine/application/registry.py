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

    Stores tables as a simple list while tracking fully qualified names in a
    set for fast duplicate detection. Iteration yields tables in deterministic
    (name-sorted) order.
    """

    def __init__(self) -> None:
        """Create an empty registry."""
        self._tables: list[DesiredTable] = []
        self._names: set[str] = set()

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
        # Validate duplicates both within this call and against existing entries
        new_desired: list[DesiredTable] = []
        seen_in_call: set[str] = set()
        for table in tables:
            desired = table.to_desired_table()
            fqn = str(desired.qualified_name)
            if fqn in self._names or fqn in seen_in_call:
                raise ValueError(f"Duplicate table registration: {fqn}")
            seen_in_call.add(fqn)
            new_desired.append(desired)

        # Append after validation succeeds
        self._tables.extend(new_desired)
        self._names.update(str(d.qualified_name) for d in new_desired)

    def __iter__(self):
        """Iterate over desired tables in fully-qualified-name order."""
        yield from (d for _, d in sorted((str(d.qualified_name), d) for d in self._tables))

    def __len__(self):
        """Return the number of registered tables."""
        return len(self._tables)
