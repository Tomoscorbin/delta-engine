"""
Prepare user table specifications for planning.

`prepare_desired_tables` is the single seam between the user-facing table
specifications and the engine's phase chain: it lowers each specification to a
domain :class:`DesiredTable`, rejects duplicate qualified names, and returns the
tables in deterministic qualified-name order so a sync's report and execution
order never depend on the order tables were passed.
"""

from typing import Protocol

from delta_engine.application.errors import DuplicateTableDefinitionError
from delta_engine.domain.model import DesiredTable


class DesiredTableSource(Protocol):
    """A user-facing table specification that can produce a domain table."""

    def to_desired_table(self) -> DesiredTable:
        """Return the domain :class:`DesiredTable` for this specification."""
        ...


def prepare_desired_tables(*tables: DesiredTableSource) -> tuple[DesiredTable, ...]:
    """
    Lower table specifications into domain tables for planning.

    Converts each source via ``to_desired_table()``, rejects duplicate
    qualified names, and returns the tables in deterministic qualified-name
    order. Passing no tables yields an empty tuple.

    Args:
        *tables: Table specifications that can convert themselves into a domain
            :class:`DesiredTable` via ``to_desired_table()``.

    Returns:
        The desired tables, deduplicated and sorted by qualified name.

    Raises:
        DuplicateTableDefinitionError: If two sources share a qualified name.

    """
    desired_by_name: dict[str, DesiredTable] = {}
    for source in tables:
        desired = source.to_desired_table()
        key = str(desired.qualified_name)
        if key in desired_by_name:
            raise DuplicateTableDefinitionError(desired.qualified_name)
        desired_by_name[key] = desired
    return tuple(desired_by_name[key] for key in sorted(desired_by_name))
