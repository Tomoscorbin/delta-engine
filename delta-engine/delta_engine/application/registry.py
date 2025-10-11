"""
In-memory registry of desired table definitions for planning.

Accepts lightweight table/column specs, converts them to domain models, and
iterates in fully qualified name order to produce deterministic planning input
for the engine.
"""

from delta_engine.application.ports import ColumnObject, TableObject
from delta_engine.domain.model import Column as DomainColumn, DesiredTable, QualifiedName


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

    def register(self, *tables: TableObject) -> None:
        """
        Register one or more table specifications.

        Args:
            *tables: Table-like objects providing ``catalog``, ``schema``,
                ``name``, and ``columns`` attributes.

        Raises:
            ValueError: If the same fully qualified name is registered twice
                (either within this call or across previous calls).

        """
        # Validate duplicates both within this call and against existing entries
        new_desired: list[DesiredTable] = []
        seen_in_call: set[str] = set()
        for table in tables:
            desired = self._to_desired_table(table)
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

    def _to_desired_table(self, spec: TableObject) -> DesiredTable:
        """Convert a table-like object into a :class:`DesiredTable`."""
        qualified_name = QualifiedName(spec.catalog, spec.schema, spec.name)
        columns = tuple(self._to_domain_column(c) for c in spec.columns)
        partition_columns = tuple(spec.partitioned_by) if spec.partitioned_by else ()
        return DesiredTable(
            qualified_name=qualified_name,
            columns=columns,
            comment=spec.comment,
            properties=spec.effective_properties,
            partitioned_by=partition_columns,
            format=spec.format,
        )

    @staticmethod
    def _to_domain_column(column: ColumnObject) -> DomainColumn:
        """Convert a column-like object into a domain :class:`Column`."""
        return DomainColumn(
            name=column.name,
            data_type=column.data_type,
            is_nullable=column.is_nullable,
            comment=column.comment,
        )
