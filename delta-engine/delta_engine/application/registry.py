from delta_engine.application.ports import ColumnObject, TableObject
from delta_engine.domain.model import Column as DomainColumn, DesiredTable, QualifiedName


class Registry:
    """In-memory registry of desired table definitions.

    Register user-supplied table specs and iterate over them in deterministic
    (name-sorted) order for planning.
    """
    def __init__(self) -> None:
        self._tables_by_name: dict[str, DesiredTable] = {}

    def register(self, *tables: TableObject) -> None:
        """Register one or more table specifications.

        Args:
            *tables: Table-like objects providing ``catalog``, ``schema``,
                ``name``, and ``columns`` attributes.

        Raises:
            ValueError: If the same fully qualified name is registered twice.
        """
        for table in tables:
            desired = self._to_desired_table(table)
            fully_qualified_name = str(desired.qualified_name)
            if fully_qualified_name in self._tables_by_name:
                raise ValueError(f"Duplicate table registration: {fully_qualified_name}")
            self._tables_by_name[fully_qualified_name] = desired

    def __iter__(self):
        """Iterate over desired tables in fully-qualified-name order."""
        for key in sorted(self._tables_by_name):
            yield self._tables_by_name[key]

    def _to_desired_table(self, spec: TableObject) -> DesiredTable:
        """Convert a table-like object into a :class:`DesiredTable`."""
        qualified_name = QualifiedName(spec.catalog, spec.schema, spec.name)
        columns = tuple(self._to_domain_column(c) for c in spec.columns)
        return DesiredTable(qualified_name=qualified_name, columns=columns)

    @staticmethod
    def _to_domain_column(column: ColumnObject) -> DomainColumn:
        """Convert a column-like object into a domain :class:`Column`."""
        return DomainColumn(
            name=column.name,
            data_type=column.data_type,
            is_nullable=column.is_nullable,
        )
