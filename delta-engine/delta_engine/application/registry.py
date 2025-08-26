from delta_engine.domain.model import (
    DesiredTable,
    QualifiedName, 
    Column as DomainColumn
)
from delta_engine.application.ports import TableObject, ColumnObject


class Registry:
    def __init__(self) -> None:
        self._tables_by_name: dict[str, DesiredTable] = {}

    def register(self, *tables: TableObject) -> None:
        for t in tables:
            desired = self._to_desired_table(t)
            key = str(desired.qualified_name)
            if key in self._tables_by_name:
                raise ValueError(f"Duplicate table registration: {key}")
            self._tables_by_name[key] = desired

    def __iter__(self):
        for key in sorted(self._tables_by_name):
            yield self._tables_by_name[key]

    def _to_desired_table(self, spec: TableObject) -> DesiredTable:
        qn = QualifiedName(spec.catalog, spec.schema, spec.name)
        cols = tuple(self._to_domain_column(c) for c in spec.columns)
        return DesiredTable(qualified_name=qn, columns=cols)
    
    @staticmethod
    def _to_domain_column(col: ColumnObject) -> DomainColumn:
        return DomainColumn(
            name=col.name,
            data_type=col.data_type,
            is_nullable=col.is_nullable,
        )
