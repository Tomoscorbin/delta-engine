
from delta_engine.domain.model import Column as DomainColumn, DesiredTable, QualifiedName
from delta_engine.schema.delta import DeltaTable


def to_desired_table(spec: DeltaTable) -> DesiredTable:
    qn = QualifiedName(spec.catalog, spec.schema, spec.name)
    cols = tuple(DomainColumn(c.name, c.data_type, c.is_nullable) for c in spec.columns)
    return DesiredTable(qualified_name=qn, columns=cols)
