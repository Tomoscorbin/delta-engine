from types import SimpleNamespace

from delta_engine.adapters.databricks.read import observed_table_from_snapshot
from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    referencing_foreign_keys_query,
    table_tags_query,
)
from delta_engine.adapters.databricks.sql.describe_json import TableSnapshot
from delta_engine.domain.model import (
    ForeignKeyConstraint,
    Integer,
    ObservedColumn,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
)

QN = QualifiedName("cat", "sch", "tbl")


def _snapshot(**overrides):
    base = dict(
        qualified_name=QN,
        columns=(ObservedColumn("id", Integer(), nullable=False),),
        comment="",
        partitioned_by=(),
        clustered_by=(),
        properties={},
        primary_key=None,
        foreign_keys=(),
    )
    base.update(overrides)
    return TableSnapshot(**base)


def _router(responses):
    return lambda query: responses.get(query, [])


def test_tags_and_inbound_fks_attached():
    responses = {
        table_tags_query(QN): [SimpleNamespace(tag_name="Owner", tag_value="Data")],
        column_tags_query(QN): [
            SimpleNamespace(column_name="ID", tag_name="pii", tag_value="low"),
        ],
        referencing_foreign_keys_query(QN): [
            SimpleNamespace(
                constraint_name="child_fk",
                referencing_catalog="cat",
                referencing_schema="sch",
                referencing_table="child",
            ),
        ],
    }
    observed = observed_table_from_snapshot(_snapshot(), run_info_schema_query=_router(responses))

    assert dict(observed.tags) == {"Owner": "Data"}
    assert dict(observed.columns[0].tags) == {"pii": "low"}
    assert observed.referencing_foreign_keys[0].referencing_table == QualifiedName(
        "cat", "sch", "child"
    )


def test_all_snapshot_fields_pass_through():
    snapshot = _snapshot(
        columns=(
            ObservedColumn("id", Integer(), nullable=False),
            ObservedColumn("region", String()),
        ),
        comment="orders",
        partitioned_by=("region",),
        clustered_by=("id",),
        properties={"delta.columnMapping.mode": "name"},
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="t_pk"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("id",),
                referenced_table=QualifiedName("cat", "sch", "other"),
                referenced_columns=("other_id",),
                constraint_name="t_fk",
            ),
        ),
    )

    observed = observed_table_from_snapshot(snapshot, run_info_schema_query=_router({}))

    assert observed.comment == "orders"
    assert observed.partitioned_by == ("region",)
    assert observed.clustered_by == ("id",)
    assert dict(observed.properties) == {"delta.columnMapping.mode": "name"}
    assert observed.primary_key.columns == ("id",)
    assert observed.foreign_keys[0].constraint_name == "t_fk"
    assert dict(observed.columns[0].tags) == {}
