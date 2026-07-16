import json
from types import SimpleNamespace

from delta_engine.adapters.databricks.read import (
    observed_table_from_description,
    read_catalog_state,
)
from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_json_query,
    referencing_foreign_keys_query,
    table_tags_query,
)
from delta_engine.adapters.databricks.sql.describe import TableDescription
from delta_engine.application.ports import ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import (
    ForeignKeyConstraint,
    Integer,
    ObservedColumn,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
)

QN = QualifiedName("cat", "sch", "tbl")


def _description(**overrides):
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
    return TableDescription(**base)


def _router(responses):
    def run(query):
        value = responses.get(query, [])
        if isinstance(value, Exception):
            raise value
        return value

    return run


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
    observed = observed_table_from_description(
        _description(), run_info_schema_query=_router(responses)
    )

    assert dict(observed.tags) == {"Owner": "Data"}
    assert dict(observed.columns[0].tags) == {"pii": "low"}
    assert observed.referencing_foreign_keys[0].referencing_table == QualifiedName(
        "cat", "sch", "child"
    )


def test_all_description_fields_pass_through():
    description = _description(
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

    observed = observed_table_from_description(description, run_info_schema_query=_router({}))

    assert observed.comment == "orders"
    assert observed.partitioned_by == ("region",)
    assert observed.clustered_by == ("id",)
    assert dict(observed.properties) == {"delta.columnMapping.mode": "name"}
    assert observed.primary_key.columns == ("id",)
    assert observed.foreign_keys[0].constraint_name == "t_fk"
    assert dict(observed.columns[0].tags) == {}


_DESCRIBE_DOC = json.dumps(
    {
        "table_name": "tbl",
        "catalog_name": "cat",
        "schema_name": "sch",
        "columns": [{"name": "id", "type": {"name": "int"}, "nullable": False}],
        "comment": "",
        "table_properties": {},
    }
)


def _describe_responses(**overrides):
    responses = {
        describe_json_query(QN): [(_DESCRIBE_DOC,)],
        table_tags_query(QN): [],
        column_tags_query(QN): [],
        referencing_foreign_keys_query(QN): [],
    }
    responses.update(overrides)
    return responses


def test_read_catalog_state_returns_the_present_table():
    state = read_catalog_state(_router(_describe_responses()), QN)

    assert isinstance(state, TablePresent)
    assert state.table.columns[0].data_type == Integer()


def test_read_catalog_state_describes_first_then_reads_info_schema():
    responses = _describe_responses()
    calls = []

    def run_query(query):
        calls.append(query)
        return responses[query]

    read_catalog_state(run_query, QN)

    assert calls[0] == describe_json_query(QN)
    assert len(calls) == 4


def test_missing_relation_on_describe_reads_as_absent():
    responses = {describe_json_query(QN): RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] nope")}

    assert isinstance(read_catalog_state(_router(responses), QN), TableAbsent)


def test_other_describe_error_reads_as_failed():
    responses = {describe_json_query(QN): RuntimeError("warehouse gone")}

    state = read_catalog_state(_router(responses), QN)

    assert isinstance(state, ReadFailed)
    assert "warehouse gone" in state.failure.message


def test_empty_describe_result_reads_as_failed():
    responses = _describe_responses(**{describe_json_query(QN): []})

    assert isinstance(read_catalog_state(_router(responses), QN), ReadFailed)


def test_missing_relation_while_reading_info_schema_reads_as_failed_not_absent():
    # Missing-relation means "table absent" only for the describe. A failure while
    # attaching tags means the table was found but the read could not complete.
    responses = _describe_responses(
        **{table_tags_query(QN): RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] tags view")}
    )

    assert isinstance(read_catalog_state(_router(responses), QN), ReadFailed)


def test_unmappable_partition_column_reads_as_failed_not_present():
    # A partition column whose type the domain cannot model is dropped from the
    # columns, which would leave partitioning naming a column that is not there.
    # ObservedTable rejects that inconsistency, so the read reports failed.
    doc = json.dumps(
        {
            "table_name": "tbl",
            "catalog_name": "cat",
            "schema_name": "sch",
            "columns": [
                {"name": "id", "type": {"name": "int"}, "nullable": False},
                {"name": "region", "type": {"name": "geography"}, "nullable": True},
            ],
            "partition_columns": ["region"],
        }
    )
    responses = _describe_responses(**{describe_json_query(QN): [(doc,)]})

    assert isinstance(read_catalog_state(_router(responses), QN), ReadFailed)
