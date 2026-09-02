import json
from types import SimpleNamespace

import pytest

from delta_engine.adapters.databricks.read import read_catalog_state
from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_json_query,
    foreign_keys_query,
    primary_key_query,
    referencing_foreign_keys_query,
    schema_exists_query,
    table_tags_query,
)
from delta_engine.application.errors import ReadError
from delta_engine.application.ports import TableAbsent, TablePresent
from delta_engine.domain.model import (
    Integer,
    ObservedForeignKeyConstraint,
    ObservedPrimaryKeyConstraint,
    QualifiedName,
    String,
    TableFeature,
    TableKind,
)
from tests.adapters.databricks.fakes import build_catalog_responses, build_describe_document

QN = QualifiedName("cat", "sch", "tbl")


def _describe_doc(**overrides):
    return build_describe_document(QN, **overrides)


def _describe_responses(describe=None, **overrides):
    return build_catalog_responses(QN, describe, **overrides)


def _router(responses):
    def run(query):
        if query not in responses:
            pytest.fail(f"unexpected SQL query: {query}", pytrace=False)
        value = responses[query]
        if isinstance(value, Exception):
            raise value
        return value

    return run


def _read_error(responses) -> ReadError:
    with pytest.raises(ReadError) as exc_info:
        read_catalog_state(_router(responses), QN)
    return exc_info.value


def test_tags_and_inbound_fks_attached():
    # Given info-schema responses carrying tags and an inbound foreign key
    responses = _describe_responses(
        **{
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
    )

    state = read_catalog_state(_router(responses), QN)

    # Then all three attach to the observed table
    assert isinstance(state, TablePresent)
    observed = state.table
    assert dict(observed.tags) == {"Owner": "Data"}
    assert dict(observed.columns[0].tags) == {"pii": "low"}
    assert observed.referencing_foreign_keys[0].referencing_table == QualifiedName(
        "cat", "sch", "child"
    )


def test_primary_and_foreign_keys_attached_from_info_schema():
    # Given info-schema rows for a primary key and a foreign key
    responses = _describe_responses(
        **{
            primary_key_query(QN): [SimpleNamespace(constraint_name="tbl_pk", column_name="id")],
            foreign_keys_query(QN): [
                SimpleNamespace(
                    constraint_name="tbl_fk",
                    local_column="id",
                    referenced_catalog="cat",
                    referenced_schema="sch",
                    referenced_table="other",
                    referenced_column="other_id",
                ),
            ],
        }
    )

    state = read_catalog_state(_router(responses), QN)

    # Then both constraints attach as value objects
    assert isinstance(state, TablePresent)
    observed = state.table
    assert observed.primary_key == ObservedPrimaryKeyConstraint(columns=("id",), name="tbl_pk")
    assert observed.foreign_keys == (
        ObservedForeignKeyConstraint(
            local_columns=("id",),
            referenced_table=QualifiedName("cat", "sch", "other"),
            referenced_columns=("other_id",),
            name="tbl_fk",
        ),
    )


def test_all_description_fields_pass_through():
    # Given a describe document with every field populated
    doc = _describe_doc(
        columns=[
            {"name": "id", "type": {"name": "int"}, "nullable": False},
            {"name": "region", "type": {"name": "string"}},
        ],
        comment="orders",
        partition_columns=["region"],
        clustering_columns=["id"],
        table_properties={"delta.columnMapping.mode": "name"},
    )
    responses = _describe_responses(describe=doc)

    state = read_catalog_state(_router(responses), QN)

    # Then every described field lands on the observed table
    assert isinstance(state, TablePresent)
    observed = state.table
    assert observed.comment == "orders"
    assert observed.partitioned_by == ("region",)
    assert observed.clustered_by == ("id",)
    assert dict(observed.properties) == {"delta.columnMapping.mode": "name"}
    assert observed.columns[1].data_type == String()
    assert dict(observed.columns[0].tags) == {}


def test_unmanaged_observed_properties_are_invisible():
    # Given observed properties mixing a managed key with protocol internals
    # no declaration can own (delta.minReaderVersion, feature flags)
    doc = _describe_doc(
        table_properties={
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "3",
            "delta.feature.columnMapping": "supported",
        }
    )
    responses = _describe_responses(describe=doc)

    state = read_catalog_state(_router(responses), QN)

    # Then only the managed key survives — internals must not read as drift
    assert isinstance(state, TablePresent)
    assert dict(state.table.properties) == {"delta.columnMapping.mode": "name"}


def test_read_catalog_state_returns_the_present_table():
    # Then a readable delta table reads as present with its modeled columns
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

    # Then the describe comes first — it decides the relation is readable at
    # all. The info-schema attachment queries carry no ordering guarantee.
    assert calls[0] == describe_json_query(QN)
    assert set(calls[1:]) == {
        column_tags_query(QN),
        table_tags_query(QN),
        primary_key_query(QN),
        foreign_keys_query(QN),
        referencing_foreign_keys_query(QN),
    }


def test_tags_attach_by_identity_without_rewriting_the_observed_name():
    # Given a column described as 'requestId' whose tag row spells it 'requestid'
    responses = _describe_responses(
        describe=_describe_doc(
            columns=[{"name": "requestId", "type": {"name": "string"}, "nullable": True}]
        ),
        **{
            column_tags_query(QN): [
                SimpleNamespace(column_name="requestid", tag_name="pii", tag_value="low"),
            ],
        },
    )

    state = read_catalog_state(_router(responses), QN)

    # Then the tag attaches by case-insensitive identity and the observed
    # spelling survives untouched
    assert isinstance(state, TablePresent)
    [column] = state.table.columns
    assert str(column.name) == "requestId"
    assert dict(column.tags) == {"pii": "low"}


def test_missing_table_in_an_existing_schema_reads_as_absent():
    # Given a describe that reports the table missing while its schema exists
    responses = {
        describe_json_query(QN): RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] nope"),
        schema_exists_query(QN): [("sch",)],
    }

    # Then the table reads as a creatable absence
    assert isinstance(read_catalog_state(_router(responses), QN), TableAbsent)


def test_missing_table_in_a_missing_schema_reads_as_failed_not_absent():
    # Given a schema probe returning no rows: the containing schema does not
    # exist. Absent means "create the table", and the engine never creates
    # schemas or catalogs, so a missing container must fail the read — reading
    # it as absent would plan a CREATE TABLE that cannot succeed, and a dry
    # run would report that impossible plan as success.
    responses = {
        describe_json_query(QN): RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] nope"),
        schema_exists_query(QN): [],
    }

    error = _read_error(responses)

    # Then the failure names the missing container
    assert "does not exist" in str(error)


def test_missing_table_in_an_unreadable_catalog_reads_as_failed_not_absent():
    # Given a nonexistent catalog: it also reports TABLE_OR_VIEW_NOT_FOUND on
    # describe, and the schema probe then fails because
    # <catalog>.information_schema cannot resolve
    responses = {
        describe_json_query(QN): RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] nope"),
        schema_exists_query(QN): RuntimeError("[SCHEMA_NOT_FOUND] cat.information_schema"),
    }

    # Then the probe's failure is the read's outcome
    _read_error(responses)


@pytest.mark.parametrize("condition", ["SCHEMA_NOT_FOUND", "CATALOG_NOT_FOUND"])
def test_missing_schema_or_catalog_on_describe_reads_as_failed_not_absent(condition):
    # Given a describe that names the missing container directly
    responses = {describe_json_query(QN): RuntimeError(f"[{condition}] nope")}

    # Then that is not a creatable absence either
    _read_error(responses)


def test_other_describe_error_reads_as_failed():
    # Given a describe failing for a reason other than a missing relation
    responses = {describe_json_query(QN): RuntimeError("warehouse gone")}

    error = _read_error(responses)

    # Then the read fails carrying the backend failure as its cause, and the
    # failure record names the backend's own exception type
    assert "warehouse gone" in str(error)
    assert isinstance(error.__cause__, RuntimeError)
    assert error.exception_type == "RuntimeError"


def test_a_defect_in_the_engines_own_read_code_propagates_rather_than_reading_as_failed():
    # Given tag rows whose shape the engine's row handling cannot process —
    # the crash happens in engine code, after the query itself succeeded
    responses = _describe_responses(**{table_tags_query(QN): [("Owner", "Data")]})

    # Then the defect propagates as itself instead of masquerading as an
    # unreadable table
    with pytest.raises(AttributeError):
        read_catalog_state(_router(responses), QN)


def test_empty_describe_result_reads_as_failed():
    # Given a describe returning no rows from a statement that must return one
    responses = _describe_responses(**{describe_json_query(QN): []})

    # Then the read fails rather than reporting absence
    _read_error(responses)


def test_missing_relation_while_reading_info_schema_reads_as_failed_not_absent():
    # Given a missing-relation failure while attaching tags: the table was
    # found, so missing-relation means "table absent" only for the describe
    responses = _describe_responses(
        **{table_tags_query(QN): RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] tags view")}
    )

    # Then the incomplete read fails
    _read_error(responses)


def test_an_external_delta_table_reads_as_present():
    # Given an external Delta table — read and reconciled like managed ones;
    # only creating them is unsupported
    doc = _describe_doc(type="EXTERNAL")
    responses = _describe_responses(describe=doc)

    # Then it reads as present
    assert isinstance(read_catalog_state(_router(responses), QN), TablePresent)


@pytest.mark.parametrize("kind", ["VIEW", "MATERIALIZED_VIEW", "FOREIGN", "FUTURE_KIND"])
def test_relation_kinds_the_engine_does_not_manage_read_as_failed(kind):
    # Given a relation kind the engine does not manage — materialized views
    # deliberately included, and kinds Databricks adds in the future
    doc = _describe_doc(type=kind)
    responses = _describe_responses(describe=doc)

    error = _read_error(responses)

    # Then the read fails rather than diffing it as though it were a table
    assert error.exception_type == "UnsupportedRelationError"


@pytest.mark.parametrize("provider", ["iceberg", "parquet", "csv"])
def test_non_delta_formats_read_as_failed(provider):
    # Given a table whose storage format is not Delta
    doc = _describe_doc(provider=provider)
    responses = _describe_responses(describe=doc)

    error = _read_error(responses)

    # Then the read refuses the relation
    assert error.exception_type == "UnsupportedRelationError"


@pytest.mark.parametrize("missing", ["type", "provider"])
def test_document_without_relation_kind_or_provider_reads_as_failed(missing):
    # Given a describe document missing the field that admits the relation
    doc = json.loads(_describe_doc())
    doc.pop(missing)
    responses = _describe_responses(describe=json.dumps(doc))

    error = _read_error(responses)

    # Then the relation cannot be admitted
    assert error.exception_type == "UnsupportedRelationError"


def test_rejection_names_the_found_relation_and_the_supported_kinds():
    # Given a relation kind the engine refuses
    doc = _describe_doc(type="MATERIALIZED_VIEW")
    responses = _describe_responses(describe=doc)

    error = _read_error(responses)
    # Then the message names the found kind and every supported relation type
    # and provider — derived from the admit mapping, so it cannot go stale
    assert "MATERIALIZED_VIEW" in str(error)
    for kind in ("EXTERNAL", "MANAGED", "STREAMING_TABLE"):
        assert kind in str(error)
    assert "delta" in str(error)


@pytest.mark.parametrize(
    ("type_document", "diagnostic"),
    (
        pytest.param({"name": "geography"}, "geography", id="unknown-type"),
        pytest.param(
            {"name": "string", "collation": "UTF8_LCASE"},
            "UTF8_LCASE",
            id="unsupported-string-collation",
        ),
    ),
)
def test_unmappable_column_type_reads_as_failed_not_present(
    type_document: dict[str, object], diagnostic: str
) -> None:
    # Given a column whose type the domain cannot model
    doc = _describe_doc(
        columns=[
            {"name": "id", "type": {"name": "int"}, "nullable": False},
            {"name": "region", "type": type_document, "nullable": True},
        ]
    )
    responses = _describe_responses(describe=doc)

    error = _read_error(responses)

    # Then the read fails naming the type, rather than a partial present
    # state that silently omits the column
    assert diagnostic in str(error)


def test_a_streaming_table_reads_as_present_with_its_kind():
    # Given DESCRIBE ... AS JSON reporting type=STREAMING_TABLE,
    # provider=delta (pinned live in
    # tests/live/test_sql_warehouse_live_streaming_tables.py)
    doc = _describe_doc(type="STREAMING_TABLE")
    responses = _describe_responses(describe=doc)

    state = read_catalog_state(_router(responses), QN)

    # Then the observed table carries the streaming kind
    assert isinstance(state, TablePresent)
    assert state.table.kind is TableKind.STREAMING_TABLE


def test_an_ordinary_table_reads_with_the_table_kind():
    # Then a managed table reads with the plain table kind
    state = read_catalog_state(_router(_describe_responses()), QN)

    assert isinstance(state, TablePresent)
    assert state.table.kind is TableKind.TABLE


def test_a_non_delta_streaming_table_reads_as_failed():
    # Given a streaming table whose provider is not Delta
    doc = _describe_doc(type="STREAMING_TABLE", provider="iceberg")
    responses = _describe_responses(describe=doc)

    error = _read_error(responses)

    # Then the relation is refused
    assert error.exception_type == "UnsupportedRelationError"


def test_supported_features_are_observed_and_kept_out_of_properties():
    # Given catalog properties containing managed, unmanaged, and ordinary properties
    document = _describe_doc(
        table_properties={
            "delta.feature.timestampNtz": "supported",
            "delta.feature.deletionVectors": "supported",
            "delta.enableChangeDataFeed": "true",
        }
    )
    responses = _describe_responses(describe=document)

    # When reading the table's catalog state
    state = read_catalog_state(_router(responses), QN)

    # Then only the managed feature is projected as protocol state
    assert isinstance(state, TablePresent)
    assert state.table.supported_features == frozenset({TableFeature.TIMESTAMP_NTZ})
    # And feature keys remain separate from user-managed table properties
    assert "delta.feature.timestampNtz" not in state.table.properties
    assert state.table.properties["delta.enableChangeDataFeed"] == "true"


def test_unmanaged_catalog_features_are_ignored():
    # Given a supported catalog feature the engine does not manage
    document = _describe_doc(table_properties={"delta.feature.deletionVectors": "supported"})
    responses = _describe_responses(describe=document)

    # When reading the table's catalog state
    state = read_catalog_state(_router(responses), QN)

    # Then the unknown feature does not enter observed engine state
    assert isinstance(state, TablePresent)
    assert state.table.supported_features == frozenset()


def test_feature_property_must_be_supported():
    # Given a managed feature property whose catalog value is not supported
    document = _describe_doc(table_properties={"delta.feature.timestampNtz": "unsupported"})
    responses = _describe_responses(describe=document)

    # When reading the table's catalog state
    state = read_catalog_state(_router(responses), QN)

    # Then the feature is not observed as supported
    assert isinstance(state, TablePresent)
    assert state.table.supported_features == frozenset()


def test_preview_catalog_name_maps_to_the_canonical_feature():
    # Given a catalog exposing VARIANT through its preview property spelling
    document = _describe_doc(table_properties={"delta.feature.variantType-preview": "supported"})
    responses = _describe_responses(describe=document)

    # When reading the table's catalog state
    state = read_catalog_state(_router(responses), QN)

    # Then observed state carries the canonical domain identity
    assert isinstance(state, TablePresent)
    assert state.table.supported_features == frozenset({TableFeature.VARIANT})
