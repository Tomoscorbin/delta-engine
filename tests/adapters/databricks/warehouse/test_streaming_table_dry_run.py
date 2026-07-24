"""
End-to-end dry run against an observed streaming table.

Wires the real warehouse reader, engine, validation, and SQL compiler over a
canned DESCRIBE AS JSON document: a tags-scope declaration against a streaming
table plans ALTER STREAMING TABLE statements, and any wider scope fails
validation before SQL is planned.
"""

import json
from types import SimpleNamespace

from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_json_query,
    foreign_keys_query,
    primary_key_query,
    referencing_foreign_keys_query,
    table_tags_query,
)
from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner
from delta_engine.adapters.databricks.warehouse.executor import WarehouseExecutor
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.engine import Engine
from delta_engine.application.failures import ValidationFailure
from delta_engine.application.report import TableRunStatus
from delta_engine.domain.model import QualifiedName
from delta_engine.schema import Column, DeltaTable, Integer

QN = QualifiedName("cat", "sch", "clicks")

_STREAMING_DOC = json.dumps(
    {
        "table_name": "clicks",
        "catalog_name": "cat",
        "schema_name": "sch",
        "type": "STREAMING_TABLE",
        "provider": "delta",
        "columns": [{"name": "id", "type": {"name": "int"}, "nullable": True}],
        "comment": "",
        "table_properties": {},
    }
)


class RoutedCursor:
    def __init__(self, responses):
        self._responses = responses

    def execute(self, query):
        self._current = self._responses.get(query, [])

    def fetchall(self):
        return list(self._current)

    def close(self):
        pass


class RoutedConnection:
    def __init__(self, responses):
        self._responses = responses

    def cursor(self):
        return RoutedCursor(self._responses)


def _streaming_table_connection() -> RoutedConnection:
    return RoutedConnection(
        {
            describe_json_query(QN): [(_STREAMING_DOC,)],
            table_tags_query(QN): [SimpleNamespace(tag_name="stale", tag_value="remove-me")],
            column_tags_query(QN): [],
            primary_key_query(QN): [],
            foreign_keys_query(QN): [],
            referencing_foreign_keys_query(QN): [],
        }
    )


def _engine() -> Engine:
    connection = _streaming_table_connection()
    runner = WarehouseSqlRunner(connection)
    return Engine(
        reader=WarehouseReader(runner),
        executor=WarehouseExecutor(runner),
    )


def test_tags_scope_dry_run_plans_streaming_table_ddl():
    # Given a tags-scope declaration over a described streaming table with
    # one tag to set, one to unset, and one column tag to set
    declaration = DeltaTable(
        "cat",
        "sch",
        "clicks",
        columns=(Column("id", Integer(), tags={"pii": "low"}),),
        tags={"owner": "governance"},
        scope="tags",
    )

    # When dry-running a sync
    report = _engine().sync(declaration, dry_run=True)

    # Then the planned SQL carries the streaming-table dialect end to end
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.SUCCESS
    assert set(table_report.planned_sql_statements) == {
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` SET TAGS ('owner'='governance')",
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` UNSET TAGS ('stale')",
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` ALTER COLUMN `id` SET TAGS ('pii'='low')",
    }


def test_full_scope_dry_run_fails_validation_against_a_streaming_table():
    # Given a full-scope declaration whose shape exactly matches the streaming
    # table — zero drift, but the declaration claims the whole table
    declaration = DeltaTable(
        "cat",
        "sch",
        "clicks",
        columns=(Column("id", Integer()),),
        tags={"stale": "remove-me"},
    )

    # When dry-running a sync
    report = _engine().sync(declaration, dry_run=True)

    # Then validation rejects the declaration on kind alone; no SQL is planned
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    assert table_report.planned_sql_statements == ()
    rule_names = {
        failure.rule_name
        for failure in table_report.failures
        if isinstance(failure, ValidationFailure)
    }
    assert "StreamingTableTagsOnly" in rule_names
