"""
The observed relation kind reaches the compiler and the safety rules.

A streaming table is discovered, never declared: the reader derives the kind
from DESCRIBE ... AS JSON, and every downstream decision depends on the engine
carrying that kind through. The statement text belongs to
tests/adapters/databricks/sql/test_compile.py and the refusal rule to
tests/application/test_validation.py, both of which take the kind as given.
What is pinned here is only that the kind the reader observed is the kind they
are handed.
"""

from types import SimpleNamespace

from delta_engine.adapters.databricks.sql import table_tags_query
from delta_engine.adapters.databricks.warehouse._runner import WarehouseSqlRunner
from delta_engine.adapters.databricks.warehouse.executor import WarehouseExecutor
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.engine import Engine
from delta_engine.application.failures import ValidationFailure
from delta_engine.application.report import TableRunStatus
from delta_engine.domain.model import QualifiedName
from delta_engine.schema import Column, DeltaTable, Integer
from tests.adapters.databricks.fakes import (
    RoutedConnection,
    build_catalog_responses,
    build_describe_document,
)

QN = QualifiedName("cat", "sch", "clicks")

# The comments are stale on purpose: a declaration that differs from them plans
# the statement under test, and one that mirrors them plans nothing at all.
_STREAMING_DOC = build_describe_document(
    QN,
    type="STREAMING_TABLE",
    columns=[{"name": "id", "type": {"name": "int"}, "nullable": True, "comment": "stale id"}],
    comment="stale table comment",
)


def _streaming_table_connection() -> RoutedConnection:
    return RoutedConnection(
        build_catalog_responses(
            QN,
            describe=_STREAMING_DOC,
            **{table_tags_query(QN): [SimpleNamespace(tag_name="stale", tag_value="remove-me")]},
        )
    )


def _engine() -> Engine:
    connection = _streaming_table_connection()
    runner = WarehouseSqlRunner(connection)
    return Engine(
        reader=WarehouseReader(runner),
        executor=WarehouseExecutor(runner),
    )


def test_an_observed_streaming_table_compiles_in_the_streaming_dialect():
    # Given an annotations-scope declaration whose column comment differs from
    # the observed streaming table's
    declaration = DeltaTable(
        "cat",
        "sch",
        "clicks",
        columns=(Column("id", Integer(), comment="the id"),),
        comment="stale table comment",
        tags={"stale": "remove-me"},
        scope="annotations",
    )

    # When dry-running a sync
    report = _engine().sync(declaration, dry_run=True)

    # Then the column work carries the streaming prefix and nothing falls back
    # to the plain one. Nothing declares the kind, so this is the only check
    # that the compiler was handed the kind the reader observed
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.SUCCESS
    assert table_report.compiled is not None
    planned = table_report.compiled.statements
    assert (
        "ALTER STREAMING TABLE `cat`.`sch`.`clicks` ALTER COLUMN `id` COMMENT 'the id'" in planned
    )
    assert not any(statement.startswith("ALTER TABLE ") for statement in planned)


def test_an_observed_streaming_table_refuses_a_wider_scope_before_planning():
    # Given a full-scope declaration whose shape exactly matches the observed
    # streaming table — zero drift, but the declaration claims the whole table
    declaration = DeltaTable(
        "cat",
        "sch",
        "clicks",
        columns=(Column("id", Integer(), comment="stale id"),),
        comment="stale table comment",
        tags={"stale": "remove-me"},
    )

    # When dry-running a sync
    report = _engine().sync(declaration, dry_run=True)

    # Then the observed kind reaches validation, which refuses the declaration
    # on kind alone and leaves nothing to execute
    [table_report] = list(report)
    assert table_report.status is TableRunStatus.PLANNING_FAILED
    assert table_report.compiled is None
    rule_names = {
        failure.rule_name
        for failure in table_report.failures
        if isinstance(failure, ValidationFailure)
    }
    assert "StreamingTableAnnotationsOnly" in rule_names
