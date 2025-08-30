from collections.abc import Iterable
from dataclasses import dataclass

from delta_engine.application.ports import (
    CatalogStateReader,
    ColumnObject,
    PlanExecutor,
    TableObject,
)
from delta_engine.application.results import (
    ActionStatus,
    ExecutionResult,
    ReadResult,
)
from delta_engine.domain.model.qualified_name import QualifiedName
from delta_engine.domain.plan.actions import ActionPlan

# ----------------------------
# CatalogStateReader Protocol
# ----------------------------


class GoodReader:
    # Correct method name; returns a ReadResult
    def fetch_state(self, qualified_name: QualifiedName) -> ReadResult:
        # pretend we couldn't find the table
        return ReadResult.create_absent()


class BadReaderMissingMethod:
    # No fetch_state at all
    pass


class BadReaderWrongName:
    # Has a method, but wrong name
    def get_state(self, qualified_name: QualifiedName) -> ReadResult:
        return ReadResult.create_absent()


def test_catalog_state_reader_runtime_conformance_passes_with_required_method() -> None:
    r = GoodReader()
    assert isinstance(r, CatalogStateReader)  # runtime structural check


def test_catalog_state_reader_runtime_conformance_fails_when_method_missing() -> None:
    r = BadReaderMissingMethod()
    assert not isinstance(r, CatalogStateReader)

    r2 = BadReaderWrongName()
    assert not isinstance(r2, CatalogStateReader)


# ----------------------------
# PlanExecutor Protocol
# ----------------------------


class GoodExecutor:
    # Current interface says: execute(plan) -> ExecutionResult (single)
    def execute(self, plan: ActionPlan) -> ExecutionResult:
        # trivial “no-op success” result
        return ExecutionResult(
            action="NOOP",
            action_index=0,
            status=ActionStatus.NOOP,
            statement_preview="",
        )


class BadExecutorMissingMethod:
    pass


def test_plan_executor_runtime_conformance_passes_with_execute() -> None:
    e = GoodExecutor()
    assert isinstance(e, PlanExecutor)


def test_plan_executor_runtime_conformance_fails_without_execute() -> None:
    e = BadExecutorMissingMethod()
    assert not isinstance(e, PlanExecutor)


# ----------------------------
# TableObject / ColumnObject Protocols
# ----------------------------


@dataclass
class ColSpec:
    name: str
    data_type: object
    is_nullable: bool = True


@dataclass
class TableSpec:
    catalog: str | None
    schema: str | None
    name: str
    columns: Iterable[ColSpec]


def test_columnobject_and_tableobject_runtime_conformance() -> None:
    col = ColSpec(name="id", data_type=int, is_nullable=False)
    tbl = TableSpec(catalog="dev", schema="silver", name="people", columns=(col,))
    assert isinstance(col, ColumnObject)
    assert isinstance(tbl, TableObject)


def test_tableobject_runtime_conformance_fails_when_member_missing() -> None:
    @dataclass
    class NoColumnsTable:
        catalog: str | None
        schema: str | None
        name: str
        # columns missing

    t = NoColumnsTable(catalog="dev", schema="silver", name="people")
    assert not isinstance(t, TableObject)
