import pytest

from delta_engine.api import Column, DeltaTable, String
from delta_engine.application.engine import Engine
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    CatalogState,
    ExecutionFailed,
    ExecutionFailure,
    ExecutionResult,
    ExecutionSucceeded,
    ExecutionSummary,
    ReadFailed,
    ReadFailure,
    SyncReport,
    TableAbsent,
    TablePresent,
    TableRunStatus,
)
from delta_engine.domain.model import ObservedTable, QualifiedName
from delta_engine.domain.plan import ActionPlan

# --------- helpers/fakes


def _spec(fqn: str) -> DeltaTable:
    """Build a minimal real table definition from a 'catalog.schema.name' string."""
    catalog, schema, name = fqn.split(".")
    return DeltaTable(catalog, schema, name, columns=(Column("id", String()),))


def _spec_adding_not_null(fqn: str) -> DeltaTable:
    """
    Build a spec that adds a NOT NULL column on top of the baseline 'id' column.

    Diffed against an existing 'id'-only table, this trips the real
    NonNullableColumnAdd rule, so the engine drives a genuine validation failure
    instead of a faked one.
    """
    catalog, schema, name = fqn.split(".")
    return DeltaTable(
        catalog,
        schema,
        name,
        columns=(Column("id", String()), Column("order_id", String(), nullable=False)),
    )


def _existing_id_table(fqn: str) -> TablePresent:
    """Build the present-state read of an existing table with a single 'id' column."""
    catalog, schema, name = fqn.split(".")
    return TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName(catalog, schema, name),
            columns=(Column("id", String()),),
        )
    )


class _FakeReader:
    def __init__(self, mapping: dict[str, CatalogState]) -> None:
        self.mapping = mapping

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        return self.mapping.get(str(qualified_name), TableAbsent())


class _FakeExecutor:
    def __init__(self, results: tuple[ExecutionResult, ...]) -> None:
        self.results = results

    def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
        return ExecutionSummary(self.results)


def _ok_exec(idx: int = 0) -> ExecutionResult:
    return ExecutionSucceeded(action="X", action_index=idx, statement_preview="-- ok")


def _failed_exec(idx: int = 0, exc="AnalysisException", msg="boom") -> ExecutionResult:
    return ExecutionFailed(
        action="X",
        action_index=idx,
        failure=ExecutionFailure(
            action_index=idx, exception_type=exc, message=msg, statement_preview="-- bad sql"
        ),
    )


class _SeqExecutor:
    """Returns a different result tuple on each call."""

    def __init__(self, per_call_results: list[tuple[ExecutionResult, ...]]) -> None:
        self._seq = list(per_call_results)

    def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
        return ExecutionSummary(self._seq.pop(0))


# ---------- Tests


def test_raises_when_any_table_has_read_failure():
    # Given a registry with one table and a reader that fails to read it
    t = _spec("c.s.read_fail")
    reg = Registry()
    reg.register(t)
    reader = _FakeReader({"c.s.read_fail": ReadFailed(ReadFailure("IOError", "cannot read"))})
    executor = _FakeExecutor(results=(_ok_exec(0),))  # would be fine if reached

    # When syncing
    # Then the engine raises SyncFailedError because read failed
    engine = Engine(reader=reader, executor=executor)
    with pytest.raises(SyncFailedError):
        engine.sync(reg)


def test_skips_execution_and_raises_when_validation_fails():
    # Given an existing table whose desired spec adds a NOT NULL column
    # (a real rule violation, not a faked verdict)
    reg = Registry()
    reg.register(_spec_adding_not_null("c.s.val_fail"))
    reader = _FakeReader({"c.s.val_fail": _existing_id_table("c.s.val_fail")})
    # Executor would return OK, but must not be used because validation fails
    executor = _FakeExecutor(results=(_ok_exec(0),))

    # When syncing
    # Then the engine raises SyncFailedError because validation failed
    engine = Engine(reader=reader, executor=executor)
    with pytest.raises(SyncFailedError):
        engine.sync(reg)


def test_raises_when_execution_contains_any_failure():
    # Given a table that reads & validates successfully, but execution has a failed action
    reg = Registry()
    reg.register(_spec("c.s.exec_fail"))
    reader = _FakeReader({"c.s.exec_fail": TableAbsent()})
    executor = _FakeExecutor(results=(_ok_exec(0), _failed_exec(1), _ok_exec(2)))

    # When syncing
    # Then the engine raises SyncFailedError because execution failed
    engine = Engine(reader=reader, executor=executor)
    with pytest.raises(SyncFailedError):
        engine.sync(reg)


def test_returns_report_when_all_tables_succeed():
    # Given two tables that read present/absent, validate cleanly, and execute with no failures
    reg = Registry()
    reg.register(
        _spec("c.a.users"), _spec("c.b.orders")
    )  # registry will yield in name-sorted order
    reader = _FakeReader(
        {
            "c.a.users": TableAbsent(),
            "c.b.orders": TableAbsent(),
        }
    )
    executor = _FakeExecutor(results=(_ok_exec(0), _ok_exec(1)))
    engine = Engine(reader=reader, executor=executor)

    # When syncing
    report = engine.sync(reg)

    # Then the successful run is returned as a SyncReport for programmatic use
    assert isinstance(report, SyncReport)
    assert report.any_failures is False
    assert [tr.status for tr in report] == [
        TableRunStatus.SUCCESS,
        TableRunStatus.SUCCESS,
    ]


def test_engine_reads_all_tables_then_raises_on_any_read_failure():
    # Given two tables; first read fails, second reads OK (absent)
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": ReadFailed(ReadFailure("IOError", "cannot read")),
            "c.s.b": TableAbsent(),
        }
    )
    executor = _FakeExecutor(results=(_ok_exec(0),))  # irrelevant for a; used for b
    engine = Engine(reader=reader, executor=executor)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then both tables appear in the report; one READ_FAILED, one SUCCESS
    [tr_a, tr_b] = list(err.value.report)
    assert tr_a.status is TableRunStatus.READ_FAILED
    assert tr_b.status is TableRunStatus.SUCCESS
    # And the surviving table was actually executed, not just reported as success
    # with an empty (never-run) execution summary
    assert tr_b.execution.results != ()


def test_engine_validates_all_tables_executes_only_the_passing_ones_then_raises():
    # Given two tables that read OK; 'a' adds a NOT NULL column to an existing
    # table (a real validation failure) while 'b' is a clean creation
    reg = Registry()
    reg.register(_spec_adding_not_null("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": _existing_id_table("c.s.a"),  # existing -> add NOT NULL is rejected
            "c.s.b": TableAbsent(),  # absent -> clean create
        }
    )
    executor = _FakeExecutor(results=(_ok_exec(0), _ok_exec(1)))  # used only for b
    engine = Engine(reader=reader, executor=executor)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then the report shows a VALIDATION_FAILED and a SUCCESS
    [tr_a, tr_b] = list(err.value.report)
    assert tr_a.status is TableRunStatus.VALIDATION_FAILED
    assert tr_a.execution.results == ()  # a was not executed
    assert tr_b.status is TableRunStatus.SUCCESS
    assert tr_b.execution.results != ()  # b was executed


def test_engine_executes_all_tables_then_raises_if_any_execution_failed():
    # Given both tables read & validate OK; execution fails only for the second
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": TableAbsent(),
            "c.s.b": TableAbsent(),
        }
    )
    executor = _SeqExecutor(
        [
            (_ok_exec(0), _ok_exec(1)),  # execution for a: all good
            (_ok_exec(0), _failed_exec(1)),  # execution for b: one failed
        ]
    )
    engine = Engine(reader=reader, executor=executor)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then both tables are in the report; first SUCCESS, second EXECUTION_FAILED
    statuses = [tr.status for tr in err.value.report]
    assert statuses == [TableRunStatus.SUCCESS, TableRunStatus.EXECUTION_FAILED]


def test_engine_executes_remaining_tables_even_if_first_execution_fails():
    # Given both tables read & validate OK; execution fails for the FIRST table
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": TableAbsent(),
            "c.s.b": TableAbsent(),
        }
    )
    executor = _SeqExecutor(
        [
            (_failed_exec(0),),  # execution for 'a' fails
            (_ok_exec(0), _ok_exec(1)),  # execution for 'b' succeeds
        ]
    )
    engine = Engine(reader=reader, executor=executor)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then both tables appear; first is EXECUTION_FAILED, second is SUCCESS (i.e. it still executed)
    [tr_a, tr_b] = list(err.value.report)
    assert tr_a.status is TableRunStatus.EXECUTION_FAILED
    assert tr_b.status is TableRunStatus.SUCCESS
    assert tr_b.execution.results != ()  # proves 'b' actually executed


def test_syncing_an_empty_registry_returns_an_empty_report_without_raising():
    # Given a registry with no tables registered (e.g. nothing matched a filter)
    reg = Registry()
    engine = Engine(reader=_FakeReader({}), executor=_FakeExecutor(results=()))

    # When syncing
    report = engine.sync(reg)

    # Then an empty, non-failing report comes back -- no SyncFailedError
    assert isinstance(report, SyncReport)
    assert report.any_failures is False
    assert tuple(report) == ()


def test_read_phase_attempts_all_tables_before_any_execution():
    # Given three tables; the middle one fails to read
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"), _spec("c.s.c"))
    read_order: list[str] = []

    class _TrackingReader:
        def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
            read_order.append(str(qualified_name))
            if str(qualified_name) == "c.s.b":
                return ReadFailed(ReadFailure("IOError", "boom"))
            return TableAbsent()

    executor = _FakeExecutor(results=(_ok_exec(0), _ok_exec(0)))
    engine = Engine(_TrackingReader(), executor)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then all three reads happened before any execution
    assert read_order == ["c.s.a", "c.s.b", "c.s.c"]
    statuses = [tr.status for tr in err.value.report]
    assert statuses == [
        TableRunStatus.SUCCESS,
        TableRunStatus.READ_FAILED,
        TableRunStatus.SUCCESS,
    ]


def test_validate_phase_validates_all_tables_before_any_execution():
    # Given two tables; 'a' fails validation, 'b' is clean
    reg = Registry()
    reg.register(_spec_adding_not_null("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": _existing_id_table("c.s.a"),
            "c.s.b": TableAbsent(),
        }
    )
    execute_order: list[str] = []

    class _TrackingExecutor:
        def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
            execute_order.append(str(qualified_name))
            return ExecutionSummary((_ok_exec(0),))

    engine = Engine(reader, _TrackingExecutor())

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then only 'b' was executed (validation failed for 'a')
    # and execution only started after both tables were validated
    assert execute_order == ["c.s.b"]
    [tr_a, tr_b] = list(err.value.report)
    assert tr_a.status is TableRunStatus.VALIDATION_FAILED
    assert tr_b.status is TableRunStatus.SUCCESS
