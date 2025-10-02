import pytest

from delta_engine.application.engine import Engine
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    ActionStatus,
    ExecutionFailure,
    ExecutionResult,
    ReadFailure,
    ReadResult,
    TableRunStatus,
    ValidationFailure,
)
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import ActionPlan

# --------- helpers/fakes


class _SpecColumn:
    def __init__(
        self, name: str, dt: str = "string", nullable: bool = True, comment: str | None = None
    ):
        self.name = name
        self.data_type = dt
        self.is_nullable = nullable
        self.comment = comment


class _SpecTable:
    def __init__(self, fqn: str):
        self.catalog, self.schema, self.name = fqn.split(".")
        self.columns = (_SpecColumn("id", "string", True, None),)
        self.comment = None
        self.properties: dict[str, str] = {}
        self.partitioned_by = ()  # or None

    @property
    def effective_properties(self) -> dict[str, str]:
        return self.properties


def _spec(fqn: str) -> _SpecTable:
    return _SpecTable(fqn)


class _FakeReader:
    def __init__(self, mapping: dict[str, ReadResult]) -> None:
        self.mapping = mapping

    def fetch_state(self, qualified_name: QualifiedName) -> ReadResult:
        return self.mapping.get(str(qualified_name), ReadResult.create_absent())


class _FakeValidator:
    def __init__(
        self, failures_by_fqn: dict[str, tuple[ValidationFailure, ...]] | None = None
    ) -> None:
        self.failures_by_fqn = failures_by_fqn or {}

    def validate(self, ctx) -> tuple[ValidationFailure, ...]:
        return self.failures_by_fqn.get(str(ctx.desired.qualified_name), ())


class _FakeExecutor:
    def __init__(self, results: tuple[ExecutionResult, ...]) -> None:
        self.results = results

    def execute(self, plan: ActionPlan) -> tuple[ExecutionResult, ...]:
        return self.results


def _ok_exec(idx: int = 0) -> ExecutionResult:
    return ExecutionResult(
        action="X", action_index=idx, status=ActionStatus.OK, statement_preview="-- ok"
    )


def _noop_exec(idx: int = 0) -> ExecutionResult:
    return ExecutionResult(
        action="X", action_index=idx, status=ActionStatus.NOOP, statement_preview="-- noop"
    )


def _failed_exec(idx: int = 0, exc="AnalysisException", msg="boom") -> ExecutionResult:
    return ExecutionResult(
        action="X",
        action_index=idx,
        status=ActionStatus.FAILED,
        statement_preview="-- bad sql",
        failure=ExecutionFailure(action_index=idx, exception_type=exc, message=msg),
    )


class _SeqExecutor:
    """Returns a different result tuple on each call."""

    def __init__(self, per_call_results: list[tuple[ExecutionResult, ...]]) -> None:
        self._seq = list(per_call_results)

    def execute(self, plan: ActionPlan) -> tuple[ExecutionResult, ...]:
        return self._seq.pop(0)


# ---------- Tests


def test_raises_when_any_table_has_read_failure():
    # Given a registry with one table and a reader that fails to read it
    t = _spec("c.s.read_fail")
    reg = Registry()
    reg.register(t)
    reader = _FakeReader(
        {"c.s.read_fail": ReadResult.create_failed(ReadFailure("IOError", "cannot read"))}
    )
    validator = _FakeValidator()  # no validation failures
    executor = _FakeExecutor(results=(_ok_exec(0),))  # would be fine if reached

    # When syncing
    # Then the engine raises SyncFailedError because read failed
    engine = Engine(reader=reader, executor=executor, validator=validator)
    with pytest.raises(SyncFailedError):
        engine.sync(reg)


def test_skips_execution_and_raises_when_validation_fails():
    # Given a table that reads successfully but fails validation
    reg = Registry()
    reg.register(_spec("c.s.val_fail"))
    reader = _FakeReader({"c.s.val_fail": ReadResult.create_absent()})
    validator = _FakeValidator({"c.s.val_fail": (ValidationFailure("RuleX", "nope"),)})
    # Executor would return OK, but must not be used because validation fails
    executor = _FakeExecutor(results=(_ok_exec(0),))

    # When syncing
    # Then the engine raises SyncFailedError because validation failed
    engine = Engine(reader=reader, executor=executor, validator=validator)
    with pytest.raises(SyncFailedError):
        engine.sync(reg)


def test_raises_when_execution_contains_any_failure():
    # Given a table that reads & validates successfully, but execution has a failed action
    reg = Registry()
    reg.register(_spec("c.s.exec_fail"))
    reader = _FakeReader({"c.s.exec_fail": ReadResult.create_absent()})
    validator = _FakeValidator()  # passes
    executor = _FakeExecutor(results=(_ok_exec(0), _failed_exec(1), _noop_exec(2)))

    # When syncing
    # Then the engine raises SyncFailedError because execution failed
    engine = Engine(reader=reader, executor=executor, validator=validator)
    with pytest.raises(SyncFailedError):
        engine.sync(reg)


def test_returns_success_when_all_tables_succeed():
    # Given two tables that read present/absent, validate cleanly, and execute with no failures
    reg = Registry()
    reg.register(
        _spec("c.a.users"), _spec("c.b.orders")
    )  # registry will yield in name-sorted order
    reader = _FakeReader(
        {
            "c.a.users": ReadResult.create_absent(),
            "c.b.orders": ReadResult.create_absent(),
        }
    )
    validator = _FakeValidator()
    executor = _FakeExecutor(results=(_ok_exec(0), _noop_exec(1)))
    engine = Engine(reader=reader, executor=executor, validator=validator)

    # When syncing
    # Then no exception is raised (i.e., overall success)
    engine.sync(reg)  # would raise on failure; reaching here means success


def test_engine_reads_all_tables_then_raises_on_any_read_failure():
    # Given two tables; first read fails, second reads OK (absent)
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": ReadResult.create_failed(ReadFailure("IOError", "cannot read")),
            "c.s.b": ReadResult.create_absent(),
        }
    )
    validator = _FakeValidator()  # both would pass if reached
    executor = _FakeExecutor(results=(_ok_exec(0),))  # irrelevant for a; used for b
    engine = Engine(reader=reader, executor=executor, validator=validator)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then both tables appear in the report; one READ_FAILED, one SUCCESS
    statuses = [tr.status for tr in err.value.report]
    assert statuses == [TableRunStatus.READ_FAILED, TableRunStatus.SUCCESS]


def test_engine_validates_all_tables_executes_only_the_passing_ones_then_raises():
    # Given both tables read OK; one fails validation
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": ReadResult.create_absent(),
            "c.s.b": ReadResult.create_absent(),
        }
    )
    validator = _FakeValidator(
        {
            "c.s.a": (ValidationFailure("RuleX", "nope"),),  # a fails validation
            # b passes
        }
    )
    executor = _FakeExecutor(results=(_ok_exec(0), _noop_exec(1)))  # used only for b
    engine = Engine(reader=reader, executor=executor, validator=validator)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then the report shows a VALIDATION_FAILED and a SUCCESS
    [tr_a, tr_b] = list(err.value.report)
    assert tr_a.status is TableRunStatus.VALIDATION_FAILED
    assert tr_a.execution_results == ()  # a was not executed
    assert tr_b.status is TableRunStatus.SUCCESS
    assert tr_b.execution_results != ()  # b was executed


def test_engine_executes_all_tables_then_raises_if_any_execution_failed():
    # Given both tables read & validate OK; execution fails only for the second
    reg = Registry()
    reg.register(_spec("c.s.a"), _spec("c.s.b"))
    reader = _FakeReader(
        {
            "c.s.a": ReadResult.create_absent(),
            "c.s.b": ReadResult.create_absent(),
        }
    )
    validator = _FakeValidator()  # both pass
    executor = _SeqExecutor(
        [
            (_ok_exec(0), _noop_exec(1)),  # execution for a: all good
            (_ok_exec(0), _failed_exec(1)),  # execution for b: one failed
        ]
    )
    engine = Engine(reader=reader, executor=executor, validator=validator)

    # When
    with pytest.raises(SyncFailedError) as err:
        engine.sync(reg)

    # Then both tables are in the report; first SUCCESS, second EXECUTION_FAILED
    statuses = [tr.status for tr in err.value.report]
    assert statuses == [TableRunStatus.SUCCESS, TableRunStatus.EXECUTION_FAILED]
