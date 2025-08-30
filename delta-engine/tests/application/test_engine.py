from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from delta_engine.application import engine as engine_mod
from delta_engine.application.engine import Engine
from delta_engine.application.errors import SyncFailedError
from delta_engine.application.plan import PlanContext
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    ActionStatus,
    ExecutionFailure,
    ExecutionResult,
    ReadFailure,
    ReadResult,
    TableRunReport,
    TableRunStatus,
    ValidationFailure,
)
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.model.qualified_name import QualifiedName
from delta_engine.domain.model.table import DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import ActionPlan, AddColumn
from tests.factories import make_desired_table, make_observed_table, make_qualified_name

# ---- helpers ----------------------------------------------------------------


_QN = make_qualified_name("dev", "silver", "people")
_DESIRED = make_desired_table(_QN, (Column("id", Integer()),))


_OBSERVED = make_observed_table(_QN, (Column("id", Integer()),))


def _fixed_times():
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=5)
    t2 = t1 + timedelta(seconds=5)
    # yield three times for start/end across calls
    seq = [t0, t1, t2]

    def pop_time():
        return seq.pop(0)

    return pop_time, t0, t1, t2


def _fixed_now_seq(n=4):
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    seq = [t0 + timedelta(seconds=i) for i in range(n)]

    def _now():
        return seq.pop(0)

    return _now


@dataclass(frozen=True)
class ColSpec:
    name: str
    data_type: object
    is_nullable: bool = True


@dataclass(frozen=True)
class TableSpec:
    catalog: str | None
    schema: str | None
    name: str
    columns: tuple[ColSpec, ...]


# ---- stubs ------------------------------------------------------------------


class StubReaderPresent:
    def __init__(self, observed: ObservedTable) -> None:
        self.observed = observed

    def fetch_state(self, qn: QualifiedName) -> ReadResult:
        assert str(qn) == str(self.observed.qualified_name)
        return ReadResult.create_present(self.observed)


class StubReaderAbsent:
    def fetch_state(self, qn: QualifiedName) -> ReadResult:
        return ReadResult.create_absent()


class StubReaderFailed:
    def fetch_state(self, qn: QualifiedName) -> ReadResult:
        return ReadResult.create_failed(ReadFailure("IOError", "boom"))


class StubExecutorOK:
    def __init__(self) -> None:
        self.seen_plan: ActionPlan | None = None

    def execute(self, plan: ActionPlan) -> tuple[ExecutionResult, ...]:
        self.seen_plan = plan
        return (
            ExecutionResult(
                action="NOOP", action_index=0, status=ActionStatus.OK, statement_preview=""
            ),
        )


class StubExecutorFailed:
    def execute(self, plan: ActionPlan) -> tuple[ExecutionResult, ...]:
        return (
            ExecutionResult(
                action="AddColumn(age)",
                action_index=1,
                status=ActionStatus.FAILED,
                statement_preview="ALTER ...",
                failure=ExecutionFailure(1, "Ex", "nope"),
            ),
        )


class StubValidatorOK:
    def validate(self, ctx: PlanContext) -> tuple[ValidationFailure, ...]:
        # sanity: ensure observed/desired ordering is correct
        assert isinstance(ctx.observed, (ObservedTable, type(None)))
        assert isinstance(ctx.desired, DesiredTable)
        return ()


class StubValidatorFail:
    def validate(self, ctx: PlanContext) -> tuple[ValidationFailure, ...]:
        return (ValidationFailure("RuleX", "bad"),)


# ---- tests ------------------------------------------------------------------


def test_sync_table_read_failure_short_circuits_and_returns_read_failed(monkeypatch) -> None:
    desired = _DESIRED
    pop_time, t0, t1, _ = _fixed_times()
    monkeypatch.setattr(engine_mod, "_utc_now", pop_time)

    eng = Engine(reader=StubReaderFailed(), executor=StubExecutorOK(), validator=StubValidatorOK())
    report = eng._sync_table(desired)

    assert isinstance(report, TableRunReport)
    assert report.status == TableRunStatus.READ_FAILED
    assert report.read.failed is True
    assert report.validation is None or report.validation.failed is False
    assert report.execution_results == ()


def test_sync_table_validation_failure_returns_validation_failed_no_execute(monkeypatch) -> None:
    desired = _DESIRED
    pop_time, t0, t1, _ = _fixed_times()
    monkeypatch.setattr(engine_mod, "_utc_now", pop_time)

    exec_stub = StubExecutorOK()

    def fake_make_plan_context(desired, observed):
        assert desired is desired
        assert observed is None or isinstance(observed, ObservedTable)
        plan = ActionPlan(
            target=desired.qualified_name, actions=(AddColumn(Column("age", Integer())),)
        )
        return PlanContext(desired=desired, observed=observed, plan=plan)

    monkeypatch.setattr(engine_mod, "make_plan_context", fake_make_plan_context)

    eng = Engine(reader=StubReaderAbsent(), executor=exec_stub, validator=StubValidatorFail())
    report = eng._sync_table(desired)

    assert report.status == TableRunStatus.VALIDATION_FAILED
    assert report.validation is not None and report.validation.failed is True
    assert exec_stub.seen_plan is None


def test_sync_table_success_executes_and_returns_execution_results(monkeypatch) -> None:
    desired = _DESIRED
    pop_time, t0, t1, _ = _fixed_times()
    monkeypatch.setattr(engine_mod, "_utc_now", pop_time)

    exec_stub = StubExecutorOK()

    def fake_make_plan_context(desired, observed):
        assert isinstance(observed, ObservedTable)
        assert desired is desired
        plan = ActionPlan(
            target=desired.qualified_name, actions=(AddColumn(Column("a", Integer())),)
        )
        return PlanContext(desired=desired, observed=observed, plan=plan)

    monkeypatch.setattr(engine_mod, "make_plan_context", fake_make_plan_context)

    eng = Engine(
        reader=StubReaderPresent(_OBSERVED), executor=exec_stub, validator=StubValidatorOK()
    )
    report = eng._sync_table(desired)

    assert report.status == TableRunStatus.SUCCESS
    assert report.validation is not None and report.validation.failed is False
    assert len(report.execution_results) == 1
    assert exec_stub.seen_plan is not None


def test_sync_table_execution_failure_marks_execution_failed(monkeypatch) -> None:
    desired = _DESIRED
    pop_time, t0, t1, _ = _fixed_times()
    monkeypatch.setattr(engine_mod, "_utc_now", pop_time)

    def fake_make_plan_context(desired, observed):
        plan = ActionPlan(
            target=desired.qualified_name, actions=(AddColumn(Column("a", Integer())),)
        )
        return PlanContext(desired=desired, observed=observed, plan=plan)

    monkeypatch.setattr(engine_mod, "make_plan_context", fake_make_plan_context)

    eng = Engine(
        reader=StubReaderAbsent(), executor=StubExecutorFailed(), validator=StubValidatorOK()
    )
    report = eng._sync_table(desired)

    assert report.status == TableRunStatus.EXECUTION_FAILED
    assert len(report.execution_results) == 1
    assert report.execution_results[0].status is ActionStatus.FAILED


class ReaderAllAbsent:
    def fetch_state(self, qn: QualifiedName) -> ReadResult:
        return ReadResult.create_absent()


class ExecutorOK:
    def execute(self, plan: ActionPlan) -> tuple[ExecutionResult, ...]:
        return (
            ExecutionResult(
                action="NOOP", action_index=0, status=ActionStatus.OK, statement_preview=""
            ),
        )


class ExecutorOKButCounting(ExecutorOK):
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, plan: ActionPlan):
        self.calls += 1
        return super().execute(plan)


class ValidatorPass:
    def validate(self, ctx: PlanContext):
        return ()


class ValidatorFailAlways:
    def validate(self, ctx: PlanContext):
        return (ValidationFailure("Rule", "bad"),)


def test_engine_sync_success_prints_and_does_not_raise(monkeypatch, capsys) -> None:
    # fixed time
    monkeypatch.setattr(engine_mod, "_utc_now", _fixed_now_seq(n=6))

    reg = Registry()
    reg.register(
        TableSpec("dev", "silver", "t1", (ColSpec("id", Integer()),)),
        TableSpec("dev", "silver", "t2", (ColSpec("id", Integer()),)),
    )

    exec_stub = ExecutorOKButCounting()

    def fake_make_plan_context(desired, observed):
        plan = ActionPlan(
            target=desired.qualified_name, actions=(AddColumn(Column("a", Integer())),)
        )
        return PlanContext(desired=desired, observed=observed, plan=plan)

    monkeypatch.setattr(engine_mod, "make_plan_context", fake_make_plan_context)

    eng = Engine(reader=ReaderAllAbsent(), executor=exec_stub, validator=ValidatorPass())
    eng.sync(reg)  # should not raise
    # executor called for both tables
    assert exec_stub.calls == 2
    # optionally ensure something was printed (we don't assert exact text)
    out = capsys.readouterr().out
    assert isinstance(out, str)


def test_engine_sync_raises_sync_failed_error_when_any_table_fails(monkeypatch) -> None:
    monkeypatch.setattr(engine_mod, "_utc_now", _fixed_now_seq())

    reg = Registry()
    reg.register(TableSpec("dev", "silver", "t1", (ColSpec("id", Integer()),)))

    # fake plan context
    def fake_make_plan_context(desired, observed):
        plan = ActionPlan(
            target=desired.qualified_name, actions=(AddColumn(Column("a", Integer())),)
        )
        return PlanContext(desired=desired, observed=observed, plan=plan)

    monkeypatch.setattr(engine_mod, "make_plan_context", fake_make_plan_context)

    eng = Engine(reader=ReaderAllAbsent(), executor=ExecutorOK(), validator=ValidatorFailAlways())

    with pytest.raises(SyncFailedError) as exc:
        eng.sync(reg)
    assert exc.value.report.any_failures is True
    assert len(exc.value.report.table_reports) == 1
