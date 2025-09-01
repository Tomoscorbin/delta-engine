"""
End-to-end Engine tests using real domain models and simple stubs.

Simplified to focus on meaningful outcomes only (statuses/errors),
avoiding output text assertions or time monkeypatching.
"""

import pytest

from delta_engine.adapters.schema.delta.table import DeltaTable
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
from delta_engine.application.validation import PlanValidator
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.model.qualified_name import QualifiedName
from delta_engine.domain.plan.actions import ActionPlan
from tests.factories import (
    make_column,
    make_observed_default,
)

# ---------- Tiny adapter stubs ----------


class StubReader:
    """Configurable reader: map FQN -> ReadResult; default is 'absent'."""

    def __init__(self, mapping: dict[str, ReadResult] | None = None):
        self.mapping = mapping or {}

    def fetch_state(self, qualified_name: QualifiedName) -> ReadResult:
        return self.mapping.get(str(qualified_name), ReadResult.create_absent())


class StubExecutor:
    """Returns OK by default. If index in fail_at, returns FAILED at that action."""

    def __init__(self, *, fail_at: set[int] | None = None):
        self.fail_at = fail_at or set()

    def execute(self, plan: ActionPlan) -> tuple[ExecutionResult, ...]:
        results = []
        for idx, action in enumerate(plan):
            failed = idx in self.fail_at
            results.append(
                ExecutionResult(
                    action=type(action).__name__,
                    action_index=idx,
                    status=ActionStatus.FAILED if failed else ActionStatus.OK,
                    statement_preview=f"-- {type(action).__name__}",
                    failure=ExecutionFailure(idx, "RuntimeError", "boom") if failed else None,
                )
            )
        return tuple(results)


class StubValidator(PlanValidator):
    """Return provided failures verbatim (default none)."""

    def __init__(self, failures: tuple[ValidationFailure, ...] = ()):
        self._failures = failures

    def validate(self, ctx) -> tuple[ValidationFailure, ...]:
        return self._failures


# ---------- Tests ----------


def test_happy_path_absent_table_plan_valid_executes_ok():
    reg = Registry()
    reg.register(
        DeltaTable(
            "dev",
            "silver",
            "people",
            (
                make_column("id", Integer(), is_nullable=False),
                make_column("name", String(), is_nullable=True),
            ),
        )
    )

    eng = Engine(reader=StubReader(), executor=StubExecutor(), validator=StubValidator())

    # should not raise
    eng.sync(reg)


def test_validation_failure_causes_sync_failed_and_status():
    reg = Registry()
    reg.register(
        DeltaTable("dev", "silver", "t1", (make_column("name", Integer(), is_nullable=False),))
    )

    eng = Engine(
        reader=StubReader(),  # absent → CreateTable planned
        executor=StubExecutor(),
        validator=StubValidator(
            failures=(ValidationFailure("NonNullableColumnAdd", "policy violation"),)
        ),
    )

    with pytest.raises(SyncFailedError) as exc:
        eng.sync(reg)

    (tbl,) = exc.value.report.table_reports
    assert tbl.status is TableRunStatus.VALIDATION_FAILED


def test_read_failure_causes_sync_failed():
    reg = Registry()
    reg.register(
        DeltaTable("dev", "silver", "t1", (make_column("name", Integer(), is_nullable=False),))
    )

    reader = StubReader({"dev.silver.t1": ReadResult.create_failed(ReadFailure("IOError", "boom"))})
    eng = Engine(reader=reader, executor=StubExecutor(), validator=StubValidator())

    with pytest.raises(SyncFailedError) as exc:
        eng.sync(reg)

    (tbl,) = exc.value.report.table_reports
    assert tbl.status is TableRunStatus.READ_FAILED


def test_execution_failure_on_second_action():
    desired = DeltaTable(
        "dev",
        "silver",
        "t1",
        (
            make_column("id", Integer(), is_nullable=False),
            make_column("name", String(), is_nullable=True),
        ),
    )
    observed = make_observed_default(
        "t1",
        (
            make_column("id", Integer(), is_nullable=False),
            make_column("nickname", String(), is_nullable=True),
        ),
    )

    reg = Registry()
    reg.register(desired)

    eng = Engine(
        reader=StubReader({"dev.silver.t1": ReadResult.create_present(observed)}),
        executor=StubExecutor(fail_at={1}),  # second action fails
        validator=StubValidator(),
    )

    with pytest.raises(SyncFailedError) as exc:
        eng.sync(reg)

    (tbl,) = exc.value.report.table_reports
    assert tbl.status is TableRunStatus.EXECUTION_FAILED


def test_multi_table_partial_failure_statuses():
    reg = Registry()
    reg.register(
        DeltaTable("dev", "silver", "z", (make_column("id", Integer(), is_nullable=False),)),
        DeltaTable("dev", "silver", "a", (make_column("id", Integer(), is_nullable=False),)),
    )

    class ConditionalValidator(PlanValidator):
        def __init__(self):
            super().__init__(rules=())

        def validate(self, ctx) -> tuple[ValidationFailure, ...]:
            return (
                (ValidationFailure("RuleX", "bad"),)
                if str(ctx.desired.qualified_name).endswith(".a")
                else ()
            )

    eng = Engine(reader=StubReader(), executor=StubExecutor(), validator=ConditionalValidator())

    with pytest.raises(SyncFailedError) as exc:
        eng.sync(reg)

    report = exc.value.report
    assert report.any_failures is True
    # Only care that we have one failed and one success; order is unimportant here
    statuses_by_name = {t.fully_qualified_name: t.status for t in report.table_reports}
    assert set(statuses_by_name.keys()) == {"dev.silver.a", "dev.silver.z"}
    assert statuses_by_name["dev.silver.a"] is TableRunStatus.VALIDATION_FAILED
    assert statuses_by_name["dev.silver.z"] is TableRunStatus.SUCCESS


def test_non_nullable_add_on_present_table_trips_default_validator():
    desired = DeltaTable(
        "dev",
        "silver",
        "people",
        (
            make_column("id", Integer(), is_nullable=False),
            make_column("age", Integer(), is_nullable=False),
        ),
    )
    observed = make_observed_default("people", (make_column("id", Integer(), is_nullable=False),))

    reg = Registry()
    reg.register(desired)

    from delta_engine.application.validation import DEFAULT_VALIDATOR

    eng = Engine(
        reader=StubReader({"dev.silver.people": ReadResult.create_present(observed)}),
        executor=StubExecutor(),
        validator=DEFAULT_VALIDATOR,  # test the real rule
    )

    with pytest.raises(SyncFailedError) as exc:
        eng.sync(reg)

    (tbl,) = exc.value.report.table_reports
    assert tbl.status is TableRunStatus.VALIDATION_FAILED


def test_idempotent_noop_when_observed_equals_desired():
    desired = DeltaTable(
        "dev", "silver", "noop", (make_column("id", Integer(), is_nullable=False),)
    )
    observed = make_observed_default("noop", (make_column("id", Integer(), is_nullable=False),))

    reg = Registry()
    reg.register(desired)

    class RecordingExecutor(StubExecutor):
        def __init__(self):
            super().__init__()
            self.calls = 0
            self.seen_actions = []

        def execute(self, plan: ActionPlan):
            self.calls += 1
            self.seen_actions = list(plan)
            return tuple()

    exec_rec = RecordingExecutor()

    eng = Engine(
        reader=StubReader({"dev.silver.noop": ReadResult.create_present(observed)}),
        executor=exec_rec,
        validator=StubValidator(),
    )

    # should not raise; plan should only include property syncs (no column changes)
    eng.sync(reg)
    assert exec_rec.calls == 1
    assert all(type(a).__name__ == "SetProperty" for a in exec_rec.seen_actions)
