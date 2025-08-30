from datetime import UTC, datetime, timedelta

import pytest

import delta_engine.application.engine as engine_mod
from delta_engine.application.engine import Engine as EngineMod
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
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.model.qualified_name import QualifiedName
from delta_engine.domain.model.table import DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import ActionPlan


def _fixed_now_seq(n: int):
    """Return a function that yields n increasing datetimes, then repeats last."""
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    seq = [t0 + timedelta(seconds=i) for i in range(n)]
    last = seq[-1]

    def _now():
        return seq.pop(0) if seq else last

    return _now


# ---------- Tiny adapter stubs ----------


class StubReader:
    """Configurable reader: map FQN->ReadResult; default absent."""

    def __init__(self, mapping: dict[str, ReadResult] | None = None):
        self.mapping = mapping or {}

    def fetch_state(self, qualified_name: QualifiedName) -> ReadResult:
        return self.mapping.get(str(qualified_name), ReadResult.create_absent())


class StubExecutor:
    """Returns OK by default, optional fail_at index -> FAILED result there."""

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


# ---------- Helpers to build tables ----------


def _qn(name: str) -> QualifiedName:
    return QualifiedName("dev", "silver", name)


def _desired(name: str, cols: tuple[Column, ...]) -> DesiredTable:
    return DesiredTable(qualified_name=_qn(name), columns=cols)


def _observed(name: str, cols: tuple[Column, ...]) -> ObservedTable:
    return ObservedTable(qualified_name=_qn(name), columns=cols)


# ---------- Tests ----------


def test_happy_path_absent_table_plan_valid_executes_ok(monkeypatch, capsys):
    # One table -> run start + tbl start + tbl end + run end = 4 timestamps
    monkeypatch.setattr(engine_mod, "_utc_now", _fixed_now_seq(n=4))

    reg = Registry()
    reg.register(
        # desired: create id(INT), name(STRING)
        # observed: absent
        type(
            "Spec",
            (),
            {
                "catalog": "dev",
                "schema": "silver",
                "name": "people",
                "columns": (
                    type("C", (), {"name": "id", "data_type": Integer(), "is_nullable": False})(),
                    type("C", (), {"name": "name", "data_type": String(), "is_nullable": True})(),
                ),
            },
        )()
    )

    reader = StubReader()  # absent by default
    executor = StubExecutor()
    validator = StubValidator()

    eng = EngineMod(reader=reader, executor=executor, validator=validator)

    # Should not raise; prints report
    eng.sync(reg)
    out = capsys.readouterr().out
    assert "dev.silver.people" in out
    assert "SUCCESS" in out


def test_validation_failure_causes_sync_failed_and_status(monkeypatch):
    monkeypatch.setattr(engine_mod, "_utc_now", _fixed_now_seq(n=4))

    reg = Registry()
    reg.register(
        type(
            "Spec",
            (),
            {
                "catalog": "dev",
                "schema": "silver",
                "name": "t1",
                "columns": (
                    type("C", (), {"name": "id", "data_type": Integer(), "is_nullable": False})(),
                ),
            },
        )()
    )

    reader = StubReader()  # absent → CreateTable planned
    validator = StubValidator(  # force a validation failure
        failures=(ValidationFailure("NonNullableColumnAdd", "policy violation"),)
    )
    executor = StubExecutor()

    eng = EngineMod(reader=reader, executor=executor, validator=validator)

    with pytest.raises(SyncFailedError) as exc:
        eng.sync(reg)

    report = exc.value.report
    assert report.any_failures is True
    (tbl,) = report.table_reports
    assert tbl.status is TableRunStatus.VALIDATION_FAILED


def test_read_failure_causes_sync_failed(monkeypatch):
    monkeypatch.setattr(engine_mod, "_utc_now", _fixed_now_seq(n=4))

    reg = Registry()
    reg.register(
        type(
            "Spec",
            (),
            {
                "catalog": "dev",
                "schema": "silver",
                "name": "t1",
                "columns": (
                    type("C", (), {"name": "id", "data_type": Integer(), "is_nullable": False})(),
                ),
            },
        )()
    )

    reader = StubReader({"dev.silver.t1": ReadResult.create_failed(ReadFailure("IOError", "boom"))})
    validator = StubValidator()
    executor = StubExecutor()

    eng = EngineMod(reader=reader, executor=executor, validator=validator)

    with pytest.raises(SyncFailedError) as exc:
        eng.sync(reg)

    report = exc.value.report
    (tbl,) = report.table_reports
    assert tbl.status is TableRunStatus.READ_FAILED


def test_execution_failure_on_second_action(monkeypatch):
    monkeypatch.setattr(engine_mod, "_utc_now", _fixed_now_seq(n=4))

    # observed has id,nickname ; desired has id,name  → add 'name', drop 'nickname' (2 actions)
    desired = _desired("t1", (Column("id", Integer(), False), Column("name", String(), True)))
    observed = _observed("t1", (Column("id", Integer(), False), Column("nickname", String(), True)))

    reg = Registry()
    reg.register(
        type(
            "Spec",
            (),
            {
                "catalog": "dev",
                "schema": "silver",
                "name": "t1",
                "columns": tuple(
                    type(
                        "C",
                        (),
                        {"name": c.name, "data_type": c.data_type, "is_nullable": c.is_nullable},
                    )()
                    for c in desired.columns
                ),
            },
        )()
    )

    reader = StubReader({"dev.silver.t1": ReadResult.create_present(observed)})
    validator = StubValidator()
    executor = StubExecutor(fail_at={1})  # second action fails

    eng = EngineMod(reader=reader, executor=executor, validator=validator)

    with pytest.raises(SyncFailedError) as exc:
        eng.sync(reg)

    report = exc.value.report
    (tbl,) = report.table_reports
    assert tbl.status is TableRunStatus.EXECUTION_FAILED


def test_multi_table_sorted_and_partial_failure(monkeypatch):
    # 2 tables → 6 timestamps
    monkeypatch.setattr(engine_mod, "_utc_now", _fixed_now_seq(n=6))

    # Register unsorted names; registry iterates in FQN order
    reg = Registry()
    reg.register(
        type(
            "Spec",
            (),
            {
                "catalog": "dev",
                "schema": "silver",
                "name": "z",
                "columns": (
                    type("C", (), {"name": "id", "data_type": Integer(), "is_nullable": False})(),
                ),
            },
        )(),
        type(
            "Spec",
            (),
            {
                "catalog": "dev",
                "schema": "silver",
                "name": "a",
                "columns": (
                    type("C", (), {"name": "id", "data_type": Integer(), "is_nullable": False})(),
                ),
            },
        )(),
    )

    # Make 'a' fail validation, 'z' succeed
    reader = StubReader()  # both absent
    validator = StubValidator(failures=(ValidationFailure("RuleX", "bad"),))

    # provide a reader that marks a specific table as present to force
    # different path → but we want validation to fail only for 'a'.
    # make validator that fails only when target FQN endswith '.a'
    class ConditionalValidator:
        def validate(self, ctx):
            return (
                (ValidationFailure("RuleX", "bad"),)
                if str(ctx.desired.qualified_name).endswith(".a")
                else ()
            )

    validator = ConditionalValidator()

    executor = StubExecutor()

    eng = EngineMod(reader=reader, executor=executor, validator=validator)

    with pytest.raises(SyncFailedError) as exc:
        eng.sync(reg)

    report = exc.value.report
    assert report.any_failures is True
    # Order: dev.silver.a then dev.silver.z
    names = [t.fully_qualified_name for t in report.table_reports]
    assert names == ["dev.silver.a", "dev.silver.z"]
    statuses = [t.status for t in report.table_reports]
    assert statuses == [TableRunStatus.VALIDATION_FAILED, TableRunStatus.SUCCESS]


def test_non_nullable_add_on_present_table_trips_default_validator(monkeypatch):
    monkeypatch.setattr(engine_mod, "_utc_now", _fixed_now_seq(n=4))

    # observed: id only; desired: id + age NOT NULL  → AddColumn(non-nullable)
    desired = _desired("people", (Column("id", Integer(), False), Column("age", Integer(), False)))
    observed = _observed("people", (Column("id", Integer(), False),))

    # registry for desired
    reg = Registry()
    reg.register(
        type(
            "Spec",
            (),
            {
                "catalog": "dev",
                "schema": "silver",
                "name": "people",
                "columns": tuple(
                    type(
                        "C",
                        (),
                        {"name": c.name, "data_type": c.data_type, "is_nullable": c.is_nullable},
                    )()
                    for c in desired.columns
                ),
            },
        )()
    )

    # present table
    reader = StubReader({"dev.silver.people": ReadResult.create_present(observed)})
    # use the real default validator to test the actual rule
    from delta_engine.application.validation import DEFAULT_VALIDATOR

    executor = StubExecutor()

    eng = EngineMod(reader=reader, executor=executor, validator=DEFAULT_VALIDATOR)

    with pytest.raises(SyncFailedError) as exc:
        eng.sync(reg)

    (tbl,) = exc.value.report.table_reports
    assert tbl.status is TableRunStatus.VALIDATION_FAILED


def test_idempotent_noop_when_observed_equals_desired(monkeypatch, capsys):
    monkeypatch.setattr(engine_mod, "_utc_now", _fixed_now_seq(n=4))
    observed = _observed("noop", (Column("id", Integer(), False),))

    reg = Registry()
    reg.register(
        type(
            "Spec",
            (),
            {
                "catalog": "dev",
                "schema": "silver",
                "name": "noop",
                "columns": (
                    type("C", (), {"name": "id", "data_type": Integer(), "is_nullable": False})(),
                ),
            },
        )()
    )

    # reader says present & identical
    reader = StubReader({"dev.silver.noop": ReadResult.create_present(observed)})

    # executor that records if it was called. should be called with empty plan and return ()
    class RecordingExecutor(StubExecutor):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def execute(self, plan: ActionPlan):
            self.calls += 1
            assert len(plan) == 0  # empty action plan
            return ()

    exec_rec = RecordingExecutor()

    validator = StubValidator()

    eng = EngineMod(reader=reader, executor=exec_rec, validator=validator)

    # Should not raise - prints report
    eng.sync(reg)
    out = capsys.readouterr().out
    assert "dev.silver.noop" in out
    assert "SUCCESS" in out
    assert "executed" not in out
    assert "failures" not in out
    assert exec_rec.calls == 1  # engine still invokes executor - plan is empty


def test_plan_action_ordering_adds_before_drops_and_subject_alpha(monkeypatch):
    monkeypatch.setattr(engine_mod, "_utc_now", _fixed_now_seq(n=4))

    # observed: id, y  ; desired: id, a, x  → adds: a,x  ; drops: y
    desired = _desired(
        "ord",
        (Column("id", Integer(), False), Column("a", String(), True), Column("x", String(), True)),
    )
    observed = _observed("ord", (Column("id", Integer(), False), Column("y", String(), True)))

    reg = Registry()
    reg.register(
        type(
            "Spec",
            (),
            {
                "catalog": "dev",
                "schema": "silver",
                "name": "ord",
                "columns": tuple(
                    type(
                        "C",
                        (),
                        {"name": c.name, "data_type": c.data_type, "is_nullable": c.is_nullable},
                    )()
                    for c in desired.columns
                ),
            },
        )()
    )

    reader = StubReader({"dev.silver.ord": ReadResult.create_present(observed)})
    validator = StubValidator()

    class RecordingExecutor(StubExecutor):
        def __init__(self):
            super().__init__()
            self.action_kinds = []
            self.subjects = []

        def execute(self, plan: ActionPlan):
            # record the actual ordered plan coming out of make_plan_context + ordering
            for act in plan:
                self.action_kinds.append(type(act).__name__)
                # subject_name logic: AddColumn.column.name, DropColumn.column_name
                if hasattr(act, "column"):
                    self.subjects.append(act.column.name)
                else:
                    self.subjects.append(getattr(act, "column_name", ""))
            return super().execute(plan)

    exec_rec = RecordingExecutor()
    eng = EngineMod(reader=reader, executor=exec_rec, validator=validator)

    # should not raise (no failures); we only care about internal order
    eng.sync(reg)

    # expect AddColumn(a), AddColumn(x), DropColumn(y)
    assert exec_rec.action_kinds == ["AddColumn", "AddColumn", "DropColumn"]
    assert exec_rec.subjects == ["a", "x", "y"]
