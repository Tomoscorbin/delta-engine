import pytest

import tabula.application.engine as engine_mod
from tabula.application.engine import Engine
from tabula.application.errors import ExecutionFailedError, ValidationFailedError

# ----------------------------
# Small, readable test doubles
# ----------------------------

class FakeQualifiedName:
    def __init__(self, fully_qualified_name: str) -> None:
        self.fully_qualified_name = fully_qualified_name

    def __str__(self) -> str:  # Engine uses str(qualified_name) as the dict key
        return self.fully_qualified_name


class FakeDesiredTable:
    def __init__(self, name: str) -> None:
        self.qualified_name = FakeQualifiedName(name)


class FakeObservedTable:
    def __init__(self, name: str) -> None:
        self.name = name


class FakePlan:
    def __init__(self, name: str, action_count: int) -> None:
        self.name = name
        # Minimal surface: Engine only inspects .actions length
        self.actions = tuple(range(action_count))


class FakePlanContext:
    def __init__(self, plan: FakePlan) -> None:
        self.plan = plan


class RecordingReader:
    """Captures calls to fetch_state so we can assert they were made correctly."""

    def __init__(self, observed_by_name: dict[str, FakeObservedTable]) -> None:
        self.observed_by_name = observed_by_name
        self.calls: list[FakeQualifiedName] = []

    def fetch_state(self, qn: FakeQualifiedName) -> FakeObservedTable:
        self.calls.append(qn)
        return self.observed_by_name[str(qn)]


class ConfigurableValidator:
    """Return failures by table name. Provide a dict[str, tuple[ValidationFailure, ...]]
    where an empty tuple means "no failures".
    """

    def __init__(self, failures_by_name: dict[str, tuple]) -> None:
        self.failures_by_name = failures_by_name
        self.calls: list[str] = []

    def evaluate(self, ctx: FakePlanContext):
        self.calls.append(ctx.plan.name)
        return self.failures_by_name.get(ctx.plan.name, ())


class RecordingExecutor:
    """Executes plans unless configured to raise for specific table names.
    returns_by_name: dict[name, object] (pretend ExecutionReport)
    errors_by_name: dict[name, Exception] to raise instead of returning.
    """

    def __init__(self, returns_by_name: dict[str, object] | None = None,
                 errors_by_name: dict[str, Exception] | None = None) -> None:
        self.returns_by_name = returns_by_name or {}
        self.errors_by_name = errors_by_name or {}
        self.calls: list[str] = []

    def execute(self, plan: FakePlan):
        self.calls.append(plan.name)
        if plan.name in self.errors_by_name:
            raise self.errors_by_name[plan.name]
        return self.returns_by_name.get(plan.name, object())


# ----------------------------
# Helper to stub make_plan_context
# ----------------------------

def install_make_plan_context(monkeypatch, name_to_action_count: dict[str, int]):
    """Patches engine_mod.make_plan_context(desired, observed) to return a FakePlanContext
    with a FakePlan(name=<str(desired.qualified_name)>, action_count given by mapping).
    """
    def fake_make_plan_context(*, desired, observed):
        name = str(desired.qualified_name)
        plan = FakePlan(name=name, action_count=name_to_action_count[name])
        return FakePlanContext(plan)

    monkeypatch.setattr(engine_mod, "make_plan_context", fake_make_plan_context)


# ----------------------------
# _build_contexts
# ----------------------------

def test_build_contexts_uses_reader_and_make_plan_context_and_keys_are_str(monkeypatch):
    desired = [FakeDesiredTable("cat.schema.table_a"), FakeDesiredTable("cat.schema.table_b")]

    reader = RecordingReader(
        observed_by_name={
            "cat.schema.table_a": FakeObservedTable("obs_a"),
            "cat.schema.table_b": FakeObservedTable("obs_b"),
        }
    )
    # Give table_a 2 actions and table_b 0 actions for variety
    install_make_plan_context(monkeypatch, {"cat.schema.table_a": 2, "cat.schema.table_b": 0})

    # Executor/validator not used in _build_contexts, but Engine requires them
    engine = Engine(reader=reader, executor=RecordingExecutor(), validator=ConfigurableValidator({}))

    contexts = engine._build_contexts(desired)
    assert set(contexts.keys()) == {"cat.schema.table_a", "cat.schema.table_b"}
    # Reader called for each desired
    assert [str(qn) for qn in reader.calls] == ["cat.schema.table_a", "cat.schema.table_b"]
    # Returned contexts contain a plan attached by our fake make_plan_context
    assert isinstance(contexts["cat.schema.table_a"].plan, FakePlan)
    assert len(contexts["cat.schema.table_a"].plan.actions) == 2


# ----------------------------
# _validate_all
# ----------------------------

def test_validate_all_collects_only_failures(monkeypatch):
    desired = [FakeDesiredTable("a"), FakeDesiredTable("b")]
    install_make_plan_context(monkeypatch, {"a": 1, "b": 1})
    reader = RecordingReader({"a": FakeObservedTable("oa"), "b": FakeObservedTable("ob")})
    validator = ConfigurableValidator(failures_by_name={"a": ("fail-x",), "b": ()})
    engine = Engine(reader=reader, executor=RecordingExecutor(), validator=validator)
    contexts = engine._build_contexts(desired)

    failures = engine._validate_all(contexts)

    assert set(failures.keys()) == {"a"}
    assert failures["a"] == ("fail-x",)
    # Validator called once per table
    assert validator.calls == ["a", "b"]


# ----------------------------
# _build_previews
# ----------------------------

def test_build_previews_uses_plan_length_and_summary_counts(monkeypatch):
    desired = [FakeDesiredTable("a"), FakeDesiredTable("b")]
    install_make_plan_context(monkeypatch, {"a": 0, "b": 3})
    reader = RecordingReader({"a": FakeObservedTable("oa"), "b": FakeObservedTable("ob")})

    # Spy on plan_summary_counts
    calls: list[str] = []

    def fake_plan_summary_counts(plan):
        calls.append(plan.name)
        # Distinct returns for each plan so we can assert mapping passed through
        return {"a_key": 1} if plan.name == "a" else {"b_key": 2}

    monkeypatch.setattr(engine_mod, "plan_summary_counts", fake_plan_summary_counts)

    engine = Engine(reader=reader, executor=RecordingExecutor(), validator=ConfigurableValidator({}))
    contexts = engine._build_contexts(desired)
    previews = engine._build_previews(contexts)

    assert set(previews.keys()) == {"a", "b"}
    # a: no actions → noop preview
    assert previews["a"].is_noop is True
    assert previews["a"].total_actions == 0
    assert previews["a"].summary_counts == {"a_key": 1}
    # b: has actions
    assert previews["b"].is_noop is False
    assert previews["b"].total_actions == 3
    assert previews["b"].summary_counts == {"b_key": 2}
    # Summary counts called for each plan
    assert calls == ["a", "b"]


# ----------------------------
# _execute_all
# ----------------------------

def test_execute_all_collects_successes_and_failures(monkeypatch):
    desired = [FakeDesiredTable("a"), FakeDesiredTable("b")]
    install_make_plan_context(monkeypatch, {"a": 1, "b": 1})
    reader = RecordingReader({"a": FakeObservedTable("oa"), "b": FakeObservedTable("ob")})
    executor = RecordingExecutor(
        returns_by_name={"a": "ok-a"},
        errors_by_name={"b": RuntimeError("boom-b")},
    )
    engine = Engine(reader=reader, executor=executor, validator=ConfigurableValidator({}))
    contexts = engine._build_contexts(desired)

    executions, failures = engine._execute_all(contexts)

    assert executions == {"a": "ok-a", "b": None} or set(executions.keys()) == {"a"}  # depending on how you prefer to handle failed ones
    assert failures == {"b": "boom-b"}
    assert executor.calls == ["a", "b"]


# ----------------------------
# sync() behaviour: success, validation fail, execution fail
# ----------------------------

def test_sync_returns_full_report_on_success(monkeypatch):
    desired = [FakeDesiredTable("a"), FakeDesiredTable("b")]
    install_make_plan_context(monkeypatch, {"a": 1, "b": 0})
    reader = RecordingReader({"a": FakeObservedTable("oa"), "b": FakeObservedTable("ob")})
    validator = ConfigurableValidator(failures_by_name={})  # all clean
    executor = RecordingExecutor(returns_by_name={"a": "exec-a", "b": "exec-b"})

    # Deterministic summary
    monkeypatch.setattr(engine_mod, "plan_summary_counts", lambda plan: {"count": len(plan.actions)})

    engine = Engine(reader=reader, executor=executor, validator=validator)
    report = engine.sync(desired)  # list is fine; Engine iterates

    # Report shape
    assert set(report.previews.keys()) == {"a", "b"}
    assert report.previews["a"].is_noop is False
    assert report.previews["b"].is_noop is True
    assert report.previews["a"].summary_counts == {"count": 1}
    assert report.previews["b"].summary_counts == {"count": 0}
    assert report.executions == {"a": "exec-a", "b": "exec-b"}
    # Calls happened in both phases
    assert validator.calls == ["a", "b"]
    assert executor.calls == ["a", "b"]


def test_sync_raises_validation_failed_error_and_does_not_execute(monkeypatch):
    desired = [FakeDesiredTable("a"), FakeDesiredTable("b")]
    install_make_plan_context(monkeypatch, {"a": 2, "b": 1})
    reader = RecordingReader({"a": FakeObservedTable("oa"), "b": FakeObservedTable("ob")})
    validator = ConfigurableValidator(failures_by_name={"a": ("bad-a-1", "bad-a-2")})
    executor = RecordingExecutor()

    monkeypatch.setattr(engine_mod, "plan_summary_counts", lambda plan: {"n": len(plan.actions)})

    engine = Engine(reader=reader, executor=executor, validator=validator)

    with pytest.raises(ValidationFailedError) as err:
        engine.sync(desired)

    exc = err.value
    # The exception should carry all failures and a partial report with previews but no executions
    assert set(exc.failures_by_table.keys()) == {"a"}
    assert exc.failures_by_table["a"] == ("bad-a-1", "bad-a-2")
    assert set(exc.report.previews.keys()) == {"a", "b"}
    assert exc.report.executions == {}
    # Critically: no execution attempted when validation fails
    assert executor.calls == []


def test_sync_raises_execution_failed_error_after_attempting_all(monkeypatch):
    desired = [FakeDesiredTable("a"), FakeDesiredTable("b"), FakeDesiredTable("c")]
    install_make_plan_context(monkeypatch, {"a": 1, "b": 1, "c": 0})
    reader = RecordingReader({n: FakeObservedTable(f"o{n}") for n in ("a", "b", "c")})
    validator = ConfigurableValidator(failures_by_name={})  # all pass validation
    executor = RecordingExecutor(
        returns_by_name={"a": "ok-a", "c": "ok-c"},
        errors_by_name={"b": RuntimeError("boom-b")},
    )

    monkeypatch.setattr(engine_mod, "plan_summary_counts", lambda plan: {"n": len(plan.actions)})

    engine = Engine(reader=reader, executor=executor, validator=validator)

    with pytest.raises(ExecutionFailedError) as err:
        engine.sync(desired)

    exc = err.value
    # One failure, but all tables were attempted
    assert exc.failures_by_table == {"b": "boom-b"}
    # Partial report should include previews and the executions we *did* manage
    assert set(exc.report.previews.keys()) == {"a", "b", "c"}
    assert exc.report.executions.get("a") == "ok-a"
    assert "b" not in exc.report.executions  # failed one won't have a successful execution record
    assert exc.report.executions.get("c") == "ok-c"
    # Execution was attempted for each table after validation passed
    assert executor.calls == ["a", "b", "c"]
