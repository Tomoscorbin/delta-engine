import pytest
from dataclasses import dataclass

from tabula.adapters.databricks.catalog.executor import UCExecutor
from tabula.application.change_target import load_change_target
from tabula.application.plan.plan import preview_plan
from tabula.application.errors import ValidationError
from tabula.domain.model import (
    QualifiedName,
    DesiredTable,
    ObservedTable,
    Column,
    DataType,
)
from tabula.domain.plan.actions import CreateTable, AddColumn, DropColumn


# ---------- helpers -----------------------------------------------------------


def make_qualified_name() -> QualifiedName:
    # Keep this consistent across tests to make exact SQL assertions stable.
    return QualifiedName("dev", "sales", "orders")


def col(name: str, dtype: str = "string", *, nullable: bool = True) -> Column:
    # Use Databricks-friendly logical names (e.g., "int", "string").
    return Column(name=name, data_type=DataType(dtype), is_nullable=nullable)


def desired(cols: list[Column]) -> DesiredTable:
    return DesiredTable(qualified_name=make_qualified_name(), columns=tuple(cols))


def observed(cols: list[Column], *, is_empty: bool = True) -> ObservedTable:
    # is_empty=True allows NOT NULL adds under typical safety rules.
    return ObservedTable(
        qualified_name=make_qualified_name(), columns=tuple(cols), is_empty=is_empty
    )


# Structural double for a catalog reader (what load_change_target expects)
@dataclass
class FakeCatalog:
    by_name: dict[QualifiedName, ObservedTable]

    def fetch_state(self, qualified_name: QualifiedName) -> ObservedTable | None:
        return self.by_name.get(qualified_name)


# Recorder to capture what UCExecutor actually executes
class Recorder:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def __call__(self, statement: str) -> None:
        self.sql.append(statement)


# ---------- end-to-end tests --------------------------------------------------


def test_e2e_create_then_execute_sql() -> None:
    reader = FakeCatalog(by_name={})
    rec = Recorder()
    executor = UCExecutor(run_sql=rec)

    # CreateTable(id INT NOT NULL)
    dt = desired([col("id", "integer", nullable=False)])

    subject = load_change_target(reader, dt)
    preview = preview_plan(subject)
    assert preview.is_noop is False

    # Plan contains exactly one CreateTable
    actions = list(preview.plan)
    assert len(actions) == 1
    assert isinstance(actions[0], CreateTable)

    out = executor.execute(preview.plan)
    assert out.success is True

    expected = [
        "CREATE TABLE IF NOT EXISTS `dev`.`sales`.`orders` (`id` INT NOT NULL)",
    ]
    assert rec.sql == expected  # what actually ran
    assert out.executed_sql == tuple(expected)  # what executor recorded
    assert out.executed_count == 1
    # Message format is adapter’s contract:
    assert out.messages == ("OK 1",)


def test_e2e_adds_preserve_spec_order_and_drop_then_execute_sql() -> None:
    # Existing table has columns a (keep) and OLD (drop after case-normalization)
    existing = observed(
        [col("a", "string", nullable=False), col("OLD", "string", nullable=True)], is_empty=True
    )
    reader = FakeCatalog(by_name={make_qualified_name(): existing})
    rec = Recorder()
    executor = UCExecutor(run_sql=rec)

    # Desired spec order: a, b, c  (all NOT NULL to make SQL unambiguous)
    dt = desired(
        [
            col("a", "string", nullable=False),
            col("b", "string", nullable=False),
            col("c", "string", nullable=False),
        ]
    )

    subject = load_change_target(reader, dt)
    preview = preview_plan(subject)
    assert preview.is_noop is False

    # Expect: Add(b), Add(c), Drop(old) — add order follows spec; drop is normalized
    action_types = [type(a) for a in preview.plan]
    assert action_types == [AddColumn, AddColumn, DropColumn]

    out = executor.execute(preview.plan)
    assert out.success is True

    expected = [
        "ALTER TABLE `dev`.`sales`.`orders` ADD COLUMN IF NOT EXISTS `b` STRING NOT NULL",
        "ALTER TABLE `dev`.`sales`.`orders` ADD COLUMN IF NOT EXISTS `c` STRING NOT NULL",
        "ALTER TABLE `dev`.`sales`.`orders` DROP COLUMN IF EXISTS `old`",
    ]
    assert rec.sql == expected
    assert out.executed_sql == tuple(expected)
    assert out.executed_count == 3
    assert out.messages == ("OK 1", "OK 2", "OK 3")


def test_e2e_noop_when_observed_matches_desired() -> None:
    existing = observed(
        [col("id", "int", nullable=False), col("note", "string", nullable=True)], is_empty=False
    )
    reader = FakeCatalog(by_name={make_qualified_name(): existing})

    dt = desired([col("id", "int", nullable=False), col("note", "string", nullable=True)])

    subject = load_change_target(reader, dt)
    preview = preview_plan(subject)
    assert preview.is_noop is True
    assert len(tuple(preview.plan)) == 0  # no actions

    # Executing an empty plan should be a no-op that succeeds cleanly.
    rec = Recorder()
    out = UCExecutor(run_sql=rec).execute(preview.plan)
    assert out.success is True
    assert out.executed_count == 0
    assert out.executed_sql == ()
    assert out.messages == ()


def test_e2e_dry_run_records_would_be_sql_without_executing() -> None:
    class MustNotBeCalled:
        def __call__(self, _sql: str) -> None:  # pragma: no cover
            raise AssertionError("run_sql should not be called during dry_run")

    reader = FakeCatalog(by_name={})
    executor = UCExecutor(run_sql=MustNotBeCalled(), dry_run=True)

    dt = desired([col("id", "integer", nullable=False)])

    subject = load_change_target(reader, dt)
    preview = preview_plan(subject)
    actions = list(preview.plan)
    assert len(actions) == 1 and isinstance(actions[0], CreateTable)

    out = executor.execute(preview.plan)
    assert out.success is True
    assert out.executed_count == 0

    expected = ("CREATE TABLE IF NOT EXISTS `dev`.`sales`.`orders` (`id` INT NOT NULL)",)
    assert out.executed_sql == expected
    assert out.messages == (f"DRY RUN 1: {expected[0]}",)


def test_e2e_continue_on_error_runs_rest() -> None:
    # Existing has only 'a'. Desired adds 'bad' (will fail) and 'c' (succeeds).
    existing = observed([col("a", "string", nullable=False)], is_empty=True)
    reader = FakeCatalog(by_name={make_qualified_name(): existing})

    def flaky_run(sql: str) -> None:
        # Simulate failure only for the 'bad' column add
        if "`bad`" in sql:
            raise RuntimeError("boom")

    executor = UCExecutor(run_sql=flaky_run, on_error="continue")

    dt = desired(
        [
            col("a", "string", nullable=False),
            col("bad", "string", nullable=False),
            col("c", "string", nullable=False),
        ]
    )

    subject = load_change_target(reader, dt)
    preview = preview_plan(subject)
    action_types = [type(a) for a in preview.plan]
    # Should be: Add(bad), Add(c) — only adds (no drops)
    assert action_types == [AddColumn, AddColumn]

    out = executor.execute(preview.plan)

    # One failed, one succeeded.
    assert out.success is False
    assert out.executed_count == 1
    # Only the successful SQL is recorded in executed_sql
    assert out.executed_sql == (
        "ALTER TABLE `dev`.`sales`.`orders` ADD COLUMN IF NOT EXISTS `c` STRING NOT NULL",
    )
    # Messages reflect OK / ERROR / OK sequencing depending on executor impl
    assert len(out.messages) == 2
    assert any(m.startswith("ERROR 1: RuntimeError: boom") for m in out.messages) or any(
        m.startswith("OK 1") for m in out.messages
    )


def test_e2e_stop_on_first_error_stops() -> None:
    existing = observed([col("a", "string", nullable=False)], is_empty=True)
    reader = FakeCatalog(by_name={make_qualified_name(): existing})

    def flaky_run(sql: str) -> None:
        raise ValueError("nope")

    executor = UCExecutor(run_sql=flaky_run, on_error="stop")

    dt = desired([col("a", "string", nullable=False), col("b", "string", nullable=False)])

    subject = load_change_target(reader, dt)
    preview = preview_plan(subject)
    out = executor.execute(preview.plan)

    assert out.success is False
    assert out.executed_count == 0
    assert out.executed_sql == ()
    assert out.messages == ("ERROR 1: ValueError: nope",)


def test_e2e_validator_blocks_add_on_non_empty_table() -> None:
    """New policy: AddColumn on non-empty tables is forbidden."""
    existing = observed([col("a", "string", nullable=False)], is_empty=False)  # non-empty
    reader = FakeCatalog(by_name={make_qualified_name(): existing})

    dt = desired(
        [col("a", "string", nullable=False), col("b", "string", nullable=False)]
    )  # would add 'b'

    subject = load_change_target(reader, dt)
    with pytest.raises(ValidationError) as excinfo:
        _ = preview_plan(subject)  # validation happens inside preview_plan

    s = str(excinfo.value)
    assert "NO_ADD_ON_NON_EMPTY_TABLE" in s
    assert "AddColumn actions are not allowed on non-empty tables." in s
