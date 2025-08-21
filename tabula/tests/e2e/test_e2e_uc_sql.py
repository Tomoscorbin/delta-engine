from dataclasses import dataclass

# --- outbound SQL adapter ---
from tabula.adapters.databricks.catalog.executor import UCExecutor

# --- application layer ---
from tabula.application.plan import plan_actions

# --- domain model ---
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.table import DesiredTable, ObservedTable
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType
from tabula.domain.plan.actions import CreateTable, AddColumn, DropColumn


# ---------- helpers ----------

def make_qualified_name() -> QualifiedName:
    return QualifiedName("dev", "sales", "orders")


def col(name: str, dtype: str = "string", nullable: bool = True) -> Column:
    return Column(name=name, data_type=DataType(dtype), is_nullable=nullable)


def desired(cols: list[Column]) -> DesiredTable:
    return DesiredTable(qualified_name=make_qualified_name(), columns=tuple(cols))


def observed(cols: list[Column], *, is_empty: bool | None = True) -> ObservedTable:
    # is_empty=True allows ADD NOT NULL under your validator policy
    return ObservedTable(qualified_name=make_qualified_name(), columns=tuple(cols), is_empty=is_empty)


# Fake CatalogReader (structural typing; no Protocol inheritance needed)
@dataclass
class FakeCatalog:
    by_name: dict[QualifiedName, ObservedTable]

    def fetch_state(self, qualified_name: QualifiedName) -> ObservedTable | None:
        return self.by_name.get(qualified_name)


# Simple callable recorder for UCExecutor
class Recorder:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def __call__(self, statement: str) -> None:
        self.sql.append(statement)


# ---------- e2e tests ----------

def test_e2e_create_then_execute_sql():
    reader = FakeCatalog(by_name={})
    rec = Recorder()
    executor = UCExecutor(run_sql=rec)

    dt = desired([Column("id", DataType("integer"), is_nullable=False)])

    preview = plan_actions(dt, reader)
    assert preview.is_noop is False

    # Assert plan by action types (no .summary on PlanPreview)
    actions = list(preview.plan)
    assert len(actions) == 1
    assert isinstance(actions[0], CreateTable)

    result = executor.execute(preview.plan)
    assert result.success is True

    expected = ["CREATE TABLE IF NOT EXISTS `dev`.`sales`.`orders` (`id` INT NOT NULL)"]
    # Verify what actually ran
    assert rec.sql == expected
    # And what the executor recorded
    assert result.executed_sql == tuple(expected)
    assert result.executed_count == 1
    assert len(result.durations_ms) == 1


def test_e2e_adds_in_spec_order_and_drop_then_execute_sql():
    existing = observed([col("a"), col("OLD")], is_empty=True)
    reader = FakeCatalog(by_name={make_qualified_name(): existing})
    rec = Recorder()
    executor = UCExecutor(run_sql=rec)

    dt = desired([col("a"), col("b"), col("c")])

    preview = plan_actions(dt, reader)
    assert preview.is_noop is False

    # Expect: Add(b), Add(c), Drop(old) — adds preserve spec order; drop case-folded
    actions = list(preview.plan)
    assert [type(a) for a in actions] == [AddColumn, AddColumn, DropColumn]

    result = executor.execute(preview.plan)
    assert result.success is True

    expected = [
        "ALTER TABLE `dev`.`sales`.`orders` ADD COLUMN IF NOT EXISTS `b` STRING NOT NULL",
        "ALTER TABLE `dev`.`sales`.`orders` ADD COLUMN IF NOT EXISTS `c` STRING NOT NULL",
        "ALTER TABLE `dev`.`sales`.`orders` DROP COLUMN IF EXISTS `old`",
    ]
    assert rec.sql == expected
    assert result.executed_sql == tuple(expected)
    assert result.executed_count == 3
    assert len(result.durations_ms) == 3


def test_e2e_dry_run_records_would_be_sql_without_executing():
    # runner that would explode if called — ensures dry_run truly skips execution
    class MustNotBeCalled:
        def __call__(self, _sql: str) -> None:
            raise AssertionError("run_sql should not be called during dry_run")

    reader = FakeCatalog(by_name={})
    executor = UCExecutor(run_sql=MustNotBeCalled(), dry_run=True)

    dt = desired([Column("id", DataType("integer"), is_nullable=False)])

    preview = plan_actions(dt, reader)
    actions = list(preview.plan)
    assert len(actions) == 1 and isinstance(actions[0], CreateTable)

    result = executor.execute(preview.plan)
    assert result.success is True

    expected = ["CREATE TABLE IF NOT EXISTS `dev`.`sales`.`orders` (`id` INT NOT NULL)"]
    # No real executions happened:
    assert result.executed_count == 0
    # But the would-be SQL is surfaced:
    assert result.executed_sql == tuple(expected)
    assert result.durations_ms == (0.0,)
