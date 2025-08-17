from tabula.domain.model.full_name import FullName
from tabula.domain.model.column import Column
from tabula.domain.model.data_type import DataType
from tabula.domain.model.table_spec import TableSpec
from tabula.domain.model.table_state import TableState
from tabula.domain.model.actions import CreateTable, AddColumn, DropColumn
from tabula.domain.services.differ import diff, table_exists

def col(name: str) -> Column:
    return Column(name, DataType("string"))

def spec(cols):
    return TableSpec(FullName("dev","sales","orders"), tuple(cols))

def state(cols):
    return TableState(FullName("dev","sales","orders"), tuple(cols))

def test_table_exists_predicate():
    assert table_exists(None) is False
    assert table_exists(state([col("x")])) is True

def test_diff_creates_when_absent():
    plan = diff(None, spec([col("id")]))
    assert len(plan) == 1
    assert isinstance(plan.actions[0], CreateTable)

def test_diff_adds_and_drops_when_present():
    observed = state([col("a"), col("old")])
    desired  = spec([col("a"), col("b"), col("c")])
    plan = diff(observed, desired)
    kinds = [type(a).__name__ for a in plan.actions]
    assert kinds == ["AddColumn", "AddColumn", "DropColumn"]
    assert [a.column.name for a in plan.actions if isinstance(a, AddColumn)] == ["b", "c"]
    assert [a.column_name for a in plan.actions if isinstance(a, DropColumn)] == ["old"]

def test_diff_raises_on_identity_mismatch():
    observed = TableState(FullName("dev","s","X"), (col("id"),))
    desired  = TableSpec(FullName("dev","s","Y"), (col("id"),))
    try:
        _ = diff(observed, desired)
        assert False, "expected ValueError"
    except ValueError:
        pass

def test_diff_no_changes_returns_empty_plan():
    observed = state([col("id")])
    desired  = spec([col("id")])
    plan = diff(observed, desired)
    assert len(plan) == 0
