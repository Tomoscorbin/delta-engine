import pytest

from delta_engine.application.ordering import (
    action_sort_key,
    subject_name,
)
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.plan.actions import Action, AddColumn, CreateTable, DropColumn

# --- Helpers -----------------------------------------------------------------


def _add(name: str) -> AddColumn:
    return AddColumn(Column(name, Integer()))


def _drop(name: str) -> DropColumn:
    # DropColumn has `column_name`, not `name`
    return DropColumn(name)


def _create(*cols: str) -> CreateTable:
    return CreateTable(columns=tuple(Column(c, String()) for c in cols))


# --- subject_name behaviour ------------------------------------


def test_subject_name_for_addcolumn_is_column_name() -> None:
    assert subject_name(_add("Age")) == "age"  # normalized by Column


def test_action_sort_key_sorts_dropcolumns_by_subject_name() -> None:
    from delta_engine.application.ordering import action_sort_key
    from delta_engine.domain.plan.actions import DropColumn

    d1 = DropColumn("a_col")
    d2 = DropColumn("b_col")
    ordered = sorted([d2, d1], key=action_sort_key)
    assert ordered == [d1, d2]


# --- action_sort_key ordering ------------------------------------------------


def test_action_sort_key_phase_order_create_add_drop() -> None:
    c = _create("id")
    a = _add("age")
    d = _drop("old")
    # Expect phase ranks: CreateTable < AddColumn < DropColumn
    keys = [action_sort_key(x) for x in (c, a, d)]
    assert keys[0][0] < keys[1][0] < keys[2][0]


def test_action_sort_key_sorts_addcolumns_by_subject_name_when_same_phase() -> None:
    a1 = _add("a_col")
    a2 = _add("b_col")
    ordered = sorted([a2, a1], key=action_sort_key)
    assert ordered == [a1, a2]  # lexicographic by subject name


def test_action_sort_key_is_stable_with_equal_keys() -> None:
    # Equal subject names => equal keys; Python's sort is stable
    a1 = _add("x")
    a2 = _add("x")
    ordered = sorted([a1, a2], key=action_sort_key)
    assert ordered == [a1, a2]


def test_action_sort_key_raises_for_unknown_action_type() -> None:
    class RenameColumn(Action):
        pass

    with pytest.raises(ValueError) as exc:
        action_sort_key(RenameColumn())
    assert "Place RenameColumn in PHASE_ORDER" in str(exc.value)


def test_action_sort_key_orders_by_phase_then_subject_name() -> None:
    from delta_engine.application.ordering import action_sort_key
    from delta_engine.domain.model.column import Column
    from delta_engine.domain.model.data_type import Integer
    from delta_engine.domain.plan.actions import AddColumn, DropColumn

    a1 = AddColumn(Column("a_col", Integer()))
    a2 = AddColumn(Column("b_col", Integer()))
    d1 = DropColumn("a_col")
    d2 = DropColumn("b_col")

    shuffled = [d2, a2, d1, a1]
    ordered = sorted(shuffled, key=action_sort_key)

    # Phase: AddColumn before DropColumn; within each phase, alphabetical by column
    assert ordered == [a1, a2, d1, d2]
