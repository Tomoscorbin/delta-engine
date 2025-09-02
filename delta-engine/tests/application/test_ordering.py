import random

import pytest

from delta_engine.application.ordering import action_sort_key, subject_name
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.plan.actions import (
    AddColumn,
    CreateTable,
    DropColumn,
    SetColumnComment,
    SetProperty,
    SetTableComment,
    UnsetProperty,
)
from tests.factories import make_qualified_name

# --- subject_name -------------------------------------------------------------


def test_subject_name_uses_relevant_field_per_action_type() -> None:
    add = AddColumn(Column("age", Integer()))
    drop = DropColumn("nickname")
    set_prop = SetProperty(name="owner", value="data")
    unset_prop = UnsetProperty(name="ttl")
    set_col_comment = SetColumnComment(column_name="display_name", comment="shown")
    set_table_comment = SetTableComment(comment="table note")

    assert subject_name(add) == "age"
    assert subject_name(drop) == "nickname"
    assert subject_name(set_prop) == "owner"
    assert subject_name(unset_prop) == "ttl"
    assert subject_name(set_col_comment) == "display_name"
    assert subject_name(set_table_comment) == ""  # no subject for table-level comment


def test_subject_name_falls_back_to_name_attribute_for_unknown_objects() -> None:
    class Dummy:
        def __init__(self) -> None:
            self.name = "whatever"

    assert subject_name(Dummy()) == "whatever"


# --- action_sort_key ------------------


def test_action_sort_key_is_monotonic_by_phase_for_mixed_actions() -> None:
    """
    We don't assert the *specific* phase sequence, only that sorting
    groups by phase (monotonic rank).

    """
    qn = make_qualified_name("dev", "silver", "people")
    create = CreateTable(DesiredTable(qn, (Column("id", Integer()),)))
    actions = [
        UnsetProperty("c"),
        SetTableComment("note"),
        SetColumnComment("b", ""),
        DropColumn("d"),
        AddColumn(Column("a", Integer())),
        SetProperty("owner", "eng"),
        create,
    ]
    random.shuffle(actions)  # ensure input order doesn't bias the outcome

    sorted_actions = sorted(actions, key=action_sort_key)
    ranks = [action_sort_key(a)[0] for a in sorted_actions]

    assert ranks == sorted(ranks)  # monotonic by phase rank (whatever that mapping is)


@pytest.mark.parametrize(
    "builder,extract",
    [
        (lambda s: SetProperty(s, "v"), lambda a: a.name),
        (lambda s: UnsetProperty(s), lambda a: a.name),
        (lambda s: AddColumn(Column(s, Integer())), lambda a: a.column.name),
        (lambda s: DropColumn(s), lambda a: a.column_name),
        (lambda s: SetColumnComment(s, ""), lambda a: a.column_name),
    ],
)
def test_action_sort_key_orders_within_same_phase_by_subject(builder, extract) -> None:
    """Within a phase, ordering is by the subject identifier (no reliance on case rules)."""
    items = [builder("m"), builder("a"), builder("z")]
    random.shuffle(items)

    sorted_items = sorted(items, key=action_sort_key)
    subjects = [extract(a) for a in sorted_items]

    assert subjects == ["a", "m", "z"]


def test_action_sort_key_raises_for_unknown_action_type() -> None:
    class Rogue:
        pass

    with pytest.raises(ValueError) as excinfo:
        _ = action_sort_key(Rogue())  # type: ignore[arg-type]

    assert "Place Rogue in PHASE_ORDER" in str(excinfo.value)
