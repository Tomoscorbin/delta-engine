from __future__ import annotations
from dataclasses import dataclass, FrozenInstanceError
import pytest
from tabula.domain.model import TableSnapshot, DesiredTable, ObservedTable


@dataclass(frozen=True, slots=True)
class DummyColumn:
    name: str


def test_order_is_preserved_and_tuple_is_immutable(make_qn) -> None:
    cols = (DummyColumn("id"), DummyColumn("name"), DummyColumn("created_at"))
    ts = TableSnapshot(qualified_name=make_qn(), columns=cols)

    # Order preserved
    assert [c.name for c in ts.columns] == ["id", "name", "created_at"]

    # Dataclass is frozen and columns is a tuple
    with pytest.raises(FrozenInstanceError):
        ts.columns = ()  # type: ignore[attr-defined]


def test_duplicate_names_are_rejected_case_insensitive_adjacent(make_qn) -> None:
    with pytest.raises(ValueError):
        TableSnapshot(qualified_name=make_qn(), columns=(DummyColumn("id"), DummyColumn("ID")))


def test_duplicate_names_are_rejected_case_insensitive_non_adjacent(make_qn) -> None:
    with pytest.raises(ValueError):
        TableSnapshot(
            qualified_name=make_qn(),
            columns=(DummyColumn("id"), DummyColumn("name"), DummyColumn("ID")),
        )


def test_contains_with_string_is_case_insensitive_for_any_stored_case(make_qn) -> None:
    # Stored uppercase, queried lowercase and mixed
    ts = TableSnapshot(make_qn(), (DummyColumn("ID"), DummyColumn("NaMe")))
    assert "id" in ts
    assert "name" in ts
    assert "Missing" not in ts


def test_contains_with_column_is_name_based_and_case_insensitive(make_qn) -> None:
    ts = TableSnapshot(make_qn(), (DummyColumn("id"), DummyColumn("name")))
    # Same *name*, different object -> membership True
    assert DummyColumn("ID") in ts
    assert DummyColumn("NAME") in ts
    assert DummyColumn("missing") not in ts


def test_get_column_is_case_insensitive_and_returns_original_object(make_qn) -> None:
    c_id = DummyColumn("ID")  # stored uppercase on purpose
    c_nm = DummyColumn("name")
    ts = TableSnapshot(make_qn(), (c_id, c_nm))

    got = ts.get_column("id")
    assert got is c_id

    got2 = ts.get_column("NAME")
    assert got2 is c_nm

    assert ts.get_column("missing") is None


def test_desired_table_inherits_behavior(make_qn) -> None:
    dt = DesiredTable(make_qn(), (DummyColumn("id"),))
    assert "ID" in dt
    assert dt.get_column("id") is not None


def test_observed_table_inherits_behavior_and_exposes_is_empty(make_qn) -> None:
    ot = ObservedTable(make_qn(), (DummyColumn("id"),), is_empty=True)
    assert "id" in ot
    assert ot.get_column("ID") is not None
    assert ot.is_empty is True


def test_table_must_have_at_least_one_column(make_qn) -> None:
    with pytest.raises(ValueError):
        TableSnapshot(qualified_name=make_qn(), columns=())
