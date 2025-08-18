from dataclasses import dataclass, FrozenInstanceError
import pytest

from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.table import TableSnapshot, DesiredTable, ObservedTable


# A minimal stand-in for Column for these tests.
# We deliberately DO NOT normalize here to ensure TableSnapshot
# does its own case-insensitive handling.
@dataclass(frozen=True, slots=True)
class DummyColumn:
    name: str

def test_order_is_preserved_and_tuple_identity(make_qn) -> None:
    cols = (DummyColumn("id"), DummyColumn("name"), DummyColumn("created_at"))
    ts = TableSnapshot(qualified_name=make_qn(), columns=cols)
    # Order preserved
    assert [c.name for c in ts.columns] == ["id", "name", "created_at"]
    # Immutability of dataclass + tuple
    with pytest.raises(FrozenInstanceError):
        ts.columns = ()  # type: ignore[attr-defined]


def test_duplicate_names_are_rejected_case_insensitive(make_qn) -> None:
    cols = (DummyColumn("id"), DummyColumn("ID"))
    with pytest.raises(ValueError):
        TableSnapshot(qualified_name=make_qn(), columns=cols)


def test_contains_with_string_is_case_insensitive(make_qn) -> None:
    ts = TableSnapshot(make_qn(), (DummyColumn("id"), DummyColumn("name")))
    assert "ID" in ts
    assert "Name" in ts
    assert "missing" not in ts


def test_contains_with_column_is_name_based_and_case_insensitive(make_qn) -> None:
    ts = TableSnapshot(make_qn(), (DummyColumn("id"), DummyColumn("name")))
    # Same name, different object -> should be True if name-based
    assert DummyColumn("ID") in ts
    assert DummyColumn("NAME") in ts
    assert DummyColumn("missing") not in ts


def test_get_column_is_case_insensitive_and_returns_original_object(make_qn) -> None:
    c_id = DummyColumn("id")
    c_nm = DummyColumn("name")
    ts = TableSnapshot(make_qn(), (c_id, c_nm))

    got = ts.get_column("ID")
    assert got is c_id

    got2 = ts.get_column("Name")
    assert got2 is c_nm

    assert ts.get_column("missing") is None


def test_desired_table_inherits_behavior(make_qn) -> None:
    dt = DesiredTable(make_qn(), (DummyColumn("id"),))
    assert "id" in dt
    assert dt.get_column("ID") is not None


def test_table_must_have_at_least_one_column(make_qn) -> None:
    with pytest.raises(ValueError):
        TableSnapshot(qualified_name=make_qn(), columns=())