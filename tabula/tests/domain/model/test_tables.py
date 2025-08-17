import pytest
from hypothesis import given
from tests.conftest import qn, col, desired, observed, columns_strat
from tabula.domain.model.table import DesiredTable, ObservedTable
from tabula.domain.model.column import Column
from tabula.domain.model.types import integer
from tabula.domain.model.qualified_name import QualifiedName


def test_duplicate_column_names_case_insensitive_rejected():
    with pytest.raises(ValueError):
        desired(qn(), col("A"), col("a"))

def test_membership_and_get_column():
    t = desired(qn(), col("A"), col("B"))
    assert "a" in t
    assert t.get_column("B").name == "b"
    assert t.get_column("missing") is None

@given(columns_strat(min_size=0, max_size=6))
def test_order_preserved(cols):
    t = desired(qn(), *cols)
    assert [c.name for c in t.columns] == [c.name for c in cols]

def qn() -> QualifiedName:
    return QualifiedName("Cat", "Sch", "Tbl")

def test_duplicate_column_names_detected_case_insensitive():
    with pytest.raises(ValueError):
        DesiredTable(qualified_name=qn(), columns=(Column("A", integer()), Column("a", integer())))
    with pytest.raises(ValueError):
        ObservedTable(qualified_name=qn(), columns=(Column("X", integer()), Column("x", integer())))

def test_contains_accepts_str_and_column_instances():
    t = DesiredTable(qn(), (Column("A", integer()), Column("B", integer())))
    assert "a" in t
    assert Column("B", integer()) in t  # equality by fields

def test_get_column_returns_same_instance_by_identity_when_present():
    a = Column("A", integer())
    b = Column("B", integer())
    t = DesiredTable(qn(), (a, b))
    assert t.get_column("A") is a
    assert t.get_column("b") is b
    assert t.get_column("missing") is None

def test_immutability_enforced_for_snapshots():
    t = DesiredTable(qn(), (Column("A", integer()),))
    with pytest.raises(Exception):
        t.columns += (Column("B", integer()),)  # tuple is immutable