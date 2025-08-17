import pytest
from hypothesis import given
from tests.conftest import qname_strat
from tabula.domain.model.qualified_name import QualifiedName

def make_name(c="Cat", s="Sales", n="Orders") -> QualifiedName:
    return QualifiedName(c, s, n)


def test_str_is_canonical_dotted():
    q = QualifiedName("Cat", "Sch", "Tbl")
    assert str(q) == "cat.sch.tbl"

def test_rejects_dots_and_empty():
    with pytest.raises(ValueError): QualifiedName("a.b", "s", "n")
    with pytest.raises(ValueError): QualifiedName("", "s", "n")
    with pytest.raises(ValueError): QualifiedName("a", " ", "n")

@given(qname_strat())
def test_case_insensitive_normalization(q):
    # already normalized in constructor
    assert q.catalog == q.catalog.casefold()
    assert q.schema == q.schema.casefold()
    assert q.name == q.name.casefold()

def test_equality_and_hash_case_insensitive():
    a = QualifiedName("Cat", "Sch", "Tbl")
    b = QualifiedName("cat", "SCH", "tBl")
    assert a == b
    assert hash(a) == hash(b)
    # usable as dict keys
    d = {a: "x"}
    assert d[b] == "x"

def test_str_is_canonical_and_stable():
    q = make_name("CAT", "DATA", "WIDGETS")
    assert str(q) == "cat.data.widgets"

def test_reject_embedded_dot_in_any_part():
    with pytest.raises(ValueError): QualifiedName("a.b", "c", "d")
    with pytest.raises(ValueError): QualifiedName("a", "c.d", "e")
    with pytest.raises(ValueError): QualifiedName("a", "c", "d.e")

def test_reject_empty_or_whitespace_only_parts():
    with pytest.raises(ValueError): QualifiedName("", "c", "t")
    with pytest.raises(ValueError): QualifiedName("a", " ", "t")
    with pytest.raises(ValueError): QualifiedName("a", "c", "\t")

def test_reject_leading_or_trailing_whitespace_parts():
    # If your implementation currently allows this, tighten validation.
    with pytest.raises(ValueError): QualifiedName(" a", "c", "t")
    with pytest.raises(ValueError): QualifiedName("a", "c ", "t")
    with pytest.raises(ValueError): QualifiedName("a", "c", " t ")

def test_repr_is_unambiguous_for_debugging():
    q = make_name()
    r = repr(q)
    assert "QualifiedName" in r and "cat" in r and "sales" in r and "orders" in r