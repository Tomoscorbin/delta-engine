from __future__ import annotations

from dataclasses import FrozenInstanceError
import pytest

from tabula.domain.model import Column, DataType
from tabula.domain.model.data_type.types import integer, string, decimal

# ---------------------------
# Construction & normalization
# ---------------------------

def test_name_normalization_ascii_lower():
    c = Column("UserID", integer(), is_nullable=False)
    assert c.name == "userid"

def test_default_nullability_is_true():
    c = Column("email", string())  # default nullable
    assert c.is_nullable is True

def test_non_ascii_name_rejected():
    with pytest.raises(ValueError):
        Column("İD", integer())  # dotted capital I (non-ASCII)

# ---------------------------
# Validation: empty / whitespace / dots / types
# ---------------------------

def test_reject_empty_name():
    with pytest.raises(ValueError):
        Column("", string())

def test_reject_whitespace_name():
    with pytest.raises(ValueError):
        Column("   ", integer())

@pytest.mark.parametrize("bad", [" id", "id ", "\tid", "id\n"])
def test_reject_leading_or_trailing_whitespace(bad):
    with pytest.raises(ValueError):
        Column(bad, integer())

@pytest.mark.parametrize("bad", ["first name", "first\tname", "first\nname", "na\u00A0me"])
def test_reject_internal_whitespace_including_nbsp(bad):
    with pytest.raises(ValueError):
        Column(bad, string())

def test_reject_dot_in_name():
    with pytest.raises(ValueError):
        Column("user.id", integer())

def test_non_string_name_raises_type_error():
    with pytest.raises(TypeError):
        Column(123, integer())  # type: ignore[arg-type]

def test_data_type_must_be_datatype_instance():
    with pytest.raises(TypeError):
        Column("id", None)  # type: ignore[arg-type]

# ---------------------------
# Equality & hashing
# ---------------------------

def test_equality_is_case_insensitive_on_name_and_sensitive_on_rest():
    c1 = Column("UserId", integer(), is_nullable=False)
    c2 = Column("userid", integer(), is_nullable=False)
    c3 = Column("UserID", string(), is_nullable=False)
    assert c1 == c2
    assert hash(c1) == hash(c2)
    assert c1 != c3  # different type

def test_equality_differs_on_nullability():
    a = Column("id", integer(), is_nullable=False)
    b = Column("ID", integer(), is_nullable=True)
    assert a != b

def test_columns_with_same_name_and_type_are_equal_even_if_casing_differs():
    a = Column("CreatedAt", string())
    b = Column("createdat", string())
    assert a == b
    assert hash(a) == hash(b)

def test_column_equality_is_sensitive_to_datatype_parameters():
    a = Column("price", decimal(18, 2))
    b = Column("price", decimal(18, 3))
    assert a != b

def test_column_equality_uses_datatype_value_semantics():
    a = Column("note", DataType("STRING"))
    b = Column("note", DataType("string"))
    assert a == b

def test_hash_compatibility_with_dict_keys():
    a = Column("id", integer(), is_nullable=False)
    b = Column("ID", integer(), is_nullable=False)
    d = {a: 123}
    assert d[b] == 123

# ---------------------------
# Immutability
# ---------------------------

def test_frozen_prevents_reassignment():
    c = Column("id", integer())
    with pytest.raises(FrozenInstanceError):
        c.name = "x"  # type: ignore[attr-defined]
    with pytest.raises(FrozenInstanceError):
        c.data_type = string()  # type: ignore[attr-defined]
    with pytest.raises(FrozenInstanceError):
        c.is_nullable = False  # type: ignore[attr-defined]
