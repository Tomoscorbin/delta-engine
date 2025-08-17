import pytest
from tabula.domain.model.column import Column
from tabula.domain.model.types import integer, string

def test_name_normalization_and_spec():
    c = Column("UserID", integer(), is_nullable=False)
    assert c.name == "userid"
    assert str(c) == "userid int not null"

def test_reject_empty_name():
    with pytest.raises(ValueError):
        Column("", string())

def test_equality_is_case_insensitive_on_name_and_sensitive_on_rest():
    c1 = Column("UserId", integer(), is_nullable=False)
    c2 = Column("userid", integer(), is_nullable=False)
    c3 = Column("UserID", string(), is_nullable=False)
    assert c1 == c2
    assert c1 != c3  # different type

def test_spec_shows_not_null_suffix():
    c = Column("Email", string(), is_nullable=False)
    assert str(c).endswith("not null")

def test_reject_blank_or_whitespace_names():
    with pytest.raises(ValueError): Column("", integer())
    with pytest.raises(ValueError): Column("   ", integer())

def test_name_is_normalized_and_preserved_in_spec():
    c = Column("CamelCase", integer())
    assert c.name == "camelcase"
    assert str(c).startswith("camelcase ")