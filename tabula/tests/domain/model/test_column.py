import pytest

from tabula.domain.model.column import Column
from tabula.domain.model.types import integer, string


# ---------------------------
# Construction & normalization
# ---------------------------

def test_name_normalization():
    c = Column("UserID", integer(), is_nullable=False)
    assert c.name == "userid"
    assert str(c) == "userid int not null"
    assert repr(c) == "Column(name='userid', data_type=DataType(name='int', parameters=()), is_nullable=False)"


def test_name_is_normalized():
    c = Column("CamelCase", integer())
    assert c.name == "camelcase"
    assert str(c).startswith("camelcase ")


def test_default_nullability_is_true():
    c = Column("email", string())  # default nullable
    assert c.is_nullable is True

# ---------------------------
# Validation: empty / whitespace / dots / type
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


@pytest.mark.parametrize("bad", ["first name", "first\tname", "first\nname"])
def test_reject_internal_whitespace(bad):
    with pytest.raises(ValueError):
        Column(bad, string())


def test_reject_dot_in_name():
    with pytest.raises(ValueError):
        Column("user.id", integer())


def test_non_string_name_raises_type_error():
    with pytest.raises(TypeError):
        Column(123, integer())


def test_data_type_must_be_datatype_instance():
    with pytest.raises(TypeError):
        Column("id", None)


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
