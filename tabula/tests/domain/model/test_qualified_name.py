import pytest
from dataclasses import FrozenInstanceError

from tabula.domain.model.qualified_name import QualifiedName


# ---------------------------
# Construction & normalization
# ---------------------------

def test_normalization_casefold() -> None:
    qn = QualifiedName("CAT", "Sales", "Transactions")
    assert qn.catalog == "cat"
    assert qn.schema == "sales"
    assert qn.name == "transactions"


def test_equality_and_hash_are_case_insensitive() -> None:
    a = QualifiedName("Cat", "Sales", "Transactions")
    b = QualifiedName("CAT", "SALES", "TRANSACTIONS")
    assert a == b
    assert hash(a) == hash(b)
    as_dict = {a: "value"}
    assert as_dict[b] == "value"  # hashing compatibility


# ---------------------------
# Convenience properties / dunder
# ---------------------------

def test_parts_and_dotted_and_str() -> None:
    qn = QualifiedName("c", "s", "n")
    assert qn.parts == ("c", "s", "n")
    assert qn.dotted == "c.s.n"
    assert str(qn) == "c.s.n"


def test_repr_contains_fields() -> None:
    qn = QualifiedName("c", "s", "n")
    r = repr(qn)
    assert r == "QualifiedName(catalog='c', schema='s', name='n')"

# ---------------------------
# Immutability
# ---------------------------

def test_immutability() -> None:
    qn = QualifiedName("c", "s", "n")
    with pytest.raises(FrozenInstanceError):
        qn.catalog = "x"


# ---------------------------
# Validation: None / empty
# ---------------------------

@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": None, "schema": "s", "name": "n"},
        {"catalog": "c", "schema": None, "name": "n"},
        {"catalog": "c", "schema": "s", "name": None},
    ],
)
def test_none_values_raise_value_error(kwargs) -> None:
    with pytest.raises(ValueError):
        QualifiedName(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": "", "schema": "s", "name": "n"},
        {"catalog": "c", "schema": "", "name": "n"},
        {"catalog": "c", "schema": "s", "name": ""},
    ],
)
def test_empty_values_raise_value_error(kwargs) -> None:
    with pytest.raises(ValueError):
        QualifiedName(**kwargs)


# ---------------------------
# Validation: whitespace rules (strict)
#   - Reject leading/trailing whitespace
#   - Reject ANY internal whitespace (space, tab, newline, etc.)
# ---------------------------

@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": " c", "schema": "s", "name": "n"},
        {"catalog": "c ", "schema": "s", "name": "n"},
        {"catalog": "c", "schema": "\ts", "name": "n"},
        {"catalog": "c", "schema": "s", "name": "n\n"},
    ],
)
def test_leading_or_trailing_whitespace_rejected(kwargs) -> None:
    with pytest.raises(ValueError):
        QualifiedName(**kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": "c at", "schema": "s", "name": "n"},
        {"catalog": "c", "schema": "s\tales", "name": "n"},
        {"catalog": "c", "schema": "s", "name": "na\nme"},
    ],
)
def test_any_internal_whitespace_is_rejected(kwargs) -> None:
    with pytest.raises(ValueError):
        QualifiedName(**kwargs)


# ---------------------------
# Validation: dot inside a part
# ---------------------------

@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": "c.at", "schema": "s", "name": "n"},
        {"catalog": "c", "schema": "s.ch", "name": "n"},
        {"catalog": "c", "schema": "s", "name": "n.a"},
    ],
)
def test_parts_must_not_contain_dot(kwargs) -> None:
    with pytest.raises(ValueError):
        QualifiedName(**kwargs)


# ---------------------------
# Type safety (strict)
# ---------------------------

@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": 123, "schema": "s", "name": "n"},
        {"catalog": "c", "schema": object(), "name": "n"},
        {"catalog": "c", "schema": "s", "name": ["n"]},
    ],
)
def test_non_string_values_raise_type_error(kwargs) -> None:
    with pytest.raises(TypeError):
        QualifiedName(**kwargs)

def test_identifier_valid_examples() -> None:
    QualifiedName("catlog_1", "sales_2025", "transactions_v2")  # no exception

def test_allows_symbols_except_whitespace_and_dot() -> None:
    q1 = QualifiedName("1catalog", "s", "n")         # leading digit allowed
    q2 = QualifiedName("c", "sales-data", "n")       # hyphen allowed
    q3 = QualifiedName("c", "s", "orders/data")      # slash allowed
    q4 = QualifiedName("c", "s", "100%")             # percent allowed

    assert q1.dotted == "1catalog.s.n"
    assert q2.dotted == "c.sales-data.n"
    assert q3.dotted == "c.s.orders/data"
    assert q4.dotted == "c.s.100%"
