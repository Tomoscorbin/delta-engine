from __future__ import annotations
from dataclasses import FrozenInstanceError
import pytest
from tabula.domain.model import QualifiedName

def test_normalization_casefold_ascii() -> None:
    qn = QualifiedName("CAT", "Sales", "Transactions")
    assert qn.catalog == "cat"
    assert qn.schema == "sales"
    assert qn.name == "transactions"

def test_equality_and_hash_are_case_insensitive() -> None:
    a = QualifiedName("Cat", "Sales", "Transactions")
    b = QualifiedName("CAT", "SALES", "TRANSACTIONS")
    assert a == b
    assert hash(a) == hash(b)
    mapping = {a: "value"}
    assert mapping[b] == "value"

def test_parts_and_dotted() -> None:
    qn = QualifiedName("c", "s", "n")
    assert qn.parts == ("c", "s", "n")
    assert qn.dotted == "c.s.n"

def test_immutability_all_fields() -> None:
    qn = QualifiedName("c", "s", "n")
    with pytest.raises(FrozenInstanceError): qn.catalog = "x"  # type: ignore[attr-defined]
    with pytest.raises(FrozenInstanceError): qn.schema = "x"   # type: ignore[attr-defined]
    with pytest.raises(FrozenInstanceError): qn.name = "x"     # type: ignore[attr-defined]

@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": None, "schema": "s", "name": "n"},     # type: ignore[arg-type]
        {"catalog": "c", "schema": None, "name": "n"},     # type: ignore[arg-type]
        {"catalog": "c", "schema": "s", "name": None},     # type: ignore[arg-type]
        {"catalog": 123, "schema": "s", "name": "n"},      # type: ignore[arg-type]
    ],
)
def test_non_string_values_raise_type_error(kwargs) -> None:
    with pytest.raises(TypeError):
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

@pytest.mark.parametrize(
    "kwargs",
    [
        {"catalog": " c", "schema": "s", "name": "n"},
        {"catalog": "c ", "schema": "s", "name": "n"},
        {"catalog": "c", "schema": "\ts", "name": "n"},
        {"catalog": "c", "schema": "s", "name": "n\n"},
        {"catalog": "c", "schema": "s a", "name": "n"},
    ],
)
def test_whitespace_rejected(kwargs) -> None:
    with pytest.raises(ValueError):
        QualifiedName(**kwargs)

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

def test_non_ascii_rejected() -> None:
    with pytest.raises(ValueError):
        QualifiedName("c", "Satış", "n")   # non-ASCII 'ş'
    with pytest.raises(ValueError):
        QualifiedName("KİTAB", "s", "n")   # 'İ'
