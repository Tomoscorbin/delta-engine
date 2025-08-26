import pytest

from tabula.domain.model.qualified_name import QualifiedName


def test_qualified_name_normalizes_each_part_and_compares_case_insensitively():
    a = QualifiedName(catalog="CAT", schema="SALES", name="ORDERS")
    b = QualifiedName(catalog="cat", schema="sales", name="orders")
    assert a == b


def test_str_is_canonical_join_of_parts():
    qn = QualifiedName(catalog="core", schema="gold", name="orders")
    assert str(qn) == "core.gold.orders"


@pytest.mark.parametrize("kw", [
    {"catalog": "bad.name", "schema": "x", "name": "y"},
    {"catalog": "x", "schema": "bad name", "name": "y"},
    {"catalog": "x", "schema": "y", "name": "bad name"},
])
def test_qualified_name_rejects_invalid_parts(kw):
    with pytest.raises(ValueError):
        QualifiedName(**kw)  # type: ignore[arg-type]
