from dataclasses import FrozenInstanceError

import pytest

from delta_engine.domain.model.qualified_name import QualifiedName


def test_qualified_name_normalizes_and_stringifies() -> None:
    qn = QualifiedName(" Dev ", "Silver", "Test")
    assert qn.fully_qualified_name == "dev.silver.test"
    assert str(qn) == "dev.silver.test"


def test_qualified_name_is_frozen() -> None:
    qn = QualifiedName("dev", "silver", "test")
    with pytest.raises(FrozenInstanceError):
        qn.schema = "x"  # type: ignore[misc]


@pytest.mark.parametrize(
    "catalog,schema,name",
    [
        ("dev", "silver", "bad.name"),
        ("dev", "sil ver", "test"),
        ("naïve", "silver", "test"),
    ],
)
def test_qualified_name_invalid_parts_raise(catalog: str, schema: str, name: str) -> None:
    with pytest.raises(ValueError):
        QualifiedName(catalog, schema, name)


def test_qualified_name_equality_and_hash_after_normalization() -> None:
    a = QualifiedName("DEV", "SILVER", "TEST")
    b = QualifiedName("dev", "silver", "test")
    c = QualifiedName("dev", "silver", "other")
    assert a == b
    assert a != c

    d = {a: 1, b: 2}
    assert len(d) == 1
    assert d[b] == 2
