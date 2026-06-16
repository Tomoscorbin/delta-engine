import pytest

from delta_engine.domain.model.qualified_name import QualifiedName


def test_string_representation_is_canonical_fully_qualified_name() -> None:
    # Given: mixed-case parts
    qn = QualifiedName("core", "public", "orders")
    # When: converting to string
    # Then: the canonical fully-qualified form is returned
    assert str(qn) == "core.public.orders"


def test_raises_when_any_part_is_not_lowercase() -> None:
    # Given / When / Then: each part raises when it contains uppercase
    with pytest.raises(ValueError, match="lowercase"):
        QualifiedName("MyCatalog", "schema", "table")

    with pytest.raises(ValueError, match="lowercase"):
        QualifiedName("catalog", "MySchema", "table")

    with pytest.raises(ValueError, match="lowercase"):
        QualifiedName("catalog", "schema", "MyTable")
