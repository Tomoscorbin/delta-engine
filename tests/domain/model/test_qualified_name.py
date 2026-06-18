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


@pytest.mark.parametrize(
    "catalog, schema, name",
    [
        ("", "schema", "table"),
        ("catalog", "  ", "table"),
        ("catalog", "schema", "\t"),
    ],
    ids=["empty-catalog", "whitespace-schema", "whitespace-name"],
)
def test_raises_when_any_part_is_blank(catalog: str, schema: str, name: str) -> None:
    # Given a blank or whitespace-only part (would render as a malformed
    # identifier like `.schema.table` or `catalog..table`)
    # When/Then construction fails rather than producing invalid SQL
    with pytest.raises(ValueError, match="blank"):
        QualifiedName(catalog, schema, name)
