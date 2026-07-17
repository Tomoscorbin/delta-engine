import pytest

from delta_engine.domain.model.qualified_name import QualifiedName


def test_string_representation_is_canonical_fully_qualified_name() -> None:
    # Given: mixed-case parts
    qn = QualifiedName("core", "public", "orders")
    # When: converting to string
    # Then: the canonical fully-qualified form is returned
    assert str(qn) == "core.public.orders"


def test_mixed_case_parts_normalize_to_lowercase() -> None:
    # Given mixed-case parts (identifiers are case-insensitive on Databricks,
    # and Unity Catalog stores object names lowercase)
    qn = QualifiedName("MyCatalog", "MySchema", "MyTable")

    # Then every part is stored in the canonical lowercase form
    assert qn.parts == ("mycatalog", "myschema", "mytable")
    assert qn == QualifiedName("mycatalog", "myschema", "mytable")


def test_lowercase_unicode_parts_are_preserved_verbatim() -> None:
    # Given a part that is already lowercase but casefold-divergent
    # (casefold would rewrite 'straße' to 'strasse' — a different identifier)
    qn = QualifiedName("catalog", "schema", "straße")

    # Then normalization does not change an already-lowercase name
    assert qn.name == "straße"


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


def test_parse_reads_a_canonical_three_part_name() -> None:
    # Given a canonical catalog.schema.name string
    # When parsing it
    parsed = QualifiedName.parse("core.public.orders")

    # Then each part is populated
    assert parsed == QualifiedName("core", "public", "orders")


@pytest.mark.parametrize(
    "raw",
    ["orders", "schema.orders", "cat.schema.orders.extra", ""],
    ids=["one-part", "two-parts", "four-parts", "empty"],
)
def test_parse_rejects_names_without_exactly_three_parts(raw: str) -> None:
    # Given a string that is not catalog.schema.name
    # When / Then parsing fails
    with pytest.raises(ValueError, match="fully qualified"):
        QualifiedName.parse(raw)


def test_parse_normalizes_parts_like_the_constructor() -> None:
    # Given a three-part string with an uppercase part
    # When parsing it
    parsed = QualifiedName.parse("Core.public.orders")

    # Then the part is normalized to the same canonical form the constructor produces
    assert parsed == QualifiedName("core", "public", "orders")
