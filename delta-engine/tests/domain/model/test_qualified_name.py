import pytest

from delta_engine.domain.model.qualified_name import QualifiedName


def test_canonicalizes_identifier_parts_case_insensitively() -> None:
    # Given: mixed/whitespace-padded identifier parts
    # When: constructing a qualified name
    qn = QualifiedName(" Dev ", "Silver", "Test")
    # Then: parts are canonicalized (trimmed, lowercased, validated)
    assert (qn.catalog, qn.schema, qn.name) == ("dev", "silver", "test")


def test_value_equality_is_case_insensitive() -> None:
    # Given: two identifiers differing only by input case
    a = QualifiedName("CORE", "PUBLIC", "ORDERS")
    b = QualifiedName("core", "public", "orders")
    # When/Then: they represent the same canonical identity
    assert a == b


@pytest.mark.parametrize(
    "catalog,schema,name",
    [
        ("dev", "silver", "bad.name"),  # dot in name
        ("dev", "sil ver", "test"),  # space in schema
        ("naïve", "silver", "test"),  # non-normal ASCII (policy: reject)
        ("", "silver", "test"),  # empty catalog
        ("core/prod", "silver", "test"),  # slash in catalog
    ],
)
def test_rejects_invalid_identifier_parts(catalog: str, schema: str, name: str) -> None:
    # Given: an invalid identifier component
    # When/Then: construction fails with a validation error
    with pytest.raises(ValueError):
        QualifiedName(catalog, schema, name)


def test_string_representation_is_canonical_fully_qualified_name() -> None:
    # Given: mixed-case parts
    qn = QualifiedName("Core", "Public", "Orders")
    # When: converting to string
    # Then: the canonical fully-qualified form is returned
    assert str(qn) == "core.public.orders"
