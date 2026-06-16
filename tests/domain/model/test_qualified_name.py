from delta_engine.domain.model.qualified_name import QualifiedName


def test_string_representation_is_canonical_fully_qualified_name() -> None:
    # Given: mixed-case parts
    qn = QualifiedName("core", "public", "orders")
    # When: converting to string
    # Then: the canonical fully-qualified form is returned
    assert str(qn) == "core.public.orders"


def test_parts_are_normalised_to_lowercase() -> None:
    # Given: a qualified name with mixed-case parts
    # When: constructing a QualifiedName
    qn = QualifiedName("Mycatalog", "MySchema", "MyTable")
    # Then: all parts are stored in lowercase
    assert qn.catalog == "mycatalog"
    assert qn.schema == "myschema"
    assert qn.name == "mytable"


def test_qualified_names_with_same_parts_different_case_are_equal() -> None:
    # Given: two qualified names with the same parts in different cases
    # When: comparing them
    # Then: they are equal because parts are normalised
    assert QualifiedName("Dev", "Silver", "Orders") == QualifiedName("dev", "silver", "orders")
