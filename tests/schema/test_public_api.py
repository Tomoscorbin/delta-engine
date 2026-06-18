"""The user-facing schema package re-exports the names needed to define a table."""

from delta_engine.domain.model import Array, Decimal, Map
import delta_engine.schema as schema

_EXPECTED = {
    "Array",
    "Boolean",
    "Column",
    "Date",
    "Decimal",
    "Double",
    "Float",
    "Integer",
    "Long",
    "Map",
    "String",
    "Timestamp",
}


def test_schema_exposes_column_and_all_data_types():
    # Given the schema package a user imports to define a table
    # Then Column and every data type -- scalar and parameterised -- are importable
    # from it directly, so a Decimal/Array/Map column needs no reach into the
    # internal domain layer
    for name in _EXPECTED:
        assert hasattr(schema, name), f"{name} not importable from delta_engine.schema"

    # And the declared surface advertises them
    assert _EXPECTED <= set(schema.__all__)

    # And the parameterised re-exports resolve to the real domain types (single identity)
    assert schema.Decimal is Decimal
    assert schema.Array is Array
    assert schema.Map is Map
