"""The user-facing schema package re-exports the names needed to define a table."""

from delta_engine.domain.model import Array, Decimal, Map
import delta_engine.schema as schema
from delta_engine.schema.table import DeltaTable as DeltaTableImpl

_EXPECTED = {
    "DeltaTable",
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


def test_schema_exposes_delta_table_column_and_all_data_types():
    # Given the schema package a user imports to define a table
    # Then DeltaTable, Column, and every data type -- scalar and parameterised --
    # are importable from it directly, so defining a table never reaches into the
    # internal domain layer
    for name in _EXPECTED:
        assert hasattr(schema, name), f"{name} not importable from delta_engine.schema"

    # And the declared surface is EXACTLY this set -- so dropping or adding a name
    # to __all__ fails here rather than slipping through a subset check
    assert set(schema.__all__) == _EXPECTED

    # And the re-exports resolve to the real types (single identity, not a shadow)
    assert schema.DeltaTable is DeltaTableImpl
    assert schema.Decimal is Decimal
    assert schema.Array is Array
    assert schema.Map is Map
