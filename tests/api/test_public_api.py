"""The user-facing api package re-exports the names needed to define a table."""

import delta_engine.api as api
from delta_engine.api.properties import Property as PropertyImpl
from delta_engine.api.table import DeltaTable as DeltaTableImpl
from delta_engine.domain.model import Array, Decimal, Map

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
    "Property",
    "String",
    "Timestamp",
}


def test_api_exposes_delta_table_column_and_all_data_types():
    # Given the api package a user imports to define a table
    # Then DeltaTable, Column, and every data type -- scalar and parameterised --
    # are importable from it directly, so defining a table never reaches into the
    # internal domain layer
    for name in _EXPECTED:
        assert hasattr(api, name), f"{name} not importable from delta_engine.api"

    # And the declared surface is EXACTLY this set -- so dropping or adding a name
    # to __all__ fails here rather than slipping through a subset check
    assert set(api.__all__) == _EXPECTED

    # And the re-exports resolve to the real types (single identity, not a shadow)
    assert api.DeltaTable is DeltaTableImpl
    assert api.Decimal is Decimal
    assert api.Array is Array
    assert api.Map is Map
    assert api.Property is PropertyImpl


def test_property_enum_lists_the_keys_deltatable_accepts():
    # Given the property vocabulary a user must declare against
    # Then the keys DeltaTable validates against are discoverable via the public
    # Property enum, rather than only surfacing as a runtime rejection
    from delta_engine.api.properties import MANAGED_PROPERTY_KEYS

    assert {member.value for member in api.Property} == set(MANAGED_PROPERTY_KEYS)

    # And Property is a str enum, so its members can be used directly as dict keys
    assert api.Property.COLUMN_MAPPING_MODE == "delta.columnMapping.mode"
