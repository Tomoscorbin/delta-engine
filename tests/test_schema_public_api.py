"""The delta_engine.schema module is the public declaration import path."""

from delta_engine.api.table import (
    DeltaTable as DeltaTableImpl,
    ForeignKey as ForeignKeyImpl,
    Self as SelfImpl,
)
from delta_engine.application.properties import Property as PropertyImpl
from delta_engine.domain.model import (
    Array,
    Boolean,
    Column,
    Date,
    Decimal,
    Double,
    Float,
    Integer,
    Long,
    Map,
    String,
    Timestamp,
)
import delta_engine.schema as schema

_EXPECTED = {
    "DeltaTable",
    "Array",
    "Boolean",
    "Column",
    "Date",
    "Decimal",
    "Double",
    "Float",
    "ForeignKey",
    "Integer",
    "Long",
    "Map",
    "Property",
    "Self",
    "String",
    "Timestamp",
}


def test_schema_exposes_delta_table_column_and_all_data_types():
    # Given the public schema import path
    # Then DeltaTable, Column, and every data type -- scalar and parameterised --
    # are importable from it directly.
    for name in _EXPECTED:
        assert hasattr(schema, name), f"{name} not importable from delta_engine.schema"

    # And the declared surface is EXACTLY this set -- so dropping or adding a name
    # to __all__ fails here rather than slipping through a subset check
    assert set(schema.__all__) == _EXPECTED

    # And the re-exports resolve to the real types (single identity, not a shadow)
    assert schema.DeltaTable is DeltaTableImpl
    assert schema.ForeignKey is ForeignKeyImpl
    assert schema.Self is SelfImpl
    assert schema.Column is Column
    assert schema.Array is Array
    assert schema.Boolean is Boolean
    assert schema.Date is Date
    assert schema.Decimal is Decimal
    assert schema.Double is Double
    assert schema.Float is Float
    assert schema.Integer is Integer
    assert schema.Long is Long
    assert schema.Map is Map
    assert schema.String is String
    assert schema.Timestamp is Timestamp
    assert schema.Property is PropertyImpl


def test_property_enum_lists_the_keys_deltatable_accepts():
    # Given the property vocabulary a user must declare against — the enum is
    # the single source, and the catalogue the engine validates against is
    # built from it, so the registry covers exactly the enum's keys
    from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY

    assert {member.value for member in schema.Property} == set(DELTA_PROPERTY_REGISTRY)

    # And Property is a str enum, so its members can be used directly as dict keys
    assert schema.Property.COLUMN_MAPPING_MODE == "delta.columnMapping.mode"
