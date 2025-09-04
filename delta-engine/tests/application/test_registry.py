import pytest

from delta_engine.adapters.schema.column import Column
from delta_engine.adapters.schema.delta.properties import Property
from delta_engine.adapters.schema.delta.table import DeltaTable
from delta_engine.application.registry import Registry
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.model.table import DesiredTable


def test_register_builds_desiredtable_and_preserves_types_nullability_and_comments() -> None:
    reg = Registry()
    reg.register(
        DeltaTable(
            "dev",
            "silver",
            "things",
            [
                Column("id", Integer(), True, comment="pk"),
                Column("name", String(), False, comment="person name"),
            ],
        )
    )

    dt = next(iter(reg))
    assert isinstance(dt, DesiredTable)
    assert len(dt.columns) == 2
    assert [type(c.data_type) for c in dt.columns] == [Integer, String]
    assert [c.is_nullable for c in dt.columns] == [True, False]
    assert [c.comment for c in dt.columns] == ["pk", "person name"]


def test_register_supports_varargs_multiple_tables() -> None:
    reg = Registry()
    a = DeltaTable("dev", "silver", "people", (Column("id", Integer()),))
    b = DeltaTable("dev", "gold", "orders", (Column("order_id", Integer()),))
    reg.register(a, b)
    fqns = [str(d.qualified_name) for d in reg]
    assert set(fqns) == {"dev.silver.people", "dev.gold.orders"}


def test_iteration_is_sorted_by_fully_qualified_name() -> None:
    reg = Registry()
    # Intentionally register out of order
    reg.register(
        DeltaTable(
            "dev",
            "beta",
            "orders",
            [Column("id", Integer())],
        ),
        DeltaTable("dev", "alpha", "zzz", [Column("id", Integer())]),
        DeltaTable("dev", "alpha", "aaa", [Column("id", Integer())]),
    )
    fqns = [str(d.qualified_name) for d in reg]
    assert fqns == sorted(fqns)


def test_register_rejects_duplicate_fqn_across_calls() -> None:
    reg = Registry()
    a = DeltaTable("dev", "silver", "people", [Column("id", Integer())])
    b = DeltaTable("dev", "silver", "people", [Column("id", Integer())])
    reg.register(a)
    with pytest.raises(ValueError):
        reg.register(b)


def test_register_rejects_duplicate_fqn_in_same_call() -> None:
    reg = Registry()
    a = DeltaTable("dev", "silver", "people", [Column("id", Integer())])
    b = DeltaTable("dev", "silver", "people", [Column("id", Integer())])
    with pytest.raises(ValueError):
        reg.register(a, b)


def test_register_preserves_properties_from_spec_without_injection() -> None:
    reg = Registry()
    spec = DeltaTable(
        "dev",
        "silver",
        "with_props",
        [Column("id", Integer())],
        properties={Property.CHANGE_DATA_FEED.value: "true"},
    )
    reg.register(spec)
    dt = next(iter(reg))
    assert (Property.CHANGE_DATA_FEED.value, "true") in dt.properties.items()
