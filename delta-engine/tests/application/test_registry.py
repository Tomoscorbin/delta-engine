import pytest

from delta_engine.application.registry import Registry
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.model.table import DesiredTable
from tests.factories import ColumnSpec, make_table_spec

# --- Tests -------------------------------------------------------------------


def test_register_converts_to_desiredtable_with_normalized_identifiers() -> None:
    reg = Registry()
    spec = make_table_spec(
        " Dev ",
        "Silver",
        "Test_1",
        (ColumnSpec("  ID  ", Integer()), ColumnSpec("Name", String(), False)),
    )
    reg.register(spec)

    items = list(reg)
    assert len(items) == 1
    dt = items[0]
    assert isinstance(dt, DesiredTable)
    assert str(dt.qualified_name) == "dev.silver.test_1"
    assert [col.name for col in dt.columns] == ["id", "name"]
    assert [type(col.data_type) for col in dt.columns] == [Integer, String]
    assert [col.is_nullable for col in dt.columns] == [True, False]


def test_iteration_is_sorted_by_fully_qualified_name() -> None:
    reg = Registry()
    # Intentionally register out of order
    reg.register(
        make_table_spec("dev", "beta", "orders", (ColumnSpec("id", Integer()),)),
        make_table_spec("dev", "alpha", "zzz", (ColumnSpec("id", Integer()),)),
        make_table_spec("dev", "alpha", "aaa", (ColumnSpec("id", Integer()),)),
    )
    fqns = [str(d.qualified_name) for d in reg]
    assert (
        fqns
        == sorted(fqns)
        == [
            "dev.alpha.aaa",
            "dev.alpha.zzz",
            "dev.beta.orders",
        ]
    )


def test_register_rejects_duplicate_fqn_case_insensitive_across_calls() -> None:
    reg = Registry()
    a = make_table_spec("Dev", "Silver", "People", (ColumnSpec("id", Integer()),))
    b = make_table_spec("dev", "silver", "people", (ColumnSpec("id", Integer()),))
    reg.register(a)
    with pytest.raises(ValueError) as exc:
        reg.register(b)
    assert "Duplicate table registration" in str(exc.value)


def test_register_rejects_duplicate_fqn_in_same_call() -> None:
    reg = Registry()
    a = make_table_spec("dev", "silver", "people", (ColumnSpec("id", Integer()),))
    b = make_table_spec("DEV", "SILVER", "PEOPLE", (ColumnSpec("id", Integer()),))
    with pytest.raises(ValueError):
        reg.register(a, b)


def test_register_supports_varargs_multiple_tables() -> None:
    reg = Registry()
    a = make_table_spec("dev", "silver", "people", (ColumnSpec("id", Integer()),))
    b = make_table_spec("dev", "gold", "orders", (ColumnSpec("order_id", Integer()),))
    reg.register(a, b)
    fqns = [str(d.qualified_name) for d in reg]
    assert set(fqns) == {"dev.silver.people", "dev.gold.orders"}


def test_column_mapping_preserves_type_and_nullability() -> None:
    reg = Registry()
    spec = make_table_spec(
        "dev",
        "silver",
        "things",
        (ColumnSpec("x", Integer(), False), ColumnSpec("y", String(), True)),
    )
    reg.register(spec)
    dt = next(iter(reg))
    assert [type(col.data_type) for col in dt.columns] == [Integer, String]
    assert [col.is_nullable for col in dt.columns] == [False, True]


def test_invalid_column_identifier_bubbles_up() -> None:
    reg = Registry()
    bad = make_table_spec(
        "dev", "silver", "badcols", (ColumnSpec("bad-name", Integer()),)
    )  # hyphen invalid by our identifier rules
    with pytest.raises(ValueError):
        reg.register(bad)


def test_duplicate_column_names_bubble_up_from_desired_table() -> None:
    # Two columns that normalize to the same name ("ID" vs "id")
    reg = Registry()
    dup = make_table_spec(
        "dev", "silver", "dupcols", (ColumnSpec("ID", Integer()), ColumnSpec("id", String()))
    )
    with pytest.raises(ValueError) as exc:
        reg.register(dup)
    assert "Duplicate column name" in str(exc.value)
