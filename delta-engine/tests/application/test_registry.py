from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import pytest

from delta_engine.application.ports import ColumnObject
from delta_engine.application.registry import Registry
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.model.table import DesiredTable

# --- table/column specs (duck-typed to the ports) ----------------


@dataclass
class ColSpec:
    name: str
    data_type: Any
    is_nullable: bool = True


@dataclass
class TableSpec:
    catalog: str
    schema: str
    name: str
    columns: Iterable[ColumnObject]


# --- Helpers -----------------------------------------------------------------


def t(catalog: str, schema: str, name: str, cols: tuple[ColSpec, ...]) -> TableSpec:
    return TableSpec(catalog=catalog, schema=schema, name=name, columns=cols)


def c(name: str, dtype: object, nullable: bool = True) -> ColSpec:
    return ColSpec(name=name, data_type=dtype, is_nullable=nullable)


# --- Tests -------------------------------------------------------------------


def test_register_converts_to_desiredtable_with_normalized_identifiers() -> None:
    reg = Registry()
    spec = t(" Dev ", "Silver", "Test_1", (c("  ID  ", Integer()), c("Name", String(), False)))
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
        t("dev", "beta", "orders", (c("id", Integer()),)),
        t("dev", "alpha", "zzz", (c("id", Integer()),)),
        t("dev", "alpha", "aaa", (c("id", Integer()),)),
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
    a = t("Dev", "Silver", "People", (c("id", Integer()),))
    b = t("dev", "silver", "people", (c("id", Integer()),))
    reg.register(a)
    with pytest.raises(ValueError) as exc:
        reg.register(b)
    assert "Duplicate table registration" in str(exc.value)


def test_register_rejects_duplicate_fqn_in_same_call() -> None:
    reg = Registry()
    a = t("dev", "silver", "people", (c("id", Integer()),))
    b = t("DEV", "SILVER", "PEOPLE", (c("id", Integer()),))
    with pytest.raises(ValueError):
        reg.register(a, b)


def test_register_supports_varargs_multiple_tables() -> None:
    reg = Registry()
    a = t("dev", "silver", "people", (c("id", Integer()),))
    b = t("dev", "gold", "orders", (c("order_id", Integer()),))
    reg.register(a, b)
    fqns = [str(d.qualified_name) for d in reg]
    assert set(fqns) == {"dev.silver.people", "dev.gold.orders"}


def test_column_mapping_preserves_type_and_nullability() -> None:
    reg = Registry()
    spec = t("dev", "silver", "things", (c("x", Integer(), False), c("y", String(), True)))
    reg.register(spec)
    dt = next(iter(reg))
    assert [type(col.data_type) for col in dt.columns] == [Integer, String]
    assert [col.is_nullable for col in dt.columns] == [False, True]


def test_invalid_column_identifier_bubbles_up() -> None:
    reg = Registry()
    bad = t(
        "dev", "silver", "badcols", (c("bad-name", Integer()),)
    )  # hyphen invalid by our identifier rules
    with pytest.raises(ValueError):
        reg.register(bad)


def test_duplicate_column_names_bubble_up_from_desired_table() -> None:
    # Two columns that normalize to the same name ("ID" vs "id")
    reg = Registry()
    dup = t("dev", "silver", "dupcols", (c("ID", Integer()), c("id", String())))
    with pytest.raises(ValueError) as exc:
        reg.register(dup)
    assert "Duplicate column name" in str(exc.value)
