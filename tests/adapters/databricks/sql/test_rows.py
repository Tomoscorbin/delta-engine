"""
Direct tests for the shared information_schema reads.

No Spark session, no fakes beyond a stub runner keyed by the exact query
text: rows are plain attribute-style rows — matching how the real query
results are accessed — and each read returns a domain value.
"""

from types import SimpleNamespace

from hypothesis import given, strategies as st
import pytest

from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    foreign_keys_query,
    primary_key_query,
    read_column_tags,
    read_foreign_keys,
    read_primary_key,
    read_referencing_foreign_keys,
    read_table_tags,
    referencing_foreign_keys_query,
    schema_exists,
    schema_exists_query,
    table_tags_query,
)
from delta_engine.domain.model import (
    ForeignKeyConstraint,
    ForeignKeyReference,
    PrimaryKeyConstraint,
    QualifiedName,
)
from tests.adapters.databricks.sql.strategies import SQL_IDENTIFIERS

QN = QualifiedName("dev", "silver", "orders")


def _runner(query, rows):
    """Answer exactly one expected query with the given rows."""

    def run(actual_query):
        assert actual_query == query
        return rows

    return run


# ---------- schema existence ----------


def test_schema_exists_when_the_probe_returns_a_row() -> None:
    assert schema_exists(_runner(schema_exists_query(QN), [("silver",)]), QN) is True


def test_schema_does_not_exist_when_the_probe_returns_no_rows() -> None:
    assert schema_exists(_runner(schema_exists_query(QN), []), QN) is False


# ---------- primary key ----------


def test_primary_key_rows_map_to_ordered_casefolded_columns() -> None:
    rows = [
        SimpleNamespace(constraint_name="Orders_PK", column_name="Order_Id"),
        SimpleNamespace(constraint_name="Orders_PK", column_name="Line_No"),
    ]

    result = read_primary_key(_runner(primary_key_query(QN), rows), QN)

    assert result == PrimaryKeyConstraint(
        columns=("order_id", "line_no"), constraint_name="orders_pk"
    )


def test_primary_key_empty_rows_map_to_none() -> None:
    assert read_primary_key(_runner(primary_key_query(QN), []), QN) is None


# ---------- owned foreign keys ----------


def test_foreign_key_rows_map_to_casefolded_constraint() -> None:
    rows = [
        SimpleNamespace(
            constraint_name="Orders_Customer_FK",
            local_column="Customer_Id",
            referenced_catalog="Dev",
            referenced_schema="Silver",
            referenced_table="Customer",
            referenced_column="Id",
        ),
    ]

    result = read_foreign_keys(_runner(foreign_keys_query(QN), rows), QN)

    assert result == (
        ForeignKeyConstraint(
            local_columns=("customer_id",),
            referenced_table=QualifiedName("dev", "silver", "customer"),
            referenced_columns=("id",),
            constraint_name="orders_customer_fk",
        ),
    )


def test_composite_foreign_key_keeps_each_local_referenced_pair_together() -> None:
    # Rows arrive in the foreign key's column order; the read preserves each
    # (local, referenced) pair so the domain's canonical sort keeps them aligned.
    rows = [
        SimpleNamespace(
            constraint_name="fk_ab",
            local_column="b",
            referenced_catalog="c",
            referenced_schema="s",
            referenced_table="parent",
            referenced_column="y",
        ),
        SimpleNamespace(
            constraint_name="fk_ab",
            local_column="a",
            referenced_catalog="c",
            referenced_schema="s",
            referenced_table="parent",
            referenced_column="x",
        ),
    ]

    [fk] = read_foreign_keys(_runner(foreign_keys_query(QN), rows), QN)

    assert fk.local_columns == ("a", "b")
    assert fk.referenced_columns == ("x", "y")  # a->x and b->y preserved through the sort
    assert fk.referenced_table == QualifiedName("c", "s", "parent")


def test_multiple_foreign_keys_group_by_constraint_name() -> None:
    rows = [
        SimpleNamespace(
            constraint_name="fk_one",
            local_column="a",
            referenced_catalog="c",
            referenced_schema="s",
            referenced_table="p1",
            referenced_column="x",
        ),
        SimpleNamespace(
            constraint_name="fk_two",
            local_column="b",
            referenced_catalog="c",
            referenced_schema="s",
            referenced_table="p2",
            referenced_column="y",
        ),
    ]

    result = read_foreign_keys(_runner(foreign_keys_query(QN), rows), QN)

    assert len(result) == 2
    assert {fk.constraint_name for fk in result} == {"fk_one", "fk_two"}


def test_foreign_keys_empty_rows_map_to_empty_tuple() -> None:
    assert read_foreign_keys(_runner(foreign_keys_query(QN), []), QN) == ()


# ---------- referencing foreign keys ----------


def test_referencing_foreign_keys_rows_map_to_casefolded_references() -> None:
    rows = [
        SimpleNamespace(
            constraint_name="Orders_Customer_FK",
            referencing_catalog="Dev",
            referencing_schema="Silver",
            referencing_table="Orders",
        ),
    ]

    result = read_referencing_foreign_keys(_runner(referencing_foreign_keys_query(QN), rows), QN)

    assert result == (
        ForeignKeyReference(
            constraint_name="orders_customer_fk",
            referencing_table=QualifiedName("dev", "silver", "orders"),
        ),
    )


def test_referencing_foreign_keys_empty_rows_map_to_empty_tuple() -> None:
    assert read_referencing_foreign_keys(_runner(referencing_foreign_keys_query(QN), []), QN) == ()


# ---------- table tags ----------


def test_table_tags_read_returns_empty_read_only_mapping_for_no_rows():
    tags = read_table_tags(_runner(table_tags_query(QN), []), QN)
    assert dict(tags) == {}
    with pytest.raises(TypeError):
        tags["x"] = "y"  # type: ignore[index]


def test_table_tags_read_preserves_tag_key_and_value_case():
    rows = [
        SimpleNamespace(tag_name="Owner", tag_value="Data-Platform"),
        SimpleNamespace(tag_name="tier", tag_value="Gold"),
    ]
    tags = read_table_tags(_runner(table_tags_query(QN), rows), QN)
    assert dict(tags) == {"Owner": "Data-Platform", "tier": "Gold"}


# ---------- column tags ----------


def test_column_tags_read_returns_empty_mapping_for_no_rows():
    assert dict(read_column_tags(_runner(column_tags_query(QN), []), QN)) == {}


def test_column_tags_read_lowercases_column_names_but_preserves_tag_case():
    rows = [
        SimpleNamespace(column_name="EMAIL", tag_name="PII", tag_value="Email"),
        SimpleNamespace(column_name="email", tag_name="mask", tag_value="hash"),
        SimpleNamespace(column_name="id", tag_name="key", tag_value="primary"),
    ]
    tags = read_column_tags(_runner(column_tags_query(QN), rows), QN)
    assert dict(tags["email"]) == {"PII": "Email", "mask": "hash"}
    assert dict(tags["id"]) == {"key": "primary"}


@st.composite
def _foreign_key_row_permutations(draw: st.DrawFn):
    size = draw(st.integers(min_value=1, max_value=5))
    local_columns = draw(st.lists(SQL_IDENTIFIERS, min_size=size, max_size=size, unique=True))
    referenced_columns = draw(st.lists(SQL_IDENTIFIERS, min_size=size, max_size=size, unique=True))
    constraint_name = draw(SQL_IDENTIFIERS)
    rows = [
        SimpleNamespace(
            constraint_name=constraint_name.upper(),
            local_column=local.upper(),
            referenced_catalog="DEV",
            referenced_schema="SILVER",
            referenced_table="PARENT",
            referenced_column=referenced.upper(),
        )
        for local, referenced in zip(local_columns, referenced_columns, strict=True)
    ]
    expected = ForeignKeyConstraint(
        local_columns=tuple(local_columns),
        referenced_table=QualifiedName("dev", "silver", "parent"),
        referenced_columns=tuple(referenced_columns),
        constraint_name=constraint_name,
    )
    return draw(st.permutations(rows)), expected


@given(_foreign_key_row_permutations())
def test_foreign_key_row_order_does_not_break_local_to_referenced_pairing(case) -> None:
    rows, expected = case

    assert read_foreign_keys(_runner(foreign_keys_query(QN), rows), QN) == (expected,)


_TAG_NAMES = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_",
    min_size=1,
    max_size=12,
)


@st.composite
def _column_tag_row_permutations(draw: st.DrawFn):
    expected = draw(
        st.dictionaries(
            SQL_IDENTIFIERS,
            st.dictionaries(_TAG_NAMES, st.text(max_size=30), min_size=1, max_size=4),
            min_size=1,
            max_size=4,
        )
    )
    rows = [
        SimpleNamespace(column_name=column.upper(), tag_name=name, tag_value=value)
        for column, tags in expected.items()
        for name, value in tags.items()
    ]
    return draw(st.permutations(rows)), expected


@given(_column_tag_row_permutations())
def test_column_tag_grouping_is_row_order_independent_and_preserves_tag_case(case) -> None:
    rows, expected = case

    actual = read_column_tags(_runner(column_tags_query(QN), rows), QN)

    assert {column: dict(tags) for column, tags in actual.items()} == expected
