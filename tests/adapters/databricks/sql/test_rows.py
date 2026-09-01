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
    ObservedForeignKeyConstraint,
    ObservedPrimaryKeyConstraint,
    QualifiedName,
)
from tests.adapters.databricks.sql.strategies import (
    CANONICAL_IDENTIFIERS,
    TAG_KEYS,
    TAG_VALUES,
)

QN = QualifiedName("dev", "silver", "orders")


def _runner(query, rows):
    """Answer exactly one expected query with the given rows."""

    def run(actual_query):
        assert actual_query == query
        return rows

    return run


# ---------- schema existence ----------


def test_schema_exists_when_the_probe_returns_a_row() -> None:
    # Then a row from the probe means the schema exists
    assert schema_exists(_runner(schema_exists_query(QN), [("silver",)]), QN) is True


def test_schema_does_not_exist_when_the_probe_returns_no_rows() -> None:
    # Then no rows from the probe means the schema does not exist
    assert schema_exists(_runner(schema_exists_query(QN), []), QN) is False


# ---------- primary key ----------


def test_primary_key_rows_preserve_catalog_spelling() -> None:
    # Given catalog rows carrying mixed-case column spellings
    rows = [
        SimpleNamespace(constraint_name="Orders_PK", column_name="Order_Id"),
        SimpleNamespace(constraint_name="Orders_PK", column_name="Line_No"),
    ]

    result = read_primary_key(_runner(primary_key_query(QN), rows), QN)

    # Then each spelling is preserved and columns hold their canonical order
    assert result is not None
    assert isinstance(result, ObservedPrimaryKeyConstraint)
    assert tuple(str(column) for column in result.columns) == ("Line_No", "Order_Id")
    assert str(result.name) == "Orders_PK"


def test_primary_key_empty_rows_map_to_none() -> None:
    # Then no rows means no primary key
    assert read_primary_key(_runner(primary_key_query(QN), []), QN) is None


# ---------- owned foreign keys ----------


def test_foreign_key_rows_preserve_constraint_and_column_spelling() -> None:
    # Given catalog rows carrying mixed-case spellings
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

    [fk] = read_foreign_keys(_runner(foreign_keys_query(QN), rows), QN)

    # Then constraint and column spellings carry verbatim
    assert isinstance(fk, ObservedForeignKeyConstraint)
    assert str(fk.name) == "Orders_Customer_FK"
    assert tuple(str(column) for column in fk.local_columns) == ("Customer_Id",)
    assert fk.referenced_table == QualifiedName("dev", "silver", "customer")
    assert tuple(str(column) for column in fk.referenced_columns) == ("Id",)


def test_composite_foreign_key_keeps_each_local_referenced_pair_together() -> None:
    # Given rows arriving in the foreign key's column order
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

    # Then each (local, referenced) pair stays together through the domain's
    # canonical sort: a->x and b->y
    assert fk.local_columns == ("a", "b")
    assert fk.referenced_columns == ("x", "y")
    assert fk.referenced_table == QualifiedName("c", "s", "parent")


def test_multiple_foreign_keys_group_by_constraint_name() -> None:
    # Given rows belonging to two different constraints
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

    # Then each constraint becomes its own foreign key
    assert len(result) == 2
    assert {fk.name for fk in result} == {"fk_one", "fk_two"}


def test_foreign_keys_empty_rows_map_to_empty_tuple() -> None:
    # Then no rows means no foreign keys
    assert read_foreign_keys(_runner(foreign_keys_query(QN), []), QN) == ()


# ---------- referencing foreign keys ----------


def test_referencing_foreign_key_rows_preserve_constraint_spelling() -> None:
    # Given an inbound-key row carrying mixed-case spellings
    rows = [
        SimpleNamespace(
            constraint_name="Orders_Customer_FK",
            referencing_catalog="Dev",
            referencing_schema="Silver",
            referencing_table="Orders",
        ),
    ]

    [reference] = read_referencing_foreign_keys(
        _runner(referencing_foreign_keys_query(QN), rows), QN
    )

    # Then the constraint name carries verbatim
    assert str(reference.name) == "Orders_Customer_FK"
    assert reference.referencing_table == QualifiedName("dev", "silver", "orders")


def test_referencing_foreign_keys_empty_rows_map_to_empty_tuple() -> None:
    # Then no rows means no inbound foreign keys
    assert read_referencing_foreign_keys(_runner(referencing_foreign_keys_query(QN), []), QN) == ()


# ---------- table tags ----------


def test_table_tags_read_returns_empty_read_only_mapping_for_no_rows():
    tags = read_table_tags(_runner(table_tags_query(QN), []), QN)

    # Then the mapping is empty and refuses writes
    assert dict(tags) == {}
    with pytest.raises(TypeError):
        tags["x"] = "y"  # type: ignore[index]


def test_table_tags_read_preserves_tag_key_and_value_case():
    # Given tag rows with mixed-case names and values
    rows = [
        SimpleNamespace(tag_name="Owner", tag_value="Data-Platform"),
        SimpleNamespace(tag_name="tier", tag_value="Gold"),
    ]

    tags = read_table_tags(_runner(table_tags_query(QN), rows), QN)

    # Then both spellings carry verbatim
    assert dict(tags) == {"Owner": "Data-Platform", "tier": "Gold"}


# ---------- column tags ----------


def test_column_tags_read_returns_empty_mapping_for_no_rows():
    # Then no rows means no column has tags
    assert dict(read_column_tags(_runner(column_tags_query(QN), []), QN)) == {}


def test_column_tags_read_keys_by_identifier_identity_and_preserves_tag_case():
    # Given tag rows spelling the same column two ways
    rows = [
        SimpleNamespace(column_name="EMAIL", tag_name="PII", tag_value="Email"),
        SimpleNamespace(column_name="email", tag_name="mask", tag_value="hash"),
        SimpleNamespace(column_name="id", tag_name="key", tag_value="primary"),
    ]

    tags = read_column_tags(_runner(column_tags_query(QN), rows), QN)

    # Then both rows group under one case-insensitive column identity while
    # tag spellings carry verbatim
    assert dict(tags["email"]) == {"PII": "Email", "mask": "hash"}
    assert dict(tags["id"]) == {"key": "primary"}


@st.composite
def _column_tag_row_permutations(draw: st.DrawFn):
    expected = draw(
        st.dictionaries(
            CANONICAL_IDENTIFIERS,
            st.dictionaries(TAG_KEYS, TAG_VALUES, min_size=1, max_size=4),
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
    # Given generated tag rows in an arbitrary order
    rows, expected = case

    actual = read_column_tags(_runner(column_tags_query(QN), rows), QN)

    # Then grouping recovers the expected mapping regardless of row order
    assert {column: dict(tags) for column, tags in actual.items()} == expected
