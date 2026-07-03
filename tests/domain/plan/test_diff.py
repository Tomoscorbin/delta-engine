import pytest

from delta_engine.domain.model import Column, DesiredTable, Integer, QualifiedName
from delta_engine.domain.plan.diff import (
    Changed,
    ColumnChanged,
    KeyValue,
    TableDrift,
    TableMissing,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def test_column_changed_requires_at_least_one_difference():
    # Given a ColumnChanged carrying no sub-fact at all
    # Then construction is rejected — a vacuous entry is a malformed diff
    with pytest.raises(ValueError, match="no differences"):
        ColumnChanged(column_name="id")


def test_column_changed_accepts_a_single_difference():
    # Given exactly one differing attribute
    entry = ColumnChanged(column_name="id", comment=Changed(desired="pk", observed=""))

    # Then the entry holds that fact and nothing else
    assert entry.comment == Changed(desired="pk", observed="")
    assert entry.data_type is None
    assert entry.nullability is None
    assert entry.tags == ()


def test_column_changed_accepts_tags_only():
    # Given only tag entries differ
    entry = ColumnChanged(
        column_name="id",
        tags=(Changed(KeyValue("pii", "true"), KeyValue("pii", "false")),),
    )

    # Then the entry is valid
    assert len(entry.tags) == 1


def test_table_drift_defaults_to_no_differences():
    # Given a drift built with no arguments
    drift = TableDrift()

    # Then every dimension reports no difference
    assert drift.columns == ()
    assert drift.table_comment is None
    assert drift.properties == ()
    assert drift.table_tags == ()
    assert drift.partitioning is None
    assert drift.primary_key is None
    assert drift.foreign_keys == ()


def test_table_missing_carries_the_desired_table():
    # Given a desired table for a table absent from the catalog
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),)
    )

    # Then the missing-table variant is self-contained
    assert TableMissing(desired=desired).desired is desired
