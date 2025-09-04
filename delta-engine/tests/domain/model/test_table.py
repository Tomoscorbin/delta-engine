import pytest

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.model.table import DesiredTable, ObservedTable, TableSnapshot
from tests.factories import make_qualified_name

_QN = make_qualified_name("dev", "silver", "orders")


def test_table_snapshot_requires_at_least_one_column() -> None:
    with pytest.raises(ValueError) as exc:
        TableSnapshot(_QN, ())
    assert "Table requires at least one column" in str(exc.value)


def test_table_snapshot_rejects_duplicate_column_names_case_insensitive() -> None:
    cols = (Column("ID", Integer()), Column("id", String()))
    with pytest.raises(ValueError) as exc:
        TableSnapshot(_QN, cols)
    assert "Duplicate column name:" in str(exc.value)


def test_table_snapshot_preserves_column_order() -> None:
    c1 = Column("id", Integer())
    c2 = Column("name", String())
    c3 = Column("created_at", String())

    ts = TableSnapshot(_QN, (c1, c2, c3))
    assert ts.columns == (c1, c2, c3)


def test_desired_and_observed_table_construct_like_snapshot() -> None:
    cols = (Column("id", Integer()),)
    dt = DesiredTable(_QN, cols)
    ot = ObservedTable(_QN, cols)

    assert isinstance(dt, TableSnapshot)
    assert isinstance(ot, TableSnapshot)
    assert dt.columns == cols
    assert ot.columns == cols


def test_partition_columns_must_exist_on_snapshot() -> None:
    cols = (Column("id", Integer()),)
    with pytest.raises(ValueError) as exc:
        TableSnapshot(_QN, cols, partitioned_by=("created_at",))
    assert "Partition column not found: created_at" in str(exc.value)
