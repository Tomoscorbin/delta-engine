from dataclasses import FrozenInstanceError

import pytest

from tabula.domain.model.column import Column
from tabula.domain.model.data_type import Int64, String
from tabula.domain.model.qualified_name import QualifiedName
from tabula.domain.model.table import DesiredTable, ObservedTable, TableSnapshot


def make_table(cols=None):
    cols = cols or (
        Column("id", Int64()),
        Column("name", String()),
    )
    qn = QualifiedName("core", "gold", "customers")
    return TableSnapshot(qualified_name=qn, columns=cols)


def test_table_snapshot_holds_name_and_columns_immutably():
    t = make_table()
    assert str(t.qualified_name) == "core.gold.customers"
    assert tuple(c.name for c in t.columns) == ("id", "name")
    # frozen dataclass: attribute reassignment should fail
    with pytest.raises(FrozenInstanceError):
        t.columns = ()  # type: ignore[misc]


def test_desired_and_observed_tables_extend_snapshot():
    cols = (
        Column("id", Int64()),
    )
    qn = QualifiedName("core", "silver", "orders")
    desired = DesiredTable(qualified_name=qn, columns=cols)
    observed = ObservedTable(qualified_name=qn, columns=cols, is_empty=False)
    assert isinstance(desired, TableSnapshot)
    assert isinstance(observed, TableSnapshot)
    assert observed.is_empty is False
