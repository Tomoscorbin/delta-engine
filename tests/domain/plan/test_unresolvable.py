import pytest

from delta_engine.domain.model import Identifier
from delta_engine.domain.plan.unresolvable import (
    ColumnCaseDrift,
    ColumnRenameConflict,
    PartitioningChanged,
)


def test_column_rename_conflict_rejects_no_difference() -> None:
    # When old and new names are identical, then construction fails
    with pytest.raises(ValueError):
        ColumnRenameConflict(old_name="same", new_name="same")


def test_partitioning_changed_rejects_no_difference() -> None:
    # When desired and observed partitioning agree, then construction fails
    with pytest.raises(ValueError):
        PartitioningChanged(desired_partitioning=("ds",), observed_partitioning=("ds",))


def test_column_case_drift_requires_a_real_spelling_difference() -> None:
    # When both spellings are identical, then construction fails — there is no drift
    with pytest.raises(ValueError):
        ColumnCaseDrift(declared_name="orderid", observed_name="orderid")


def test_column_case_drift_requires_the_same_identifier() -> None:
    # When the two spellings name different columns, then construction fails —
    # case drift relates spellings of one identifier only
    with pytest.raises(ValueError):
        ColumnCaseDrift(declared_name="orderid", observed_name="customer_id")


def test_column_case_drift_equality_is_exact_even_when_built_from_identifiers() -> None:
    # Given a drift built from Identifier names, as the differ builds them
    drift = ColumnCaseDrift(Identifier("OrderId"), Identifier("orderid"))

    # Then the record compares by exact spelling, not case-insensitively
    assert drift == ColumnCaseDrift("OrderId", "orderid")
    assert drift != ColumnCaseDrift("ORDERID", "orderid")
