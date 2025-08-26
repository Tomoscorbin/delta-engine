import pytest

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Int64, String
from delta_engine.domain.model.table import DesiredTable, ObservedTable
from delta_engine.domain.services.differ import diff_tables
from tests.factories import columns, qualified_name


def test_diff_tables_returns_create_table_when_observed_missing():
    desired = DesiredTable(
        qualified_name=qualified_name("core", "gold", "customers"),
        columns=columns([("id", Int64()), ("name", String())]),
    )

    plan = diff_tables(observed=None, desired=desired)

    from delta_engine.domain.plan.actions import CreateTable
    assert str(plan.target) == "core.gold.customers"
    assert len(plan.actions) == 1
    assert isinstance(plan.actions[0], CreateTable)
    assert plan.actions[0].columns == desired.columns  # same tuple object OK


def test_diff_tables_raises_on_mismatched_qualified_name():
    desired = DesiredTable(qualified_name("core", "gold", "customers"), columns([("id", Int64())]))
    observed = ObservedTable(qualified_name("core", "gold", "orders"), columns([("id", Int64())]), is_empty=False)

    with pytest.raises(ValueError):
        diff_tables(observed=observed, desired=desired)


def test_diff_tables_delegates_to_column_diff(monkeypatch):
    desired = DesiredTable(qualified_name("core", "gold", "customers"), columns([("id", Int64())]))
    observed = ObservedTable(qualified_name("core", "gold", "customers"), columns([("id", Int64())]), is_empty=False)

    # Stub diff_columns to prove delegation and passthrough
    from delta_engine.domain.model.data_type import String
    from delta_engine.domain.plan.actions import AddColumn

    fake_action = AddColumn(column=Column("email", String()))

    import delta_engine.domain.services.differ as differ_mod
    monkeypatch.setattr(differ_mod, "diff_columns", lambda d, o: (fake_action,))

    plan = diff_tables(observed=observed, desired=desired)
    assert plan.actions == (fake_action,)
    assert str(plan.target) == "core.gold.customers"
