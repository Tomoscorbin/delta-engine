from random import shuffle
from hypothesis import given
from hypothesis import strategies as st
from tests.conftest import columns_strat
from tabula.domain.services.column_diff import diff_columns, diff_columns_for_adds, diff_columns_for_drops
from tabula.domain.model.actions import AddColumn, DropColumn
from tabula.domain.model.column import Column
from tabula.domain.model.types import integer

@given(columns_strat(min_size=0, max_size=6), columns_strat(min_size=0, max_size=6))
def test_diff_set_semantics_and_determinism(desired_cols, observed_cols):
    actions = diff_columns(desired_cols, observed_cols)
    add_names = [a.column.name for a in actions if isinstance(a, AddColumn)]
    drop_names = [a.column_name for a in actions if isinstance(a, DropColumn)]

    desired_names = {c.name for c in desired_cols}
    observed_names = {c.name for c in observed_cols}

    assert set(add_names) == desired_names - observed_names
    assert set(drop_names) == observed_names - desired_names

    # determinism: adds follow desired order
    expected_add_order = [c.name for c in desired_cols if c.name not in observed_names]
    assert add_names == expected_add_order

    # determinism: drops are sorted lexicographically
    assert drop_names == sorted(drop_names)

def names(actions):
    adds = [a.column.name for a in actions if isinstance(a, AddColumn)]
    drops = [a.column_name for a in actions if isinstance(a, DropColumn)]
    return adds, drops

def test_no_actions_when_identical_sets_ignoring_order_and_case():
    desired = [Column("A", integer()), Column("B", integer()), Column("C", integer())]
    observed = [Column("c", integer()), Column("b", integer()), Column("a", integer())]
    actions = diff_columns(desired, observed)
    assert list(actions) == []

def test_adds_preserve_desired_order_even_if_observed_permuted():
    desired = [Column("A", integer()), Column("B", integer()), Column("D", integer())]
    observed = [Column("B", integer())]
    # Permute desired to check determinism based on input order (should be preserved for adds)
    permuted_desired = desired[:] ; shuffle(permuted_desired)
    actions = diff_columns(permuted_desired, observed)
    add_names, drop_names = names(actions)
    expected_add_order = [c.name for c in permuted_desired if c.name not in {"b"}]
    assert add_names == expected_add_order
    assert drop_names == []  # nothing to drop in this case
