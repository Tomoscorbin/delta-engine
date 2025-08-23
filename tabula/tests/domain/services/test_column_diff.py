
from tabula.domain.model.column import Column
from tabula.domain.model.data_type.types import integer, string
from tabula.domain.plan.actions import AddColumn, DropColumn
from tabula.domain.services.column_diff import (
    diff_columns,
    diff_columns_for_adds,
    diff_columns_for_drops,
)


def col(name: str, *, dtype=None, is_nullable: bool = True) -> Column:
    return Column(name, dtype or integer(), is_nullable=is_nullable)


# ---------------------------
# diff_columns_for_adds
# ---------------------------


def test_adds_when_desired_has_new_columns():
    desired = [col("a"), col("b"), col("d"), col("e")]
    observed = [col("a"), col("c"), col("e")]
    adds = diff_columns_for_adds(desired, observed)
    assert tuple(type(a) for a in adds) == (AddColumn, AddColumn)
    assert [a.column.name for a in adds] == ["b", "d"]  # desired order preserved


def test_adds_are_case_insensitive_on_column_name():
    desired = [col("A")]
    observed = [col("a")]
    assert diff_columns_for_adds(desired, observed) == ()


def test_adds_ignore_type_or_nullability_differences():
    desired = [Column("id", string(), is_nullable=False)]
    observed = [Column("id", integer(), is_nullable=True)]
    assert diff_columns_for_adds(desired, observed) == ()


def test_adds_when_observed_is_empty_adds_all_in_order():
    desired = [col("b"), col("a"), col("c")]
    adds = diff_columns_for_adds(desired, [])
    assert [a.column.name for a in adds] == ["b", "a", "c"]


# ---------------------------
# diff_columns_for_drops
# ---------------------------


def test_drops_when_observed_has_extra_columns():
    desired = [col("a"), col("d")]
    observed = [col("b"), col("c"), col("a")]
    drops = diff_columns_for_drops(desired, observed)
    assert tuple(type(d) for d in drops) == (DropColumn, DropColumn)


def test_drops_are_case_insensitive_on_column_name():
    desired = [col("A")]
    observed = [col("a"), col("B")]
    drops = diff_columns_for_drops(desired, observed)
    assert [d.column_name for d in drops] == ["b"]


def test_drops_ignore_type_or_nullability_differences():
    desired = [Column("id", string(), is_nullable=False)]
    observed = [Column("id", integer(), is_nullable=True)]
    assert diff_columns_for_drops(desired, observed) == ()


# ---------------------------
# diff_columns (combined)
# ---------------------------


def test_diff_columns_emits_adds_then_drops_in_that_order():
    desired = [col("a"), col("c"), col("d")]
    observed = [col("a"), col("b"), col("c")]
    actions = diff_columns(desired, observed)
    # Adds first (desired order), then drops (sorted by name)
    assert [type(a).__name__ for a in actions] == ["AddColumn", "DropColumn"]
    assert [(getattr(a, "column", None) and a.column.name) or a.column_name for a in actions] == [
        "d",
        "b",
    ]


def test_diff_columns_returns_empty_tuple_when_sets_equal():
    desired = [col("a"), col("b")]
    observed = [col("b"), col("a")]
    assert diff_columns(desired, observed) == ()


def test_diff_columns_accepts_generators_safely_if_callers_pass_them():
    # Your differ() passes tuples from tables, but the helpers still work with generators.
    desired_gen = (col(n) for n in ["a", "x"])
    observed_gen = (col(n) for n in ["a"])
    actions = diff_columns(desired_gen, observed_gen)
    assert [type(a).__name__ for a in actions] == ["AddColumn"]
    assert actions[0].column.name == "x"
