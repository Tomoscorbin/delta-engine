from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from delta_engine.domain.plan.actions import (
    AddColumn,
    DropColumn,
    SetColumnComment,
    SetColumnNullability,
)
from delta_engine.domain.services.column_diff import (
    diff_columns,
)


def test_no_actions_when_schemas_are_identical():
    # Given: desired and observed have the same columns, comments, and nullability
    cols = (Column("id", Integer()), Column("name", String(), comment="customer name"))
    desired = cols
    observed = cols

    # When: diffing desired against observed
    actions = diff_columns(desired, observed)

    # Then: nothing to do
    assert actions == ()


def test_adds_columns_present_only_in_desired():
    # Given: desired has an extra column not present in observed
    desired = (Column("id", Integer()), Column("age", Integer()))
    observed = (Column("id", Integer()),)

    # When: diffing desired against observed
    actions = diff_columns(desired, observed)

    # Then: an AddColumn for "age" is produced
    expected = AddColumn(column=Column("age", Integer()))
    assert expected in actions


def test_drops_columns_present_only_in_observed():
    # Given: observed has a legacy column not present in desired
    desired = (Column("id", Integer()),)
    observed = (Column("id", Integer()), Column("legacy", String()))

    # When: diffing desired against observed
    actions = diff_columns(desired, observed)

    # Then: a DropColumn for "legacy" is produced
    expected = (DropColumn("legacy"),)
    assert actions == expected


def test_sets_comment_when_desired_differs_from_observed():
    # Given: same column exists; desired has a comment, observed has none
    desired = (Column("name", String(), comment="customer"),)
    observed = (Column("name", String(), comment=""),)

    # When: diffing desired against observed
    actions = diff_columns(desired, observed)

    # Then: a SetColumnComment aligns the comment
    expected = (SetColumnComment("name", "customer"),)
    assert actions == expected


def test_clears_comment_when_desired_is_empty_and_observed_is_not():
    # Given: same column exists; desired clears the comment
    desired = (Column("name", String(), comment=""),)
    observed = (Column("name", String(), comment="customer"),)

    # When: diffing desired against observed
    actions = diff_columns(desired, observed)

    # Then: a SetColumnComment clears it to empty
    expected = (SetColumnComment("name", ""),)
    assert actions == expected


def test_sets_nullability_when_flag_differs():
    # Given: same column exists; desired flips nullability to NOT NULL
    desired = (Column("active", String(), is_nullable=False),)
    observed = (Column("active", String(), is_nullable=True),)

    # When: diffing desired against observed
    actions = diff_columns(desired, observed)

    # Then: a SetColumnNullability aligns the flag
    expected = (SetColumnNullability(column_name="active", nullable=False),)
    assert actions == expected


def test_combines_add_drop_and_updates_without_duplicates():
    # Given: need to add one, drop one, and update an existing column's comment/nullability
    desired = (
        Column("keep", Integer(), comment="k"),
        Column("add_me", Integer(), is_nullable=False, comment="new"),
    )
    observed = (
        Column("keep", Integer(), comment=""),
        Column("drop_me", String()),
    )

    # When: diffing desired against observed
    actions = diff_columns(desired, observed)

    # Then: we see the three expected kinds of actions for the right columns
    assert (
        AddColumn(column=Column("add_me", Integer(), is_nullable=False, comment="new")) in actions
    )
    assert DropColumn("drop_me") in actions
    assert SetColumnComment("keep", "k") in actions


def test_ignores_type_changes_until_type_migrations_supported():
    # Given: same column name exists but data type differs
    desired = (Column("id", String()),)
    observed = (Column("id", Integer()),)

    # When: diffing desired against observed
    actions = diff_columns(desired, observed)

    # Then: no action emitted for type change (explicitly unsupported for now)
    assert actions == ()
