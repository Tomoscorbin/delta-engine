from delta_engine.domain.model import (
    Column,
    Date,
    DesiredTable,
    Integer,
    ObservedTable,
    QualifiedName,
    String,
)
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
    CreateTable,
    DropColumn,
    SetColumnComment,
    SetColumnNullability,
    SetProperty,
    SetTableComment,
)
from delta_engine.domain.services.differ import diff_tables

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")
_BASELINE_COLUMNS = (Column("id", Integer()),)


def _desired(
    *,
    columns=_BASELINE_COLUMNS,
    comment="",
    properties=None,
    partitioned_by=(),
) -> DesiredTable:
    """Build a DesiredTable, defaulting every dimension to a no-op baseline."""
    return DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=columns,
        comment=comment,
        properties=properties or {},
        partitioned_by=partitioned_by,
    )


def _observed(
    *,
    columns=_BASELINE_COLUMNS,
    comment="",
    properties=None,
    partitioned_by=(),
) -> ObservedTable:
    """Build an ObservedTable matching `_desired`'s baseline so a single dimension can vary."""
    return ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=columns,
        comment=comment,
        properties=properties or {},
        partitioned_by=partitioned_by,
    )


# ---------- whole-table behaviour ----------


def test_creates_table_when_observed_is_missing():
    # Given: a desired table definition and no observed table (missing)
    desired = _desired(
        columns=(Column("id", Integer()),),
        comment="core table",
        properties={"owner": "cdm"},
    )

    # When: diffing desired vs None
    plan = diff_tables(desired, observed=None)

    # Then: we get a CreateTable wrapped in an ActionPlan
    assert plan.target == desired.qualified_name
    assert plan.actions == (CreateTable(desired),)


def test_no_actions_when_desired_equals_observed():
    # Given: identical desired and observed definitions
    columns = (
        Column("id", Integer()),
        Column("name", String(), comment="customer"),
        Column("event_date", Date()),
    )
    desired = _desired(
        columns=columns,
        comment="core table",
        properties={"owner": "cdm"},
        partitioned_by=("event_date",),
    )
    observed = _observed(
        columns=columns,
        comment="core table",
        properties={"owner": "cdm"},
        partitioned_by=("event_date",),
    )

    # When
    plan = diff_tables(desired, observed)

    # Then: nothing to do
    assert plan.target == desired.qualified_name
    assert plan.actions == ()


def test_combines_column_property_comment_and_partition_diffs():
    # Given: differences across all dimensions
    desired = _desired(
        columns=(
            Column("id", Integer()),
            Column("name", String(), comment="customer"),
            Column("event_date", Date()),
            Column("country", String()),
            Column("age", Integer()),  # new column to add
        ),
        comment="core table",  # updated comment
        properties={"owner": "cdm", "delta.appendOnly": "false"},  # set/update
        partitioned_by=("event_date", "country"),  # partition spec differs
    )
    observed = _observed(
        columns=(
            Column("id", Integer()),
            Column("name", String(), comment=""),  # comment missing
            Column("event_date", Date()),
            Column("country", String()),
        ),
        comment="",  # will be set
        properties={"owner": "cdm", "obsolete": "1"},  # extra prop (unset checked elsewhere)
        partitioned_by=("event_date",),  # different partition spec
    )

    # When
    plan = diff_tables(desired, observed)

    # Then: the plan contains the expected representative actions
    assert isinstance(plan, ActionPlan)
    assert plan.target == desired.qualified_name

    # Column add
    assert AddColumn(column=Column("age", Integer())) in plan.actions
    # Property set/update
    assert SetProperty(name="delta.appendOnly", value="false") in plan.actions
    # Comment update
    assert SetTableComment(comment="core table") in plan.actions
    # Partition change is detected by the validator (no PartitionBy action in plan)


# ---------- column diffs ----------


def test_no_column_actions_when_columns_are_identical():
    # Given: desired and observed have the same columns, comments, and nullability
    columns = (Column("id", Integer()), Column("name", String(), comment="customer name"))

    # When
    plan = diff_tables(_desired(columns=columns), _observed(columns=columns))

    # Then: nothing to do
    assert plan.actions == ()


def test_adds_columns_present_only_in_desired():
    # Given: desired has an extra column not present in observed
    desired = _desired(columns=(Column("id", Integer()), Column("age", Integer())))
    observed = _observed(columns=(Column("id", Integer()),))

    # When
    plan = diff_tables(desired, observed)

    # Then: an AddColumn for "age" is produced
    assert AddColumn(column=Column("age", Integer())) in plan.actions


def test_drops_columns_present_only_in_observed():
    # Given: observed has a legacy column not present in desired
    desired = _desired(columns=(Column("id", Integer()),))
    observed = _observed(columns=(Column("id", Integer()), Column("legacy", String())))

    # When
    plan = diff_tables(desired, observed)

    # Then: a DropColumn for "legacy" is produced
    assert plan.actions == (DropColumn("legacy"),)


def test_sets_column_comment_when_desired_differs_from_observed():
    # Given: same column exists; desired has a comment, observed has none
    desired = _desired(columns=(Column("name", String(), comment="customer"),))
    observed = _observed(columns=(Column("name", String(), comment=""),))

    # When
    plan = diff_tables(desired, observed)

    # Then: a SetColumnComment aligns the comment
    assert plan.actions == (SetColumnComment("name", "customer"),)


def test_clears_column_comment_when_desired_is_empty_and_observed_is_not():
    # Given: same column exists; desired clears the comment
    desired = _desired(columns=(Column("name", String(), comment=""),))
    observed = _observed(columns=(Column("name", String(), comment="customer"),))

    # When
    plan = diff_tables(desired, observed)

    # Then: a SetColumnComment clears it to empty
    assert plan.actions == (SetColumnComment("name", ""),)


def test_sets_column_nullability_when_flag_differs():
    # Given: same column exists; desired flips nullability to NOT NULL
    desired = _desired(columns=(Column("active", String(), nullable=False),))
    observed = _observed(columns=(Column("active", String(), nullable=True),))

    # When
    plan = diff_tables(desired, observed)

    # Then: a SetColumnNullability aligns the flag
    assert plan.actions == (SetColumnNullability(column_name="active", nullable=False),)


def test_combines_column_add_drop_and_updates_without_duplicates():
    # Given: need to add one, drop one, and update an existing column's comment
    desired = _desired(
        columns=(
            Column("keep", Integer(), comment="k"),
            Column("add_me", Integer(), nullable=False, comment="new"),
        )
    )
    observed = _observed(
        columns=(Column("keep", Integer(), comment=""), Column("drop_me", String()))
    )

    # When
    plan = diff_tables(desired, observed)

    # Then: exactly three actions — no redundant comment/nullability for the added column
    assert plan.actions == (
        AddColumn(column=Column("add_me", Integer(), nullable=False, comment="new")),
        DropColumn("drop_me"),
        SetColumnComment("keep", "k"),
    )


def test_adding_column_to_existing_table_emits_only_add_column():
    # Given: an existing table and a desired schema with one new column
    desired = _desired(
        columns=(
            Column("id", Integer()),
            Column("age", Integer(), comment="user age", nullable=False),
        )
    )
    observed = _observed(columns=(Column("id", Integer()),))

    # When
    plan = diff_tables(desired, observed)

    # Then: only one AddColumn; no redundant SetColumnComment or SetColumnNullability
    assert plan.actions == (
        AddColumn(column=Column("age", Integer(), comment="user age", nullable=False)),
    )


def test_ignores_column_type_changes_until_type_migrations_supported():
    # Given: same column name exists but data type differs
    desired = _desired(columns=(Column("id", String()),))
    observed = _observed(columns=(Column("id", Integer()),))

    # When
    plan = diff_tables(desired, observed)

    # Then: no action emitted for type change (explicitly unsupported for now)
    assert plan.actions == ()


# ---------- property diffs ----------


def test_no_property_actions_when_mappings_are_identical():
    # Given: desired and observed have identical properties
    props = {"delta.appendOnly": "true", "owner": "cdm"}

    # When
    plan = diff_tables(_desired(properties=props), _observed(properties=props))

    # Then: nothing to do
    assert plan.actions == ()


def test_sets_property_when_missing_in_observed():
    # Given: desired has a property missing from observed
    desired = _desired(properties={"delta.appendOnly": "true"})
    observed = _observed(properties={})

    # When
    plan = diff_tables(desired, observed)

    # Then: a SetProperty is emitted with the desired value
    assert plan.actions == (SetProperty(name="delta.appendOnly", value="true"),)


def test_updates_property_when_value_differs():
    # Given: key matches but value differs
    desired = _desired(properties={"delta.appendOnly": "false"})
    observed = _observed(properties={"delta.appendOnly": "true"})

    # When
    plan = diff_tables(desired, observed)

    # Then: a single SetProperty updates the value
    assert plan.actions == (SetProperty(name="delta.appendOnly", value="false"),)


def test_ignores_observed_only_properties():
    # Given: observed contains a property the user never declared
    #        (e.g. one Databricks set autonomously)
    desired = _desired(properties={"owner": "cdm"})
    observed = _observed(properties={"owner": "cdm", "delta.minReaderVersion": "2"})

    # When
    plan = diff_tables(desired, observed)

    # Then: the undeclared property is left untouched — no unset is emitted
    assert plan.actions == ()


# ---------- table comment diffs ----------


def test_no_comment_action_when_comments_match():
    # Given: same comment on desired and observed
    plan = diff_tables(_desired(comment="core table"), _observed(comment="core table"))

    # Then
    assert plan.actions == ()


def test_sets_table_comment_when_comment_differs():
    # Given: desired has a different comment than observed
    plan = diff_tables(_desired(comment="core table"), _observed(comment=""))

    # Then: a single SetTableComment is emitted with the desired text
    assert plan.actions == (SetTableComment(comment="core table"),)


def test_clears_table_comment_when_desired_is_empty():
    # Given: observed has a comment; desired clears it
    plan = diff_tables(_desired(comment=""), _observed(comment="legacy"))

    # Then: a single SetTableComment clears to empty
    assert plan.actions == (SetTableComment(comment=""),)
