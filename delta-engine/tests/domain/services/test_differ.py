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
    PartitionBy,
    SetProperty,
    SetTableComment,
)
from delta_engine.domain.services.differ import diff_tables

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def test_creates_table_when_observed_is_missing():
    # Given: a desired table definition and no observed table (missing)
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),),
        comment="core table",
        properties={"owner": "cdm"},
        partitioned_by=(),
    )

    # When: diffing desired vs None
    plan = diff_tables(desired, observed=None)

    # Then: we get a CreateTable wrapped in an ActionPlan
    assert plan.target == desired.qualified_name
    assert plan.actions == (CreateTable(desired),)


def test_no_actions_when_desired_equals_observed():
    # Given: identical desired and observed definitions
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(
            Column("id", Integer()),
            Column("name", String(), comment="customer"),
            Column("event_date", Date()),
        ),
        comment="core table",
        properties={"owner": "cdm"},
        partitioned_by=("event_date",),
    )
    observed = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(
            Column("id", Integer()),
            Column("name", String(), comment="customer"),
            Column("event_date", Date()),
        ),
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
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
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
    observed = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(
            Column("id", Integer()),
            Column("name", String(), comment=""),  # comment missing
            Column("event_date", Date()),
            Column("country", String()),
        ),
        comment="",  # will be set
        properties={"owner": "cdm", "obsolete": "1"},  # has an extra prop (unset checked elsewhere)
        partitioned_by=("event_date",),  # different partition spec
    )

    # When
    plan = diff_tables(desired, observed)

    # Then: the plan contains the expected representative actions
    # (We assert presence of key actions; detailed property unsets are covered in table_diff tests.)
    assert isinstance(plan, ActionPlan)
    assert plan.target == desired.qualified_name

    # Column add
    assert AddColumn(column=Column("age", Integer())) in plan.actions
    # Property set/update
    assert SetProperty(name="delta.appendOnly", value="false") in plan.actions
    # Comment update
    assert SetTableComment(comment="core table") in plan.actions
    # Partition warning surfaced
    assert PartitionBy(("event_date", "country")) in plan.actions
