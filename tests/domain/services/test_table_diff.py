from delta_engine.domain.model import (
    Column,
    DesiredTable,
    Integer,
    ObservedTable,
    QualifiedName,
    TableFormat,
)
from delta_engine.domain.plan.actions import (
    SetProperty,
    SetTableComment,
    UnsetProperty,
)
from delta_engine.domain.services.table_diff import (
    diff_partition_columns,
    diff_properties,
    diff_table_comments,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def test_no_property_actions_when_mappings_are_identical():
    # Given: desired and observed have identical properties
    props = {"delta.appendOnly": "true", "owner": "cdm"}

    # When: diffing properties
    actions = diff_properties(props, props)

    # Then: nothing to do
    assert actions == ()


def test_sets_property_when_missing_in_observed():
    # Given: desired has a property missing from observed
    desired_props = {"delta.appendOnly": "true"}
    observed_props = {}

    # When: diffing properties
    actions = diff_properties(desired_props, observed_props)

    # Then: a SetProperty is emitted with the desired value
    expected = (SetProperty(name="delta.appendOnly", value="true"),)
    assert actions == expected


def test_updates_property_when_value_differs():
    # Given: key matches but value differs
    desired_props = {"delta.appendOnly": "false"}
    observed_props = {"delta.appendOnly": "true"}

    # When
    actions = diff_properties(desired_props, observed_props)

    # Then: a single SetProperty updates the value
    expected = (SetProperty(name="delta.appendOnly", value="false"),)
    assert actions == expected


def test_unsets_property_when_not_in_desired():
    # Given: observed contains an extra property
    desired_props = {"owner": "cdm"}
    observed_props = {"owner": "cdm", "obsolete": "1"}

    # When
    actions = diff_properties(desired_props, observed_props)

    # Then: an UnsetProperty removes the extra key
    expected = (UnsetProperty(name="obsolete"),)
    assert actions == expected


def test_combines_sets_and_unsets_without_extras():
    # Given: one set and one unset needed
    desired_props = {"owner": "cdm", "delta.appendOnly": "false"}
    observed_props = {"owner": "cdm", "obsolete": "1"}

    # When
    actions = diff_properties(desired_props, observed_props)

    # Then: exactly the expected operations are present (order not asserted)
    assert SetProperty(name="delta.appendOnly", value="false") in actions
    assert UnsetProperty(name="obsolete") in actions


def test_no_comment_action_when_comments_match():
    # Given: same comment on desired and observed
    d = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),),
        comment="core table",
        properties={},
        partitioned_by=(),
        format=TableFormat.DELTA,
    )
    o = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),),
        comment="core table",
        properties={},
        partitioned_by=(),
    )

    # When
    actions = diff_table_comments(d, o)

    # Then
    assert actions == ()


def test_sets_comment_when_comment_differs():
    # Given: desired has a different comment than observed
    d = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),),
        comment="core table",
        properties={},
        partitioned_by=(),
        format=TableFormat.DELTA,
    )
    o = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),),
        comment="",
        properties={},
        partitioned_by=(),
    )

    # When
    actions = diff_table_comments(d, o)

    # Then: a single SetTableComment is emitted with the desired text
    expected = (SetTableComment(comment="core table"),)
    assert actions == expected


def test_clears_comment_when_desired_is_empty():
    # Given: observed has a comment; desired clears it
    d = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),),
        comment="",
        properties={},
        partitioned_by=(),
        format=TableFormat.DELTA,
    )
    o = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),),
        comment="legacy",
        properties={},
        partitioned_by=(),
    )

    # When
    actions = diff_table_comments(d, o)

    # Then: a single SetTableComment clears to empty
    expected = (SetTableComment(comment=""),)
    assert actions == expected


def test_no_partition_action_when_partition_columns_match():
    # Given: same partition spec on desired and observed
    d = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("event_date", Integer()),),
        properties={},
        partitioned_by=("event_date",),
        format=TableFormat.DELTA,
    )
    o = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("event_date", Integer()),),
        properties={},
        partitioned_by=("event_date",),
    )

    # When
    actions = diff_partition_columns(d, o)

    # Then: no action
    assert actions == ()
