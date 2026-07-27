import pytest

from delta_engine.application.planning import (
    PlanningFailed,
    PlanningSucceeded,
    plan_diff,
)
from delta_engine.application.scopes import METADATA_ASPECTS
from delta_engine.domain.model import (
    DesiredColumn,
    DesiredTable,
    ForeignKeyConstraint,
    ForeignKeyReference,
    Integer,
    Long,
    ObservedColumn,
    ObservedTable,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
    TableAspect,
    TableKind,
    TimestampNtz,
)
from delta_engine.domain.plan import (
    AddColumn,
    AlterColumnType,
    CreateTable,
    DropForeignKey,
    DropPrimaryKey,
    RenameColumn,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetTableTag,
    diff_table,
)

_NAME = QualifiedName("dev", "silver", "test")


def _desired(**overrides) -> DesiredTable:
    values = {
        "qualified_name": _NAME,
        "columns": (DesiredColumn("id", Integer()),),
    }
    return DesiredTable(**(values | overrides))


def _observed(**overrides) -> ObservedTable:
    values = {
        "qualified_name": _NAME,
        "columns": (ObservedColumn("id", Integer()),),
    }
    return ObservedTable(**(values | overrides))


def _foreign_key(
    *,
    local_columns: tuple[str, ...],
    referenced_table: QualifiedName,
    referenced_columns: tuple[str, ...],
    constraint_name: str,
) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=local_columns,
        referenced_table=referenced_table,
        referenced_columns=referenced_columns,
        constraint_name=constraint_name,
    )


def test_plan_diff_accepts_safe_actions():
    diff = diff_table(
        _desired(columns=(DesiredColumn("id", Integer()), DesiredColumn("age", Integer()))),
        _observed(),
    )

    result = plan_diff(diff)

    assert isinstance(result, PlanningSucceeded)
    assert result.plan.target == _NAME
    assert result.plan.actions == (AddColumn(DesiredColumn("age", Integer())),)


def test_plan_diff_rejects_unsafe_actions_without_constructing_a_plan():
    diff = diff_table(
        _desired(
            columns=(
                DesiredColumn("id", Integer()),
                DesiredColumn("required", Integer(), nullable=False),
            )
        ),
        _observed(),
    )

    result = plan_diff(diff)

    assert isinstance(result, PlanningFailed)
    assert [failure.rule_name for failure in result.failures] == ["NonNullableColumnAdd"]
    assert not hasattr(result, "plan")


def test_plan_diff_rejects_unmanaged_actions_without_constructing_a_plan():
    diff = diff_table(
        _desired(
            columns=(DesiredColumn("id", Integer()), DesiredColumn("age", Integer())),
            managed_aspects=frozenset({TableAspect.TABLE_COMMENT}),
        ),
        _observed(),
    )

    result = plan_diff(diff)

    assert isinstance(result, PlanningFailed)
    assert [failure.rule_name for failure in result.failures] == ["UnmanagedAspectDrift"]
    assert not hasattr(result, "plan")


@pytest.mark.parametrize(
    "desired, observed, expected_rule",
    [
        (
            _desired(columns=(DesiredColumn("new", String(), renamed_from="old"),)),
            _observed(columns=(ObservedColumn("new", String()), ObservedColumn("old", String()))),
            "AmbiguousColumnRename",
        ),
        (
            _desired(properties={}),
            _observed(properties={"delta.columnMapping.mode": "name"}),
            "PropertyMustBeDeclared",
        ),
        (
            _desired(
                columns=(DesiredColumn("id", Integer()), DesiredColumn("day", String())),
                partitioned_by=("day",),
            ),
            _observed(columns=(ObservedColumn("id", Integer()), ObservedColumn("day", String()))),
            "PartitioningChangeNotSupported",
        ),
    ],
)
def test_plan_diff_rejects_each_non_action_difference(desired, observed, expected_rule):
    result = plan_diff(diff_table(desired, observed))

    assert isinstance(result, PlanningFailed)
    assert expected_rule in {failure.rule_name for failure in result.failures}
    assert not hasattr(result, "plan")


def test_plan_diff_accepts_no_op_as_an_empty_plan():
    result = plan_diff(diff_table(_desired(), _observed()))

    assert isinstance(result, PlanningSucceeded)
    assert result.plan.target == _NAME
    assert result.plan.actions == ()


def test_plan_diff_accepts_missing_table_and_builds_follow_up_actions():
    # Given a missing table declared with tags and a foreign key
    foreign_key = _foreign_key(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "parent"),
        referenced_columns=("id",),
        constraint_name="test_id_fk",
    )
    desired = _desired(
        columns=(DesiredColumn("id", Integer(), tags={"pii": "false"}),),
        tags={"env": "dev"},
        foreign_keys=(foreign_key,),
    )

    # When planning
    result = plan_diff(diff_table(desired, None))

    # Then the create is followed by tag and constraint actions
    assert isinstance(result, PlanningSucceeded)
    assert result.plan.target == desired.qualified_name
    assert result.plan.actions == (
        CreateTable(desired),
        SetTableTag(name="env", desired_value="dev", observed_value=None),
        SetColumnTag(column_name="id", name="pii", desired_value="false", observed_value=None),
        SetForeignKey(constraint=foreign_key),
    )


def test_plan_diff_rejects_missing_table_when_table_existence_is_unmanaged():
    desired = _desired(managed_aspects=frozenset({TableAspect.TABLE_COMMENT}))

    result = plan_diff(diff_table(desired, None))

    assert isinstance(result, PlanningFailed)
    assert [failure.rule_name for failure in result.failures] == ["MissingTableUnmanaged"]
    assert not hasattr(result, "plan")


def test_plan_diff_keeps_rename_and_residual_drift_under_the_new_name():
    # Given a rename hint plus a type change on the renamed column
    desired = _desired(
        columns=(DesiredColumn("amount", Long(), renamed_from="amt"),),
        properties={"delta.enableTypeWidening": "true"},
    )
    observed = _observed(
        columns=(ObservedColumn("amt", Integer()),),
        properties={"delta.enableTypeWidening": "true"},
    )

    # When planning
    result = plan_diff(diff_table(desired, observed))

    # Then the residual drift lands under the new name
    assert isinstance(result, PlanningSucceeded)
    assert result.plan.actions == (
        RenameColumn(old_name="amt", new_name="amount"),
        AlterColumnType(column_name="amount", desired_type=Long(), observed_type=Integer()),
    )


def test_plan_diff_replaces_a_primary_key_explicitly_across_a_rename():
    # Given a primary key moving to a renamed column
    desired_key = PrimaryKeyConstraint(("customer_name",), "test_pk")
    observed_key = PrimaryKeyConstraint(("customer_nm",), "legacy_pk")
    desired = _desired(
        columns=(DesiredColumn("customer_name", String(), False, renamed_from="customer_nm"),),
        primary_key=desired_key,
    )
    observed = _observed(
        columns=(ObservedColumn("customer_nm", String(), False),),
        primary_key=observed_key,
    )

    # When planning
    result = plan_diff(diff_table(desired, observed))

    # Then the plan drops the old key, renames, then sets the new key
    assert isinstance(result, PlanningSucceeded)
    assert result.plan.actions == (
        DropPrimaryKey(primary_key=observed_key, referencing_foreign_keys=()),
        RenameColumn("customer_nm", "customer_name"),
        SetPrimaryKey(primary_key=desired_key),
    )


def test_plan_diff_rejects_rename_of_a_primary_key_referenced_by_foreign_keys():
    # Given an inbound reference to the primary key being renamed
    reference = ForeignKeyReference(
        constraint_name="orders_customer_id_fk",
        referencing_table=QualifiedName("dev", "silver", "orders"),
    )
    desired_key = PrimaryKeyConstraint(("customer_name",), "test_pk")
    observed_key = PrimaryKeyConstraint(("customer_nm",), "legacy_pk")
    desired = _desired(
        columns=(DesiredColumn("customer_name", String(), False, renamed_from="customer_nm"),),
        primary_key=desired_key,
    )
    observed = _observed(
        columns=(ObservedColumn("customer_nm", String(), False),),
        primary_key=observed_key,
        referencing_foreign_keys=(reference,),
    )

    # When planning
    result = plan_diff(diff_table(desired, observed))

    # Then planning fails with the referenced-key rule
    assert isinstance(result, PlanningFailed)
    assert [failure.rule_name for failure in result.failures] == [
        "PrimaryKeyReferencedByForeignKeys"
    ]
    assert not hasattr(result, "plan")


def test_plan_diff_replaces_a_foreign_key_explicitly_across_a_rename():
    # Given a foreign key whose local column is being renamed
    parent = QualifiedName("dev", "silver", "parent")
    desired_key = _foreign_key(
        local_columns=("parent_id",),
        referenced_table=parent,
        referenced_columns=("id",),
        constraint_name="test_parent_id_fk",
    )
    observed_key = _foreign_key(
        local_columns=("parent",),
        referenced_table=parent,
        referenced_columns=("id",),
        constraint_name="legacy_fk",
    )
    desired = _desired(
        columns=(DesiredColumn("parent_id", Integer(), renamed_from="parent"),),
        foreign_keys=(desired_key,),
    )
    observed = _observed(
        columns=(ObservedColumn("parent", Integer()),), foreign_keys=(observed_key,)
    )

    # When planning
    result = plan_diff(diff_table(desired, observed))

    # Then the plan drops the old key, renames, then sets the new key
    assert isinstance(result, PlanningSucceeded)
    assert result.plan.actions == (
        DropForeignKey(constraint=observed_key),
        RenameColumn("parent", "parent_id"),
        SetForeignKey(constraint=desired_key),
    )


def test_plan_diff_replaces_a_self_referencing_foreign_key_explicitly_across_a_rename():
    # Given a self-referencing key whose referenced column is being renamed
    desired_key = _foreign_key(
        local_columns=("manager_id",),
        referenced_table=_NAME,
        referenced_columns=("employee_id",),
        constraint_name="test_manager_id_fk",
    )
    observed_key = _foreign_key(
        local_columns=("manager_id",),
        referenced_table=_NAME,
        referenced_columns=("id",),
        constraint_name="legacy_fk",
    )
    desired = _desired(
        columns=(
            DesiredColumn("employee_id", Integer(), renamed_from="id"),
            DesiredColumn("manager_id", Integer()),
        ),
        foreign_keys=(desired_key,),
    )
    observed = _observed(
        columns=(ObservedColumn("id", Integer()), ObservedColumn("manager_id", Integer())),
        foreign_keys=(observed_key,),
    )

    # When planning
    result = plan_diff(diff_table(desired, observed))

    # Then the plan drops the old key, renames, then sets the new key
    assert isinstance(result, PlanningSucceeded)
    assert result.plan.actions == (
        DropForeignKey(constraint=observed_key),
        RenameColumn("id", "employee_id"),
        SetForeignKey(constraint=desired_key),
    )


def test_plan_diff_drops_an_observed_only_foreign_key_alongside_a_rename():
    # Given an observed-only foreign key unrelated to the rename
    unrelated_key = _foreign_key(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "parent"),
        referenced_columns=("id",),
        constraint_name="legacy_fk",
    )
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("customer_name", String(), renamed_from="customer_nm"),
        )
    )
    observed = _observed(
        columns=(ObservedColumn("id", Integer()), ObservedColumn("customer_nm", String())),
        foreign_keys=(unrelated_key,),
    )

    # When planning
    result = plan_diff(diff_table(desired, observed))

    # Then the drop still lands alongside the rename
    assert isinstance(result, PlanningSucceeded)
    assert result.plan.actions == (
        DropForeignKey(constraint=unrelated_key),
        RenameColumn("customer_nm", "customer_name"),
    )


def test_plan_carries_the_observed_relation_kind():
    # Given tag drift against a streaming table, under a tags-only declaration
    desired = _desired(
        columns=(DesiredColumn("id", Integer(), tags={"pii": "low"}),),
        managed_aspects=frozenset({TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS}),
    )
    observed = _observed(kind=TableKind.STREAMING_TABLE)

    # When planning the diff
    result = plan_diff(diff_table(desired, observed))

    # Then the plan knows what its actions lower against
    assert isinstance(result, PlanningSucceeded)
    assert result.plan.target == desired.qualified_name
    assert result.plan.kind is TableKind.STREAMING_TABLE


def test_creation_plan_carries_the_ordinary_table_kind():
    # Given a missing table — absence has no observed kind, and the engine
    # only creates ordinary tables
    result = plan_diff(diff_table(_desired(), None))

    assert isinstance(result, PlanningSucceeded)
    assert result.plan.target == _NAME
    assert result.plan.kind is TableKind.TABLE


def test_feature_enablement_outside_column_structure_scope_is_rejected():
    # Given matching columns whose implied feature is absent, under metadata-only scope
    # Columns agree, so enablement is the only action: rejection can only
    # come from its aspect, which a metadata-scoped declaration excludes.
    desired = _desired(
        columns=(DesiredColumn("seen_at", TimestampNtz()),),
        managed_aspects=METADATA_ASPECTS,
    )
    observed = _observed(columns=(ObservedColumn("seen_at", TimestampNtz()),))

    # When planning the resulting feature drift
    result = plan_diff(diff_table(desired, observed))

    # Then the column-structure action is rejected as out of scope
    assert isinstance(result, PlanningFailed)
    assert any("column structure" in failure.message for failure in result.failures)


def test_foreign_key_to_an_unregistered_parent_keeps_its_declared_referenced_spelling():
    constraint = ForeignKeyConstraint(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "unregistered_parent"),
        referenced_columns=("parent_id",),
        constraint_name="test_id_fk",
    )
    desired = DesiredTable(
        qualified_name=_NAME,
        columns=(DesiredColumn("id", Integer()),),
        foreign_keys=(constraint,),
    )
    diff = diff_table(desired, _observed())

    result = plan_diff(diff)

    assert isinstance(result, PlanningSucceeded)
    [action] = [action for action in result.plan if isinstance(action, SetForeignKey)]
    assert tuple(str(c) for c in action.constraint.referenced_columns) == ("parent_id",)


def test_primary_key_action_uses_the_observed_column_name():
    desired = _desired(
        columns=(DesiredColumn("requestid", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("requestid",), constraint_name="test_pk"),
    )
    observed = _observed(columns=(ObservedColumn("requestId", String(), nullable=False),))

    result = plan_diff(diff_table(desired, observed))

    assert isinstance(result, PlanningSucceeded)
    [action] = [action for action in result.plan if isinstance(action, SetPrimaryKey)]
    assert tuple(str(c) for c in action.primary_key.columns) == ("requestId",)


def test_created_table_uses_its_columns_spelling_for_internal_references():
    desired = _desired(
        columns=(DesiredColumn("requestId", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("REQUESTID",), constraint_name="test_pk"),
        clustered_by=("REQUESTID",),
    )

    result = plan_diff(diff_table(desired, None))

    assert isinstance(result, PlanningSucceeded)
    [create] = [action for action in result.plan if isinstance(action, CreateTable)]
    assert create.table.primary_key is not None
    assert tuple(str(c) for c in create.table.primary_key.columns) == ("requestId",)
    assert tuple(str(c) for c in create.table.clustered_by) == ("requestId",)


def test_foreign_key_action_uses_observed_column_names_on_both_sides():
    # Given a child foreign key declared in lowercase on both sides
    parent_name = QualifiedName("dev", "silver", "parent")
    child_constraint = ForeignKeyConstraint(
        local_columns=("orderref",),
        referenced_table=parent_name,
        referenced_columns=("orderid",),
        constraint_name="test_orderref_fk",
    )
    child_desired = _desired(
        columns=(DesiredColumn("orderref", Integer()),),
        foreign_keys=(child_constraint,),
    )
    child_observed = _observed(columns=(ObservedColumn("orderRef", Integer()),))
    # And a parent whose observed spelling is mixed-case
    parent_observed = ObservedTable(
        qualified_name=parent_name,
        columns=(ObservedColumn("OrderId", Integer(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("OrderId",), constraint_name="parent_pk"),
    )

    # When diffing with the observed parent available and then planning
    child_diff = diff_table(
        child_desired,
        child_observed,
        {
            parent_name: {
                column.name: column.name for column in parent_observed.columns
            }
        },
    )
    result = plan_diff(child_diff)

    # Then both sides use their observed physical names
    assert isinstance(result, PlanningSucceeded)
    [action] = [action for action in result.plan if isinstance(action, SetForeignKey)]
    assert tuple(str(c) for c in action.constraint.local_columns) == ("orderRef",)
    assert tuple(str(c) for c in action.constraint.referenced_columns) == ("OrderId",)


def test_foreign_key_to_a_renamed_parent_key_keeps_the_new_declared_name():
    # Given a parent renaming its key column
    parent_name = QualifiedName("dev", "silver", "parent")
    parent_desired = DesiredTable(
        qualified_name=parent_name,
        columns=(DesiredColumn("orderNumber", Integer(), nullable=False, renamed_from="orderid"),),
        primary_key=PrimaryKeyConstraint(columns=("orderNumber",), constraint_name="parent_pk"),
    )
    parent_observed = ObservedTable(
        qualified_name=parent_name,
        columns=(ObservedColumn("OrderId", Integer(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("OrderId",), constraint_name="parent_pk"),
    )
    # And a child key referencing the parent's declared rename target
    child_constraint = ForeignKeyConstraint(
        local_columns=("ref",),
        referenced_table=parent_name,
        referenced_columns=parent_desired.primary_key_columns,
        constraint_name="test_ref_fk",
    )
    child_desired = _desired(
        columns=(DesiredColumn("ref", Integer()),),
        foreign_keys=(child_constraint,),
    )
    child_diff = diff_table(
        child_desired,
        _observed(columns=(ObservedColumn("ref", Integer()),)),
        {
            parent_name: {
                column.name: column.name
                for column in (*parent_desired.columns, *parent_observed.columns)
            }
        },
    )

    # When planning
    result = plan_diff(child_diff)

    # Then the referenced side keeps the new declared name
    assert isinstance(result, PlanningSucceeded)
    [action] = [action for action in result.plan if isinstance(action, SetForeignKey)]
    assert tuple(str(c) for c in action.constraint.referenced_columns) == ("orderNumber",)


def test_self_referencing_foreign_key_uses_its_observed_column_names():
    # Given a self-referencing key declared lowercase against mixed-case physical spellings
    constraint = ForeignKeyConstraint(
        local_columns=("parentref",),
        referenced_table=_NAME,
        referenced_columns=("id",),
        constraint_name="test_parentref_fk",
    )
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer(), nullable=False),
            DesiredColumn("parentref", Integer()),
        ),
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk"),
        foreign_keys=(constraint,),
    )
    observed = _observed(
        columns=(
            ObservedColumn("Id", Integer(), nullable=False),
            ObservedColumn("ParentRef", Integer()),
        ),
        primary_key=PrimaryKeyConstraint(columns=("Id",), constraint_name="test_pk"),
    )
    diff = diff_table(desired, observed)

    # When planning
    result = plan_diff(diff)

    # Then both sides use their observed physical names
    assert isinstance(result, PlanningSucceeded)
    [action] = [action for action in result.plan if isinstance(action, SetForeignKey)]
    assert tuple(str(c) for c in action.constraint.local_columns) == ("ParentRef",)
    assert tuple(str(c) for c in action.constraint.referenced_columns) == ("Id",)
