import pytest

from delta_engine.application.planning import (
    PlanningAccepted,
    PlanningDeferred,
    PlanningRejected,
    accepted_plan,
    plan_changes,
)
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
    TableKind,
    TableScope,
    TimestampNtz,
)
from delta_engine.domain.plan import (
    ActionPlan,
    AddColumn,
    AlterColumnType,
    ColumnRenameConflict,
    CreateTable,
    DropForeignKey,
    DropPrimaryKey,
    RenameColumn,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetTableTag,
    TableDrift,
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
    merged = values | overrides
    return ObservedTable(**merged)


def _foreign_key(
    *,
    local_columns: tuple[str, ...],
    referenced_table: QualifiedName,
    referenced_columns: tuple[str, ...],
    name: str,
) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=local_columns,
        referenced_table=referenced_table,
        referenced_columns=referenced_columns,
        name=name,
    )


def test_plan_changes_accepts_safe_actions():
    # Given drift whose one difference is a safe nullable column addition
    result = plan_changes(
        _desired(columns=(DesiredColumn("id", Integer()), DesiredColumn("age", Integer()))),
        _observed(),
    )

    # Then planning accepts and the plan carries exactly that action
    assert isinstance(result, PlanningAccepted)
    assert result.plan.target == _NAME
    assert result.plan.actions == (AddColumn(DesiredColumn("age", Integer())),)


def test_plan_changes_rejects_unsafe_actions():
    # Given drift adding a NOT NULL column to an existing table
    result = plan_changes(
        _desired(
            columns=(
                DesiredColumn("id", Integer()),
                DesiredColumn("required", Integer(), nullable=False),
            )
        ),
        _observed(),
    )

    # Then planning rejects with the safety rule's failure and no plan exists
    assert isinstance(result, PlanningRejected)
    assert [failure.rule_name for failure in result.failures] == ["NonNullableColumnAdd"]


def test_plan_changes_rejects_unmanaged_actions():
    # Given column-structure drift under a declaration managing only annotations
    result = plan_changes(
        _desired(
            columns=(DesiredColumn("id", Integer()), DesiredColumn("age", Integer())),
            scope=TableScope.ANNOTATIONS,
        ),
        _observed(),
    )

    # Then the eligibility check rejects the out-of-scope work
    assert isinstance(result, PlanningRejected)
    assert [failure.rule_name for failure in result.failures] == ["UnmanagedAspectDrift"]


def test_an_accepted_outcome_retains_the_diff_it_planned_from():
    # Given a diff that planning will accept
    desired = _desired(columns=(DesiredColumn("id", Integer()), DesiredColumn("age", Integer())))
    observed = _observed()

    # When planning
    result = plan_changes(desired, observed)

    # Then the outcome retains the diff, so a report can show what drifted
    assert isinstance(result, PlanningAccepted)
    assert result.diff == diff_table(desired, observed)


def test_a_refused_outcome_retains_the_diff_it_refused():
    # Given a diff that planning will reject
    desired = _desired(
        columns=(
            DesiredColumn("id", Integer()),
            DesiredColumn("required", Integer(), nullable=False),
        )
    )
    observed = _observed()

    # When planning
    result = plan_changes(desired, observed)

    # Then the rejected outcome still retains the diff it refused
    assert isinstance(result, PlanningRejected)
    assert result.diff == diff_table(desired, observed)


def test_an_accepted_outcome_rejects_a_plan_for_another_tables_diff():
    # Given a diff built for one table and a plan built for another
    diff = diff_table(_desired(), None)
    other_plan = ActionPlan(target=QualifiedName("dev", "silver", "other"))

    # Then the outcome refuses to pair them
    with pytest.raises(ValueError):
        PlanningAccepted(diff=diff, plan=other_plan)


def test_an_accepted_outcome_rejects_unresolvable_differences():
    # Given a diff still carrying an unresolvable difference
    diff = TableDrift(
        desired=_desired(),
        observed=_observed(),
        unresolvable=(ColumnRenameConflict(old_name="old", new_name="new"),),
    )

    # Then it cannot be paired with an accepted plan
    with pytest.raises(ValueError):
        PlanningAccepted(diff=diff, plan=ActionPlan(target=_NAME))


def test_plan_changes_accepts_no_op_as_an_empty_plan():
    # Given desired and observed states that already agree
    result = plan_changes(_desired(), _observed())

    # Then planning accepts with an empty, target-bearing plan — the natural zero
    assert isinstance(result, PlanningAccepted)
    assert result.plan.target == _NAME
    assert result.plan.actions == ()


def test_plan_changes_accepts_missing_table_and_builds_follow_up_actions():
    # Given a missing table declared with tags and a foreign key
    foreign_key = _foreign_key(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "parent"),
        referenced_columns=("id",),
        name="test_id_fk",
    )
    desired = _desired(
        columns=(DesiredColumn("id", Integer(), tags={"pii": "false"}),),
        tags={"env": "dev"},
        foreign_keys=(foreign_key,),
    )

    # When planning
    result = plan_changes(desired, None)

    # Then the create is followed by tag and constraint actions
    assert isinstance(result, PlanningAccepted)
    assert result.plan.target == desired.qualified_name
    assert result.plan.actions == (
        CreateTable(desired),
        SetTableTag(name="env", desired_value="dev", observed_value=None),
        SetColumnTag(column_name="id", name="pii", desired_value="false", observed_value=None),
        SetForeignKey(constraint=foreign_key),
    )


@pytest.mark.parametrize(
    "scope",
    [TableScope.TAGS, TableScope.ANNOTATIONS, TableScope.METADATA],
)
def test_plan_changes_defers_missing_table_when_table_existence_is_unmanaged(scope):
    # Given a missing table whose declaration cannot create it
    desired = _desired(scope=scope)

    # When planning against absence
    result = plan_changes(desired, None)

    # Then the outcome is a deferral, not a failure
    assert isinstance(result, PlanningDeferred)


def test_a_deferred_outcome_retains_the_creation_diff_it_deferred():
    # Given a deferred missing table
    desired = _desired(scope=TableScope.ANNOTATIONS)

    result = plan_changes(desired, None)

    # Then the deferral retains the creation diff, like every planning outcome
    assert isinstance(result, PlanningDeferred)
    assert result.diff == diff_table(desired, None)


def test_a_deferred_outcome_narrows_to_no_accepted_plan():
    # Given a deferred missing table
    desired = _desired(scope=TableScope.ANNOTATIONS)

    result = plan_changes(desired, None)

    # Then narrowing the outcome yields no plan to execute
    assert accepted_plan(result) is None


def test_plan_changes_keeps_rename_and_residual_drift_under_the_new_name():
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
    result = plan_changes(desired, observed)

    # Then the residual drift lands under the new name
    assert isinstance(result, PlanningAccepted)
    assert result.plan.actions == (
        RenameColumn(old_name="amt", new_name="amount"),
        AlterColumnType(column_name="amount", desired_type=Long(), observed_type=Integer()),
    )


def test_plan_changes_replaces_a_primary_key_explicitly_across_a_rename():
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
    result = plan_changes(desired, observed)

    # Then the plan drops the old key, renames, then sets the new key
    assert isinstance(result, PlanningAccepted)
    assert result.plan.actions == (
        DropPrimaryKey("legacy_pk"),
        RenameColumn("customer_nm", "customer_name"),
        SetPrimaryKey(primary_key=desired_key),
    )


def test_plan_changes_rejects_rename_of_a_primary_key_referenced_by_foreign_keys():
    # Given an inbound reference to the primary key being renamed
    reference = ForeignKeyReference(
        name="orders_customer_id_fk",
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
    result = plan_changes(desired, observed)

    # Then planning fails with the referenced-key rule
    assert isinstance(result, PlanningRejected)
    assert [failure.rule_name for failure in result.failures] == [
        "PrimaryKeyReferencedByForeignKeys"
    ]


def test_plan_changes_replaces_a_foreign_key_explicitly_across_a_rename():
    # Given a foreign key whose local column is being renamed
    parent = QualifiedName("dev", "silver", "parent")
    desired_key = _foreign_key(
        local_columns=("parent_id",),
        referenced_table=parent,
        referenced_columns=("id",),
        name="test_parent_id_fk",
    )
    observed_key = _foreign_key(
        local_columns=("parent",),
        referenced_table=parent,
        referenced_columns=("id",),
        name="legacy_fk",
    )
    desired = _desired(
        columns=(DesiredColumn("parent_id", Integer(), renamed_from="parent"),),
        foreign_keys=(desired_key,),
    )
    observed = _observed(
        columns=(ObservedColumn("parent", Integer()),), foreign_keys=(observed_key,)
    )

    # When planning
    result = plan_changes(desired, observed)

    # Then the plan drops the old key, renames, then sets the new key
    assert isinstance(result, PlanningAccepted)
    assert result.plan.actions == (
        DropForeignKey(name="legacy_fk"),
        RenameColumn("parent", "parent_id"),
        SetForeignKey(constraint=desired_key),
    )


def test_plan_changes_replaces_a_self_referencing_foreign_key_explicitly_across_a_rename():
    # Given a self-referencing key whose referenced column is being renamed
    desired_key = _foreign_key(
        local_columns=("manager_id",),
        referenced_table=_NAME,
        referenced_columns=("employee_id",),
        name="test_manager_id_fk",
    )
    observed_key = _foreign_key(
        local_columns=("manager_id",),
        referenced_table=_NAME,
        referenced_columns=("id",),
        name="legacy_fk",
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
    result = plan_changes(desired, observed)

    # Then the plan drops the old key, renames, then sets the new key
    assert isinstance(result, PlanningAccepted)
    assert result.plan.actions == (
        DropForeignKey(name="legacy_fk"),
        RenameColumn("id", "employee_id"),
        SetForeignKey(constraint=desired_key),
    )


def test_plan_changes_drops_an_observed_only_foreign_key_alongside_a_rename():
    # Given an observed-only foreign key unrelated to the rename
    unrelated_key = _foreign_key(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "parent"),
        referenced_columns=("id",),
        name="legacy_fk",
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
    result = plan_changes(desired, observed)

    # Then the drop still lands alongside the rename
    assert isinstance(result, PlanningAccepted)
    assert result.plan.actions == (
        DropForeignKey(name="legacy_fk"),
        RenameColumn("customer_nm", "customer_name"),
    )


def test_plan_carries_the_observed_relation_kind():
    # Given tag drift against a streaming table, under a tags-only declaration
    desired = _desired(
        columns=(DesiredColumn("id", Integer(), tags={"pii": "low"}),),
        scope=TableScope.TAGS,
    )
    observed = _observed(kind=TableKind.STREAMING_TABLE)

    # When planning the diff
    result = plan_changes(desired, observed)

    # Then the plan knows what its actions lower against
    assert isinstance(result, PlanningAccepted)
    assert result.plan.target == desired.qualified_name
    assert result.plan.kind is TableKind.STREAMING_TABLE


def test_creation_plan_carries_the_ordinary_table_kind():
    # Given a missing table — absence has no observed kind, and the engine
    # only creates ordinary tables
    result = plan_changes(_desired(), None)

    assert isinstance(result, PlanningAccepted)
    assert result.plan.target == _NAME
    assert result.plan.kind is TableKind.TABLE


def test_feature_enablement_outside_column_structure_scope_is_rejected():
    # Given matching columns whose implied feature is absent, under metadata-only scope
    # Columns agree, so enablement is the only action: rejection can only
    # come from its aspect, which a metadata-scoped declaration excludes.
    desired = _desired(
        columns=(DesiredColumn("seen_at", TimestampNtz()),),
        scope=TableScope.METADATA,
    )
    observed = _observed(columns=(ObservedColumn("seen_at", TimestampNtz()),))

    # When planning the resulting feature drift
    result = plan_changes(desired, observed)

    # Then the column-structure action is rejected as out of scope
    assert isinstance(result, PlanningRejected)
    assert any("column structure" in failure.message for failure in result.failures)


def test_foreign_key_to_an_unregistered_parent_keeps_its_declared_referenced_spelling():
    # Given a foreign key to a table outside this sync
    constraint = ForeignKeyConstraint(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "unregistered_parent"),
        referenced_columns=("parent_id",),
        name="test_id_fk",
    )
    desired = DesiredTable(
        qualified_name=_NAME,
        columns=(DesiredColumn("id", Integer()),),
        foreign_keys=(constraint,),
    )
    # When planning
    result = plan_changes(desired, _observed())

    # Then the referenced spelling passes through planning untouched
    assert isinstance(result, PlanningAccepted)
    [action] = [action for action in result.plan if isinstance(action, SetForeignKey)]
    assert tuple(str(c) for c in action.constraint.referenced_columns) == ("parent_id",)


def test_created_table_uses_its_columns_spelling_for_internal_references():
    # Given a mixed-case table to create with key and layout references
    desired = _desired(
        columns=(DesiredColumn("requestId", String(), nullable=False),),
        primary_key=PrimaryKeyConstraint(columns=("requestId",), name="test_pk"),
        clustered_by=("requestId",),
    )

    # When planning the creation
    result = plan_changes(desired, None)

    # Then the creation plan carries the declared spelling untouched
    assert isinstance(result, PlanningAccepted)
    [create] = [action for action in result.plan if isinstance(action, CreateTable)]
    assert create.table.primary_key is not None
    assert tuple(str(c) for c in create.table.primary_key.columns) == ("requestId",)
    assert tuple(str(c) for c in create.table.clustered_by) == ("requestId",)


# ---------- foreign-key actions ----------


def test_foreign_key_actions_join_the_validated_plan_in_phase_order():
    # Given an in-scope drift whose only difference is a declared foreign key
    fk = _foreign_key(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("dev", "silver", "customers"),
        referenced_columns=("id",),
        name="orders_customers_fk",
    )
    # When planning
    result = plan_changes(
        _desired(
            columns=(DesiredColumn("id", Integer()), DesiredColumn("customer_id", Integer())),
            foreign_keys=(fk,),
        ),
        _observed(
            columns=(ObservedColumn("id", Integer()), ObservedColumn("customer_id", Integer())),
        ),
    )

    # Then the accepted plan carries the foreign-key action
    assert isinstance(result, PlanningAccepted)
    assert result.plan.actions == (SetForeignKey(constraint=fk),)


def test_pk_drop_exemption_sees_same_sync_foreign_key_drops():
    # Given a drift dropping its PK while this table's own FK references it,
    # with that FK dropped in the same diff
    reference = ForeignKeyReference(
        name="test_parent_id_fk",
        referencing_table=_NAME,
    )
    own_fk = _foreign_key(
        local_columns=("parent_id",),
        referenced_table=_NAME,
        referenced_columns=("id",),
        name="test_parent_id_fk",
    )
    # When planning
    result = plan_changes(
        _desired(
            columns=(DesiredColumn("id", Integer()), DesiredColumn("parent_id", Integer())),
        ),
        _observed(
            columns=(ObservedColumn("id", Integer()), ObservedColumn("parent_id", Integer())),
            primary_key=PrimaryKeyConstraint(("id",), "test_pk"),
            foreign_keys=(own_fk,),
            referencing_foreign_keys=(reference,),
        ),
    )

    # Then the exemption found the drop in the same stream, and the plan
    # phases the FK drop before the PK drop
    assert isinstance(result, PlanningAccepted)
    assert [type(action) for action in result.plan.actions] == [DropForeignKey, DropPrimaryKey]


def test_foreign_key_drift_on_an_unmanaged_aspect_fails_eligibility():
    # Given a declaration that does not manage foreign keys but declares one
    fk = _foreign_key(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("dev", "silver", "customers"),
        referenced_columns=("id",),
        name="orders_customers_fk",
    )
    # When planning
    result = plan_changes(
        _desired(
            columns=(DesiredColumn("id", Integer()), DesiredColumn("customer_id", Integer())),
            foreign_keys=(fk,),
            scope=TableScope.ANNOTATIONS,
        ),
        _observed(
            columns=(ObservedColumn("id", Integer()), ObservedColumn("customer_id", Integer())),
        ),
    )

    # Then the eligibility check rejects the unmanaged work
    assert isinstance(result, PlanningRejected)
    assert [failure.rule_name for failure in result.failures] == ["UnmanagedAspectDrift"]


def test_missing_table_plan_contains_the_declared_foreign_keys():
    # Given a missing table declaring one foreign key
    fk = _foreign_key(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("dev", "silver", "customers"),
        referenced_columns=("id",),
        name="orders_customers_fk",
    )
    desired = _desired(
        columns=(DesiredColumn("id", Integer()), DesiredColumn("customer_id", Integer())),
        foreign_keys=(fk,),
    )

    # When planning
    result = plan_changes(desired, None)

    # Then the accepted plan creates the table and then adds the constraint
    assert isinstance(result, PlanningAccepted)
    assert [type(action) for action in result.plan.actions] == [CreateTable, SetForeignKey]
