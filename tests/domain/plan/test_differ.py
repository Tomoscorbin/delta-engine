from hypothesis import given, strategies as st

from delta_engine.domain.model import (
    Boolean,
    Column,
    Date,
    Decimal,
    DesiredTable,
    Double,
    Float,
    Integer,
    Long,
    ObservedTable,
    QualifiedName,
    String,
    Timestamp,
)
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
    ColumnTypeChange,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    PartitioningChange,
    SetColumnComment,
    SetColumnNullability,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
)
from delta_engine.domain.plan.differ import compute_plan

# ----- Hypothesis strategies for valid domain objects

_SIMPLE_DATA_TYPES = st.one_of(
    st.just(Integer()),
    st.just(Long()),
    st.just(Float()),
    st.just(Double()),
    st.just(Boolean()),
    st.just(String()),
    st.just(Date()),
    st.just(Timestamp()),
    st.builds(
        Decimal,
        precision=st.integers(min_value=1, max_value=38),
        scale=st.integers(min_value=0, max_value=0),
    ).filter(lambda d: d.scale <= d.precision),
)

# Valid column name: non-empty, lowercase, no special chars that break the model
_COLUMN_NAME = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)

_COLUMN = st.builds(
    Column,
    name=_COLUMN_NAME,
    data_type=_SIMPLE_DATA_TYPES,
    nullable=st.booleans(),
    comment=st.text(max_size=40),
)


def _unique_columns(columns: list[Column]) -> list[Column]:
    """Deduplicate columns by name, keeping first occurrence."""
    seen: set[str] = set()
    result = []
    for column in columns:
        if column.name not in seen:
            seen.add(column.name)
            result.append(column)
    return result


_COLUMNS = st.lists(_COLUMN, min_size=1, max_size=6).map(_unique_columns).filter(bool)

_PROPERTY_KEY = st.from_regex(r"[a-z][a-z.]{0,19}", fullmatch=True)
_PROPERTIES = st.dictionaries(_PROPERTY_KEY, st.text(max_size=20), max_size=4)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


@st.composite
def _desired_table(draw: st.DrawFn) -> DesiredTable:
    columns = draw(_COLUMNS)
    column_names = [c.name for c in columns]
    partitioned_by = draw(
        st.lists(
            st.sampled_from(column_names), max_size=min(2, len(column_names)), unique=True
        ).map(tuple)
    )
    primary_key = draw(
        st.lists(
            st.sampled_from(column_names), max_size=min(2, len(column_names)), unique=True
        ).map(tuple)
    )
    return DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=tuple(columns),
        comment=draw(st.text(max_size=40)),
        properties=draw(_PROPERTIES),
        partitioned_by=partitioned_by,
        primary_key=primary_key,
    )


_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")
_BASELINE_COLUMNS = (Column("id", Integer()),)


def _desired(
    *,
    columns=_BASELINE_COLUMNS,
    comment="",
    properties=None,
    partitioned_by=(),
    primary_key=(),
) -> DesiredTable:
    """Build a DesiredTable, defaulting every dimension to a no-op baseline."""
    return DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=columns,
        comment=comment,
        properties=properties or {},
        partitioned_by=partitioned_by,
        primary_key=primary_key,
    )


def _observed(
    *,
    columns=_BASELINE_COLUMNS,
    comment="",
    properties=None,
    partitioned_by=(),
    primary_key=(),
) -> ObservedTable:
    """Build an ObservedTable matching `_desired`'s baseline so a single dimension can vary."""
    return ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=columns,
        comment=comment,
        properties=properties or {},
        partitioned_by=partitioned_by,
        primary_key=primary_key,
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
    plan = compute_plan(desired, observed=None)

    # Then: we get a CreateTable wrapped in an ActionPlan
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
    plan = compute_plan(desired, observed)

    # Then: nothing to do
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
    plan = compute_plan(desired, observed)

    # Then: the plan contains the expected representative actions
    assert isinstance(plan, ActionPlan)

    # Column add
    assert AddColumn(column=Column("age", Integer())) in plan.actions
    # Property set/update
    assert SetProperty(name="delta.appendOnly", value="false") in plan.actions
    # Comment update
    assert SetTableComment(comment="core table") in plan.actions
    # Partition change is surfaced as a PartitioningChange action in the plan
    assert (
        PartitioningChange(
            desired_partitioning=("event_date", "country"),
            observed_partitioning=("event_date",),
        )
        in plan.actions
    )


# ---------- column diffs ----------


def test_no_column_actions_when_columns_are_identical():
    # Given: desired and observed have the same columns, comments, and nullability
    columns = (Column("id", Integer()), Column("name", String(), comment="customer name"))

    # When
    plan = compute_plan(_desired(columns=columns), _observed(columns=columns))

    # Then: nothing to do
    assert plan.actions == ()


def test_adds_columns_present_only_in_desired():
    # Given: desired has an extra column not present in observed
    desired = _desired(columns=(Column("id", Integer()), Column("age", Integer())))
    observed = _observed(columns=(Column("id", Integer()),))

    # When
    plan = compute_plan(desired, observed)

    # Then: an AddColumn for "age" is produced
    assert AddColumn(column=Column("age", Integer())) in plan.actions


def test_drops_columns_present_only_in_observed():
    # Given: observed has a legacy column not present in desired
    desired = _desired(columns=(Column("id", Integer()),))
    observed = _observed(columns=(Column("id", Integer()), Column("legacy", String())))

    # When
    plan = compute_plan(desired, observed)

    # Then: a DropColumn for "legacy" is produced
    assert plan.actions == (DropColumn("legacy"),)


def test_sets_column_comment_when_desired_differs_from_observed():
    # Given: same column exists; desired has a comment, observed has none
    desired = _desired(columns=(Column("name", String(), comment="customer"),))
    observed = _observed(columns=(Column("name", String(), comment=""),))

    # When
    plan = compute_plan(desired, observed)

    # Then: a SetColumnComment aligns the comment
    assert plan.actions == (SetColumnComment("name", "customer"),)


def test_clears_column_comment_when_desired_is_empty_and_observed_is_not():
    # Given: same column exists; desired clears the comment
    desired = _desired(columns=(Column("name", String(), comment=""),))
    observed = _observed(columns=(Column("name", String(), comment="customer"),))

    # When
    plan = compute_plan(desired, observed)

    # Then: a SetColumnComment clears it to empty
    assert plan.actions == (SetColumnComment("name", ""),)


def test_sets_column_nullability_when_flag_differs():
    # Given: same column exists; desired flips nullability to NOT NULL
    desired = _desired(columns=(Column("active", String(), nullable=False),))
    observed = _observed(columns=(Column("active", String(), nullable=True),))

    # When
    plan = compute_plan(desired, observed)

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
    plan = compute_plan(desired, observed)

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
    plan = compute_plan(desired, observed)

    # Then: only one AddColumn; no redundant SetColumnComment or SetColumnNullability
    assert plan.actions == (
        AddColumn(column=Column("age", Integer(), comment="user age", nullable=False)),
    )


def test_emits_column_type_change_action_when_type_differs():
    # Given: same column name exists but data type differs
    desired = _desired(columns=(Column("id", String()),))
    observed = _observed(columns=(Column("id", Integer()),))

    # When
    plan = compute_plan(desired, observed)

    # Then: a ColumnTypeChange makes the drift visible in the plan so validation can reject it
    assert plan.actions == (
        ColumnTypeChange(column_name="id", from_type=Integer(), to_type=String()),
    )


def test_emits_partitioning_change_action_when_partition_spec_differs():
    # Given: desired and observed partition specs differ
    columns = (Column("id", Integer()), Column("ds", String()))
    desired = _desired(columns=columns, partitioned_by=("ds",))
    observed = _observed(columns=columns, partitioned_by=())

    # When
    plan = compute_plan(desired, observed)

    # Then: a PartitioningChange makes the conflict visible so validation can reject it
    assert plan.actions == (
        PartitioningChange(desired_partitioning=("ds",), observed_partitioning=()),
    )


def test_no_partitioning_action_when_partition_spec_is_unchanged():
    # Given: identical partition specs
    columns = (Column("id", Integer()), Column("ds", String()))
    plan = compute_plan(
        _desired(columns=columns, partitioned_by=("ds",)),
        _observed(columns=columns, partitioned_by=("ds",)),
    )

    # Then: nothing to do
    assert plan.actions == ()


# ---------- property diffs ----------


def test_no_property_actions_when_mappings_are_identical():
    # Given: desired and observed have identical properties
    props = {"delta.appendOnly": "true", "owner": "cdm"}

    # When
    plan = compute_plan(_desired(properties=props), _observed(properties=props))

    # Then: nothing to do
    assert plan.actions == ()


def test_sets_property_when_missing_in_observed():
    # Given: desired has a property missing from observed
    desired = _desired(properties={"delta.appendOnly": "true"})
    observed = _observed(properties={})

    # When
    plan = compute_plan(desired, observed)

    # Then: a SetProperty is emitted with the desired value
    assert plan.actions == (SetProperty(name="delta.appendOnly", value="true"),)


def test_updates_property_when_value_differs():
    # Given: key matches but value differs
    desired = _desired(properties={"delta.appendOnly": "false"})
    observed = _observed(properties={"delta.appendOnly": "true"})

    # When
    plan = compute_plan(desired, observed)

    # Then: a single SetProperty updates the value
    assert plan.actions == (SetProperty(name="delta.appendOnly", value="false"),)


def test_ignores_observed_only_properties():
    # Given: observed contains a property the user never declared
    #        (e.g. one Databricks set autonomously)
    desired = _desired(properties={"owner": "cdm"})
    observed = _observed(properties={"owner": "cdm", "delta.minReaderVersion": "2"})

    # When
    plan = compute_plan(desired, observed)

    # Then: the undeclared property is left untouched — no unset is emitted
    assert plan.actions == ()


# ---------- table comment diffs ----------


def test_no_comment_action_when_comments_match():
    # Given: same comment on desired and observed
    plan = compute_plan(_desired(comment="core table"), _observed(comment="core table"))

    # Then
    assert plan.actions == ()


def test_sets_table_comment_when_comment_differs():
    # Given: desired has a different comment than observed
    plan = compute_plan(_desired(comment="core table"), _observed(comment=""))

    # Then: a single SetTableComment is emitted with the desired text
    assert plan.actions == (SetTableComment(comment="core table"),)


def test_clears_table_comment_when_desired_is_empty():
    # Given: observed has a comment; desired clears it
    plan = compute_plan(_desired(comment=""), _observed(comment="legacy"))

    # Then: a single SetTableComment clears to empty
    assert plan.actions == (SetTableComment(comment=""),)


# ---------- property: reflexivity ----------


@given(_desired_table())
def test_compute_plan_produces_no_actions_when_desired_equals_observed(
    desired: DesiredTable,
) -> None:
    # Given: an arbitrary desired table and an observed table identical to it
    observed = ObservedTable(
        qualified_name=desired.qualified_name,
        columns=desired.columns,
        comment=desired.comment,
        properties=desired.properties,
        partitioned_by=desired.partitioned_by,
        primary_key=desired.primary_key,
    )

    # When: computing the plan
    plan = compute_plan(desired, observed)

    # Then: there is nothing to do — the differ is reflexive
    assert plan.actions == ()


# ---------- primary key diffs ----------


def _desired_with_pk(pk_columns: list[str]) -> DesiredTable:
    """Build a DesiredTable whose listed columns are NOT NULL and in the primary key."""
    extra_columns = [] if "name" in pk_columns else ["name"]
    all_column_names = pk_columns + extra_columns
    all_columns = tuple(
        Column(name, Integer(), nullable=name not in pk_columns) for name in all_column_names
    )
    return DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=all_columns,
        primary_key=tuple(pk_columns),
    )


def _observed_with_pk(pk_columns: list[str]) -> ObservedTable:
    """Build an ObservedTable with a given primary key (columns default to nullable=True)."""
    all_columns = (Column("id", Integer()), Column("name", String()))
    return ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=all_columns,
        primary_key=tuple(pk_columns),
    )


def test_no_pk_actions_when_both_desired_and_observed_have_no_pk():
    # Given: no primary key on either side
    plan = compute_plan(_desired(columns=(Column("id", Integer()),)), _observed())

    # Then: no PK actions
    assert not any(isinstance(a, (DropPrimaryKey, SetPrimaryKey)) for a in plan.actions)


def test_emits_set_primary_key_when_desired_has_pk_and_observed_has_none():
    # Given: desired declares a PK; observed has none
    desired = _desired_with_pk(["id"])
    observed = _observed_with_pk([])

    # When
    plan = compute_plan(desired, observed)

    # Then: a SetPrimaryKey is emitted; no DropPrimaryKey
    pk_actions = [a for a in plan.actions if isinstance(a, SetPrimaryKey)]
    assert len(pk_actions) == 1
    assert pk_actions[0].columns == (Column("id", Integer(), nullable=False),)
    assert pk_actions[0].constraint_name == "test_pk"
    assert not any(isinstance(a, DropPrimaryKey) for a in plan.actions)


def test_emits_drop_primary_key_when_desired_has_no_pk_and_observed_has_one():
    # Given: desired removes the PK; observed had one
    desired = _desired(columns=(Column("id", Integer()),))
    observed = _observed_with_pk(["id"])

    # When
    plan = compute_plan(desired, observed)

    # Then: a DropPrimaryKey is emitted; no SetPrimaryKey
    assert any(isinstance(a, DropPrimaryKey) for a in plan.actions)
    assert not any(isinstance(a, SetPrimaryKey) for a in plan.actions)


def test_emits_drop_and_set_when_pk_columns_change():
    # Given: desired changes the PK columns
    desired = _desired_with_pk(["id"])
    observed = _observed_with_pk(["name"])

    # When
    plan = compute_plan(desired, observed)

    # Then: both DropPrimaryKey and SetPrimaryKey are emitted
    assert any(isinstance(a, DropPrimaryKey) for a in plan.actions)
    assert any(isinstance(a, SetPrimaryKey) for a in plan.actions)


def test_no_pk_actions_when_pk_columns_match_regardless_of_order():
    # Given: desired and observed have the same PK columns in different order
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(
            Column("id", Integer(), nullable=False),
            Column("tenant_id", Integer(), nullable=False),
        ),
        primary_key=("id", "tenant_id"),
    )
    observed = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(
            Column("id", Integer(), nullable=False),
            Column("tenant_id", Integer(), nullable=False),
        ),
        primary_key=("tenant_id", "id"),
    )

    # When
    plan = compute_plan(desired, observed)

    # Then: order difference alone does not trigger a PK change
    assert not any(isinstance(a, (DropPrimaryKey, SetPrimaryKey)) for a in plan.actions)


def test_drop_primary_key_runs_before_add_column_in_plan():
    # Given: a plan that both drops the PK and adds a column
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer(), nullable=False), Column("new_col", String())),
        primary_key=(),
    )
    observed = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer(), nullable=False),),
        primary_key=("id",),
    )

    # When
    plan = compute_plan(desired, observed)

    types = [type(a) for a in plan.actions]
    drop_idx = types.index(DropPrimaryKey)
    add_idx = types.index(AddColumn)

    # Then: DropPrimaryKey comes before AddColumn
    assert drop_idx < add_idx


def test_set_primary_key_runs_after_set_column_nullability_in_plan():
    # Given: a plan that sets nullability and adds a PK in the same sync
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer(), nullable=False),),
        primary_key=("id",),
    )
    observed = ObservedTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer(), nullable=True),),
        primary_key=(),
    )

    # When
    plan = compute_plan(desired, observed)

    types = [type(a) for a in plan.actions]
    null_idx = types.index(SetColumnNullability)
    pk_idx = types.index(SetPrimaryKey)

    # Then: SetColumnNullability runs before SetPrimaryKey
    assert null_idx < pk_idx


# ---------- foreign key diffs ----------


def _orders_with_fk(fk: ForeignKeyConstraint) -> DesiredTable:
    return DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(
            Column("id", Integer()),
            Column("customer_id", Integer()),
        ),
        foreign_keys=(fk,),
    )


def _observed_orders(observed_fks: tuple[ForeignKeyConstraint, ...] = ()) -> ObservedTable:
    return ObservedTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(
            Column("id", Integer()),
            Column("customer_id", Integer()),
        ),
        foreign_keys=observed_fks,
    )


_FK = ForeignKeyConstraint(
    local_columns=("customer_id",),
    references="cat.sch.customers",
    referenced_columns=("id",),
)

_FK_WITH_EXPLICIT_NAME = ForeignKeyConstraint(
    local_columns=("customer_id",),
    references="cat.sch.customers",
    referenced_columns=("id",),
    constraint_name="custom_fk_name",
)


def test_no_fk_on_either_side_produces_no_fk_actions():
    # Given tables with no FKs
    desired = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(Column("id", Integer()),),
    )
    observed = ObservedTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(Column("id", Integer()),),
    )

    # When
    plan = compute_plan(desired, observed)

    # Then
    fk_actions = [a for a in plan if isinstance(a, (DropForeignKey, SetForeignKey))]
    assert fk_actions == []


def test_new_fk_on_desired_only_emits_set_foreign_key():
    # Given desired has a FK but observed has none
    desired = _orders_with_fk(_FK)
    observed = _observed_orders()

    # When
    plan = compute_plan(desired, observed)

    # Then exactly one SetForeignKey action is emitted
    set_actions = [a for a in plan if isinstance(a, SetForeignKey)]
    assert len(set_actions) == 1
    assert set_actions[0].foreign_key == _FK
    assert set_actions[0].constraint_name == "orders_customer_id_fk"


def test_fk_removed_from_desired_emits_drop_then_no_set():
    # Given observed has a FK but desired has none
    desired = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(Column("id", Integer()), Column("customer_id", Integer())),
    )
    observed = _observed_orders((_FK,))

    # When
    plan = compute_plan(desired, observed)

    # Then a DropForeignKey is emitted, no SetForeignKey
    drop_actions = [a for a in plan if isinstance(a, DropForeignKey)]
    set_actions = [a for a in plan if isinstance(a, SetForeignKey)]
    assert len(drop_actions) == 1
    assert drop_actions[0].constraint_name == "orders_customer_id_fk"
    assert set_actions == []


def test_fk_same_on_both_sides_produces_no_fk_actions():
    # Given desired and observed have identical FKs
    desired = _orders_with_fk(_FK)
    observed = _observed_orders((_FK,))

    # When
    plan = compute_plan(desired, observed)

    # Then no FK actions
    fk_actions = [a for a in plan if isinstance(a, (DropForeignKey, SetForeignKey))]
    assert fk_actions == []


def test_fk_with_explicit_constraint_name_uses_that_name():
    # Given desired has a FK with an explicit constraint name, observed has none
    desired = _orders_with_fk(_FK_WITH_EXPLICIT_NAME)
    observed = _observed_orders()

    # When
    plan = compute_plan(desired, observed)

    # Then SetForeignKey uses the explicit name
    set_actions = [a for a in plan if isinstance(a, SetForeignKey)]
    assert len(set_actions) == 1
    assert set_actions[0].constraint_name == "custom_fk_name"


def test_fk_changed_emits_drop_and_set():
    # Given the FK's referenced table changes between observed and desired
    old_fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.old_customers",
        referenced_columns=("id",),
    )
    new_fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.new_customers",
        referenced_columns=("id",),
    )
    desired = _orders_with_fk(new_fk)
    observed = _observed_orders((old_fk,))

    # When
    plan = compute_plan(desired, observed)

    # Then drop the old one, set the new one
    drop_actions = [a for a in plan if isinstance(a, DropForeignKey)]
    set_actions = [a for a in plan if isinstance(a, SetForeignKey)]
    assert len(drop_actions) == 1
    assert drop_actions[0].constraint_name == "orders_customer_id_fk"
    assert len(set_actions) == 1
    assert set_actions[0].foreign_key == new_fk
    assert set_actions[0].constraint_name == "orders_customer_id_fk"


def test_new_table_with_fk_includes_set_foreign_key_in_plan():
    # Given a brand-new table (observed=None) with a FK
    desired = _orders_with_fk(_FK)

    # When
    plan = compute_plan(desired, None)

    # Then plan contains CreateTable — and a SetForeignKey (FK applied after creation)
    assert any(isinstance(a, CreateTable) for a in plan)
    set_actions = [a for a in plan if isinstance(a, SetForeignKey)]
    assert len(set_actions) == 1
    assert set_actions[0].foreign_key == _FK
    assert set_actions[0].constraint_name == "orders_customer_id_fk"


def test_sync_is_idempotent_when_catalog_fk_has_externally_chosen_name():
    # Given: desired FK has no explicit name (derives orders_customer_id_fk);
    #        observed has the same relationship but a name chosen outside this engine
    desired_fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
    )
    observed_fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
        constraint_name="fk_made_in_the_console",
    )
    desired = _orders_with_fk(desired_fk)
    observed = _observed_orders((observed_fk,))

    # When
    plan = compute_plan(desired, observed)

    # Then no FK actions — the relationship already exists, name notwithstanding
    fk_actions = [a for a in plan if isinstance(a, (DropForeignKey, SetForeignKey))]
    assert fk_actions == []


def test_sync_is_idempotent_when_fk_already_exists_in_catalog():
    # Given: desired has a FK with no explicit constraint_name;
    #        observed has the same FK but with the catalog-stored derived name
    desired_fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
        # no constraint_name — user did not specify one
    )
    observed_fk = ForeignKeyConstraint(
        local_columns=("customer_id",),
        references="cat.sch.customers",
        referenced_columns=("id",),
        constraint_name="orders_customer_id_fk",  # catalog stored the derived name
    )
    desired = DesiredTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(Column("id", Integer()), Column("customer_id", Integer())),
        foreign_keys=(desired_fk,),
    )
    observed = ObservedTable(
        qualified_name=QualifiedName("cat", "sch", "orders"),
        columns=(Column("id", Integer()), Column("customer_id", Integer())),
        foreign_keys=(observed_fk,),
    )

    # When
    plan = compute_plan(desired, observed)

    # Then no FK actions are emitted — the FK is already in the right state
    fk_actions = [a for a in plan if isinstance(a, (DropForeignKey, SetForeignKey))]
    assert fk_actions == []
