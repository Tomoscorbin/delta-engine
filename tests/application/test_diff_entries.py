import inspect
import typing

import pytest

from delta_engine.application.diff_entries import (
    DiffCategory,
    DiffEntry,
    DiffOperation,
    action_entries,
    unresolvable_entries,
)
from delta_engine.domain.model import (
    Array,
    Decimal,
    DesiredColumn,
    DesiredTable,
    ForeignKeyConstraint,
    Integer,
    Long,
    Map,
    ObservedColumn,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
    Struct,
    StructField,
    TableFeature,
)
from delta_engine.domain.plan import (
    ColumnCaseDrift,
    ColumnRenameConflict,
    PartitioningChanged,
    PropertyUndeclared,
    actions as actions_module,
)
from delta_engine.domain.plan.actions import (
    Action,
    AddColumn,
    AlterClustering,
    AlterColumnType,
    CreateTable,
    DropColumn,
    DropForeignKey,
    DropPrimaryKey,
    EnableTableFeature,
    RenameColumn,
    SetColumnComment,
    SetColumnNullability,
    SetColumnTag,
    SetForeignKey,
    SetPrimaryKey,
    SetProperty,
    SetTableComment,
    SetTableTag,
    UnsetColumnTag,
    UnsetProperty,
    UnsetTableTag,
)
from delta_engine.domain.plan.unresolvable import Unresolvable


def _primary_key(
    columns: tuple[str, ...] = ("id",), name: str | None = "tbl_pk"
) -> PrimaryKeyConstraint:
    return PrimaryKeyConstraint(columns, name)


def _foreign_key(name: str | None = "orders_customer_id_fk") -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=("customer_id",),
        referenced_table=QualifiedName("cat", "sch", "customers"),
        referenced_columns=("id",),
        name=name,
    )


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (
            AddColumn(DesiredColumn("age", Integer())),
            (DiffEntry(DiffCategory.COLUMNS, DiffOperation.ADD, "age", ("Integer",)),),
        ),
        (
            AddColumn(DesiredColumn("age", Integer(), nullable=False)),
            (DiffEntry(DiffCategory.COLUMNS, DiffOperation.ADD, "age", ("Integer", "NOT NULL")),),
        ),
        # The comment rides the column line; the empty middle phrase holds the
        # NOT NULL position so comments align down a mixed group.
        (
            AddColumn(DesiredColumn("age", Integer(), comment="Age in years")),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.ADD,
                    "age",
                    ("Integer", "", "'Age in years'"),
                ),
            ),
        ),
        (
            AddColumn(DesiredColumn("age", Integer(), nullable=False, comment="Age in years")),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.ADD,
                    "age",
                    ("Integer", "NOT NULL", "'Age in years'"),
                ),
            ),
        ),
        (
            DropColumn(column=ObservedColumn("legacy", Integer())),
            (DiffEntry(DiffCategory.COLUMNS, DiffOperation.REMOVE, "legacy"),),
        ),
        (
            SetColumnNullability(column_name="id", desired_nullable=False, observed_nullable=True),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "id",
                    ("set NOT NULL (was nullable)",),
                ),
            ),
        ),
        (
            SetColumnNullability(column_name="id", desired_nullable=True, observed_nullable=False),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "id",
                    ("drop NOT NULL (was NOT NULL)",),
                ),
            ),
        ),
        (
            AlterColumnType(column_name="id", desired_type=Long(), observed_type=Integer()),
            (DiffEntry(DiffCategory.COLUMNS, DiffOperation.CHANGE, "id", ("Integer → Long",)),),
        ),
        # Decimal renders its parameters — the bare class name would hide a
        # precision widen.
        (
            AlterColumnType(
                column_name="amount",
                desired_type=Decimal(12, 2),
                observed_type=Decimal(10, 2),
            ),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "amount",
                    ("Decimal(10,2) → Decimal(12,2)",),
                ),
            ),
        ),
        # A nested type spells its own structure, so its delimiters have to
        # nest too — unbalanced ones leave a reader unable to tell where an
        # inner type ends.
        (
            AddColumn(
                DesiredColumn(
                    "payload",
                    Struct(
                        (
                            StructField("id", Integer()),
                            StructField("labels", Map(String(), Array(String()))),
                        )
                    ),
                )
            ),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.ADD,
                    "payload",
                    ("Struct<id: Integer, labels: Map<String, Array<String>>>",),
                ),
            ),
        ),
        (
            SetPrimaryKey(primary_key=_primary_key(("id", "tenant_id"))),
            (
                DiffEntry(
                    DiffCategory.KEYS,
                    DiffOperation.ADD,
                    "primary key tbl_pk",
                    ("(id, tenant_id)",),
                ),
            ),
        ),
        (
            DropPrimaryKey("legacy_pk"),
            (DiffEntry(DiffCategory.KEYS, DiffOperation.REMOVE, "primary key legacy_pk"),),
        ),
        (
            SetForeignKey(constraint=_foreign_key()),
            (
                DiffEntry(
                    DiffCategory.KEYS,
                    DiffOperation.ADD,
                    "foreign key orders_customer_id_fk",
                    ("(customer_id)", "→ cat.sch.customers"),
                ),
            ),
        ),
        (
            DropForeignKey(name="orders_customer_id_fk"),
            (
                DiffEntry(
                    DiffCategory.KEYS, DiffOperation.REMOVE, "foreign key orders_customer_id_fk"
                ),
            ),
        ),
        # A permanent protocol upgrade is stated as such.
        (
            EnableTableFeature(feature=TableFeature.TIMESTAMP_NTZ),
            (
                DiffEntry(
                    DiffCategory.FEATURES,
                    DiffOperation.ADD,
                    "timestampNtz",
                    ("— permanent protocol upgrade",),
                ),
            ),
        ),
        (
            SetProperty(
                name="delta.enableChangeDataFeed", desired_value="true", observed_value=None
            ),
            (
                DiffEntry(
                    DiffCategory.PROPERTIES,
                    DiffOperation.ADD,
                    "delta.enableChangeDataFeed",
                    ("= 'true'",),
                ),
            ),
        ),
        # The old value trails as its own phrase, so it aligns down the group.
        (
            SetProperty(
                name="delta.enableChangeDataFeed",
                desired_value="true",
                observed_value="false",
            ),
            (
                DiffEntry(
                    DiffCategory.PROPERTIES,
                    DiffOperation.CHANGE,
                    "delta.enableChangeDataFeed",
                    ("= 'true'", "(was 'false')"),
                ),
            ),
        ),
        (
            UnsetProperty(name="delta.logRetentionDuration", observed_value="old"),
            (
                DiffEntry(
                    DiffCategory.PROPERTIES, DiffOperation.REMOVE, "delta.logRetentionDuration"
                ),
            ),
        ),
        (
            SetTableTag(name="env", desired_value="prod", observed_value=None),
            (DiffEntry(DiffCategory.TAGS, DiffOperation.ADD, "env", ("= 'prod'",)),),
        ),
        (
            SetTableTag(name="env", desired_value="prod", observed_value="dev"),
            (
                DiffEntry(
                    DiffCategory.TAGS, DiffOperation.CHANGE, "env", ("= 'prod'", "(was 'dev')")
                ),
            ),
        ),
        (
            UnsetTableTag(name="env"),
            (DiffEntry(DiffCategory.TAGS, DiffOperation.REMOVE, "env"),),
        ),
        (
            SetColumnTag(
                column_name="email", name="pii", desired_value="true", observed_value=None
            ),
            (DiffEntry(DiffCategory.TAGS, DiffOperation.ADD, "column email.pii", ("= 'true'",)),),
        ),
        (
            SetColumnTag(
                column_name="email",
                name="pii",
                desired_value="true",
                observed_value="false",
            ),
            (
                DiffEntry(
                    DiffCategory.TAGS,
                    DiffOperation.CHANGE,
                    "column email.pii",
                    ("= 'true'", "(was 'false')"),
                ),
            ),
        ),
        (
            UnsetColumnTag(column_name="email", name="pii"),
            (DiffEntry(DiffCategory.TAGS, DiffOperation.REMOVE, "column email.pii"),),
        ),
        # The subject names what carries the comment; alignment separates it
        # from the text, so no colon is needed. A comment set where none
        # existed adds; replacing one changes; clearing one removes —
        # mirroring properties and tags.
        (
            SetColumnComment(column_name="id", desired_comment="the key", observed_comment=""),
            (DiffEntry(DiffCategory.COMMENTS, DiffOperation.ADD, "column id", ("'the key'",)),),
        ),
        (
            SetColumnComment(column_name="id", desired_comment="the key", observed_comment="old"),
            (DiffEntry(DiffCategory.COMMENTS, DiffOperation.CHANGE, "column id", ("'the key'",)),),
        ),
        (
            SetColumnComment(column_name="id", desired_comment="", observed_comment="old"),
            (DiffEntry(DiffCategory.COMMENTS, DiffOperation.REMOVE, "column id"),),
        ),
        (
            SetTableComment(desired_comment="core table", observed_comment=""),
            (DiffEntry(DiffCategory.COMMENTS, DiffOperation.ADD, "table", ("'core table'",)),),
        ),
        (
            SetTableComment(desired_comment="core table", observed_comment="old"),
            (DiffEntry(DiffCategory.COMMENTS, DiffOperation.CHANGE, "table", ("'core table'",)),),
        ),
        (
            SetTableComment(desired_comment="", observed_comment="old"),
            (DiffEntry(DiffCategory.COMMENTS, DiffOperation.REMOVE, "table"),),
        ),
        (
            AlterClustering(desired_clustering=("region", "day"), observed_clustering=()),
            (
                DiffEntry(
                    DiffCategory.CLUSTERING,
                    DiffOperation.CHANGE,
                    "clustering",
                    ("(region, day)", "— run OPTIMIZE FULL to recluster existing data"),
                ),
            ),
        ),
        # Removal carries no OPTIMIZE hint: OPTIMIZE FULL errors on a table
        # without clustering columns. It names the keys being removed — the
        # subject alone restates the heading and would render an empty line.
        (
            AlterClustering(desired_clustering=(), observed_clustering=("region",)),
            (
                DiffEntry(
                    DiffCategory.CLUSTERING, DiffOperation.REMOVE, "clustering", ("(region)",)
                ),
            ),
        ),
        (
            RenameColumn(old_name="customer_nm", new_name="customer_name"),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "customer_nm",
                    ("renamed → customer_name",),
                ),
            ),
        ),
    ],
)
def test_action_entries_render_expected(action, expected):
    # Then each action lowers to its category-tagged diff entries
    assert action_entries(action) == expected


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        pytest.param(
            SetPrimaryKey(primary_key=_primary_key(("id", "tenant_id"), None)),
            DiffEntry(
                DiffCategory.KEYS,
                DiffOperation.ADD,
                "primary key (id, tenant_id)",
            ),
            id="primary-key",
        ),
        pytest.param(
            SetForeignKey(constraint=_foreign_key(None)),
            DiffEntry(
                DiffCategory.KEYS,
                DiffOperation.ADD,
                "foreign key (customer_id)",
                ("→ cat.sch.customers",),
            ),
            id="foreign-key",
        ),
    ],
)
def test_unnamed_key_entries_identify_constraints_by_columns(action, expected):
    # Then the column list identifies the key and never repeats as detail
    assert action_entries(action) == (expected,)


def test_create_table_entries_include_clustering_without_optimize_hint():
    # Given a CREATE TABLE that declares clustering keys
    action = CreateTable(
        table=DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "tbl"),
            columns=(DesiredColumn("id", Integer()), DesiredColumn("region", String())),
            clustered_by=("region",),
        )
    )
    # When rendering its diff entries
    entries = action_entries(action)
    # Then a clustering line is present with no OPTIMIZE hint (new table, no data)
    clustering = [e for e in entries if e.category is DiffCategory.CLUSTERING]
    assert clustering == [
        DiffEntry(DiffCategory.CLUSTERING, DiffOperation.ADD, "clustering", ("(region)",))
    ]


def test_create_table_entries_include_all_state_embedded_in_create():
    # Given a CREATE TABLE carrying structural, layout, property, and comment state
    action = CreateTable(
        table=DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "tbl"),
            columns=(
                DesiredColumn("id", Integer(), nullable=False, comment="identifier"),
                DesiredColumn("day", String(), comment="partition date"),
            ),
            comment="daily orders",
            properties={
                "delta.appendOnly": "true",
                "delta.logRetentionDuration": None,
            },
            partitioned_by=("day",),
            primary_key=PrimaryKeyConstraint(("id",), "tbl_pk"),
        )
    )

    # Then reporting states every fact that CREATE TABLE establishes, with
    # each column's comment on its own line rather than exiled to the
    # comments group. A None property asserts absence and is therefore not a
    # creation change.
    assert action_entries(action) == (
        DiffEntry(
            DiffCategory.COLUMNS, DiffOperation.ADD, "id", ("Integer", "NOT NULL", "'identifier'")
        ),
        DiffEntry(
            DiffCategory.COLUMNS, DiffOperation.ADD, "day", ("String", "", "'partition date'")
        ),
        DiffEntry(DiffCategory.KEYS, DiffOperation.ADD, "primary key tbl_pk", ("(id)",)),
        DiffEntry(DiffCategory.PARTITIONING, DiffOperation.ADD, "partitioning", ("(day)",)),
        DiffEntry(DiffCategory.PROPERTIES, DiffOperation.ADD, "delta.appendOnly", ("= 'true'",)),
        DiffEntry(DiffCategory.COMMENTS, DiffOperation.ADD, "table", ("'daily orders'",)),
    )


def test_create_table_entry_identifies_unnamed_primary_key_by_columns():
    # Given a CREATE TABLE whose primary key requests no name
    action = CreateTable(
        table=DesiredTable(
            qualified_name=QualifiedName("cat", "sch", "tbl"),
            columns=(DesiredColumn("id", Integer(), nullable=False),),
            primary_key=PrimaryKeyConstraint(("id",)),
        )
    )

    key_entries = [entry for entry in action_entries(action) if entry.category is DiffCategory.KEYS]

    # Then the key entry is identified by its column list
    assert key_entries == [DiffEntry(DiffCategory.KEYS, DiffOperation.ADD, "primary key (id)")]


def test_every_category_names_itself_in_singular_and_plural():
    # Given every diff category
    # Then each names itself both ways, so a new one cannot reach a report unnamed
    for category in DiffCategory:
        assert category.plural
        assert category.counted(1).startswith("1 ")
        assert category.counted(2).startswith("2 ")


@pytest.mark.parametrize(
    ("category", "count", "expected"),
    [
        (DiffCategory.COLUMNS, 1, "1 column"),
        (DiffCategory.COLUMNS, 3, "3 columns"),
        (DiffCategory.KEYS, 1, "1 key"),
        # Some nouns do not inflect: "1 clustering" and "2 clustering" both read
        # correctly, where "2 clusterings" would not.
        (DiffCategory.CLUSTERING, 2, "2 clustering"),
        (DiffCategory.FEATURES, 1, "1 table feature"),
        (DiffCategory.FEATURES, 2, "2 table features"),
    ],
)
def test_a_category_counts_itself_with_the_right_noun(category, count, expected):
    # Then the humanised count uses the noun that agrees with it
    assert category.counted(count) == expected


def test_every_action_type_has_registered_diff_entries():
    # Given every concrete Action subclass the plan vocabulary defines
    concrete_action_types = [
        obj
        for _, obj in inspect.getmembers(actions_module, inspect.isclass)
        if issubclass(obj, Action) and obj is not Action
    ]

    # Then each dispatches to a real arm, not the NotImplementedError fallback
    fallback = action_entries.dispatch(object)
    for action_type in concrete_action_types:
        assert action_entries.dispatch(action_type) is not fallback, (
            f"No diff entries registered for {action_type.__name__}"
        )


def test_every_unresolvable_type_has_registered_diff_entries():
    # Given every member of the Unresolvable union
    # Then each dispatches to a real arm, not the NotImplementedError fallback
    fallback = unresolvable_entries.dispatch(object)
    for unresolvable_type in typing.get_args(Unresolvable.__value__):
        assert unresolvable_entries.dispatch(unresolvable_type) is not fallback, (
            f"No diff entries registered for {unresolvable_type.__name__}"
        )


@pytest.mark.parametrize(
    ("unresolvable", "expected"),
    [
        (
            ColumnCaseDrift(declared_name="SKU", observed_name="sku"),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "SKU",
                    ("spelled 'sku' in the catalog",),
                ),
            ),
        ),
        (
            ColumnRenameConflict(old_name="old_id", new_name="id"),
            (
                DiffEntry(
                    DiffCategory.COLUMNS,
                    DiffOperation.CHANGE,
                    "old_id",
                    ("renamed → id, but both columns exist",),
                ),
            ),
        ),
        (
            PropertyUndeclared(name="delta.enableChangeDataFeed", observed_value="true"),
            (
                DiffEntry(
                    DiffCategory.PROPERTIES,
                    DiffOperation.CHANGE,
                    "delta.enableChangeDataFeed",
                    ("= 'true'", "(set on the table, undeclared)"),
                ),
            ),
        ),
        (
            PartitioningChanged(
                desired_partitioning=("region",), observed_partitioning=("country",)
            ),
            (
                DiffEntry(
                    DiffCategory.PARTITIONING,
                    DiffOperation.CHANGE,
                    "partitioning",
                    ("(country) → (region)",),
                ),
            ),
        ),
    ],
)
def test_unresolvable_differences_describe_themselves(unresolvable, expected):
    # Then each unresolvable difference states itself as a CHANGE entry
    assert unresolvable_entries(unresolvable) == expected
