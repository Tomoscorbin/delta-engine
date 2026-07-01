from delta_engine.application.validation import (
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    PrimaryKeyColumnsNullable,
    UnsupportedChangeRejected,
    validate_plan,
)
from delta_engine.domain.model import Column, DesiredTable, Integer, QualifiedName, String
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
from delta_engine.domain.plan import UnsupportedChange, UnsupportedChangeKind
from delta_engine.domain.plan.actions import (
    ActionPlan,
    AddColumn,
    CreateTable,
    DropPrimaryKey,
    SetColumnNullability,
    SetPrimaryKey,
)


def _plan(*actions) -> ActionPlan:
    return ActionPlan(actions)


# ---- NonNullableColumnAdd


def test_rejects_add_of_non_nullable_column():
    # Given a plan adding a NOT NULL column to an existing table
    rule = NonNullableColumnAdd()

    # When evaluating
    failures = rule.evaluate(_plan(AddColumn(Column("order_id", Integer(), nullable=False))))

    # Then the violation is flagged
    assert len(failures) == 1
    assert failures[0].rule_name == "NonNullableColumnAdd"


def test_rejects_all_non_nullable_column_adds_in_a_single_pass():
    # Given a plan adding three NOT NULL columns at once
    rule = NonNullableColumnAdd()

    # When evaluating
    failures = rule.evaluate(
        _plan(
            AddColumn(Column("a", Integer(), nullable=False)),
            AddColumn(Column("b", String(), nullable=False)),
            AddColumn(Column("c", Integer(), nullable=False)),
        )
    )

    # Then all three violations are reported in one pass, not just the first
    assert len(failures) == 3
    assert {f.rule_name for f in failures} == {"NonNullableColumnAdd"}
    messages = [f.message for f in failures]
    for column_name in ("a", "b", "c"):
        assert any(column_name in message for message in messages)


def test_allows_add_of_nullable_column():
    # Given a plan adding a nullable column (always safe)
    rule = NonNullableColumnAdd()

    failures = rule.evaluate(_plan(AddColumn(Column("notes", String(), nullable=True))))
    assert failures == ()


def test_non_nullable_column_add_ignores_creation_plan():
    # Given a creation plan — AddColumn does not appear; CreateTable carries the columns
    # A creation plan will never contain AddColumn, so the rule always returns ()
    failures = NonNullableColumnAdd().evaluate(_plan())
    assert failures == ()


# ---- NullabilityTighteningOnExistingColumn


def test_rejects_tightening_an_existing_column_to_not_null():
    # Given a plan that tightens a column to NOT NULL
    rule = NullabilityTighteningOnExistingColumn()

    failures = rule.evaluate(_plan(SetColumnNullability(column_name="id", nullable=False)))
    assert len(failures) == 1
    assert "id" in failures[0].message


def test_rejects_all_nullability_tightenings_in_a_single_pass():
    # Given a plan tightening two columns to NOT NULL at once
    rule = NullabilityTighteningOnExistingColumn()

    # When evaluating
    failures = rule.evaluate(
        _plan(
            SetColumnNullability(column_name="id", nullable=False),
            SetColumnNullability(column_name="name", nullable=False),
        )
    )

    # Then both violations are reported in one pass
    assert len(failures) == 2
    messages = [f.message for f in failures]
    for column_name in ("id", "name"):
        assert any(column_name in message for message in messages)


def test_allows_loosening_an_existing_column_to_nullable():
    # Given a plan that loosens a column to nullable (always safe)
    rule = NullabilityTighteningOnExistingColumn()

    failures = rule.evaluate(_plan(SetColumnNullability(column_name="id", nullable=True)))
    assert failures == ()


# ---- UnsupportedChangeRejected


def test_rejects_column_type_change():
    # Given a plan with an UnsupportedChange of kind COLUMN_TYPE
    rule = UnsupportedChangeRejected()
    failures = rule.evaluate(
        _plan(
            UnsupportedChange(
                kind=UnsupportedChangeKind.COLUMN_TYPE,
                subject_name="id",
                from_repr="Integer",
                to_repr="Long",
            )
        )
    )
    # Then it is rejected with the column name in the message
    assert len(failures) == 1
    assert failures[0].rule_name == "UnsupportedChangeRejected"
    assert "id" in failures[0].message


def test_rejects_partitioning_change():
    # Given a plan with an UnsupportedChange of kind PARTITIONING
    rule = UnsupportedChangeRejected()
    failures = rule.evaluate(
        _plan(
            UnsupportedChange(
                kind=UnsupportedChangeKind.PARTITIONING,
                subject_name="",
                from_repr="()",
                to_repr="('ds',)",
            )
        )
    )
    # Then it is rejected
    assert len(failures) == 1
    assert failures[0].rule_name == "UnsupportedChangeRejected"


def test_allows_plan_with_no_unsupported_change():
    # Given a plan with no UnsupportedChange
    rule = UnsupportedChangeRejected()
    failures = rule.evaluate(_plan(AddColumn(Column("new_col", String()))))
    assert failures == ()


# ---- validate_plan


def test_validation_passes_when_no_rule_is_broken():
    # Given a plan that violates no rule
    rules = (NonNullableColumnAdd(), UnsupportedChangeRejected())

    result = validate_plan(_plan(AddColumn(Column("x", String(), nullable=True))), rules=rules)

    assert not result.failed
    assert result.failures == ()


def test_validation_collects_a_failure_from_every_broken_rule():
    # Given a plan that breaks two rules at once
    rules = (NonNullableColumnAdd(), NullabilityTighteningOnExistingColumn())

    result = validate_plan(
        _plan(
            AddColumn(Column("order_id", Integer(), nullable=False)),
            SetColumnNullability(column_name="id", nullable=False),
        ),
        rules=rules,
    )

    assert result.failed
    assert {f.rule_name for f in result.failures} == {
        "NonNullableColumnAdd",
        "NullabilityTighteningOnExistingColumn",
    }


def test_empty_plan_produces_no_failures():
    # Given an empty plan
    rules = (NonNullableColumnAdd(), UnsupportedChangeRejected())

    result = validate_plan(_plan(), rules=rules)

    assert not result.failed
    assert result.failures == ()


def test_validation_uses_the_default_rules_when_none_are_supplied():
    # Given a plan that the default NonNullableColumnAdd rule rejects
    result = validate_plan(_plan(AddColumn(Column("order_id", Integer(), nullable=False))))

    assert result.failed
    assert {f.rule_name for f in result.failures} == {"NonNullableColumnAdd"}


def test_validation_passes_when_empty_rule_set_is_supplied():
    # Given an empty rule set and a plan that the defaults WOULD reject
    result = validate_plan(
        _plan(AddColumn(Column("order_id", Integer(), nullable=False))),
        rules=(),
    )

    assert not result.failed
    assert result.failures == ()


_QN = QualifiedName("c", "s", "t")


def _set_pk(*columns: Column) -> SetPrimaryKey:
    return SetPrimaryKey(columns=columns)


def _create_table_with_pk(*columns: Column) -> CreateTable:
    pk_tuple = tuple(c.name for c in columns if not c.nullable)
    return CreateTable(
        table=DesiredTable(
            qualified_name=_QN,
            columns=columns,
            primary_key=PrimaryKeyConstraint(columns=pk_tuple) if pk_tuple else None,
        )
    )


# ---- PrimaryKeyColumnsNullable


def test_rejects_set_primary_key_with_nullable_column():
    # Given a SetPrimaryKey carrying a nullable column
    rule = PrimaryKeyColumnsNullable()

    failures = rule.evaluate(_plan(_set_pk(Column("id", Integer(), nullable=True))))

    assert len(failures) == 1
    assert "id" in failures[0].message
    assert failures[0].rule_name == "PrimaryKeyColumnsNullable"


def test_rejects_all_nullable_pk_columns_in_a_single_pass():
    # Given a SetPrimaryKey carrying two nullable columns
    rule = PrimaryKeyColumnsNullable()

    failures = rule.evaluate(
        _plan(
            _set_pk(
                Column("id", Integer(), nullable=True),
                Column("tenant_id", Integer(), nullable=True),
            )
        )
    )

    assert len(failures) == 2
    messages = [f.message for f in failures]
    for name in ("id", "tenant_id"):
        assert any(name in m for m in messages)


def test_allows_set_primary_key_with_all_non_nullable_columns():
    # Given a SetPrimaryKey where every column is NOT NULL
    rule = PrimaryKeyColumnsNullable()

    failures = rule.evaluate(_plan(_set_pk(Column("id", Integer(), nullable=False))))

    assert failures == ()


def test_rejects_create_table_with_nullable_pk_column():
    # Given a CreateTable whose PK column is nullable
    rule = PrimaryKeyColumnsNullable()

    action = CreateTable(
        table=DesiredTable(
            qualified_name=_QN,
            columns=(Column("id", Integer(), nullable=True),),
            primary_key=PrimaryKeyConstraint(columns=("id",)),
        )
    )

    failures = rule.evaluate(_plan(action))

    assert len(failures) == 1
    assert "id" in failures[0].message


def test_allows_create_table_with_no_primary_key():
    # Given a CreateTable with no primary key defined
    rule = PrimaryKeyColumnsNullable()

    action = CreateTable(
        table=DesiredTable(
            qualified_name=_QN,
            columns=(Column("id", Integer()),),
        )
    )

    failures = rule.evaluate(_plan(action))

    assert failures == ()


def test_allows_create_table_with_non_nullable_pk_column():
    # Given a CreateTable with a NOT NULL PK column
    rule = PrimaryKeyColumnsNullable()

    action = CreateTable(
        table=DesiredTable(
            qualified_name=_QN,
            columns=(Column("id", Integer(), nullable=False),),
            primary_key=PrimaryKeyConstraint(columns=("id",)),
        )
    )

    failures = rule.evaluate(_plan(action))

    assert failures == ()


def test_pk_nullable_rule_ignores_non_pk_actions():
    # Given a plan with only column-level actions
    rule = PrimaryKeyColumnsNullable()

    failures = rule.evaluate(_plan(AddColumn(Column("x", Integer())), DropPrimaryKey()))

    assert failures == ()
