from delta_engine.application.failures import ValidationFailure
from delta_engine.application.validation import (
    DEFAULT_RULES,
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    ValidationResult,
    validate_diff,
)
from delta_engine.domain.model import (
    ALL_ASPECTS,
    Column,
    DesiredTable,
    Integer,
    Long,
    QualifiedName,
    String,
    TableAspect,
)
from delta_engine.domain.plan.diff import (
    ColumnAdded,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    DriftFact,
    PartitioningChanged,
    TableCommentChanged,
    TableDrift,
    TableMissing,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def _desired_table(managed_aspects: frozenset[TableAspect] = ALL_ASPECTS) -> DesiredTable:
    return DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),),
        managed_aspects=managed_aspects,
    )


def _drift(
    *facts: DriftFact, managed_aspects: frozenset[TableAspect] = ALL_ASPECTS
) -> TableDrift:
    return TableDrift(facts=tuple(facts), managed_aspects=managed_aspects)


def _tightening(column_name: str = "id") -> ColumnNullabilityChanged:
    return ColumnNullabilityChanged(
        column_name=column_name, desired_nullable=False, observed_nullable=True
    )


def _type_drift(column_name: str = "id") -> ColumnDataTypeChanged:
    return ColumnDataTypeChanged(
        column_name=column_name, desired_type=Long(), observed_type=Integer()
    )


# ---- NonNullableColumnAdd


def test_rejects_add_of_non_nullable_column():
    # Given a fact tuple containing a NOT NULL column addition
    rule = NonNullableColumnAdd()
    facts = (ColumnAdded(Column("order_id", Integer(), nullable=False)),)

    # When
    failures = rule.evaluate(facts)

    # Then
    assert len(failures) == 1
    assert failures[0].rule_name == "NonNullableColumnAdd"


def test_rejects_all_non_nullable_column_adds_in_a_single_pass():
    # Given three NOT NULL column additions in a single fact tuple
    rule = NonNullableColumnAdd()
    facts = (
        ColumnAdded(Column("a", Integer(), nullable=False)),
        ColumnAdded(Column("b", String(), nullable=False)),
        ColumnAdded(Column("c", Integer(), nullable=False)),
    )

    # When
    failures = rule.evaluate(facts)

    # Then
    assert len(failures) == 3
    assert {f.rule_name for f in failures} == {"NonNullableColumnAdd"}
    messages = [f.message for f in failures]
    for col in ("a", "b", "c"):
        assert any(col in m for m in messages)


def test_allows_add_of_nullable_column():
    # Given a fact tuple containing a nullable column addition
    rule = NonNullableColumnAdd()
    facts = (ColumnAdded(Column("age", Integer())),)

    assert rule.evaluate(facts) == ()


def test_non_nullable_column_add_ignores_creation():
    # Given a TableMissing diff (table does not yet exist) with a NOT NULL column
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer(), nullable=False),),
    )

    # When
    result = validate_diff(TableMissing(desired=desired))

    # Then
    assert result.failed is False


def test_non_nullable_column_add_passes_when_no_facts():
    # Given an empty fact tuple
    rule = NonNullableColumnAdd()

    assert rule.evaluate(()) == ()


# ---- NullabilityTighteningOnExistingColumn


def test_rejects_tightening_an_existing_column_to_not_null():
    # Given a nullability tightening fact on an existing column
    rule = NullabilityTighteningOnExistingColumn()
    facts = (_tightening("order_id"),)

    # When
    failures = rule.evaluate(facts)

    # Then
    assert len(failures) == 1
    assert failures[0].rule_name == "NullabilityTighteningOnExistingColumn"
    assert "order_id" in failures[0].message


def test_rejects_all_nullability_tightenings_in_a_single_pass():
    # Given two nullability tightenings in a single fact tuple
    rule = NullabilityTighteningOnExistingColumn()
    facts = (_tightening("a"), _tightening("b"))

    # When
    failures = rule.evaluate(facts)

    # Then
    assert len(failures) == 2
    messages = [f.message for f in failures]
    for col in ("a", "b"):
        assert any(col in m for m in messages)


def test_allows_loosening_an_existing_column_to_nullable():
    # Given a nullability loosening fact (NOT NULL → nullable)
    rule = NullabilityTighteningOnExistingColumn()
    loosening = ColumnNullabilityChanged(
        column_name="id", desired_nullable=True, observed_nullable=False
    )

    assert rule.evaluate((loosening,)) == ()


# ---- unsupported drift → ValidationFailure


def test_validate_diff_surfaces_type_drift_as_failure():
    # Given a drift whose only fact is a column type change
    diff = _drift(_type_drift("id"))

    # When
    result = validate_diff(diff)

    # Then
    assert result.failed is True
    assert len(result.failures) == 1
    assert "id" in result.failures[0].message


def test_validate_diff_surfaces_partitioning_change_as_failure():
    # Given a drift whose only fact is a partitioning change
    diff = _drift(PartitioningChanged(desired_partitioning=("ds",), observed_partitioning=()))

    # When
    result = validate_diff(diff)

    # Then
    assert result.failed is True
    assert any("partitioning" in f.message.lower() for f in result.failures)


def test_validate_diff_collects_both_unsupported_drift_and_rule_failures():
    # Given a drift with a type change (unsupported) AND a NOT NULL add (rule violation)
    diff = _drift(
        _type_drift("id"),
        ColumnAdded(Column("new_col", Integer(), nullable=False)),
    )

    # When
    result = validate_diff(diff)

    # Then
    assert result.failed is True
    assert len(result.failures) == 2


# ---- validate_diff


def test_validation_passes_when_no_rule_is_broken():
    # Given a drift containing only a nullable column addition (breaks no rules)
    diff = _drift(ColumnAdded(Column("age", Integer())))

    # When
    result = validate_diff(diff)

    # Then
    assert result.failed is False
    assert result.failures == ()


def test_empty_drift_produces_no_failures():
    result = validate_diff(_drift())

    assert result.failed is False


def test_missing_table_passes_validation():
    # Given a TableMissing diff (table does not yet exist)
    desired = _desired_table()

    # When
    result = validate_diff(TableMissing(desired=desired))

    # Then
    assert result.failed is False


def test_validation_uses_the_default_rules_when_none_are_supplied():
    # Given a drift with a NOT NULL column addition and no explicit rules argument
    diff = _drift(ColumnAdded(Column("x", Integer(), nullable=False)))

    # When
    result = validate_diff(diff)

    # Then
    assert result.failed is True


def test_validation_passes_when_empty_rule_set_is_supplied():
    # Given a drift that would break a rule, but no rules are supplied
    diff = _drift(ColumnAdded(Column("x", Integer(), nullable=False)))

    # When
    result = validate_diff(diff, rules=())

    # Then
    assert result.failed is False


def test_validation_result_failed_property():
    # Given a result with one failure and a result with no failures
    failing = ValidationResult(failures=(ValidationFailure(rule_name="X", message="broken"),))
    passing = ValidationResult()

    # Then
    assert failing.failed is True
    assert passing.failed is False


def test_default_rules_cover_all_safety_policies():
    # Given the DEFAULT_RULES constant — scope invariants are not rules
    rule_names = {type(rule).__name__ for rule in DEFAULT_RULES}

    assert rule_names == {
        "NonNullableColumnAdd",
        "NullabilityTighteningOnExistingColumn",
        "ColumnDataTypeChangeNotSupported",
        "PartitioningChangeNotSupported",
    }


# ---- unmanaged aspect drift (scope invariant, not a rule)


def test_unmanaged_aspect_drift_fails_when_unmanaged_aspect_has_drifted():
    # Given a declaration that only manages table tags, but column structure has drifted
    diff = _drift(
        ColumnAdded(Column("extra", Integer())),
        managed_aspects=frozenset({TableAspect.TABLE_TAGS}),
    )

    result = validate_diff(diff)

    # Then one failure names the unmanaged aspect
    assert len(result.failures) == 1
    assert result.failures[0].rule_name == "UnmanagedAspectDrift"
    assert "column structure" in result.failures[0].message.lower()


def test_unmanaged_aspect_drift_produces_one_failure_per_drifted_unmanaged_aspect():
    # Given two facts in one unmanaged aspect and one fact in another
    diff = _drift(
        ColumnAdded(Column("extra", Integer())),
        ColumnAdded(Column("more", Integer())),
        TableCommentChanged(desired_comment="new", observed_comment="old"),
        managed_aspects=frozenset({TableAspect.TABLE_TAGS}),
    )

    result = validate_diff(diff)

    # Then one failure per aspect, not per fact — in first-seen fact order
    assert len(result.failures) == 2
    assert "column structure" in result.failures[0].message.lower()
    assert "table comment" in result.failures[1].message.lower()


def test_unmanaged_aspect_drift_cannot_be_suppressed_by_empty_rules():
    # Given unmanaged drift and an empty rule set — the scope invariant still fires
    diff = _drift(
        ColumnAdded(Column("extra", Integer())),
        managed_aspects=frozenset({TableAspect.TABLE_TAGS}),
    )

    result = validate_diff(diff, rules=())

    assert result.failed is True
    assert result.failures[0].rule_name == "UnmanagedAspectDrift"


def test_unmanaged_drift_does_not_also_trip_safety_rules():
    # Given a metadata-only declaration whose live table has a type mismatch:
    # the user asserted the structure matched — they never requested a type change
    diff = _drift(
        _type_drift("id"),
        managed_aspects=frozenset({TableAspect.TABLE_COMMENT}),
    )

    result = validate_diff(diff)

    # Then the single failure is the scope violation, not
    # ColumnDataTypeChangeNotSupported judging a change nobody asked for
    assert len(result.failures) == 1
    assert result.failures[0].rule_name == "UnmanagedAspectDrift"


def test_managed_drift_still_trips_safety_rules():
    # Given a fully managed drift with a type change
    diff = _drift(_type_drift("id"), managed_aspects=ALL_ASPECTS)

    result = validate_diff(diff)

    # Then the safety rule fires — the change was requested, and it is unsafe
    assert len(result.failures) == 1
    assert result.failures[0].rule_name == "ColumnDataTypeChangeNotSupported"


def test_drift_passes_when_no_unmanaged_aspect_has_drifted():
    # Given a metadata-only drift where only a managed aspect (table comment) drifted
    diff = _drift(
        TableCommentChanged(desired_comment="new", observed_comment="old"),
        managed_aspects=frozenset({TableAspect.TABLE_COMMENT}),
    )

    result = validate_diff(diff)

    assert result.failed is False


def test_drift_passes_when_all_aspects_managed():
    # Given a fully managed drift with a nullable column addition
    diff = _drift(ColumnAdded(Column("extra", Integer())), managed_aspects=ALL_ASPECTS)

    assert validate_diff(diff).failed is False


# ---- TableMissing with COLUMN_STRUCTURE unmanaged


def test_validate_diff_fails_table_missing_when_column_structure_unmanaged():
    # Given a metadata-only definition for a table that does not exist
    desired = _desired_table(
        managed_aspects=frozenset({TableAspect.TABLE_COMMENT, TableAspect.TABLE_TAGS})
    )

    result = validate_diff(TableMissing(desired=desired))

    assert result.failed is True
    assert any("does not exist" in f.message for f in result.failures)


def test_validate_diff_passes_table_missing_when_column_structure_managed():
    # Given a fully managed definition for a missing table
    desired = _desired_table(managed_aspects=ALL_ASPECTS)

    result = validate_diff(TableMissing(desired=desired))

    assert result.failed is False
