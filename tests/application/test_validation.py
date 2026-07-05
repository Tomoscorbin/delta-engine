from delta_engine.application.failures import ValidationFailure
from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY
from delta_engine.application.validation import (
    DEFAULT_RULES,
    NonNullableColumnAdd,
    NullabilityTighteningOnExistingColumn,
    PropertyMustBeDeclared,
    PropertyTransitionNotSupported,
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
    Change,
    ColumnAdded,
    ColumnDataTypeChanged,
    ColumnNullabilityChanged,
    ColumnRemoved,
    PartitioningChanged,
    PropertySet,
    PropertyUnset,
    TableCommentChanged,
    TableDrift,
    TableMissing,
    UndeclaredProperty,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def _desired_table(managed_aspects: frozenset[TableAspect] = ALL_ASPECTS) -> DesiredTable:
    return DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),),
        managed_aspects=managed_aspects,
    )


def _drift(
    *changes: Change,
    managed_aspects: frozenset[TableAspect] = ALL_ASPECTS,
    desired: DesiredTable | None = None,
) -> TableDrift:
    if desired is None:
        desired = _desired_table(managed_aspects)
    return TableDrift(desired=desired, changes=tuple(changes))


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
    # Given a change tuple containing a NOT NULL column addition
    rule = NonNullableColumnAdd()
    changes = (ColumnAdded(Column("order_id", Integer(), nullable=False)),)

    # When
    failures = rule.evaluate(_drift(*changes))

    # Then
    assert len(failures) == 1
    assert failures[0].rule_name == "NonNullableColumnAdd"


def test_rejects_all_non_nullable_column_adds_in_a_single_pass():
    # Given three NOT NULL column additions in a single change tuple
    rule = NonNullableColumnAdd()
    changes = (
        ColumnAdded(Column("a", Integer(), nullable=False)),
        ColumnAdded(Column("b", String(), nullable=False)),
        ColumnAdded(Column("c", Integer(), nullable=False)),
    )

    # When
    failures = rule.evaluate(_drift(*changes))

    # Then
    assert len(failures) == 3
    assert {f.rule_name for f in failures} == {"NonNullableColumnAdd"}
    messages = [f.message for f in failures]
    for col in ("a", "b", "c"):
        assert any(col in m for m in messages)


def test_allows_add_of_nullable_column():
    # Given a change tuple containing a nullable column addition
    rule = NonNullableColumnAdd()
    changes = (ColumnAdded(Column("age", Integer())),)

    assert rule.evaluate(_drift(*changes)) == ()


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


def test_non_nullable_column_add_passes_when_no_changes():
    # Given an empty change tuple
    rule = NonNullableColumnAdd()

    assert rule.evaluate(_drift()) == ()


# ---- NullabilityTighteningOnExistingColumn


def test_rejects_tightening_an_existing_column_to_not_null():
    # Given a nullability tightening change on an existing column
    rule = NullabilityTighteningOnExistingColumn()
    changes = (_tightening("order_id"),)

    # When
    failures = rule.evaluate(_drift(*changes))

    # Then
    assert len(failures) == 1
    assert failures[0].rule_name == "NullabilityTighteningOnExistingColumn"
    assert "order_id" in failures[0].message


def test_rejects_all_nullability_tightenings_in_a_single_pass():
    # Given two nullability tightenings in a single change tuple
    rule = NullabilityTighteningOnExistingColumn()
    changes = (_tightening("a"), _tightening("b"))

    # When
    failures = rule.evaluate(_drift(*changes))

    # Then
    assert len(failures) == 2
    messages = [f.message for f in failures]
    for col in ("a", "b"):
        assert any(col in m for m in messages)


def test_allows_loosening_an_existing_column_to_nullable():
    # Given a nullability loosening change (NOT NULL → nullable)
    rule = NullabilityTighteningOnExistingColumn()
    loosening = ColumnNullabilityChanged(
        column_name="id", desired_nullable=True, observed_nullable=False
    )

    assert rule.evaluate(_drift(loosening)) == ()


# ---- unsupported drift → ValidationFailure


def test_validate_diff_surfaces_type_drift_as_failure():
    # Given a drift whose only change is a column type change
    diff = _drift(_type_drift("id"))

    # When
    result = validate_diff(diff)

    # Then
    assert result.failed is True
    assert len(result.failures) == 1
    assert "id" in result.failures[0].message


def test_validate_diff_surfaces_partitioning_change_as_failure():
    # Given a drift whose only change is a partitioning change
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
        "PropertyTransitionNotSupported",
        "PropertyMustBeDeclared",
        "ColumnMappingRequiredForDrop",
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
    # Given two changes in one unmanaged aspect and one change in another
    diff = _drift(
        ColumnAdded(Column("extra", Integer())),
        ColumnAdded(Column("more", Integer())),
        TableCommentChanged(desired_comment="new", observed_comment="old"),
        managed_aspects=frozenset({TableAspect.TABLE_TAGS}),
    )

    result = validate_diff(diff)

    # Then one failure per aspect, not per change — in first-seen change order
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


# ---- PropertyTransitionNotSupported


def test_blocks_column_mapping_downgrade():
    # Given a declared downgrade from name to none — Databricks rejects it
    rule = PropertyTransitionNotSupported(DELTA_PROPERTY_REGISTRY)
    changes = (
        PropertySet(name="delta.columnMapping.mode", desired_value="none", observed_value="name"),
    )

    failures = rule.evaluate(_drift(*changes))

    assert len(failures) == 1
    assert failures[0].rule_name == "PropertyTransitionNotSupported"
    assert "delta.columnMapping.mode" in failures[0].message


def test_allows_column_mapping_upgrade():
    # Given the one permitted transition: none -> name
    rule = PropertyTransitionNotSupported(DELTA_PROPERTY_REGISTRY)
    changes = (
        PropertySet(name="delta.columnMapping.mode", desired_value="name", observed_value="none"),
    )

    assert rule.evaluate(_drift(*changes)) == ()


def test_allows_first_write_of_restricted_key():
    # Given the key is absent from the catalog — first writes are always legal
    rule = PropertyTransitionNotSupported(DELTA_PROPERTY_REGISTRY)
    changes = (
        PropertySet(name="delta.columnMapping.mode", desired_value="name", observed_value=None),
    )

    assert rule.evaluate(_drift(*changes)) == ()


def test_ignores_value_changes_on_unrestricted_keys():
    # Given a value change on a key whose registry entry restricts nothing
    rule = PropertyTransitionNotSupported(DELTA_PROPERTY_REGISTRY)
    changes = (
        PropertySet(
            name="delta.enableChangeDataFeed", desired_value="false", observed_value="true"
        ),
    )

    assert rule.evaluate(_drift(*changes)) == ()


# ---- PropertyMustBeDeclared


def test_fails_undeclared_registered_key_offering_none():
    # Given an undeclared unrestricted key — removal via None is offered
    rule = PropertyMustBeDeclared(DELTA_PROPERTY_REGISTRY)
    changes = (UndeclaredProperty(name="delta.enableChangeDataFeed", observed_value="true"),)

    failures = rule.evaluate(_drift(*changes))

    assert len(failures) == 1
    assert failures[0].rule_name == "PropertyMustBeDeclared"
    assert "None" in failures[0].message


def test_fails_undeclared_unset_forbidden_key_without_offering_none():
    # Given columnMapping.mode undeclared — it cannot be removed, so the
    # message must not suggest declaring None
    rule = PropertyMustBeDeclared(DELTA_PROPERTY_REGISTRY)
    changes = (UndeclaredProperty(name="delta.columnMapping.mode", observed_value="name"),)

    failures = rule.evaluate(_drift(*changes))

    assert len(failures) == 1
    assert "cannot be unset" in failures[0].message
    assert "None" not in failures[0].message


def test_passes_when_no_undeclared_key():
    # Given no changes at all
    rule = PropertyMustBeDeclared(DELTA_PROPERTY_REGISTRY)

    assert rule.evaluate(_drift()) == ()


def test_blocks_none_declaration_on_removal_forbidden_key():
    # Given a declaration asserting columnMapping.mode absent on a table that
    # has it — a removal is a transition to absence, judged by the same rule
    rule = PropertyTransitionNotSupported(DELTA_PROPERTY_REGISTRY)
    changes = (PropertyUnset(name="delta.columnMapping.mode", observed_value="name"),)

    failures = rule.evaluate(_drift(*changes))

    assert len(failures) == 1
    assert failures[0].rule_name == "PropertyTransitionNotSupported"
    assert "cannot be removed" in failures[0].message


def test_allows_none_declaration_on_unrestricted_key():
    # Given an absence assertion on a key whose registry entry restricts nothing
    rule = PropertyTransitionNotSupported(DELTA_PROPERTY_REGISTRY)
    changes = (
        PropertyUnset(name="delta.logRetentionDuration", observed_value="interval 30 days"),
    )

    assert rule.evaluate(_drift(*changes)) == ()


# ---- column-drop precondition (through validate_diff)


def test_drop_without_column_mapping_fails_before_execution():
    # Given a plan that drops a column but the declaration lacks column mapping
    diff = _drift(ColumnRemoved(Column("stale", Integer())))

    failures = validate_diff(diff).failures

    assert any(
        f.rule_name == "ColumnMappingRequiredForDrop"
        and "delta.columnMapping.mode" in f.message
        for f in failures
    )


def test_drop_with_declared_column_mapping_passes():
    # Given the declaration states mode=name (set phases before drop)
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME,
        columns=(Column("id", Integer()),),
        properties={"delta.columnMapping.mode": "name"},
    )
    diff = _drift(ColumnRemoved(Column("stale", Integer())), desired=desired)

    assert validate_diff(diff).failed is False


def test_no_drop_means_no_precondition_failure():
    assert validate_diff(_drift()).failed is False
