import pytest

from delta_engine.domain.model import (
    Column,
    DesiredTable,
    Integer,
    Long,
    ObservedTable,
    QualifiedName,
    String,
)
from delta_engine.domain.model.foreign_key import ForeignKeyConstraint
from delta_engine.domain.model.primary_key import PrimaryKeyConstraint
from delta_engine.domain.plan.diff import (
    Added,
    Changed,
    ColumnChanged,
    KeyValue,
    Removed,
    TableDrift,
    TableMissing,
    UnhandledFact,
    diff_table,
)

_QUALIFIED_NAME = QualifiedName("dev", "silver", "test")


def test_column_changed_requires_at_least_one_difference():
    # Given a ColumnChanged carrying no sub-fact at all
    # Then construction is rejected — a vacuous entry is a malformed diff
    with pytest.raises(ValueError, match="no differences"):
        ColumnChanged(column_name="id")


def test_column_changed_accepts_a_single_difference():
    # Given exactly one differing attribute
    entry = ColumnChanged(column_name="id", comment=Changed(desired="pk", observed=""))

    # Then the entry holds that fact and nothing else
    assert entry.comment == Changed(desired="pk", observed="")
    assert entry.data_type is None
    assert entry.nullability is None
    assert entry.tags == ()


def test_column_changed_accepts_tags_only():
    # Given only tag entries differ
    entry = ColumnChanged(
        column_name="id",
        tags=(Changed(KeyValue("pii", "true"), KeyValue("pii", "false")),),
    )

    # Then the entry is valid
    assert len(entry.tags) == 1


def test_table_drift_defaults_to_no_differences():
    # Given a drift built with no arguments
    drift = TableDrift()

    # Then every dimension reports no difference
    assert drift.columns == ()
    assert drift.table_comment is None
    assert drift.properties == ()
    assert drift.table_tags == ()
    assert drift.partitioning is None
    assert drift.primary_key is None
    assert drift.foreign_keys == ()


def test_table_missing_carries_the_desired_table():
    # Given a desired table for a table absent from the catalog
    desired = DesiredTable(
        qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),)
    )

    # Then the missing-table variant is self-contained
    assert TableMissing(desired=desired).desired is desired


def _desired(**overrides) -> DesiredTable:
    defaults = dict(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))
    return DesiredTable(**{**defaults, **overrides})


def _observed(**overrides) -> ObservedTable:
    defaults = dict(qualified_name=_QUALIFIED_NAME, columns=(Column("id", Integer()),))
    return ObservedTable(**{**defaults, **overrides})


def test_missing_table_diffs_to_table_missing_carrying_desired():
    # Given no observed table
    desired = _desired()

    # When diffing against None
    diff = diff_table(desired, observed=None)

    # Then the diff is the self-contained missing-table variant
    assert diff == TableMissing(desired=desired)


def test_equal_tables_diff_to_an_all_empty_drift():
    # Given identical desired and observed definitions
    diff = diff_table(_desired(), _observed())

    # Then no dimension records a difference
    assert diff == TableDrift()


def test_desired_only_column_is_an_added_fact():
    diff = diff_table(
        _desired(columns=(Column("id", Integer()), Column("age", Integer()))),
        _observed(),
    )

    assert diff == TableDrift(columns=(Added(Column("age", Integer())),))


def test_observed_only_column_is_a_removed_fact():
    diff = diff_table(
        _desired(),
        _observed(columns=(Column("id", Integer()), Column("stale", String()))),
    )

    assert diff == TableDrift(columns=(Removed(Column("stale", String())),))


def test_type_drift_is_a_column_changed_fact_not_a_judgement():
    # Given a common column whose type differs
    diff = diff_table(
        _desired(columns=(Column("id", Integer()),)),
        _observed(columns=(Column("id", Long()),)),
    )

    # Then the diff states the fact; whether it is allowed is not its concern
    assert diff == TableDrift(
        columns=(
            ColumnChanged(
                column_name="id",
                data_type=Changed(desired=Integer(), observed=Long()),
            ),
        )
    )


def test_column_changed_carries_exactly_the_differing_attributes():
    # Given a common column differing in comment and nullability but not type
    diff = diff_table(
        _desired(columns=(Column("id", Integer(), nullable=False, comment="pk"),)),
        _observed(columns=(Column("id", Integer(), nullable=True, comment=""),)),
    )

    assert diff == TableDrift(
        columns=(
            ColumnChanged(
                column_name="id",
                nullability=Changed(desired=False, observed=True),
                comment=Changed(desired="pk", observed=""),
            ),
        )
    )


def test_column_tag_differences_ride_on_the_column_changed_entry():
    # Given a common column whose tags drift in all three ways
    diff = diff_table(
        _desired(columns=(Column("id", Integer(), tags={"pii": "true", "new": "x"}),)),
        _observed(columns=(Column("id", Integer(), tags={"pii": "false", "old": "y"}),)),
    )

    assert diff == TableDrift(
        columns=(
            ColumnChanged(
                column_name="id",
                tags=(
                    Added(KeyValue("new", "x")),
                    Changed(desired=KeyValue("pii", "true"), observed=KeyValue("pii", "false")),
                    Removed(KeyValue("old", "y")),
                ),
            ),
        )
    )


def test_property_differences_are_uniform_entries_including_removed():
    # Given properties drifting in all three ways
    diff = diff_table(
        _desired(properties={"a": "1", "b": "2"}),
        _observed(properties={"b": "9", "c": "3"}),
    )

    # Then the diff reports facts uniformly — observed-only keys included;
    # declared-subset semantics is the lowerer's policy, not a diffing idiom
    assert diff == TableDrift(
        properties=(
            Added(KeyValue("a", "1")),
            Changed(desired=KeyValue("b", "2"), observed=KeyValue("b", "9")),
            Removed(KeyValue("c", "3")),
        )
    )


def test_table_tag_differences_are_uniform_entries():
    diff = diff_table(
        _desired(tags={"env": "prod"}),
        _observed(tags={"stale": "yes"}),
    )

    assert diff == TableDrift(
        table_tags=(Added(KeyValue("env", "prod")), Removed(KeyValue("stale", "yes")))
    )


def test_table_comment_drift_is_a_changed_fact():
    diff = diff_table(_desired(comment="new"), _observed(comment="old"))

    assert diff == TableDrift(table_comment=Changed(desired="new", observed="old"))


def test_partitioning_drift_is_a_changed_fact_not_a_judgement():
    diff = diff_table(
        _desired(partitioned_by=("id",)),
        _observed(),
    )

    assert diff == TableDrift(partitioning=Changed(desired=("id",), observed=()))


def test_desired_only_primary_key_is_an_added_fact():
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="test_pk")
    diff = diff_table(
        _desired(columns=(Column("id", Integer(), nullable=False),), primary_key=pk),
        _observed(columns=(Column("id", Integer(), nullable=False),)),
    )

    assert diff == TableDrift(primary_key=Added(pk))


def test_observed_only_primary_key_is_a_removed_fact():
    pk = PrimaryKeyConstraint(columns=("id",), constraint_name="legacy_pk")
    diff = diff_table(
        _desired(columns=(Column("id", Integer(), nullable=False),)),
        _observed(columns=(Column("id", Integer(), nullable=False),), primary_key=pk),
    )

    assert diff == TableDrift(primary_key=Removed(pk))


def test_primary_keys_with_equal_column_sets_produce_no_fact():
    # Given PKs equal as column sets (order and constraint name differ)
    desired_pk = PrimaryKeyConstraint(columns=("a", "b"), constraint_name="test_pk")
    observed_pk = PrimaryKeyConstraint(columns=("b", "a"), constraint_name="other_name")
    columns = (Column("a", Integer(), nullable=False), Column("b", Integer(), nullable=False))

    diff = diff_table(
        _desired(columns=columns, primary_key=desired_pk),
        _observed(columns=columns, primary_key=observed_pk),
    )

    # Then key identity is the column set — no difference is recorded
    assert diff == TableDrift()


def test_primary_keys_with_different_column_sets_are_a_changed_fact():
    desired_pk = PrimaryKeyConstraint(columns=("a",), constraint_name="test_pk")
    observed_pk = PrimaryKeyConstraint(columns=("b",), constraint_name="test_pk")
    columns = (Column("a", Integer(), nullable=False), Column("b", Integer(), nullable=False))

    diff = diff_table(
        _desired(columns=columns, primary_key=desired_pk),
        _observed(columns=columns, primary_key=observed_pk),
    )

    assert diff == TableDrift(primary_key=Changed(desired=desired_pk, observed=observed_pk))


def _foreign_key(constraint_name: str = "test_id_fk") -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        local_columns=("id",),
        referenced_table=QualifiedName("dev", "silver", "other"),
        referenced_columns=("id",),
        constraint_name=constraint_name,
    )


def test_desired_only_foreign_key_is_an_added_fact():
    diff = diff_table(_desired(foreign_keys=(_foreign_key(),)), _observed())

    assert diff == TableDrift(foreign_keys=(Added(_foreign_key()),))


def test_observed_only_foreign_key_is_a_removed_fact():
    diff = diff_table(_desired(), _observed(foreign_keys=(_foreign_key("catalog_fk"),)))

    assert diff == TableDrift(foreign_keys=(Removed(_foreign_key("catalog_fk")),))


def test_foreign_keys_match_by_signature_regardless_of_name():
    # Given the same FK content under different constraint names
    diff = diff_table(
        _desired(foreign_keys=(_foreign_key("engine_name"),)),
        _observed(foreign_keys=(_foreign_key("external_name"),)),
    )

    # Then the signature is the identity — no difference is recorded
    assert diff == TableDrift()


def test_existing_table_with_no_observed_foreign_keys_adds_every_desired_one():
    # Given an existing table observed with no FKs and a declaration with one
    diff = diff_table(
        _desired(foreign_keys=(_foreign_key(),)),
        _observed(foreign_keys=()),
    )

    # Then the FK diff is pure addition — no removals arise from absence
    assert diff == TableDrift(foreign_keys=(Added(_foreign_key()),))


def test_changed_rejects_equal_values():
    # Given two equal values
    # Then Changed construction raises
    with pytest.raises(ValueError, match="no difference"):
        Changed(desired=42, observed=42)


def test_changed_accepts_unequal_values():
    # Given two different values
    result = Changed(desired=1, observed=2)

    # Then it holds both
    assert result.desired == 1
    assert result.observed == 2


def test_unhandled_fact_carries_description():
    fact = UnhandledFact(description="partitioning change: () → ('ds',)")

    assert fact.description == "partitioning change: () → ('ds',)"
