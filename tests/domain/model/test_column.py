from hypothesis import given, strategies as st
import pytest

from delta_engine.domain.model.column import DesiredColumn, ObservedColumn
from delta_engine.domain.model.data_type import Integer, String


def test_defaults_to_nullable_true() -> None:
    # Given: a column with no explicit nullability
    # When: constructing a DesiredColumn
    col = DesiredColumn("id", Integer())
    # Then: it defaults to nullable=True
    assert col.nullable is True


def test_mixed_case_name_is_preserved_verbatim() -> None:
    # Case is never identity on Databricks, but display spelling is real
    # catalog state: the engine stores it verbatim and compares by identity.
    col = DesiredColumn("UserId", Integer())
    assert col.name.spelling == "UserId"


def test_already_lowercase_unicode_name_is_preserved_verbatim() -> None:
    # 'straße' is already lowercase; casefold would rewrite it to 'strasse',
    # a different identifier from the one Unity Catalog stores
    assert DesiredColumn("straße", Integer()).name == "straße"


@given(st.text(min_size=1).filter(str.strip))
def test_construction_preserves_any_name_verbatim(name: str) -> None:
    assert DesiredColumn(name, Integer()).name.spelling == name


@pytest.mark.parametrize("blank", ["", "   ", "\t"], ids=["empty", "spaces", "tab"])
def test_raises_when_name_is_blank(blank: str) -> None:
    # Given: a blank or whitespace-only column name (would emit a malformed
    # `` `` identifier in DDL)
    # When/Then: constructing a DesiredColumn fails
    with pytest.raises(ValueError, match="blank"):
        DesiredColumn(blank, Integer())


def test_tags_default_to_empty() -> None:
    # Given: a column with no explicit tags
    # When: constructing a DesiredColumn
    col = DesiredColumn("id", Integer())
    # Then: it defaults to an empty tag mapping
    assert dict(col.tags) == {}


def test_tags_are_stored_verbatim() -> None:
    # Given a column declared with two tags
    col = DesiredColumn("email", Integer(), tags={"pii": "true", "classification": "restricted"})
    # Then the tags are stored exactly as given
    assert dict(col.tags) == {"pii": "true", "classification": "restricted"}


def test_tag_keys_are_case_sensitive() -> None:
    # Given tag keys differing only in case (UC tag keys are case-sensitive)
    col = DesiredColumn("email", Integer(), tags={"PII": "true", "pii": "false"})
    # Then both keys are preserved distinctly (not casefolded like the column name)
    assert dict(col.tags) == {"PII": "true", "pii": "false"}


@pytest.mark.parametrize("blank", ["", "   ", "\t"], ids=["empty", "spaces", "tab"])
def test_raises_when_tag_key_is_blank(blank: str) -> None:
    # Given a blank or whitespace-only tag key
    # When/Then: constructing a DesiredColumn fails
    with pytest.raises(ValueError, match="Tag key must not be blank"):
        DesiredColumn("id", Integer(), tags={blank: "v"})


def test_column_accepts_a_lowercase_renamed_from_hint() -> None:
    column = DesiredColumn("customer_name", String(), renamed_from="customer_nm")
    assert column.renamed_from == "customer_nm"


def test_column_defaults_to_no_rename_hint() -> None:
    assert DesiredColumn("customer_name", String()).renamed_from is None


def test_renamed_from_is_preserved_verbatim() -> None:
    column = DesiredColumn("customer_name", String(), renamed_from="Customer_NM")
    assert column.renamed_from.spelling == "Customer_NM"


def test_column_rejects_malformed_renamed_from() -> None:
    with pytest.raises(ValueError, match="blank"):
        DesiredColumn("customer_name", String(), renamed_from="  ")
    with pytest.raises(ValueError, match="itself"):
        DesiredColumn("customer_name", String(), renamed_from="customer_name")


def test_case_only_rename_collapses_to_renamed_from_itself() -> None:
    # Case is not identity, so a case-only rename still names the same column.
    with pytest.raises(ValueError, match="itself"):
        DesiredColumn("customer_name", String(), renamed_from="Customer_Name")


def test_observed_column_enforces_the_same_field_invariants_as_column() -> None:
    # Given/Then: blank names are rejected and catalog spelling is preserved.
    with pytest.raises(ValueError, match="blank"):
        ObservedColumn("  ", Integer())
    assert ObservedColumn("Amount", Integer()).name.spelling == "Amount"

    # And a well-formed observed column carries the observable fields
    column = ObservedColumn("amount", Integer(), nullable=False, comment="c", tags={"k": "v"})
    assert (column.name.spelling, column.nullable, column.comment) == ("amount", False, "c")
    assert dict(column.tags) == {"k": "v"}


def test_observed_column_preserves_catalog_spelling() -> None:
    assert ObservedColumn("requestId", Integer()).name.spelling == "requestId"


def test_column_names_compare_case_insensitively() -> None:
    desired = DesiredColumn(name="RequestId", data_type=String())
    observed = ObservedColumn(name="requestid", data_type=String())
    assert desired.name == observed.name
    assert desired.name in {observed.name}


def test_column_name_spelling_is_preserved_verbatim() -> None:
    column = DesiredColumn(name="RequestId", data_type=String())
    assert column.name.spelling == "RequestId"


def test_case_only_self_rename_is_rejected() -> None:
    with pytest.raises(ValueError, match="cannot be renamed_from itself"):
        DesiredColumn(name="RequestId", data_type=String(), renamed_from="requestid")
