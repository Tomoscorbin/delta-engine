from typing import Any

from hypothesis import example, given, strategies as st
import pytest

from delta_engine.domain.model.column import DesiredColumn, ObservedColumn
from delta_engine.domain.model.data_type import Integer, String
from tests.domain.model.strategies import NON_DATA_TYPES

_EACH_COLUMN_TYPE = pytest.mark.parametrize(
    "column_type", [DesiredColumn, ObservedColumn], ids=["desired", "observed"]
)


@example(column_type=DesiredColumn, invalid=None)
@example(column_type=ObservedColumn, invalid=None)
@given(
    column_type=st.sampled_from((DesiredColumn, ObservedColumn)),
    invalid=NON_DATA_TYPES,
)
def test_columns_reject_non_data_types(
    column_type: type[DesiredColumn] | type[ObservedColumn], invalid: Any
) -> None:
    # When the data type is not a DataType instance, then construction fails
    with pytest.raises(TypeError):
        column_type("value", invalid)


def test_defaults_to_nullable_true() -> None:
    # Given a column with no explicit nullability
    col = DesiredColumn("id", Integer())

    # Then it defaults to nullable=True
    assert col.nullable is True


# Case is never identity on Databricks, but display spelling is real catalog
# state: the engine stores it verbatim and compares by identity. 'straße' is
# already lowercase; casefold would rewrite it to 'strasse', a different
# identifier from the one Unity Catalog stores.
@example("UserId")
@example("straße")
@given(st.text(min_size=1).filter(str.strip))
def test_construction_preserves_any_name_verbatim(name: str) -> None:
    # Then the name round-trips exactly as authored
    assert str(DesiredColumn(name, Integer()).name) == name


@_EACH_COLUMN_TYPE
@pytest.mark.parametrize("blank", ["", "   ", "\t"], ids=["empty", "spaces", "tab"])
def test_raises_when_name_is_blank(
    column_type: type[DesiredColumn] | type[ObservedColumn], blank: str
) -> None:
    # Given a blank or whitespace-only column name (would emit a malformed
    # `` `` identifier in DDL)
    # When/Then: construction fails for desired and observed columns alike
    with pytest.raises(ValueError):
        column_type(blank, Integer())


def test_tags_default_to_empty() -> None:
    # Given a column with no explicit tags
    col = DesiredColumn("id", Integer())

    # Then it defaults to an empty tag mapping
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
    with pytest.raises(ValueError):
        DesiredColumn("id", Integer(), tags={blank: "v"})


def test_column_accepts_a_rename_hint() -> None:
    # Given a column declaring its previous name
    column = DesiredColumn("customer_name", String(), renamed_from="customer_nm")

    # Then the hint is carried on the declaration
    assert column.renamed_from == "customer_nm"


def test_column_defaults_to_no_rename_hint() -> None:
    # Then a plain declaration carries no rename hint
    assert DesiredColumn("customer_name", String()).renamed_from is None


def test_renamed_from_is_preserved_verbatim() -> None:
    # Given a rename hint with mixed-case spelling
    column = DesiredColumn("customer_name", String(), renamed_from="Customer_NM")

    # Then the spelling is preserved, not lowercased
    assert str(column.renamed_from) == "Customer_NM"


def test_column_rejects_a_blank_rename_source() -> None:
    # When the rename source is blank, then construction fails
    with pytest.raises(ValueError):
        DesiredColumn("customer_name", String(), renamed_from="  ")


def test_column_rejects_a_rename_from_itself() -> None:
    # When a column declares a rename from its own name, then construction fails
    with pytest.raises(ValueError):
        DesiredColumn("customer_name", String(), renamed_from="customer_name")


def test_case_only_rename_is_rejected_as_a_self_rename() -> None:
    # Given a rename hint differing from the column name only in case
    # (case is not identity, so it still names the same column)
    # When/Then: construction fails
    with pytest.raises(ValueError):
        DesiredColumn("customer_name", String(), renamed_from="Customer_Name")
