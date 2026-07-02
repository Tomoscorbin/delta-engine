import pytest

from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer


def test_defaults_to_nullable_true() -> None:
    # Given: a column with no explicit nullability
    # When: constructing a Column
    col = Column("id", Integer())
    # Then: it defaults to nullable=True
    assert col.nullable is True


def test_raises_when_name_is_not_lowercase() -> None:
    # Given: a column name containing uppercase characters
    # When: constructing a Column
    # Then: a ValueError is raised
    with pytest.raises(ValueError, match="lowercase"):
        Column("UserId", Integer())


@pytest.mark.parametrize("blank", ["", "   ", "\t"], ids=["empty", "spaces", "tab"])
def test_raises_when_name_is_blank(blank: str) -> None:
    # Given: a blank or whitespace-only column name (would emit a malformed
    # `` `` identifier in DDL)
    # When/Then: constructing a Column fails
    with pytest.raises(ValueError, match="blank"):
        Column(blank, Integer())


def test_tags_default_to_empty() -> None:
    # Given: a column with no explicit tags
    # When: constructing a Column
    col = Column("id", Integer())
    # Then: it defaults to an empty tag mapping
    assert dict(col.tags) == {}


def test_tags_are_stored_verbatim() -> None:
    # Given a column declared with two tags
    col = Column("email", Integer(), tags={"pii": "true", "classification": "restricted"})
    # Then the tags are stored exactly as given
    assert dict(col.tags) == {"pii": "true", "classification": "restricted"}


def test_tag_keys_are_case_sensitive() -> None:
    # Given tag keys differing only in case (UC tag keys are case-sensitive)
    col = Column("email", Integer(), tags={"PII": "true", "pii": "false"})
    # Then both keys are preserved distinctly (not casefolded like the column name)
    assert dict(col.tags) == {"PII": "true", "pii": "false"}


@pytest.mark.parametrize("blank", ["", "   ", "\t"], ids=["empty", "spaces", "tab"])
def test_raises_when_tag_key_is_blank(blank: str) -> None:
    # Given a blank or whitespace-only tag key
    # When/Then: constructing a Column fails
    with pytest.raises(ValueError, match="Tag key must not be blank"):
        Column("id", Integer(), tags={blank: "v"})
