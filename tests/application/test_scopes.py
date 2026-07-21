import pytest

from delta_engine.application.scopes import managed_aspects_for
from delta_engine.domain.model import ALL_ASPECTS, TableAspect


def test_full_scope_manages_every_aspect():
    assert managed_aspects_for("full") == ALL_ASPECTS


def test_metadata_scope_manages_only_catalog_metadata():
    assert managed_aspects_for("metadata") == frozenset(
        {
            TableAspect.TABLE_COMMENT,
            TableAspect.COLUMN_COMMENTS,
            TableAspect.TABLE_TAGS,
            TableAspect.COLUMN_TAGS,
            TableAspect.PRIMARY_KEY,
            TableAspect.FOREIGN_KEYS,
        }
    )


def test_tags_scope_manages_only_tags():
    assert managed_aspects_for("tags") == frozenset(
        {
            TableAspect.TABLE_TAGS,
            TableAspect.COLUMN_TAGS,
        }
    )


def test_unknown_scope_is_rejected():
    with pytest.raises(ValueError):
        managed_aspects_for("everything")
