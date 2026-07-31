import pytest

from delta_engine.application.scopes import (
    ANNOTATION_ASPECTS,
    METADATA_ASPECTS,
    TAG_ASPECTS,
    managed_aspects_for,
)
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


def test_annotations_scope_manages_comments_and_tags():
    assert managed_aspects_for("annotations") == frozenset(
        {
            TableAspect.TABLE_COMMENT,
            TableAspect.COLUMN_COMMENTS,
            TableAspect.TABLE_TAGS,
            TableAspect.COLUMN_TAGS,
        }
    )


def test_the_scopes_form_a_containment_lattice():
    # Given the four public scopes
    # Then each is contained by the enxt, so a caller who narrows a
    # scope can only ever lose authority, never trade it sideways
    assert TAG_ASPECTS < ANNOTATION_ASPECTS
    assert ANNOTATION_ASPECTS < METADATA_ASPECTS
    assert METADATA_ASPECTS < ALL_ASPECTS


def test_annotations_scope_does_not_manage_keys():
    # The reason "annotations" exists is because streaming
    # tables can't support them (kind of)
    assert TableAspect.PRIMARY_KEY not in ANNOTATION_ASPECTS
    assert TableAspect.FOREIGN_KEYS not in ANNOTATION_ASPECTS
