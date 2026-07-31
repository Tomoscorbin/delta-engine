import pytest

from delta_engine.application.scopes import (
    ANNOTATION_ASPECTS,
    METADATA_ASPECTS,
    TAG_ASPECTS,
    managed_aspects_for,
)
from delta_engine.domain.model import ALL_ASPECTS, TableAspect

COMMENTS = frozenset({TableAspect.TABLE_COMMENT, TableAspect.COLUMN_COMMENTS})
TAGS = frozenset({TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS})
KEYS = frozenset({TableAspect.PRIMARY_KEY, TableAspect.FOREIGN_KEYS})


@pytest.mark.parametrize(
    ("scope", "expected"),
    [
        ("full", ALL_ASPECTS),
        ("metadata", COMMENTS | TAGS | KEYS),
        ("annotations", COMMENTS | TAGS),
        ("tags", TAGS),
    ],
)
def test_each_scope_name_resolves_to_its_aspects(scope, expected):
    # Built from groups named here rather than from the module's own constants,
    # so this states each scope's definition instead of restating it
    assert managed_aspects_for(scope) == expected


def test_unknown_scope_is_rejected():
    with pytest.raises(ValueError):
        managed_aspects_for("everything")


def test_the_scopes_form_a_containment_lattice():
    # Given the four public scopes
    # Then each is contained by the next, so a caller who narrows a
    # scope can only ever lose authority, never trade it sideways
    assert TAG_ASPECTS < ANNOTATION_ASPECTS
    assert ANNOTATION_ASPECTS < METADATA_ASPECTS
    assert METADATA_ASPECTS < ALL_ASPECTS


def test_metadata_excludes_existence_and_physical_aspects():
    # Stated as a subtraction on purpose: a new TableAspect added to the domain
    # fails this test until someone decides whether metadata governs it. The
    # enumerated cases above would keep passing, since neither side of them grows.
    assert METADATA_ASPECTS == ALL_ASPECTS - frozenset(
        {
            TableAspect.TABLE_EXISTENCE,
            TableAspect.COLUMN_STRUCTURE,
            TableAspect.PROPERTIES,
            TableAspect.PARTITIONING,
            TableAspect.CLUSTERING,
        }
    )


def test_annotations_scope_does_not_manage_keys():
    # Keys are why "annotations" sits below "metadata": a streaming table's
    # defining SQL owns them, and a refresh can revert a change made from
    # outside the pipeline. Comments and tags survive one.
    assert TableAspect.PRIMARY_KEY not in ANNOTATION_ASPECTS
    assert TableAspect.FOREIGN_KEYS not in ANNOTATION_ASPECTS
