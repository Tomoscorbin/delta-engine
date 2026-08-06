import pytest

from delta_engine.domain.model import TableAspect, TableScope

TAGS = frozenset({TableAspect.TABLE_TAGS, TableAspect.COLUMN_TAGS})
COMMENTS = frozenset({TableAspect.TABLE_COMMENT, TableAspect.COLUMN_COMMENTS})
KEYS = frozenset({TableAspect.PRIMARY_KEY, TableAspect.FOREIGN_KEYS})
STRUCTURE = frozenset(
    {
        TableAspect.TABLE_EXISTENCE,
        TableAspect.COLUMN_STRUCTURE,
        TableAspect.PROPERTIES,
        TableAspect.PARTITIONING,
        TableAspect.CLUSTERING,
    }
)


@pytest.mark.parametrize(
    ("scope", "managed"),
    [
        (TableScope.TAGS, TAGS),
        (TableScope.ANNOTATIONS, TAGS | COMMENTS),
        (TableScope.METADATA, TAGS | COMMENTS | KEYS),
        (TableScope.FULL, TAGS | COMMENTS | KEYS | STRUCTURE),
    ],
)
def test_each_scope_manages_its_part_of_the_table(scope, managed):
    assert {aspect for aspect in TableAspect if scope.manages(aspect)} == managed


@pytest.mark.parametrize(
    ("scope", "limit", "expected"),
    [
        (TableScope.TAGS, TableScope.ANNOTATIONS, True),
        (TableScope.ANNOTATIONS, TableScope.ANNOTATIONS, True),
        (TableScope.METADATA, TableScope.ANNOTATIONS, False),
        (TableScope.FULL, TableScope.ANNOTATIONS, False),
    ],
)
def test_scope_knows_whether_it_fits_within_an_authority_limit(scope, limit, expected):
    assert scope.is_within(limit) is expected
