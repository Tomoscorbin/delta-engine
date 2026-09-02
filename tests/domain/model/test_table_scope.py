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

MANAGED_BY_SCOPE = [
    (TableScope.TAGS, TAGS),
    (TableScope.ANNOTATIONS, TAGS | COMMENTS),
    (TableScope.METADATA, TAGS | COMMENTS | KEYS),
    (TableScope.FULL, TAGS | COMMENTS | KEYS | STRUCTURE),
]


@pytest.mark.parametrize(("scope", "managed"), MANAGED_BY_SCOPE)
def test_each_scope_manages_its_part_of_the_table(
    scope: TableScope, managed: frozenset[TableAspect]
) -> None:
    # Then each scope manages exactly its slice of the aspect vocabulary
    assert {aspect for aspect in TableAspect if scope.manages(aspect)} == managed


@pytest.mark.parametrize(("scope", "managed"), MANAGED_BY_SCOPE)
def test_a_scope_ignores_properties_exactly_when_it_does_not_manage_them(
    scope: TableScope, managed: frozenset[TableAspect]
) -> None:
    # Then properties are the only ignored aspect, and only outside the scope
    ignored = {aspect for aspect in TableAspect if scope.ignores(aspect)}
    assert ignored == ({TableAspect.PROPERTIES} - managed)


@pytest.mark.parametrize(("scope", "managed"), MANAGED_BY_SCOPE)
def test_aspects_outside_a_scope_must_match_the_live_table_except_properties(
    scope: TableScope, managed: frozenset[TableAspect]
) -> None:
    # Given the aspects outside this scope
    outside = frozenset(TableAspect) - managed

    # Then every outside aspect except properties must mirror the live table
    checked = {aspect for aspect in TableAspect if scope.requires_match(aspect)}
    assert checked == outside - {TableAspect.PROPERTIES}


@pytest.mark.parametrize(
    ("scope", "limit", "expected"),
    [
        (TableScope.TAGS, TableScope.ANNOTATIONS, True),
        (TableScope.ANNOTATIONS, TableScope.ANNOTATIONS, True),
        (TableScope.METADATA, TableScope.ANNOTATIONS, False),
        (TableScope.FULL, TableScope.ANNOTATIONS, False),
    ],
)
def test_scope_knows_whether_it_fits_within_an_authority_limit(
    scope: TableScope, limit: TableScope, expected: bool
) -> None:
    # Then a scope fits within a limit exactly when it grants no more authority
    assert scope.is_within(limit) is expected


def test_aspect_label_is_human_readable() -> None:
    # Given an aspect with an underscored name
    # Then the label reads as prose (used in validation messages)
    assert TableAspect.COLUMN_STRUCTURE.label == "column structure"
    assert TableAspect.PROPERTIES.label == "properties"
