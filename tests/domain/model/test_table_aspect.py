from delta_engine.domain.model import ALL_ASPECTS, TableAspect


def test_all_aspects_contains_every_aspect():
    # Given the canonical full-management set
    # Then it is exactly the set of all enum members
    assert ALL_ASPECTS == frozenset(TableAspect)


def test_aspect_declaration_order_is_canonical():
    # Given the enum declaration
    # Then the documented canonical order holds (TargetColumnMissing.reasons relies on it)
    names = [aspect.name for aspect in TableAspect]
    assert names == [
        "COLUMN_STRUCTURE",
        "TABLE_COMMENT",
        "COLUMN_COMMENTS",
        "TABLE_TAGS",
        "COLUMN_TAGS",
        "PROPERTIES",
        "PARTITIONING",
        "PRIMARY_KEY",
        "FOREIGN_KEYS",
    ]
