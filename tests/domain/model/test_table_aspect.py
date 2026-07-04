from delta_engine.domain.model import ALL_ASPECTS, TableAspect


def test_all_aspects_contains_every_aspect():
    # Given the canonical full-management set
    # Then it equals the full enum
    assert ALL_ASPECTS == frozenset(TableAspect)


def test_aspect_declaration_order_is_canonical():
    # Given the enum declaration order (used for deterministic rendering in messages)
    names = [a.name for a in TableAspect]

    # Then the order is as documented
    assert names == [
        "COLUMN_STRUCTURE",
        "COLUMN_COMMENTS",
        "COLUMN_TAGS",
        "TABLE_COMMENT",
        "TABLE_TAGS",
        "PROPERTIES",
        "PARTITIONING",
        "PRIMARY_KEY",
        "FOREIGN_KEYS",
    ]
