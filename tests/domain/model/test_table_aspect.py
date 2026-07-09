from delta_engine.domain.model import ALL_ASPECTS, TableAspect


def test_all_aspects_contains_every_aspect():
    # Given the canonical full-management set
    # Then it equals the full enum
    assert ALL_ASPECTS == frozenset(TableAspect)


def test_aspect_label_is_human_readable():
    # Given an aspect with an underscored name
    # Then the label reads as prose (used in validation messages)
    assert TableAspect.COLUMN_STRUCTURE.label == "column structure"
    assert TableAspect.PROPERTIES.label == "properties"


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
        "CLUSTERING",
    ]


def test_clustering_is_a_managed_aspect():
    # Given the clustering aspect
    # Then it is part of the full managed-aspect set and has a readable label
    assert TableAspect.CLUSTERING in ALL_ASPECTS
    assert TableAspect.CLUSTERING.label == "clustering"
