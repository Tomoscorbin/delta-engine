"""
Delta table features the domain models, and which declared types require them.

Values are the Delta protocol feature names — protocol vocabulary, not a
platform encoding. How Databricks spells them in table properties
(``delta.feature.*`` keys, the ``'supported'`` value, preview aliases) is the
adapter's concern.

Admission rule: a feature belongs here only when it is a prerequisite of an
engine-planned action that the platform does not enable automatically as part
of executing that action. Property-gated features (appendOnly,
changeDataFeed, columnMapping, typeWidening) ride along with their enabling
property or DDL; platform-managed features (deletionVectors, rowTracking,
invariants) are not the engine's to manage. That leaves exactly the features
required by declared types, which Databricks enables on CREATE but not on
ALTER.
"""

from enum import StrEnum

from delta_engine.domain.model.data_type import (
    Array,
    DataType,
    Map,
    Struct,
    TimestampNtz,
    Variant,
)


class TableFeature(StrEnum):
    """A Delta table feature required by a declarable type."""

    TIMESTAMP_NTZ = "timestampNtz"
    VARIANT = "variantType"


def required_features(data_type: DataType) -> frozenset[TableFeature]:
    """Return every table feature required to store ``data_type``, at any depth."""
    match data_type:
        case TimestampNtz():
            return frozenset({TableFeature.TIMESTAMP_NTZ})
        case Variant():
            return frozenset({TableFeature.VARIANT})
        case Array(element=element):
            return required_features(element)
        case Map(key=key, value=value):
            return required_features(key) | required_features(value)
        case Struct(fields=fields):
            empty: frozenset[TableFeature] = frozenset()
            return empty.union(*(required_features(field.data_type) for field in fields))
        case _:
            return frozenset()
