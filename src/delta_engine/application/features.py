"""Derive the table features required by a desired Delta column tree."""

from collections.abc import Iterable
from typing import Final

from delta_engine.domain.model.column import DesiredColumn
from delta_engine.domain.model.data_type import (
    Array,
    DataType,
    Map,
    Struct,
    TimestampNtz,
    Variant,
)
from delta_engine.domain.model.table_feature import TableFeature

_REQUIRED_FEATURE_BY_TYPE: Final[dict[type[DataType], TableFeature]] = {
    TimestampNtz: TableFeature.TIMESTAMP_NTZ,
    Variant: TableFeature.VARIANT,
}


def required_features(columns: Iterable[DesiredColumn]) -> frozenset[TableFeature]:
    """Return every managed feature required anywhere in the desired type trees."""
    return frozenset(
        feature
        for column in columns
        for data_type in _walk_data_type(column.data_type)
        if (feature := _REQUIRED_FEATURE_BY_TYPE.get(type(data_type))) is not None
    )


def _walk_data_type(data_type: DataType) -> Iterable[DataType]:
    """Yield this type and every nested child type."""
    yield data_type
    match data_type:
        case Array(element=element):
            yield from _walk_data_type(element)
        case Map(key=key, value=value):
            yield from _walk_data_type(key)
            yield from _walk_data_type(value)
        case Struct(fields=fields):
            for field in fields:
                yield from _walk_data_type(field.data_type)
