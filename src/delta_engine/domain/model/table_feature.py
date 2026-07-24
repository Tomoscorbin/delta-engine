"""Delta table features the engine can represent in observed state and plans."""

from collections.abc import Iterable, Mapping
from enum import StrEnum
from types import MappingProxyType
from typing import Final

from delta_engine.domain.model.data_type import (
    Array,
    DataType,
    Map,
    Struct,
    TimestampNtz,
    Variant,
)


class TableFeature(StrEnum):
    """Closed identity vocabulary for table features managed by the engine."""

    TIMESTAMP_NTZ = "timestampNtz"
    VARIANT = "variantType"


_REQUIRED_FEATURE_BY_TYPE: Final[Mapping[type[DataType], TableFeature]] = MappingProxyType(
    {
        TimestampNtz: TableFeature.TIMESTAMP_NTZ,
        Variant: TableFeature.VARIANT,
    }
)


def required_table_features(data_types: Iterable[DataType]) -> frozenset[TableFeature]:
    """Return the table features implied by one or more data type trees."""
    return frozenset(
        feature
        for data_type in data_types
        for nested_type in _walk_data_type(data_type)
        if (feature := _REQUIRED_FEATURE_BY_TYPE.get(type(nested_type))) is not None
    )


def _walk_data_type(data_type: DataType) -> Iterable[DataType]:
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
