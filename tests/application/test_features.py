"""Tests for deriving required Delta table features from desired columns."""

from delta_engine.application.features import required_features
from delta_engine.domain.model import (
    Array,
    Date,
    DesiredColumn,
    Integer,
    Map,
    String,
    Struct,
    StructField,
    TableFeature,
    TimestampNtz,
    Variant,
)


def _required(*data_types) -> frozenset[TableFeature]:
    columns = tuple(
        DesiredColumn(f"c{index}", data_type) for index, data_type in enumerate(data_types)
    )
    return required_features(columns)


def test_feature_free_types_require_nothing():
    assert _required(Integer(), Date(), Array(String())) == frozenset()


def test_scalar_types_require_their_feature():
    assert _required(TimestampNtz()) == frozenset({TableFeature.TIMESTAMP_NTZ})
    assert _required(Variant()) == frozenset({TableFeature.VARIANT})


def test_requirements_are_found_at_every_depth():
    assert _required(Array(TimestampNtz())) == frozenset({TableFeature.TIMESTAMP_NTZ})
    assert _required(Map(String(), Variant())) == frozenset({TableFeature.VARIANT})
    assert _required(Map(TimestampNtz(), Integer())) == frozenset({TableFeature.TIMESTAMP_NTZ})
    nested = Struct(
        fields=(
            StructField("seen_at", TimestampNtz()),
            StructField("payload", Array(Struct(fields=(StructField("v", Variant()),)))),
        )
    )
    assert _required(nested) == frozenset({TableFeature.TIMESTAMP_NTZ, TableFeature.VARIANT})


def test_requirements_combine_across_columns():
    assert _required(Integer(), TimestampNtz(), Map(String(), Variant())) == frozenset(
        {TableFeature.TIMESTAMP_NTZ, TableFeature.VARIANT}
    )
