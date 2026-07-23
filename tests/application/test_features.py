"""Tests for the table-feature vocabulary and the required-features walker."""

from delta_engine.application.features import TableFeature, required_features
from delta_engine.domain.model import (
    Array,
    Date,
    Integer,
    Map,
    String,
    Struct,
    StructField,
    TimestampNtz,
    Variant,
)


def test_feature_values_are_delta_protocol_names():
    assert TableFeature.TIMESTAMP_NTZ.value == "timestampNtz"
    assert TableFeature.VARIANT.value == "variantType"


def test_feature_free_types_require_nothing():
    assert required_features(Integer()) == frozenset()
    assert required_features(Date()) == frozenset()
    assert required_features(Array(String())) == frozenset()


def test_scalar_requirements():
    assert required_features(TimestampNtz()) == frozenset({TableFeature.TIMESTAMP_NTZ})
    assert required_features(Variant()) == frozenset({TableFeature.VARIANT})


def test_requirements_are_found_at_every_depth():
    assert required_features(Array(TimestampNtz())) == frozenset({TableFeature.TIMESTAMP_NTZ})
    assert required_features(Map(String(), Variant())) == frozenset({TableFeature.VARIANT})
    assert required_features(Map(TimestampNtz(), Integer())) == frozenset(
        {TableFeature.TIMESTAMP_NTZ}
    )
    nested = Struct(
        fields=(
            StructField("seen_at", TimestampNtz()),
            StructField("payload", Array(Struct(fields=(StructField("v", Variant()),)))),
        )
    )
    assert required_features(nested) == frozenset(
        {TableFeature.TIMESTAMP_NTZ, TableFeature.VARIANT}
    )
