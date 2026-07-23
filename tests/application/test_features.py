"""Tests for the Delta table-feature policy."""

import pytest

from delta_engine.application.features import (
    DELTA_FEATURE_POLICY,
    FeatureDefinition,
    FeaturePolicy,
    TableFeature,
)
from delta_engine.domain.model import (
    Array,
    Date,
    DesiredColumn,
    Integer,
    Map,
    String,
    Struct,
    StructField,
    TimestampNtz,
    Variant,
)


def _required(*data_types) -> frozenset[str]:
    columns = tuple(
        DesiredColumn(f"c{index}", data_type) for index, data_type in enumerate(data_types)
    )
    return DELTA_FEATURE_POLICY.required_features(columns)


def test_feature_values_are_delta_protocol_names():
    assert TableFeature.TIMESTAMP_NTZ.value == "timestampNtz"
    assert TableFeature.VARIANT.value == "variantType"


def test_feature_free_types_require_nothing():
    assert _required(Integer(), Date(), Array(String())) == frozenset()


def test_scalar_requirements():
    assert _required(TimestampNtz()) == frozenset({"timestampNtz"})
    assert _required(Variant()) == frozenset({"variantType"})


def test_requirements_are_found_at_every_depth():
    assert _required(Array(TimestampNtz())) == frozenset({"timestampNtz"})
    assert _required(Map(String(), Variant())) == frozenset({"variantType"})
    assert _required(Map(TimestampNtz(), Integer())) == frozenset({"timestampNtz"})
    nested = Struct(
        fields=(
            StructField("seen_at", TimestampNtz()),
            StructField("payload", Array(Struct(fields=(StructField("v", Variant()),)))),
        )
    )
    assert _required(nested) == frozenset({"timestampNtz", "variantType"})


def test_requirements_combine_across_columns():
    assert _required(Integer(), TimestampNtz(), Map(String(), Variant())) == frozenset(
        {"timestampNtz", "variantType"}
    )


def test_supported_managed_features_are_observed():
    properties = {
        "delta.feature.timestampNtz": "supported",
        "delta.feature.variantType": "supported",
    }

    assert DELTA_FEATURE_POLICY.enabled_features(properties) == frozenset(
        {"timestampNtz", "variantType"}
    )


def test_preview_variant_name_is_observed_as_the_variant_feature():
    properties = {"delta.feature.variantType-preview": "supported"}

    assert DELTA_FEATURE_POLICY.enabled_features(properties) == frozenset({"variantType"})


def test_unmanaged_feature_names_are_ignored():
    properties = {
        "delta.feature.deletionVectors": "supported",
        "delta.feature.appendOnly": "supported",
        "delta.feature.rowTracking": "supported",
    }

    assert DELTA_FEATURE_POLICY.enabled_features(properties) == frozenset()


def test_ordinary_properties_are_ignored():
    properties = {"delta.enableChangeDataFeed": "true", "delta.columnMapping.mode": "name"}

    assert DELTA_FEATURE_POLICY.enabled_features(properties) == frozenset()


def test_managed_feature_with_unexpected_value_fails_closed():
    properties = {"delta.feature.timestampNtz": "enabled"}

    with pytest.raises(ValueError, match=r"delta\.feature\.timestampNtz"):
        DELTA_FEATURE_POLICY.enabled_features(properties)


def test_enable_property_is_a_supported_feature_key():
    assert DELTA_FEATURE_POLICY.enable_property("timestampNtz") == (
        "delta.feature.timestampNtz",
        "supported",
    )


def test_every_feature_enables_under_a_name_it_observes():
    # Round-tripping through observation is what keeps a sync idempotent: an
    # enablement the engine cannot then see would be re-planned forever.
    for feature in TableFeature:
        key, value = DELTA_FEATURE_POLICY.enable_property(feature)

        assert DELTA_FEATURE_POLICY.enabled_features({key: value}) == frozenset({feature.value})


def test_policy_rejects_a_feature_it_could_not_observe_after_enabling():
    # Given a full vocabulary whose one enable name is absent from the names
    # that same feature is observed under
    definitions = (
        FeatureDefinition(
            feature=TableFeature.TIMESTAMP_NTZ,
            required_by=TimestampNtz,
            enable_name="timestampNtzV2",
            observed_names=frozenset({"timestampNtz"}),
        ),
        FeatureDefinition(
            feature=TableFeature.VARIANT,
            required_by=Variant,
            enable_name="variantType",
            observed_names=frozenset({"variantType"}),
        ),
    )

    # When building a policy from them, construction fails rather than yielding
    # a policy whose enablements would never converge
    with pytest.raises(ValueError, match="timestampNtz"):
        FeaturePolicy(definitions)


def test_policy_rejects_a_vocabulary_member_without_an_encoding():
    definition = FeatureDefinition(
        feature=TableFeature.TIMESTAMP_NTZ,
        required_by=TimestampNtz,
        enable_name="timestampNtz",
        observed_names=frozenset({"timestampNtz"}),
    )

    with pytest.raises(ValueError, match="no encoding for: variantType"):
        FeaturePolicy((definition,))
