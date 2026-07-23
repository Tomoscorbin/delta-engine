"""Tests for the Delta table-feature policy."""

import pytest

from delta_engine.application.features import (
    DELTA_FEATURE_POLICY,
    FeatureDefinition,
    FeaturePolicy,
    ImpliedFeature,
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


def _implied(*data_types) -> frozenset[str]:
    columns = tuple(
        DesiredColumn(f"c{index}", data_type) for index, data_type in enumerate(data_types)
    )
    return DELTA_FEATURE_POLICY.implied_features(columns)


def test_feature_values_are_delta_protocol_names():
    assert ImpliedFeature.TIMESTAMP_NTZ.value == "timestampNtz"
    assert ImpliedFeature.VARIANT.value == "variantType"


def test_feature_free_types_imply_nothing():
    assert _implied(Integer(), Date(), Array(String())) == frozenset()


def test_scalar_types_imply_their_feature():
    assert _implied(TimestampNtz()) == frozenset({"timestampNtz"})
    assert _implied(Variant()) == frozenset({"variantType"})


def test_implications_are_found_at_every_depth():
    assert _implied(Array(TimestampNtz())) == frozenset({"timestampNtz"})
    assert _implied(Map(String(), Variant())) == frozenset({"variantType"})
    assert _implied(Map(TimestampNtz(), Integer())) == frozenset({"timestampNtz"})
    nested = Struct(
        fields=(
            StructField("seen_at", TimestampNtz()),
            StructField("payload", Array(Struct(fields=(StructField("v", Variant()),)))),
        )
    )
    assert _implied(nested) == frozenset({"timestampNtz", "variantType"})


def test_implications_combine_across_columns():
    assert _implied(Integer(), TimestampNtz(), Map(String(), Variant())) == frozenset(
        {"timestampNtz", "variantType"}
    )


def test_supported_managed_features_are_observed():
    properties = {
        "delta.feature.timestampNtz": "supported",
        "delta.feature.variantType": "supported",
    }

    assert DELTA_FEATURE_POLICY.supported_features(properties) == frozenset(
        {"timestampNtz", "variantType"}
    )


def test_preview_variant_name_is_observed_as_the_variant_feature():
    properties = {"delta.feature.variantType-preview": "supported"}

    assert DELTA_FEATURE_POLICY.supported_features(properties) == frozenset({"variantType"})


def test_unmanaged_feature_names_are_ignored():
    properties = {
        "delta.feature.deletionVectors": "supported",
        "delta.feature.appendOnly": "supported",
        "delta.feature.rowTracking": "supported",
    }

    assert DELTA_FEATURE_POLICY.supported_features(properties) == frozenset()


def test_ordinary_properties_are_ignored():
    properties = {"delta.enableChangeDataFeed": "true", "delta.columnMapping.mode": "name"}

    assert DELTA_FEATURE_POLICY.supported_features(properties) == frozenset()


def test_managed_feature_with_unexpected_value_reads_as_unsupported():
    # Being wrong this way costs one idempotent enablement, which normalizes
    # the value; the alternative was failing every read of the table.
    properties = {"delta.feature.timestampNtz": "enabled"}

    assert DELTA_FEATURE_POLICY.supported_features(properties) == frozenset()


def test_enable_property_rejects_a_feature_the_policy_does_not_manage():
    with pytest.raises(ValueError, match="No managed table feature"):
        DELTA_FEATURE_POLICY.enable_property("deletionVectors")


def test_enable_property_is_a_supported_feature_key():
    assert DELTA_FEATURE_POLICY.enable_property("timestampNtz") == (
        "delta.feature.timestampNtz",
        "supported",
    )


def test_every_feature_enables_under_a_name_it_observes():
    # Round-tripping through observation is what keeps a sync idempotent: an
    # enablement the engine cannot then see would be re-planned forever.
    for feature in ImpliedFeature:
        key, value = DELTA_FEATURE_POLICY.enable_property(feature)

        assert DELTA_FEATURE_POLICY.supported_features({key: value}) == frozenset({feature.value})


def _definition(feature, implied_by, enable_name, *observed_names) -> FeatureDefinition:
    return FeatureDefinition(
        feature=feature,
        implied_by=implied_by,
        enable_name=enable_name,
        observed_names=frozenset(observed_names or {enable_name}),
    )


def test_policy_rejects_a_feature_it_could_not_observe_after_enabling():
    # Given a full vocabulary whose one enable name is observed by nobody
    definitions = (
        _definition(ImpliedFeature.TIMESTAMP_NTZ, TimestampNtz, "timestampNtzV2", "timestampNtz"),
        _definition(ImpliedFeature.VARIANT, Variant, "variantType"),
    )

    # When building a policy from them, construction fails rather than yielding
    # a policy whose enablements would never converge
    with pytest.raises(ValueError, match="observe back as the same feature: timestampNtz"):
        FeaturePolicy(definitions)


def test_policy_rejects_an_enable_name_another_feature_observes():
    # The name is observable, but as the wrong feature: enabling timestampNtz
    # would read back as variantType, so the enable is re-planned forever.
    definitions = (
        _definition(ImpliedFeature.TIMESTAMP_NTZ, TimestampNtz, "variantType", "timestampNtz"),
        _definition(ImpliedFeature.VARIANT, Variant, "variantType"),
    )

    with pytest.raises(ValueError, match="observe back as the same feature: timestampNtz"):
        FeaturePolicy(definitions)


def test_policy_rejects_two_features_sharing_an_observed_name():
    definitions = (
        _definition(ImpliedFeature.TIMESTAMP_NTZ, TimestampNtz, "timestampNtz", "shared"),
        _definition(ImpliedFeature.VARIANT, Variant, "variantType", "shared"),
    )

    with pytest.raises(ValueError, match="observes 'shared' as both"):
        FeaturePolicy(definitions)


def test_policy_rejects_two_features_implied_by_the_same_type():
    definitions = (
        _definition(ImpliedFeature.TIMESTAMP_NTZ, TimestampNtz, "timestampNtz"),
        _definition(ImpliedFeature.VARIANT, TimestampNtz, "variantType"),
    )

    with pytest.raises(ValueError, match=r"implies both .* from TimestampNtz"):
        FeaturePolicy(definitions)


def test_policy_rejects_a_vocabulary_member_without_an_encoding():
    definition = FeatureDefinition(
        feature=ImpliedFeature.TIMESTAMP_NTZ,
        implied_by=TimestampNtz,
        enable_name="timestampNtz",
        observed_names=frozenset({"timestampNtz"}),
    )

    with pytest.raises(ValueError, match="no encoding for: variantType"):
        FeaturePolicy((definition,))
