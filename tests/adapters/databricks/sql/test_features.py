"""Tests for the Databricks encoding of Delta table features."""

import pytest

from delta_engine.adapters.databricks.sql.features import (
    enable_feature_property_key,
    enabled_features_from_properties,
)
from delta_engine.application.features import TableFeature


def test_supported_modeled_features_are_observed():
    properties = {
        "delta.feature.timestampNtz": "supported",
        "delta.feature.variantType": "supported",
    }

    assert enabled_features_from_properties(properties) == frozenset(
        {TableFeature.TIMESTAMP_NTZ, TableFeature.VARIANT}
    )


def test_preview_variant_name_maps_to_variant():
    properties = {"delta.feature.variantType-preview": "supported"}

    assert enabled_features_from_properties(properties) == frozenset({TableFeature.VARIANT})


def test_unmodeled_feature_names_are_ignored():
    properties = {
        "delta.feature.deletionVectors": "supported",
        "delta.feature.appendOnly": "supported",
        "delta.feature.rowTracking": "supported",
    }

    assert enabled_features_from_properties(properties) == frozenset()


def test_ordinary_properties_are_ignored():
    properties = {"delta.enableChangeDataFeed": "true", "delta.columnMapping.mode": "name"}

    assert enabled_features_from_properties(properties) == frozenset()


def test_modeled_feature_with_unexpected_value_fails_closed():
    properties = {"delta.feature.timestampNtz": "enabled"}

    with pytest.raises(ValueError, match=r"delta\.feature\.timestampNtz"):
        enabled_features_from_properties(properties)


def test_enable_keys_round_trip_through_observation():
    for feature in TableFeature:
        key = enable_feature_property_key(feature)

        assert enabled_features_from_properties({key: "supported"}) == frozenset({feature})
