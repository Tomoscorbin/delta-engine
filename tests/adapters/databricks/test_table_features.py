import pytest

from delta_engine.adapters.databricks.table_features import (
    enable_property_key,
    recognized_table_features,
    supported_features_from_properties,
)
from delta_engine.domain.model import TableFeature


@pytest.mark.parametrize(
    ("feature", "canonical_name", "enable_key"),
    [
        (
            TableFeature.TIMESTAMP_NTZ,
            "timestampNtz",
            "delta.feature.timestampNtz",
        ),
        (
            TableFeature.VARIANT,
            "variantType",
            "delta.feature.variantType-preview",
        ),
    ],
)
def test_feature_definitions_drive_recognition_and_enablement(
    feature: TableFeature,
    canonical_name: str,
    enable_key: str,
):
    enable_name = enable_key.removeprefix("delta.feature.")

    assert recognized_table_features([canonical_name, enable_name]) == frozenset({feature})
    assert enable_property_key(feature) == enable_key


def test_supported_features_are_projected_from_catalog_properties():
    properties = {
        "delta.feature.timestampNtz": "supported",
        "delta.feature.variantType-preview": "supported",
        "delta.feature.deletionVectors": "supported",
        "delta.feature.variantType": "unsupported",
        "delta.enableChangeDataFeed": "true",
    }

    assert supported_features_from_properties(properties) == frozenset(
        {
            TableFeature.TIMESTAMP_NTZ,
            TableFeature.VARIANT,
        }
    )
