import pytest

from delta_engine.adapters.databricks.table_features import (
    enable_property,
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
    # Given a feature's canonical and Databricks enablement spellings
    enable_name = enable_key.removeprefix("delta.feature.")

    # When resolving observed names and its enablement property
    recognized = recognized_table_features([canonical_name, enable_name])
    property_ = enable_property(feature)

    # Then both names resolve canonically and the complete property is returned
    assert recognized == frozenset({feature})
    assert property_.key == enable_key
    assert property_.value == "supported"


def test_supported_features_are_projected_from_catalog_properties():
    # Given mixed supported, unsupported, unknown, and unrelated catalog properties
    properties = {
        "delta.feature.timestampNtz": "supported",
        "delta.feature.variantType-preview": "supported",
        "delta.feature.deletionVectors": "supported",
        "delta.feature.variantType": "unsupported",
        "delta.enableChangeDataFeed": "true",
    }

    # When projecting the table features managed by the engine
    features = supported_features_from_properties(properties)

    # Then only supported, recognized features remain
    assert features == frozenset(
        {
            TableFeature.TIMESTAMP_NTZ,
            TableFeature.VARIANT,
        }
    )
