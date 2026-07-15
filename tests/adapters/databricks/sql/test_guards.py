"""Behaviour tests for the read-boundary representability guards."""

import pytest

from delta_engine.adapters.databricks.sql.guards import (
    UnsupportedCatalogRelationError,
    require_delta_format,
    require_supported_relation,
)
from delta_engine.domain.model import QualifiedName

QN = QualifiedName("cat", "sch", "tbl")


# ---------- relation-kind guard ----------


@pytest.mark.parametrize("table_type", ["MANAGED", "managed"])
def test_require_supported_relation_admits_managed_delta_tables(table_type):
    require_supported_relation(table_type, QN)  # does not raise


@pytest.mark.parametrize(
    "table_type",
    [
        "EXTERNAL",
        "VIEW",
        "MATERIALIZED_VIEW",
        "STREAMING_TABLE",
        "FOREIGN",
        "MANAGED_SHALLOW_CLONE",
        "EXTERNAL_SHALLOW_CLONE",
        "SOME_FUTURE_KIND",
    ],
)
def test_require_supported_relation_rejects_every_other_kind(table_type):
    with pytest.raises(UnsupportedCatalogRelationError):
        require_supported_relation(table_type, QN)


def test_require_supported_relation_names_the_object_and_kind():
    with pytest.raises(UnsupportedCatalogRelationError, match="STREAMING_TABLE"):
        require_supported_relation("STREAMING_TABLE", QN)


# ---------- format guard ----------


@pytest.mark.parametrize("table_format", ["delta", "DELTA", "Delta"])
def test_require_delta_format_admits_delta(table_format):
    require_delta_format(table_format, QN)  # does not raise


@pytest.mark.parametrize("table_format", ["iceberg", "parquet", "csv"])
def test_require_delta_format_rejects_non_delta(table_format):
    with pytest.raises(UnsupportedCatalogRelationError):
        require_delta_format(table_format, QN)
