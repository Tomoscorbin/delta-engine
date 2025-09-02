# tests/adapters/databricks/test_catalog_reader.py

from __future__ import annotations

from types import MappingProxyType, SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from delta_engine.adapters.databricks.catalog.reader import DatabricksReader
from delta_engine.application.results import ReadResult
from delta_engine.domain.model.column import Column
from delta_engine.domain.model.data_type import Integer, String
from tests.factories import make_qualified_name

_QN = make_qualified_name("dev", "silver", "people")


def _exist_df(exists: bool) -> MagicMock:
    df = MagicMock()
    df.head.return_value = [object()] if exists else []
    return df


def _detail_df(properties: dict[str, str]) -> MagicMock:
    df = MagicMock()
    # fetch_state -> _fetch_properties -> df.first() -> row["properties"]
    df.first.return_value = {"properties": properties}
    return df


@pytest.fixture
def patch_type_mapping(monkeypatch):
    # Keep this tiny so we don't retest domain_type_from_spark internals.
    import delta_engine.adapters.databricks.catalog.reader as reader_mod

    def fake_mapping(spark_type: Any):
        if spark_type == "int":
            return Integer()
        if spark_type == "string":
            return String()
        return String()

    monkeypatch.setattr(reader_mod, "domain_type_from_spark", fake_mapping)


def test_present_maps_columns_and_applies_policy(monkeypatch, patch_type_mapping) -> None:
    # spark.catalog.listColumns -> two "columns"
    # second one has no `nullable` attribute; default should be True
    cols = [
        SimpleNamespace(name="id", dataType="int", nullable=False, description=""),
        SimpleNamespace(name="name", dataType="string", description=""),
    ]
    properties_in_uc = {"delta.minReaderVersion": "2", "owner": "analytics"}
    enforced_props = MappingProxyType({"policy": "applied"})

    # enforce_property_policy must be used and its return value attached to observed.properties
    import delta_engine.adapters.databricks.catalog.reader as reader_mod

    monkeypatch.setattr(reader_mod, "enforce_property_policy", lambda props: enforced_props)

    catalog = MagicMock()
    catalog.listColumns.return_value = cols

    spark = MagicMock()
    spark.catalog = catalog
    spark.sql.side_effect = [
        _exist_df(True),  # existence check
        _detail_df(properties_in_uc),  # DESCRIBE DETAIL
    ]

    reader = DatabricksReader(cast(Any, spark))

    res = reader.fetch_state(_QN)

    assert isinstance(res, ReadResult)
    assert res.failure is None
    assert res.observed is not None

    observed = res.observed
    assert observed.qualified_name == _QN
    assert tuple(isinstance(c, Column) for c in observed.columns) == (True, True)
    assert [c.name for c in observed.columns] == ["id", "name"]

    # data type mapping and nullability
    assert observed.columns[0].data_type == Integer()
    assert observed.columns[1].data_type == String()
    assert observed.columns[0].is_nullable is False
    assert observed.columns[1].is_nullable is True  # default when attribute missing

    # properties are whatever policy returned
    assert observed.properties is enforced_props


def test_absent_when_table_does_not_exist() -> None:
    catalog = MagicMock()
    catalog.listColumns.return_value = []

    spark = MagicMock()
    spark.catalog = catalog
    spark.sql.side_effect = [_exist_df(False)]  # existence false

    reader = DatabricksReader(cast(Any, spark))

    res = reader.fetch_state(_QN)

    assert res.failure is None
    assert res.observed is None
    catalog.listColumns.assert_not_called()  # sanity: no schema read on absent


def test_failed_when_describe_detail_raises(patch_type_mapping) -> None:
    catalog = MagicMock()
    catalog.listColumns.return_value = [
        SimpleNamespace(name="id", dataType="int", nullable=False, description="")
    ]

    spark = MagicMock()
    spark.catalog = catalog
    spark.sql.side_effect = [
        _exist_df(True),  # existence true
        RuntimeError("describe detail exploded"),  # DESCRIBE DETAIL fails
    ]

    reader = DatabricksReader(cast(Any, spark))

    res = reader.fetch_state(_QN)

    assert res.failure is not None
    assert res.observed is None
    assert res.failure.exception_type == "RuntimeError"
    assert "describe detail" in res.failure.message
