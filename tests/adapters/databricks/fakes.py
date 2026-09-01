"""Shared fakes and builders for the Databricks catalog-read tests."""

import json

import pytest

from delta_engine.adapters.databricks.sql import (
    column_tags_query,
    describe_json_query,
    foreign_keys_query,
    primary_key_query,
    referencing_foreign_keys_query,
    table_tags_query,
)
from delta_engine.domain.model import QualifiedName


def build_describe_document(qualified_name: QualifiedName, **overrides: object) -> str:
    """Render a minimal valid ``DESCRIBE ... AS JSON`` document with overrides applied."""
    document: dict[str, object] = {
        "table_name": str(qualified_name.name),
        "catalog_name": str(qualified_name.catalog),
        "schema_name": str(qualified_name.schema),
        "type": "MANAGED",
        "provider": "delta",
        "columns": [{"name": "id", "type": {"name": "int"}, "nullable": False}],
        "comment": "",
        "table_properties": {},
    }
    document.update(overrides)
    return json.dumps(document)


def build_catalog_responses(
    qualified_name: QualifiedName,
    describe: str | None = None,
    **overrides,
):
    """
    Map every catalog read query to its response.

    Defaults describe a present one-column table with no tags or constraints;
    ``overrides`` replace individual query responses, keyed by query text.
    """
    if describe is None:
        describe = build_describe_document(qualified_name)
    responses = {
        describe_json_query(qualified_name): [(describe,)],
        table_tags_query(qualified_name): [],
        column_tags_query(qualified_name): [],
        primary_key_query(qualified_name): [],
        foreign_keys_query(qualified_name): [],
        referencing_foreign_keys_query(qualified_name): [],
    }
    responses.update(overrides)
    return responses


class RoutedCursor:
    """Answer ``execute`` by exact query text, failing the test on an unexpected query."""

    def __init__(self, responses) -> None:
        self._responses = responses
        self.queries: list[str] = []
        self.closed = False

    def execute(self, query: str) -> None:
        self.queries.append(query)
        if query not in self._responses:
            pytest.fail(f"unexpected SQL query: {query}", pytrace=False)
        value = self._responses[query]
        if isinstance(value, Exception):
            raise value
        self._current = value

    def fetchall(self):
        return list(self._current)

    def close(self) -> None:
        self.closed = True


class RoutedConnection:
    """Warehouse connection stand-in serving one routed cursor."""

    def __init__(self, responses) -> None:
        self.cursor_fake = RoutedCursor(responses)

    def cursor(self) -> RoutedCursor:
        return self.cursor_fake


class ClosedConnection:
    """Connection whose cursor acquisition fails, like a closed session."""

    def cursor(self) -> RoutedCursor:
        raise RuntimeError("cannot create cursor from closed connection")
