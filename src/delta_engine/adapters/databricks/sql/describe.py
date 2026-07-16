"""
Parse a ``DESCRIBE TABLE EXTENDED <table> AS JSON`` document into a table description.

Columns carry type objects (mapped by ``types.data_type_from_json``); comment,
partitioning, clustering, and properties are plain JSON. Key constraints and
tags are not read from this document — they come from information_schema as
structured rows (see ``queries`` and ``rows``), so the one embedded formatted
``table_constraints`` string this document also carries is left unread.
"""

from collections.abc import Mapping
from dataclasses import dataclass
import json
import logging
from types import MappingProxyType

from delta_engine.adapters.databricks.sql.types import data_type_from_json
from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY
from delta_engine.domain.model import ObservedColumn, QualifiedName

logger = logging.getLogger(__name__)


class MetadataParseError(Exception):
    """A DESCRIBE … AS JSON document is missing required structure."""


@dataclass(frozen=True, slots=True)
class TableDescription:
    """Backend-neutral columns and layout parsed from one AS JSON document."""

    qualified_name: QualifiedName
    columns: tuple[ObservedColumn, ...]
    comment: str
    partitioned_by: tuple[str, ...]
    clustered_by: tuple[str, ...]
    properties: Mapping[str, str]


def parse_table_description(json_text: str, qualified_name: QualifiedName) -> TableDescription:
    """Parse one AS JSON document into a ``TableDescription``."""
    try:
        document = json.loads(json_text)
    except (ValueError, TypeError) as error:
        raise MetadataParseError(
            f"{qualified_name}: DESCRIBE AS JSON was not valid JSON"
        ) from error
    if not isinstance(document, dict):
        raise MetadataParseError(f"{qualified_name}: expected a JSON object")

    return TableDescription(
        qualified_name=qualified_name,
        columns=_columns_from_json(document, qualified_name),
        comment=document.get("comment") or "",
        partitioned_by=_casefolded_list(document.get("partition_columns")),
        clustered_by=_casefolded_list(document.get("clustering_columns")),
        properties=_managed_properties(document.get("table_properties"), qualified_name),
    )


def _columns_from_json(document: dict, qualified_name: QualifiedName) -> tuple[ObservedColumn, ...]:
    columns_json = document.get("columns")
    if not isinstance(columns_json, list) or not columns_json:
        raise MetadataParseError(f"{qualified_name}: AS JSON has no columns array")

    columns: list[ObservedColumn] = []
    for entry in columns_json:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            raise MetadataParseError(f"{qualified_name}: malformed column entry {entry!r}")
        name = entry["name"].casefold()
        data_type = data_type_from_json(entry.get("type"))
        if data_type is None:
            # A column the domain cannot type is skipped rather than failed: it
            # cannot be declared in a desired table either, so its absence here
            # produces no drift. If partitioning, clustering, or a key names it,
            # ObservedTable rejects the resulting inconsistency and the read fails
            # there — one owner for that invariant, applied to every such column.
            logger.warning(
                "Skipping column %r in %s: unrecognised type %r",
                name,
                qualified_name,
                entry.get("type"),
            )
            continue
        columns.append(
            ObservedColumn(
                name=name,
                data_type=data_type,
                nullable=bool(entry.get("nullable", True)),
                comment=entry.get("comment") or "",
            )
        )
    if not columns:
        raise MetadataParseError(f"{qualified_name}: no mappable columns")
    return tuple(columns)


def _managed_properties(
    table_properties: object, qualified_name: QualifiedName
) -> Mapping[str, str]:
    if table_properties is None:
        return MappingProxyType({})
    if not isinstance(table_properties, dict):
        raise MetadataParseError(f"{qualified_name}: table_properties is not an object")
    return MappingProxyType(
        {name: value for name, value in table_properties.items() if name in DELTA_PROPERTY_REGISTRY}
    )


def _casefolded_list(value: object) -> tuple[str, ...]:
    if not value:
        return ()
    if not isinstance(value, list):
        return ()
    return tuple(str(item).casefold() for item in value)
