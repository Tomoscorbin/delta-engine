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
from types import MappingProxyType

from delta_engine.adapters.databricks.sql.types import data_type_from_json
from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY
from delta_engine.domain.model import ObservedColumn, QualifiedName


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
        partitioned_by=_casefolded_list(document, "partition_columns", qualified_name),
        clustered_by=_casefolded_list(document, "clustering_columns", qualified_name),
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
        type_obj = entry.get("type")
        if not isinstance(type_obj, dict) or not isinstance(type_obj.get("name"), str):
            raise MetadataParseError(
                f"{qualified_name}: column {name!r} has a malformed type object {type_obj!r}"
            )
        data_type = data_type_from_json(type_obj)
        if data_type is None:
            # A well-formed type object the domain does not model: an unknown or
            # future type name (interval, geography, ...), a nested type it cannot
            # represent, or a value the domain constructor rejects. Fail the read
            # rather than drop the column. This engine owns the full column set —
            # an observed column absent from the declaration is planned for DROP
            # COLUMN — so silently dropping one would read as "in sync" when it is
            # not. Surfaces as ReadFailed at the total read boundary.
            raise MetadataParseError(
                f"{qualified_name}: column {name!r} has an unsupported type {type_obj!r}"
            )
        nullable = entry.get("nullable", True)
        if not isinstance(nullable, bool):
            raise MetadataParseError(
                f"{qualified_name}: column {name!r} has a non-boolean nullable {nullable!r}"
            )
        columns.append(
            ObservedColumn(
                name=name,
                data_type=data_type,
                nullable=nullable,
                comment=entry.get("comment") or "",
            )
        )
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


def _casefolded_list(document: dict, key: str, qualified_name: QualifiedName) -> tuple[str, ...]:
    """
    Read a layout column list (partition or clustering columns), casefolded.

    Absent or null means no such layout. A present value of any other shape is
    drift, not "no layout", so it fails the read rather than silently reading as
    an empty layout.
    """
    value = document.get(key)
    if value is None:
        return ()
    if not isinstance(value, list):
        raise MetadataParseError(f"{qualified_name}: {key} is not a list, got {value!r}")
    return tuple(str(item).casefold() for item in value)
