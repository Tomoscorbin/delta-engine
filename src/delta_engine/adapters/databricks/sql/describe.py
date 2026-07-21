"""
Turn a ``DESCRIBE TABLE EXTENDED <table> AS JSON`` result into a table description.

Columns carry type objects (mapped by ``types.data_type_from_json``); comment,
partitioning, clustering, and properties are plain JSON. The relation kind,
storage format, and table properties are carried through verbatim as facts —
whether the engine reads that kind of relation, and which property keys it
manages, are the reader's decisions, not the parser's. Key constraints and
tags are not read from this
document — they come from information_schema as structured rows (see
``queries`` and ``rows``), so the one embedded formatted ``table_constraints``
string this document also carries is left unread.
"""

from collections.abc import Mapping
from dataclasses import dataclass
import json
from types import MappingProxyType

from delta_engine.adapters.databricks.sql.rows import Rows
from delta_engine.adapters.databricks.sql.types import data_type_from_json
from delta_engine.domain.model import ObservedColumn, QualifiedName


class MetadataParseError(Exception):
    """A DESCRIBE … AS JSON result is missing required structure."""


@dataclass(frozen=True, slots=True)
class TableDescription:
    """Backend-neutral relation facts, columns, and layout from one AS JSON document."""

    qualified_name: QualifiedName
    columns: tuple[ObservedColumn, ...]
    comment: str
    partitioned_by: tuple[str, ...]
    clustered_by: tuple[str, ...]
    table_properties: Mapping[str, str]
    relation_type: str | None
    provider: str | None


def table_description_from_rows(rows: Rows, qualified_name: QualifiedName) -> TableDescription:
    """
    Map a ``DESCRIBE … AS JSON`` result to a ``TableDescription``.

    The statement returns exactly one row with one column holding the JSON
    document; an empty result fails the read.
    """
    if not rows:
        raise MetadataParseError(f"{qualified_name}: DESCRIBE AS JSON returned no rows")
    return _parse_document(rows[0][0], qualified_name)


def _parse_document(json_text: str, qualified_name: QualifiedName) -> TableDescription:
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
        comment=_string_or_empty(document, "comment", qualified_name, "table comment"),
        partitioned_by=_string_list(document, "partition_columns", qualified_name),
        clustered_by=_string_list(document, "clustering_columns", qualified_name),
        table_properties=_table_properties(document.get("table_properties"), qualified_name),
        relation_type=_optional_string(document, "type"),
        provider=_optional_string(document, "provider"),
    )


def _optional_string(document: dict, key: str) -> str | None:
    """Read a string field, with any absent or non-string value as ``None``."""
    value = document.get(key)
    return value if isinstance(value, str) else None


def _string_or_empty(
    document: dict,
    key: str,
    qualified_name: QualifiedName,
    label: str,
) -> str:
    """Read an optional string, rejecting present values of another JSON type."""
    value = document.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise MetadataParseError(f"{qualified_name}: {label} is not a string, got {value!r}")
    return value


def _columns_from_json(document: dict, qualified_name: QualifiedName) -> tuple[ObservedColumn, ...]:
    columns_json = document.get("columns")
    if not isinstance(columns_json, list) or not columns_json:
        raise MetadataParseError(f"{qualified_name}: AS JSON has no columns array")

    columns: list[ObservedColumn] = []
    for entry in columns_json:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            raise MetadataParseError(f"{qualified_name}: malformed column entry {entry!r}")
        name = entry["name"]
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
            # not. Surfaces as ReadError at the reader boundary.
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
                comment=_string_or_empty(
                    entry,
                    "comment",
                    qualified_name,
                    f"column {name!r} comment",
                ),
            )
        )
    return tuple(columns)


def _table_properties(table_properties: object, qualified_name: QualifiedName) -> Mapping[str, str]:
    """Carry the document's table properties verbatim; absent means none."""
    if table_properties is None:
        return MappingProxyType({})
    if not isinstance(table_properties, dict):
        raise MetadataParseError(f"{qualified_name}: table_properties is not an object")
    if any(not isinstance(value, str) for value in table_properties.values()):
        raise MetadataParseError(f"{qualified_name}: table_properties values must be strings")
    return MappingProxyType(dict(table_properties))


def _string_list(document: dict, key: str, qualified_name: QualifiedName) -> tuple[str, ...]:
    """
    Read a layout column list (partition or clustering columns), verbatim.

    Absent or null means no such layout. A present value of any other shape is
    drift, not "no layout", so it fails the read rather than silently reading as
    an empty layout. Values carry the catalog's spelling; the domain table
    canonicalizes them on construction.
    """
    value = document.get(key)
    if value is None:
        return ()
    if not isinstance(value, list):
        raise MetadataParseError(f"{qualified_name}: {key} is not a list, got {value!r}")
    if any(not isinstance(item, str) for item in value):
        raise MetadataParseError(f"{qualified_name}: {key} entries must be strings, got {value!r}")
    return tuple(value)
