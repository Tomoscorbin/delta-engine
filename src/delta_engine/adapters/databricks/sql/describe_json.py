"""
Parse structured table metadata from ``DESCRIBE TABLE EXTENDED … AS JSON``.

The SQL warehouse reader uses this document for the facts it represents
structurally and officially: relation kind, provider, columns, table comment,
and partition columns. Properties and clustering still come from
``DESCRIBE DETAIL``; keys and tags still come from ``information_schema``.
Keeping those sources separate avoids depending on the JSON document's
undocumented formatted constraint string or runtime-specific extra fields.
"""

from dataclasses import dataclass
import json
from typing import Final

from delta_engine.adapters.databricks.sql.rows import column_from_catalog
from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    Byte,
    DataType,
    Date,
    Decimal,
    Double,
    Float,
    Integer,
    Long,
    Map,
    ObservedColumn,
    QualifiedName,
    Short,
    String,
    Struct,
    StructField,
    Timestamp,
    TimestampNtz,
    Variant,
)

_SIMPLE_TYPES: Final[dict[str, DataType]] = {
    "int": Integer(),
    "integer": Integer(),
    "bigint": Long(),
    "long": Long(),
    "smallint": Short(),
    "short": Short(),
    "tinyint": Byte(),
    "byte": Byte(),
    "float": Float(),
    "real": Float(),
    "double": Double(),
    "boolean": Boolean(),
    "string": String(),
    "date": Date(),
    "timestamp": Timestamp(),
    "timestamp_ltz": Timestamp(),
    "timestamp_ntz": TimestampNtz(),
    "binary": Binary(),
    "variant": Variant(),
}
_DEFAULT_DECIMAL_PRECISION: Final = 10
_DEFAULT_DECIMAL_SCALE: Final = 0
_SUPPORTED_RELATION_TYPES: Final = frozenset(
    {
        "MANAGED",
        "EXTERNAL",
        "MANAGED_SHALLOW_CLONE",
        "EXTERNAL_SHALLOW_CLONE",
    }
)


class MetadataParseError(Exception):
    """The table description is incomplete, malformed, or unsupported."""


@dataclass(frozen=True, slots=True)
class _DescribeJsonMetadata:
    """Private carrier for the supported facts in one description document."""

    columns: tuple[ObservedColumn, ...]
    comment: str
    partitioned_by: tuple[str, ...]


def _data_type_from_json(type_object: object) -> DataType | None:
    """Map a structured Databricks type object to a domain type, or ``None``."""
    try:
        return _data_type_from_json_unchecked(type_object)
    except (ValueError, RecursionError):
        return None


def parse_described_table(json_text: str, qualified_name: QualifiedName) -> _DescribeJsonMetadata:
    """Parse and validate the supported facts in one AS JSON document."""
    try:
        document = json.loads(json_text)
    except (TypeError, ValueError) as error:
        raise MetadataParseError(
            f"{qualified_name}: DESCRIBE TABLE AS JSON returned invalid JSON"
        ) from error
    if not isinstance(document, dict):
        raise MetadataParseError(f"{qualified_name}: expected a JSON object")

    _require_supported_delta_table(document, qualified_name)
    partitioned_by = _casefolded_string_list(
        document.get("partition_columns"),
        field_name="partition_columns",
        qualified_name=qualified_name,
    )
    comment = document.get("comment", "")
    if comment is None:
        comment = ""
    if not isinstance(comment, str):
        raise MetadataParseError(f"{qualified_name}: comment is not a string")

    return _DescribeJsonMetadata(
        columns=_columns_from_json(document, qualified_name, set(partitioned_by)),
        comment=comment,
        partitioned_by=partitioned_by,
    )


def _require_supported_delta_table(document: dict, qualified_name: QualifiedName) -> None:
    relation_type = document.get("type")
    if not isinstance(relation_type, str):
        raise MetadataParseError(f"{qualified_name}: AS JSON has no relation type")
    if relation_type.upper() not in _SUPPORTED_RELATION_TYPES:
        raise MetadataParseError(
            f"{qualified_name}: relation type {relation_type!r} is not a supported table"
        )

    provider = document.get("provider")
    if not isinstance(provider, str):
        raise MetadataParseError(f"{qualified_name}: AS JSON has no provider")
    if provider.casefold() != "delta":
        raise MetadataParseError(f"{qualified_name}: provider {provider!r} is not Delta")


def _columns_from_json(
    document: dict, qualified_name: QualifiedName, partition_names: set[str]
) -> tuple[ObservedColumn, ...]:
    entries = document.get("columns")
    if not isinstance(entries, list) or not entries:
        raise MetadataParseError(f"{qualified_name}: AS JSON has no columns array")

    columns: list[ObservedColumn] = []
    described_names: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise MetadataParseError(f"{qualified_name}: malformed column entry {entry!r}")
        raw_name = entry.get("name")
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise MetadataParseError(f"{qualified_name}: malformed column name {raw_name!r}")
        name = raw_name.casefold()
        if name in described_names:
            raise MetadataParseError(
                f"{qualified_name}: duplicate column name after casefolding: {raw_name!r}"
            )
        described_names.add(name)

        nullable = entry.get("nullable")
        if not isinstance(nullable, bool):
            raise MetadataParseError(
                f"{qualified_name}: column {raw_name!r} has non-boolean nullability"
            )
        comment = entry.get("comment", "")
        if comment is None:
            comment = ""
        if not isinstance(comment, str):
            raise MetadataParseError(
                f"{qualified_name}: column {raw_name!r} has a non-string comment"
            )

        reported_type = entry.get("type")
        try:
            column = column_from_catalog(
                name=raw_name,
                data_type=_data_type_from_json(reported_type),
                reported_type=reported_type,
                nullable=nullable,
                comment=comment,
                is_partition=name in partition_names,
                qualified_name=qualified_name,
            )
        except RuntimeError as error:
            raise MetadataParseError(str(error)) from error
        if column is not None:
            columns.append(column)

    missing_partition_columns = partition_names - described_names
    if missing_partition_columns:
        raise MetadataParseError(
            f"{qualified_name}: partition_columns names columns not present in the schema:"
            f" {sorted(missing_partition_columns)!r}"
        )
    if not columns:
        raise MetadataParseError(f"{qualified_name}: no mappable columns")
    return tuple(columns)


def _casefolded_string_list(
    value: object, *, field_name: str, qualified_name: QualifiedName
) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise MetadataParseError(f"{qualified_name}: {field_name} is not an array")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise MetadataParseError(
                f"{qualified_name}: {field_name} contains an invalid name {item!r}"
            )
        normalized.append(item.casefold())
    if len(set(normalized)) != len(normalized):
        raise MetadataParseError(
            f"{qualified_name}: {field_name} contains duplicate names after casefolding"
        )
    return tuple(normalized)


def _data_type_from_json_unchecked(type_object: object) -> DataType | None:
    if not isinstance(type_object, dict):
        return None
    raw_name = type_object.get("name")
    if not isinstance(raw_name, str):
        return None
    name = raw_name.casefold()

    if name in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[name]
    if name in ("char", "varchar", "character"):
        return String()
    if name in ("decimal", "dec", "numeric"):
        return _decimal_from_json(type_object)
    if name == "array":
        element = _data_type_from_json(type_object.get("element_type"))
        return Array(element) if element is not None else None
    if name == "map":
        key = _data_type_from_json(type_object.get("key_type"))
        value = _data_type_from_json(type_object.get("value_type"))
        return Map(key, value) if key is not None and value is not None else None
    if name == "struct":
        return _struct_from_json(type_object)
    return None


def _decimal_from_json(type_object: dict) -> DataType | None:
    precision = type_object.get("precision", _DEFAULT_DECIMAL_PRECISION)
    scale = type_object.get("scale", _DEFAULT_DECIMAL_SCALE)
    if type(precision) is not int or type(scale) is not int:
        return None
    try:
        return Decimal(precision, scale)
    except ValueError:
        return None


def _struct_from_json(type_object: dict) -> DataType | None:
    entries = type_object.get("fields")
    if not isinstance(entries, list):
        return None
    fields: list[StructField] = []
    for entry in entries:
        if not isinstance(entry, dict):
            return None
        raw_name = entry.get("name")
        data_type = _data_type_from_json(entry.get("type"))
        if not isinstance(raw_name, str) or data_type is None:
            return None
        fields.append(StructField(raw_name.casefold(), data_type))
    try:
        return Struct(tuple(fields))
    except ValueError:
        return None
