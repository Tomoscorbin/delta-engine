"""
Parse a ``DESCRIBE TABLE EXTENDED <table> AS JSON`` document into a table snapshot.

Everything but one field arrives structured: columns carry type objects (mapped
by ``types.data_type_from_json``), and comment, partitioning, clustering, and
properties are plain JSON. The one embedded formatted string —
``table_constraints`` — is parsed by ``constraints.py`` and documented there as
less structurally stable.
"""

from collections.abc import Mapping
from dataclasses import dataclass
import json
import logging
from types import MappingProxyType

from delta_engine.adapters.databricks.sql.constraints import (
    ConstraintParseError,
    parse_table_constraints,
)
from delta_engine.adapters.databricks.sql.types import data_type_from_json
from delta_engine.application.properties import DELTA_PROPERTY_REGISTRY
from delta_engine.domain.model import (
    ForeignKeyConstraint,
    ObservedColumn,
    PrimaryKeyConstraint,
    QualifiedName,
)

logger = logging.getLogger(__name__)


class MetadataParseError(Exception):
    """A DESCRIBE … AS JSON document is missing required structure."""


@dataclass(frozen=True, slots=True)
class TableSnapshot:
    """Backend-neutral table-local state parsed from one AS JSON document."""

    qualified_name: QualifiedName
    columns: tuple[ObservedColumn, ...]
    comment: str
    partitioned_by: tuple[str, ...]
    clustered_by: tuple[str, ...]
    properties: Mapping[str, str]
    primary_key: PrimaryKeyConstraint | None
    foreign_keys: tuple[ForeignKeyConstraint, ...]


def parse_table_snapshot(json_text: str, qualified_name: QualifiedName) -> TableSnapshot:
    """Parse one AS JSON document into a ``TableSnapshot``."""
    try:
        document = json.loads(json_text)
    except (ValueError, TypeError) as error:
        raise MetadataParseError(
            f"{qualified_name}: DESCRIBE AS JSON was not valid JSON"
        ) from error
    if not isinstance(document, dict):
        raise MetadataParseError(f"{qualified_name}: expected a JSON object")

    partitioned_by = _casefolded_list(document.get("partition_columns"))
    try:
        constraints = parse_table_constraints(document.get("table_constraints"))
    except ConstraintParseError as error:
        raise MetadataParseError(f"{qualified_name}: malformed table_constraints") from error
    return TableSnapshot(
        qualified_name=qualified_name,
        columns=_columns_from_json(document, qualified_name, set(partitioned_by)),
        comment=document.get("comment") or "",
        partitioned_by=partitioned_by,
        clustered_by=_casefolded_list(document.get("clustering_columns")),
        properties=_managed_properties(document.get("table_properties"), qualified_name),
        primary_key=constraints.primary_key,
        foreign_keys=constraints.foreign_keys,
    )


def _columns_from_json(
    document: dict, qualified_name: QualifiedName, partition_names: set[str]
) -> tuple[ObservedColumn, ...]:
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
            if name in partition_names:
                raise MetadataParseError(
                    f"Partition column {name!r} in {qualified_name} has an unmappable"
                    f" type {entry.get('type')!r}; observed partitioning cannot be"
                    " determined, so the table cannot be read safely."
                )
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
