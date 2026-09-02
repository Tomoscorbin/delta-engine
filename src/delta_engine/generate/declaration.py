"""Generate an importable declaration module from one observed table."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import json
import keyword
import re

from delta_engine.api.delta_table import DeltaTable, ForeignKey, Self
from delta_engine.application.scopes import ScopeName
from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    Byte,
    DataType,
    Date,
    Decimal,
    DesiredColumn,
    Double,
    Float,
    ForeignKeyConstraint,
    Integer,
    Long,
    Map,
    ObservedColumn,
    ObservedTable,
    QualifiedName,
    Short,
    String,
    Struct,
    StructField,
    TableKind,
    Timestamp,
    TimestampNtz,
    Variant,
)


class GenerationError(Exception):
    """The observed table cannot be expressed as a ``DeltaTable`` declaration."""


@dataclass(frozen=True, slots=True)
class GeneratedModule:
    """
    Python source for one declaration module, with its generation warnings.

    ``variable_name`` is the identifier the module binds its ``DeltaTable`` to.
    """

    source: str
    variable_name: str
    warnings: tuple[str, ...]


def generate_module(observed: ObservedTable) -> GeneratedModule:
    """
    Render ``observed`` as an importable declaration module.

    The module binds one ``DeltaTable`` ready for ``delta-engine plan``.

    Args:
        observed: The observed catalog state of one table.

    Returns:
        The module source and any warnings about state that was not rendered.

    Raises:
        GenerationError: The observed state cannot be expressed as a
            ``DeltaTable`` declaration.

    """
    columns = tuple(_raise_column(column) for column in observed.columns)
    primary_key = observed.primary_key
    scope: ScopeName = "annotations" if observed.kind is TableKind.STREAMING_TABLE else "full"
    # A restricted scope never compares properties, so a streaming table's
    # pipeline-owned property values stay out of the module entirely.
    properties = dict(observed.properties) if scope == "full" else {}
    try:
        foreign_keys = tuple(
            _raise_foreign_key(constraint, observed.qualified_name)
            for constraint in observed.foreign_keys
        )
        DeltaTable(
            catalog=observed.qualified_name.catalog,
            schema=observed.qualified_name.schema,
            name=observed.qualified_name.name,
            columns=columns,
            comment=observed.comment,
            properties=properties,
            tags=dict(observed.tags),
            partitioned_by=tuple(str(name) for name in observed.partitioned_by),
            clustered_by=tuple(str(name) for name in observed.clustered_by),
            primary_key=(
                tuple(str(name) for name in primary_key.columns)
                if primary_key is not None
                else None
            ),
            primary_key_name=str(primary_key.name) if primary_key is not None else None,
            foreign_keys=foreign_keys,
            scope=scope,
        )
    except ValueError as error:
        raise GenerationError(
            f"cannot generate a declaration for {observed.qualified_name}: {error}"
        ) from error

    used_names: set[str] = {"Column", "DeltaTable"}
    column_lines = [f"        {_render_column(column, used_names)}," for column in columns]

    argument_lines = [
        f"    catalog={_string_literal(observed.qualified_name.catalog)},",
        f"    schema={_string_literal(observed.qualified_name.schema)},",
        f"    name={_string_literal(observed.qualified_name.name)},",
        "    columns=[",
        *column_lines,
        "    ],",
    ]
    if observed.comment:
        argument_lines.append(f"    comment={_string_literal(observed.comment)},")
    if properties:
        argument_lines.append(f"    properties={_render_string_mapping(properties)},")
    if observed.tags:
        argument_lines.append(f"    tags={_render_string_mapping(observed.tags)},")
    if observed.partitioned_by:
        argument_lines.append(f"    partitioned_by={_render_name_list(observed.partitioned_by)},")
    if observed.clustered_by:
        argument_lines.append(f"    clustered_by={_render_name_list(observed.clustered_by)},")
    if primary_key is not None:
        argument_lines.append(f"    primary_key={_render_name_list(primary_key.columns)},")
        argument_lines.append(f"    primary_key_name={_string_literal(str(primary_key.name))},")
    if observed.foreign_keys:
        argument_lines.append("    foreign_keys=[")
        argument_lines.extend(
            f"        {_render_foreign_key(constraint, observed.qualified_name, used_names)},"
            for constraint in observed.foreign_keys
        )
        argument_lines.append("    ],")
    if scope != "full":
        argument_lines.append(f"    scope={_string_literal(scope)},")

    variable = _variable_name_for(observed.qualified_name.name)
    lines = [
        f"# Generated by: delta-engine generate {observed.qualified_name}",
        f"from delta_engine.schema import {', '.join(sorted(used_names))}",
        "",
        f"{variable} = DeltaTable(",
        *argument_lines,
        ")",
    ]
    warnings: tuple[str, ...] = ()
    if observed.kind is TableKind.STREAMING_TABLE:
        warnings = (
            'streaming table: generated with scope="annotations"; structure,'
            " properties, and keys belong to the owning pipeline",
        )
    return GeneratedModule(
        source="\n".join(lines) + "\n",
        variable_name=variable,
        warnings=warnings,
    )


def _raise_foreign_key(constraint: ForeignKeyConstraint, owner: QualifiedName) -> ForeignKey:
    """Raise one observed foreign key into its declaration counterpart."""
    references = Self if constraint.referenced_table == owner else str(constraint.referenced_table)
    return ForeignKey(
        columns=_foreign_key_columns(constraint),
        references=references,
        name=str(constraint.name),
    )


def _render_foreign_key(
    constraint: ForeignKeyConstraint,
    owner: QualifiedName,
    used_names: set[str],
) -> str:
    """Render one observed foreign key as ``ForeignKey(...)`` source, recording used names."""
    used_names.add("ForeignKey")
    if constraint.referenced_table == owner:
        used_names.add("Self")
        references = "Self"
    else:
        references = _string_literal(str(constraint.referenced_table))
    return (
        f"ForeignKey(columns={_render_string_mapping(_foreign_key_columns(constraint))},"
        f" references={references}, name={_string_literal(str(constraint.name))})"
    )


def _foreign_key_columns(constraint: ForeignKeyConstraint) -> dict[str, str]:
    """Pair each local column with the referenced column it points at."""
    return {
        str(local): str(referenced)
        for local, referenced in zip(
            constraint.local_columns, constraint.referenced_columns, strict=True
        )
    }


def _variable_name_for(table_name: str) -> str:
    """Turn a table name into the Python identifier the module binds."""
    identifier = re.sub(r"\W", "_", table_name)
    if identifier[0].isdigit():
        identifier = f"_{identifier}"
    if keyword.iskeyword(identifier):
        identifier = f"{identifier}_"
    return identifier


def _raise_column(observed: ObservedColumn) -> DesiredColumn:
    """Raise one observed column into its declaration counterpart."""
    return DesiredColumn(
        name=str(observed.name),
        data_type=observed.data_type,
        nullable=observed.nullable,
        comment=observed.comment,
        tags=dict(observed.tags),
    )


def _render_column(column: DesiredColumn, used_names: set[str]) -> str:
    """Render one column as ``Column(...)`` source, recording used names."""
    arguments = [
        _string_literal(str(column.name)),
        _render_data_type(column.data_type, used_names),
    ]
    if not column.nullable:
        arguments.append("nullable=False")
    if column.comment:
        arguments.append(f"comment={_string_literal(column.comment)}")
    if column.tags:
        arguments.append(f"tags={_render_string_mapping(column.tags)}")
    return f"Column({', '.join(arguments)})"


def _render_string_mapping(mapping: Mapping[str, str]) -> str:
    """Render a mapping as a dict literal with keys in sorted order."""
    items = ", ".join(
        f"{_string_literal(key)}: {_string_literal(mapping[key])}" for key in sorted(mapping)
    )
    return "{" + items + "}"


def _render_name_list(names: Iterable[str]) -> str:
    """Render column names as a list literal, preserving their order."""
    return "[" + ", ".join(_string_literal(str(name)) for name in names) + "]"


def _render_data_type(data_type: DataType, used_names: set[str]) -> str:
    """Render one data type as constructor source, recording used names."""
    used_names.add(type(data_type).__name__)
    match data_type:
        case (
            Integer()
            | Long()
            | Float()
            | Double()
            | Boolean()
            | String()
            | Date()
            | Timestamp()
            | Byte()
            | Short()
            | Binary()
            | TimestampNtz()
            | Variant()
        ):
            return f"{type(data_type).__name__}()"
        case Decimal(precision=precision, scale=scale):
            return f"Decimal({precision}, {scale})"
        case Array(element=element):
            return f"Array({_render_data_type(element, used_names)})"
        case Map(key=key, value=value):
            rendered_key = _render_data_type(key, used_names)
            rendered_value = _render_data_type(value, used_names)
            return f"Map({rendered_key}, {rendered_value})"
        case Struct(fields=fields):
            used_names.add("StructField")
            rendered_fields = ", ".join(_render_struct_field(field, used_names) for field in fields)
            return f"Struct([{rendered_fields}])"
        case _:
            raise TypeError(f"Unrenderable data type: {data_type!r}")


def _render_struct_field(field: StructField, used_names: set[str]) -> str:
    """Render one struct field as ``StructField(...)`` source."""
    arguments = [
        _string_literal(str(field.name)),
        _render_data_type(field.data_type, used_names),
    ]
    if not field.nullable:
        arguments.append("nullable=False")
    return f"StructField({', '.join(arguments)})"


def _string_literal(value: str) -> str:
    """Render a double-quoted Python string literal."""
    return json.dumps(value, ensure_ascii=False)
