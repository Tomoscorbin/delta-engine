from __future__ import annotations

import re
from typing import Any, Final

from tabula.domain.model import DataType

# --- UC engine tokens for the domain types we support ---
_UC_SQL: Final[dict[str, str]] = {
    "string": "STRING",
    "boolean": "BOOLEAN",
    "tinyint": "TINYINT",
    "smallint": "SMALLINT",
    "integer": "INT",
    "bigint": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "binary": "BINARY",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamp_ntz": "TIMESTAMP_NTZ",
    "variant": "VARIANT",
}

_DECIMAL_RE_UC: Final[re.Pattern[str]] = re.compile(
    r"^DECIMAL\((\d+)\s*,\s*(\d+)\)$", re.IGNORECASE
)
_DECIMAL_RE_SPARK: Final[re.Pattern[str]] = re.compile(
    r"^decimal\((\d+)\s*,\s*(\d+)\)$", re.IGNORECASE
)

# ---------------- Compiler-facing ----------------


def sql_type_for_column(column: Any) -> str:
    """
    Prefer a precomputed engine type on the column, else map its domain data_type.
    Expects:
      - column.sql_type: Optional[str]      # e.g., "STRING", "DECIMAL(10,2)"
      - column.data_type: DataType
    """
    pre = getattr(column, "sql_type", None)
    if pre:
        return pre
    dt = getattr(column, "data_type", None)
    if dt is None:
        raise ValueError("Column has neither 'sql_type' nor 'data_type'")
    return sql_type_for_data_type(dt)


def _render_spec(dt: DataType) -> str:
    """Best-effort 'name(params)' for readable error messages."""
    if not dt.parameters:
        return dt.name
    parts: list[str] = []
    for p in dt.parameters:
        parts.append(str(p) if isinstance(p, int) else _render_spec(p))
    return f"{dt.name}({','.join(parts)})"


def sql_type_for_data_type(data_type: DataType) -> str:
    """
    Map a domain DataType to a UC SQL type (STRICT).
    Only MVP scalar types + DECIMAL + VARCHAR are accepted.
    """
    name = data_type.name

    # DECIMAL(p[,s])
    if name in {"decimal", "numeric"}:
        if not data_type.parameters:
            raise ValueError("DECIMAL requires precision (and optional scale)")
        precision = int(data_type.parameters[0])
        scale = int(data_type.parameters[1]) if len(data_type.parameters) > 1 else 0
        return f"DECIMAL({precision},{scale})"

    # VARCHAR(n) -> STRING (length informational only)
    if name == "varchar":
        return "STRING"

    try:
        return _UC_SQL[name]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported data type for UC compiler: {_render_spec(data_type)}"
        ) from exc


# ---------------- Readers (tolerant) ----------------


def domain_type_from_uc(data_type_text: str) -> DataType:
    """
    Map UC information_schema 'data_type' to a domain DataType (TOLERANT).
    Keeps complex/unknown types observable (compiler/validator will restrict usage).
    """
    text = data_type_text.strip().upper()

    # DECIMAL(p,s)
    if m := _DECIMAL_RE_UC.match(text):
        precision, scale = int(m.group(1)), int(m.group(2))
        return DataType("decimal", (precision, scale))

    match text:
        case "STRING" | "VARCHAR":
            return DataType("string")
        case "BOOLEAN":
            return DataType("boolean")
        case "TINYINT":
            return DataType("tinyint")
        case "SMALLINT":
            return DataType("smallint")
        case "INT" | "INTEGER":
            return DataType("integer")
        case "BIGINT":
            return DataType("bigint")
        case "FLOAT" | "REAL":
            return DataType("float")
        case "DOUBLE" | "DOUBLE PRECISION":
            return DataType("double")
        case "BINARY":
            return DataType("binary")
        case "DATE":
            return DataType("date")
        case s if s.startswith("TIMESTAMP_NTZ"):
            return DataType("timestamp_ntz")
        case "TIMESTAMP":
            return DataType("timestamp")
        case "VARIANT":
            return DataType("variant")
        case _:
            # Observed-only passthrough (ARRAY, MAP, STRUCT, INTERVAL, GEOGRAPHY, etc.)
            return DataType(text.lower())


def domain_type_from_spark(data_type_text: str) -> DataType:
    """
    Map Spark catalog 'dataType' strings to a domain DataType (TOLERANT).
    Examples: 'int', 'string', 'double', 'decimal(10,2)', 'array<int>', ...
    """
    text = data_type_text.strip().lower()

    # DECIMAL(p,s)
    if m := _DECIMAL_RE_SPARK.match(text):
        precision, scale = int(m.group(1)), int(m.group(2))
        return DataType("decimal", (precision, scale))

    match text:
        case "string":
            return DataType("string")
        case "boolean":
            return DataType("boolean")
        case "byte" | "tinyint":
            return DataType("tinyint")
        case "smallint":
            return DataType("smallint")
        case "int" | "integer":
            return DataType("integer")
        case "bigint":
            return DataType("bigint")
        case "float" | "real":
            return DataType("float")
        case s if s.startswith("double"):
            return DataType("double")
        case "binary":
            return DataType("binary")
        case "date":
            return DataType("date")
        case s if s.startswith("timestamp_ntz"):
            return DataType("timestamp_ntz")
        case s if s.startswith("timestamp"):
            return DataType("timestamp")
        case _:
            # Observed-only passthrough (array<...>, map<...>, struct<...>, etc.)
            return DataType(text)
