"""
Unity Catalog / Delta type mapping (MVP).

Single source of truth for:
- domain -> UC SQL (sql_type_for_data_type / sql_type_for_column)
- UC information_schema -> domain (domain_type_from_uc)
- Spark catalog string -> domain (domain_type_from_spark)

Policy:
- Readers are tolerant: unknown/complex observed types are passed through
  (validator/compiler will block using them for CREATE/ADD in the MVP).
- Compiler is strict: only MVP scalar types are accepted (fail fast).
"""

from __future__ import annotations

import re
from typing import Final, Optional, Any

from tabula.domain.model.data_type import DataType

# ---------- UC SQL (engine) mapping for scalar MVP ----------
_UC_SQL: Final[dict[str, str]] = {
    "string": "STRING",
    "varchar": "STRING",     # UC normalizes VARCHAR to STRING; length is informational only
    "boolean": "BOOLEAN",

    "tinyint": "TINYINT",
    "smallint": "SMALLINT",
    "integer": "INT",
    "bigint": "BIGINT",

    "float": "FLOAT",        # 32-bit
    "double": "DOUBLE",      # 64-bit

    "binary": "BINARY",

    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamp_ntz": "TIMESTAMP_NTZ",  # UC/DBR support
    "variant": "VARIANT",              # UC JSON-like semi-structured
}

_DECIMAL_RE_UC: Final[re.Pattern[str]] = re.compile(r"^DECIMAL\((\d+)\s*,\s*(\d+)\)$", re.IGNORECASE)
_DECIMAL_RE_SPARK: Final[re.Pattern[str]] = re.compile(r"^decimal\((\d+)\s*,\s*(\d+)\)$", re.IGNORECASE)

# ---------- Compiler-facing helpers ----------

def sql_type_for_column(column: Any) -> str:
    """
    Optional convenience: prefer a precomputed engine type on the column, else map domain data_type.
    Expects:
      - column.sql_type: Optional[str]      # e.g. "STRING", "DECIMAL(10,2)"
      - column.data_type: DataType
    """
    pre_mapped: Optional[str] = getattr(column, "sql_type", None)
    if pre_mapped:
        return pre_mapped
    data_type: DataType = getattr(column, "data_type", None)
    if data_type is None:
        raise ValueError("Column has neither 'sql_type' nor 'data_type'")
    return sql_type_for_data_type(data_type)


def sql_type_for_data_type(data_type: DataType) -> str:
    """
    Map a domain DataType to a UC SQL type (MVP).
    Raises ValueError for unsupported types to fail fast in the compiler.
    """
    name = data_type.name  # domain should normalize to lowercase

    # DECIMAL(p,s)
    if name in {"decimal", "numeric"}:
        if not data_type.parameters or len(data_type.parameters) < 1:
            raise ValueError("DECIMAL requires precision (and optional scale)")
        precision = int(data_type.parameters[0])
        scale = int(data_type.parameters[1]) if len(data_type.parameters) > 1 else 0
        return f"DECIMAL({precision},{scale})"

    # VARCHAR(n) -> STRING (length informational only)
    if name == "varchar":
        return _UC_SQL["varchar"]

    try:
        return _UC_SQL[name]
    except KeyError as exc:
        raise ValueError(f"Unsupported data type for UC compiler: {data_type.specification}") from exc

# ---------- Readers (tolerant) ----------

def domain_type_from_uc(data_type_text: str) -> DataType:
    """
    Map UC information_schema 'data_type' to a domain DataType.
    Tolerant for non-MVP types so we can observe them; validator/compiler will restrict usage.
    """
    text = data_type_text.strip().upper()

    # Common scalars
    if text in {"STRING", "VARCHAR"}:
        return DataType("string")
    if text == "BOOLEAN":
        return DataType("boolean")
    if text in {"TINYINT"}:
        return DataType("tinyint")
    if text == "SMALLINT":
        return DataType("smallint")
    if text in {"INT", "INTEGER"}:
        return DataType("integer")
    if text == "BIGINT":
        return DataType("bigint")
    if text in {"FLOAT", "REAL"}:
        return DataType("float")
    if text in {"DOUBLE", "DOUBLE PRECISION"}:
        return DataType("double")
    if text == "BINARY":
        return DataType("binary")
    if text == "DATE":
        return DataType("date")
    if text == "TIMESTAMP":
        return DataType("timestamp")
    if text.startswith("TIMESTAMP_NTZ"):
        return DataType("timestamp_ntz")
    if text == "VARIANT":
        return DataType("variant")

    # DECIMAL(p,s)
    decimal_match = _DECIMAL_RE_UC.match(text)
    if decimal_match:
        precision, scale = int(decimal_match.group(1)), int(decimal_match.group(2))
        return DataType("decimal", (precision, scale))

    # Observed-only passthrough (ARRAY, MAP, STRUCT, INTERVAL, GEOGRAPHY, etc.)
    return DataType(text.lower())


def domain_type_from_spark(data_type_text: str) -> DataType:
    """
    Map Spark catalog 'dataType' strings to a domain DataType.
    Examples: 'int', 'string', 'double', 'decimal(10,2)', 'array<int>', ...
    """
    text = data_type_text.strip().lower()

    # Common scalars
    if text == "string":
        return DataType("string")
    if text == "boolean":
        return DataType("boolean")
    if text in {"byte", "tinyint"}:
        return DataType("tinyint")
    if text == "smallint":
        return DataType("smallint")
    if text in {"int", "integer"}:
        return DataType("integer")
    if text == "bigint":
        return DataType("bigint")
    if text in {"float", "real"}:
        return DataType("float")
    if text.startswith("double"):
        return DataType("double")
    if text == "binary":
        return DataType("binary")
    if text == "date":
        return DataType("date")
    if text.startswith("timestamp_ntz"):
        return DataType("timestamp_ntz")
    if text.startswith("timestamp"):
        return DataType("timestamp")

    # DECIMAL(p,s)
    decimal_match = _DECIMAL_RE_SPARK.match(text)
    if decimal_match:
        precision, scale = int(decimal_match.group(1)), int(decimal_match.group(2))
        return DataType("decimal", (precision, scale))

    # Observed-only passthrough (array<...>, map<...>, struct<...>, etc.)
    return DataType(text)
