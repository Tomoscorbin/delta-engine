# Databricks reader efficiency (AS JSON) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild both Databricks readers to construct an `ObservedTable` from one `DESCRIBE TABLE EXTENDED … AS JSON` call plus three information_schema queries, behind one shared PySpark-free core.

**Architecture:** Thin backend shells (execute SQL, extract result, classify not-found) over a shared pipeline: `parse_table_snapshot(json) -> TableSnapshot`, then `observed_table_from_snapshot(snapshot, run_info_schema_query) -> ObservedTable`. The constraint sub-string is parsed by an isolated module. The read side mirrors the write side's `execution.execute_statements(run, statements)`.

**Tech Stack:** Python 3.12, `uv`, pytest, PySpark 4.1 (dev/e2e only), databricks-sql-connector (runtime-optional), ruff, mypy, import-linter.

**Design spec:** `docs/todo/2026-07-15-databricks-reader-efficiency-design.md`. Real fixtures: `docs/todo/fixtures-describe-json-2026-07-15.md`.

## Global Constraints

- `domain` and `application` stay backend-free; `adapters` may import `application` and `domain` (matches existing `sql/rows.py`). New parser/assembly modules must stay **PySpark-free** (no `pyspark`/`delta` imports) so the warehouse backend can import them. `lint-imports` must stay green.
- Absolute imports only. Type hints on every signature. No bare `except`; broad `except Exception` only at the two `fetch_state` boundaries and at the one narrow condition-checked re-raise (missing-relation classification). Do not swallow errors.
- Identifiers (column, field, constraint, referenced-table names) are **casefolded** at the adapter boundary; tag keys/values are preserved verbatim.
- Observed properties are projected through `DELTA_PROPERTY_POLICY` (from `delta_engine.application.properties`).
- Runtime floor (document only, never preflight): `table_constraints` requires DBR 17.3+ or a SQL warehouse; base AS JSON requires DBR 16.2+. Both readers are Unity-Catalog-only.
- Conventional commits. No `Co-authored-by`. Commit after each task. Never commit to `main` (work stays on `claude/databricks-reader-efficiency-75bf90`).
- Validate per task with the narrowest useful `uv run pytest …`, then before finishing: `uv run pytest`, `uv run ruff check src tests`, `uv run ruff format src tests`, `uv run mypy src`, `uv run lint-imports`.
- A `PostToolUse` ruff hook strips not-yet-used imports after each edit; add an import in the same step as the code that uses it.

---

### Task 1: Structured JSON type mapper

Maps an AS JSON `type` object (e.g. `{"name": "decimal", "precision": 10, "scale": 2}`) to a domain `DataType`, or `None` when unmappable. Replaces the DDL-string `parse_data_type` on the read path.

**Files:**

- Create: `src/delta_engine/adapters/databricks/sql/describe_json.py`
- Test: `tests/adapters/databricks/sql/test_describe_json.py`

**Interfaces:**

- Consumes: domain types from `delta_engine.domain.model` (`Integer`, `Long`, `Short`, `Byte`, `Float`, `Double`, `Boolean`, `String`, `Date`, `Timestamp`, `TimestampNtz`, `Binary`, `Variant`, `Decimal`, `Array`, `Map`, `Struct`, `StructField`, `DataType`).
- Produces: `data_type_from_json(type_obj: object) -> DataType | None`.

- [ ] **Step 1: Write the failing test**

```python
# tests/adapters/databricks/sql/test_describe_json.py
from delta_engine.adapters.databricks.sql.describe_json import data_type_from_json
from delta_engine.domain.model import (
    Array, Boolean, Decimal, Double, Integer, Long, Map, String, Struct, StructField,
    Timestamp, TimestampNtz,
)


def test_primitive_aliases():
    assert data_type_from_json({"name": "int"}) == Integer()
    assert data_type_from_json({"name": "integer"}) == Integer()
    assert data_type_from_json({"name": "bigint"}) == Long()
    assert data_type_from_json({"name": "double"}) == Double()
    assert data_type_from_json({"name": "boolean"}) == Boolean()


def test_string_ignores_collation_and_length():
    assert data_type_from_json({"name": "string", "collation": "UTF8_BINARY"}) == String()
    assert data_type_from_json({"name": "varchar", "length": 20}) == String()


def test_timestamp_ltz_aliases_to_timestamp():
    assert data_type_from_json({"name": "timestamp"}) == Timestamp()
    assert data_type_from_json({"name": "timestamp_ltz"}) == Timestamp()
    assert data_type_from_json({"name": "timestamp_ntz"}) == TimestampNtz()


def test_decimal_reads_precision_and_scale():
    assert data_type_from_json({"name": "decimal", "precision": 10, "scale": 2}) == Decimal(10, 2)


def test_array_map_struct_nested():
    assert data_type_from_json(
        {"name": "array", "element_type": {"name": "string"}, "element_nullable": True}
    ) == Array(String())
    assert data_type_from_json(
        {"name": "map", "key_type": {"name": "string"}, "value_type": {"name": "int"}}
    ) == Map(String(), Integer())
    assert data_type_from_json(
        {"name": "struct", "fields": [
            {"name": "Age", "type": {"name": "int"}, "nullable": True},
            {"name": "label", "type": {"name": "string"}, "nullable": True},
        ]}
    ) == Struct((StructField("age", Integer()), StructField("label", String())))


def test_unmappable_returns_none():
    assert data_type_from_json({"name": "interval"}) is None
    assert data_type_from_json({"name": "struct", "fields": [
        {"name": "a", "type": {"name": "int"}}, {"name": "A", "type": {"name": "int"}},
    ]}) is None  # duplicate field name after casefold
    assert data_type_from_json({"not": "a type"}) is None
    assert data_type_from_json("string") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/databricks/sql/test_describe_json.py -q`
Expected: FAIL — `ModuleNotFoundError`/`ImportError` (module/function not defined).

- [ ] **Step 3: Write the minimal implementation**

```python
# src/delta_engine/adapters/databricks/sql/describe_json.py
"""
Parse a Databricks ``DESCRIBE TABLE EXTENDED <table> AS JSON`` document into a
backend-neutral table snapshot.

Column types arrive as structured objects keyed by ``name`` (never DDL
strings), so this is the structured twin of the write path's type rendering.
The one embedded formatted string — ``table_constraints`` — is parsed by
``constraints.py`` and is documented there as less structurally stable.
"""

from typing import Final

from delta_engine.domain.model import (
    Array, Binary, Boolean, Byte, DataType, Date, Decimal, Double, Float, Integer,
    Long, Map, Short, String, Struct, StructField, Timestamp, TimestampNtz, Variant,
)

_SIMPLE_TYPES: Final[dict[str, DataType]] = {
    "int": Integer(), "integer": Integer(),
    "bigint": Long(), "long": Long(),
    "smallint": Short(), "short": Short(),
    "tinyint": Byte(), "byte": Byte(),
    "float": Float(), "real": Float(),
    "double": Double(),
    "boolean": Boolean(),
    "string": String(),
    "date": Date(),
    "timestamp": Timestamp(), "timestamp_ltz": Timestamp(),
    "timestamp_ntz": TimestampNtz(),
    "binary": Binary(),
    "variant": Variant(),
}

_DEFAULT_DECIMAL_PRECISION: Final = 10
_DEFAULT_DECIMAL_SCALE: Final = 0


def data_type_from_json(type_obj: object) -> DataType | None:
    """
    Map an AS JSON type object to a domain ``DataType``, or ``None``.

    ``None`` covers a type the domain does not model (interval, void, geo,
    future types) and malformed input; both get the caller's skip-and-warn
    policy. Domain constructor rejections (decimal over the Delta limit,
    struct fields colliding after casefold) also yield ``None``.
    """
    if not isinstance(type_obj, dict):
        return None
    name = type_obj.get("name")
    if not isinstance(name, str):
        return None
    name = name.casefold()

    if name in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[name]
    if name in ("char", "varchar", "character"):
        return String()  # length bound not modeled (matches the write path)
    if name in ("decimal", "dec", "numeric"):
        return _decimal_from_json(type_obj)
    if name == "array":
        element = data_type_from_json(type_obj.get("element_type"))
        return Array(element) if element is not None else None
    if name == "map":
        key = data_type_from_json(type_obj.get("key_type"))
        value = data_type_from_json(type_obj.get("value_type"))
        if key is None or value is None:
            return None
        return Map(key, value)
    if name == "struct":
        return _struct_from_json(type_obj)
    return None


def _decimal_from_json(type_obj: dict) -> DataType | None:
    precision = type_obj.get("precision", _DEFAULT_DECIMAL_PRECISION)
    scale = type_obj.get("scale", _DEFAULT_DECIMAL_SCALE)
    try:
        return Decimal(int(precision), int(scale))
    except (TypeError, ValueError):
        return None


def _struct_from_json(type_obj: dict) -> DataType | None:
    fields_json = type_obj.get("fields")
    if not isinstance(fields_json, list):
        return None
    fields: list[StructField] = []
    for field in fields_json:
        if not isinstance(field, dict):
            return None
        field_name = field.get("name")
        field_type = data_type_from_json(field.get("type"))
        if not isinstance(field_name, str) or field_type is None:
            return None
        fields.append(StructField(name=field_name.casefold(), data_type=field_type))
    try:
        return Struct(tuple(fields))
    except ValueError:
        return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/databricks/sql/test_describe_json.py -q`
Expected: PASS (7 passed).

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/adapters/databricks/sql/describe_json.py tests/adapters/databricks/sql/test_describe_json.py
git commit -m "feat(reader): map structured AS JSON types to domain types"
```

---

### Task 2: Constraint-string parser

Parses the embedded `table_constraints` string (`[(name,PRIMARY KEY (\`c\`)),(name,FOREIGN KEY (\`c\`) REFERENCES \`cat\`.\`sch\`.\`t\` (\`r\`))]`) into structured values. Isolated behind a narrow interface; documented as the least stable field.

**Files:**

- Create: `src/delta_engine/adapters/databricks/sql/constraints.py`
- Test: `tests/adapters/databricks/sql/test_constraints.py`

**Interfaces:**

- Produces: `parse_table_constraints(value: str | None) -> ParsedConstraints`, with dataclasses `ParsedConstraints(primary_key: ParsedPrimaryKey | None, foreign_keys: tuple[ParsedForeignKey, ...])`, `ParsedPrimaryKey(constraint_name: str, columns: tuple[str, ...])`, `ParsedForeignKey(constraint_name: str, local_columns: tuple[str, ...], referenced_table: tuple[str, str, str], referenced_columns: tuple[str, ...])`, and exception `ConstraintParseError`.
- All identifiers returned casefolded.

- [ ] **Step 1: Write the failing test**

```python
# tests/adapters/databricks/sql/test_constraints.py
import pytest

from delta_engine.adapters.databricks.sql.constraints import (
    ConstraintParseError, ParsedForeignKey, ParsedPrimaryKey, parse_table_constraints,
)


def test_none_and_empty_mean_no_constraints():
    assert parse_table_constraints(None).primary_key is None
    assert parse_table_constraints(None).foreign_keys == ()
    assert parse_table_constraints("").foreign_keys == ()
    assert parse_table_constraints("[]").primary_key is None


def test_single_column_primary_key():
    parsed = parse_table_constraints("[(pk_dev_silver_demo_table__id,PRIMARY KEY (`id`))]")
    assert parsed.primary_key == ParsedPrimaryKey("pk_dev_silver_demo_table__id", ("id",))
    assert parsed.foreign_keys == ()


def test_composite_primary_key_preserves_order():
    parsed = parse_table_constraints("[(pk_t,PRIMARY KEY (`a`, `b`, `c`))]")
    assert parsed.primary_key == ParsedPrimaryKey("pk_t", ("a", "b", "c"))


def test_primary_key_and_foreign_key_from_real_output():
    value = (
        "[(pk_dev_gold_order_fact,PRIMARY KEY (`order_id`)), "
        "(fk_dev_gold_order_fact_product_id_to_product_dimension_product_id,"
        "FOREIGN KEY (`product_id`) REFERENCES `dev`.`gold`.`product_dimension` (`product_id`))]"
    )
    parsed = parse_table_constraints(value)
    assert parsed.primary_key == ParsedPrimaryKey("pk_dev_gold_order_fact", ("order_id",))
    assert parsed.foreign_keys == (
        ParsedForeignKey(
            constraint_name="fk_dev_gold_order_fact_product_id_to_product_dimension_product_id",
            local_columns=("product_id",),
            referenced_table=("dev", "gold", "product_dimension"),
            referenced_columns=("product_id",),
        ),
    )


def test_composite_foreign_key_pairs_positionally():
    value = (
        "[(fk_x,FOREIGN KEY (`a`, `b`) REFERENCES `c`.`s`.`t` (`x`, `y`))]"
    )
    [fk] = parse_table_constraints(value).foreign_keys
    assert fk.local_columns == ("a", "b")
    assert fk.referenced_columns == ("x", "y")


def test_identifiers_are_casefolded():
    parsed = parse_table_constraints("[(PK_T,PRIMARY KEY (`ID`))]")
    assert parsed.primary_key == ParsedPrimaryKey("pk_t", ("id",))


def test_doubled_backtick_is_a_literal_backtick():
    parsed = parse_table_constraints("[(pk,PRIMARY KEY (`we``ird`))]")
    assert parsed.primary_key.columns == ("we`ird",)


def test_malformed_raises():
    with pytest.raises(ConstraintParseError):
        parse_table_constraints("not a bracketed list")
    with pytest.raises(ConstraintParseError):
        parse_table_constraints("[(only_a_name)]")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/databricks/sql/test_constraints.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the implementation**

```python
# src/delta_engine/adapters/databricks/sql/constraints.py
"""
Parse the ``table_constraints`` field of ``DESCRIBE … AS JSON``.

Unlike the rest of the JSON, constraints arrive as one embedded formatted
string — a bracketed list of ``(constraint_name, BODY)`` pairs where BODY is
DDL-like text (``PRIMARY KEY (`c`)`` / ``FOREIGN KEY (`c`) REFERENCES
`cat`.`sch`.`tbl` (`r`)``). This field is officially undocumented and less
structurally stable than the structured keys, so it is parsed here in
isolation. Identifiers are backtick-quoted with a doubled backtick escaping a
literal backtick; the referenced table is always a 3-part backticked name.

Assumption: a constraint name does not contain an unbackticked top-level comma
or the literal keyword boundary — true for the catalog-generated names this
reads. Names are returned casefolded, matching the domain's lowercase identity.
"""

from dataclasses import dataclass, field
from typing import Final


class ConstraintParseError(Exception):
    """The table_constraints string is not in the expected format."""


@dataclass(frozen=True, slots=True)
class ParsedPrimaryKey:
    constraint_name: str
    columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ParsedForeignKey:
    constraint_name: str
    local_columns: tuple[str, ...]
    referenced_table: tuple[str, str, str]
    referenced_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ParsedConstraints:
    primary_key: ParsedPrimaryKey | None = None
    foreign_keys: tuple[ParsedForeignKey, ...] = field(default_factory=tuple)


_PRIMARY_KEY: Final = "PRIMARY KEY"
_FOREIGN_KEY: Final = "FOREIGN KEY"
_REFERENCES: Final = "REFERENCES"


def parse_table_constraints(value: str | None) -> ParsedConstraints:
    """Parse the ``table_constraints`` string into structured constraints."""
    if value is None:
        return ParsedConstraints()
    text = value.strip()
    if text in ("", "[]"):
        return ParsedConstraints()
    if not (text.startswith("[") and text.endswith("]")):
        raise ConstraintParseError(f"expected a bracketed list: {value!r}")

    primary_key: ParsedPrimaryKey | None = None
    foreign_keys: list[ParsedForeignKey] = []
    for element in _split_top_level_elements(text[1:-1]):
        name, body = _split_name_and_body(element)
        upper = body.upper()
        if upper.startswith(_PRIMARY_KEY):
            columns = _read_identifier_list(body[len(_PRIMARY_KEY):])
            primary_key = ParsedPrimaryKey(name.casefold(), columns)
        elif upper.startswith(_FOREIGN_KEY):
            foreign_keys.append(_parse_foreign_key(name.casefold(), body))
        else:
            raise ConstraintParseError(f"unknown constraint body: {body!r}")
    return ParsedConstraints(primary_key=primary_key, foreign_keys=tuple(foreign_keys))


def _split_top_level_elements(text: str) -> list[str]:
    """Split ``(…), (…)`` into balanced ``(…)`` spans, ignoring backticked content."""
    elements: list[str] = []
    depth = 0
    start: int | None = None
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char == "`":
            index = _skip_backtick(text, index)
            continue
        if char == "(":
            if depth == 0:
                start = index
            depth += 1
        elif char == ")":
            if depth == 0:
                raise ConstraintParseError("unbalanced parentheses in constraints")
            depth -= 1
            if depth == 0 and start is not None:
                elements.append(text[start : index + 1])
                start = None
        index += 1
    if depth != 0:
        raise ConstraintParseError("unbalanced parentheses in constraints")
    return elements


def _split_name_and_body(element: str) -> tuple[str, str]:
    inner = element.strip()
    if not (inner.startswith("(") and inner.endswith(")")):
        raise ConstraintParseError(f"expected a parenthesised element: {element!r}")
    content = inner[1:-1]
    comma = _find_top_level_comma(content)
    if comma is None:
        raise ConstraintParseError(f"element missing name/body separator: {element!r}")
    return content[:comma].strip(), content[comma + 1 :].strip()


def _parse_foreign_key(name: str, body: str) -> ParsedForeignKey:
    local_open = body.index("(")
    local_close = _matching_paren(body, local_open)
    local_columns = _read_identifiers(body[local_open + 1 : local_close])

    rest = body[local_close + 1 :]
    references_at = rest.upper().find(_REFERENCES)
    if references_at < 0:
        raise ConstraintParseError(f"foreign key missing REFERENCES: {body!r}")
    after = rest[references_at + len(_REFERENCES) :]

    ref_open = after.index("(")
    ref_close = _matching_paren(after, ref_open)
    referenced_table = _parse_referenced_table(after[:ref_open])
    referenced_columns = _read_identifiers(after[ref_open + 1 : ref_close])
    return ParsedForeignKey(name, local_columns, referenced_table, referenced_columns)


def _parse_referenced_table(text: str) -> tuple[str, str, str]:
    parts = _read_identifiers(text)
    if len(parts) != 3:
        raise ConstraintParseError(f"expected a 3-part referenced table: {text!r}")
    return (parts[0], parts[1], parts[2])


def _read_identifier_list(text: str) -> tuple[str, ...]:
    open_paren = text.index("(")
    close_paren = _matching_paren(text, open_paren)
    return _read_identifiers(text[open_paren + 1 : close_paren])


def _read_identifiers(text: str) -> tuple[str, ...]:
    """Read all backtick-quoted identifiers in ``text``, casefolded, in order."""
    identifiers: list[str] = []
    index = 0
    length = len(text)
    while index < length:
        if text[index] == "`":
            identifier, index = _read_backtick_identifier(text, index)
            identifiers.append(identifier.casefold())
        else:
            index += 1  # separators (commas, dots, spaces) between identifiers
    return tuple(identifiers)


def _read_backtick_identifier(text: str, index: int) -> tuple[str, int]:
    index += 1  # opening backtick
    parts: list[str] = []
    length = len(text)
    while index < length:
        char = text[index]
        if char == "`":
            if index + 1 < length and text[index + 1] == "`":
                parts.append("`")
                index += 2
                continue
            return "".join(parts), index + 1
        parts.append(char)
        index += 1
    raise ConstraintParseError("unterminated backtick identifier")


def _find_top_level_comma(text: str) -> int | None:
    depth = 0
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char == "`":
            index = _skip_backtick(text, index)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            return index
        index += 1
    return None


def _matching_paren(text: str, open_index: int) -> int:
    depth = 0
    index = open_index
    length = len(text)
    while index < length:
        char = text[index]
        if char == "`":
            index = _skip_backtick(text, index)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise ConstraintParseError("unbalanced parentheses in constraint body")


def _skip_backtick(text: str, index: int) -> int:
    """Return the index just past a backtick-quoted span starting at ``index``."""
    index += 1
    length = len(text)
    while index < length:
        if text[index] == "`":
            if index + 1 < length and text[index + 1] == "`":
                index += 2
                continue
            return index + 1
        index += 1
    raise ConstraintParseError("unterminated backtick identifier")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/databricks/sql/test_constraints.py -q`
Expected: PASS (8 passed).

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/adapters/databricks/sql/constraints.py tests/adapters/databricks/sql/test_constraints.py
git commit -m "feat(reader): parse the table_constraints string into structured keys"
```

---

### Task 3: `TableSnapshot` + `parse_table_snapshot`

Turns a whole AS JSON document into a backend-neutral `TableSnapshot`. Owns the column skip/raise policy, property-policy projection, comment/partition/clustering extraction, and delegation to Tasks 1–2.

**Files:**

- Modify: `src/delta_engine/adapters/databricks/sql/describe_json.py`
- Test: `tests/adapters/databricks/sql/test_describe_json.py` (extend)

**Interfaces:**

- Consumes: `data_type_from_json` (Task 1); `parse_table_constraints`, `ParsedConstraints` (Task 2); `DELTA_PROPERTY_POLICY` (`delta_engine.application.properties`); domain `ObservedColumn`, `PrimaryKeyConstraint`, `ForeignKeyConstraint`, `QualifiedName`.
- Produces: frozen `TableSnapshot(qualified_name, columns: tuple[ObservedColumn, ...], comment: str, partitioned_by: tuple[str, ...], clustered_by: tuple[str, ...], properties: Mapping[str, str], primary_key: PrimaryKeyConstraint | None, foreign_keys: tuple[ForeignKeyConstraint, ...])`; `parse_table_snapshot(json_text: str, qualified_name: QualifiedName) -> TableSnapshot`; exception `MetadataParseError`.

- [ ] **Step 1: Write the failing test**

```python
# tests/adapters/databricks/sql/test_describe_json.py  (append)
import json

import pytest

from delta_engine.adapters.databricks.sql.describe_json import (
    MetadataParseError, parse_table_snapshot,
)
from delta_engine.domain.model import Integer, QualifiedName, String

QN = QualifiedName("dev", "silver", "demo_table")


def _doc(**overrides):
    base = {
        "table_name": "demo_table", "catalog_name": "dev", "schema_name": "silver",
        "columns": [
            {"name": "id", "type": {"name": "int"}, "nullable": False, "comment": "pk"},
            {"name": "name", "type": {"name": "string", "collation": "UTF8_BINARY"},
             "nullable": True},
        ],
        "comment": "",
    }
    base.update(overrides)
    return json.dumps(base)


def test_columns_types_nullability_comments_and_order():
    snap = parse_table_snapshot(_doc(), QN)
    assert [c.name for c in snap.columns] == ["id", "name"]
    assert snap.columns[0].data_type == Integer()
    assert snap.columns[0].nullable is False
    assert snap.columns[0].comment == "pk"
    assert snap.columns[1].data_type == String()
    assert snap.columns[1].comment == ""  # omitted -> empty


def test_empty_table_comment_is_empty_string():
    assert parse_table_snapshot(_doc(comment=""), QN).comment == ""
    doc = json.loads(_doc()); doc.pop("comment")
    assert parse_table_snapshot(json.dumps(doc), QN).comment == ""


def test_partitioning_and_clustering_casefolded_in_order():
    snap = parse_table_snapshot(
        _doc(partition_columns=["Region", "Store"], clustering_columns=["ID"],
             columns=[
                 {"name": "id", "type": {"name": "int"}, "nullable": True},
                 {"name": "region", "type": {"name": "string"}, "nullable": True},
                 {"name": "store", "type": {"name": "string"}, "nullable": True},
             ]),
        QN,
    )
    assert snap.partitioned_by == ("region", "store")
    assert snap.clustered_by == ("id",)


def test_observed_properties_are_projected_through_policy():
    snap = parse_table_snapshot(
        _doc(table_properties={
            "delta.columnMapping.mode": "name",
            "delta.feature.clustering": "supported",
            "delta.minReaderVersion": "3",
        }),
        QN,
    )
    assert dict(snap.properties) == {"delta.columnMapping.mode": "name"}


def test_constraints_lowered_to_domain():
    snap = parse_table_snapshot(
        _doc(table_constraints="[(pk_demo,PRIMARY KEY (`id`))]"), QN
    )
    assert snap.primary_key is not None
    assert snap.primary_key.columns == ("id",)
    assert snap.primary_key.constraint_name == "pk_demo"


def test_unmappable_non_partition_column_is_skipped():
    snap = parse_table_snapshot(
        _doc(columns=[
            {"name": "ok", "type": {"name": "int"}, "nullable": True},
            {"name": "weird", "type": {"name": "geography"}, "nullable": True},
        ]),
        QN,
    )
    assert [c.name for c in snap.columns] == ["ok"]


def test_unmappable_partition_column_raises():
    with pytest.raises(MetadataParseError):
        parse_table_snapshot(
            _doc(partition_columns=["p"],
                 columns=[{"name": "p", "type": {"name": "geography"}, "nullable": True}]),
            QN,
        )


def test_malformed_json_and_missing_columns_raise():
    with pytest.raises(MetadataParseError):
        parse_table_snapshot("{not json", QN)
    with pytest.raises(MetadataParseError):
        parse_table_snapshot('{"comment": ""}', QN)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/databricks/sql/test_describe_json.py -q`
Expected: FAIL — `parse_table_snapshot`/`MetadataParseError` not defined.

- [ ] **Step 3: Write the implementation**

Append to `src/delta_engine/adapters/databricks/sql/describe_json.py`:

```python
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from delta_engine.adapters.databricks.sql.constraints import (
    ParsedConstraints, parse_table_constraints,
)
from delta_engine.application.properties import DELTA_PROPERTY_POLICY
from delta_engine.domain.model import (
    ForeignKeyConstraint, ObservedColumn, PrimaryKeyConstraint, QualifiedName,
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
        raise MetadataParseError(f"{qualified_name}: DESCRIBE AS JSON was not valid JSON") from error
    if not isinstance(document, dict):
        raise MetadataParseError(f"{qualified_name}: expected a JSON object")

    partitioned_by = _casefolded_list(document.get("partition_columns"))
    constraints = _lower_constraints(parse_table_constraints(document.get("table_constraints")))
    return TableSnapshot(
        qualified_name=qualified_name,
        columns=_columns_from_json(document, qualified_name, set(partitioned_by)),
        comment=document.get("comment") or "",
        partitioned_by=partitioned_by,
        clustered_by=_casefolded_list(document.get("clustering_columns")),
        properties=_project_observed_properties(
            document.get("table_properties"), qualified_name
        ),
        primary_key=constraints[0],
        foreign_keys=constraints[1],
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
                name, qualified_name, entry.get("type"),
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


def _project_observed_properties(
    table_properties: object, qualified_name: QualifiedName
) -> Mapping[str, str]:
    if table_properties is None:
        return MappingProxyType({})
    if not isinstance(table_properties, dict):
        raise MetadataParseError(f"{qualified_name}: table_properties is not an object")
    return DELTA_PROPERTY_POLICY.project_observed(table_properties)


def _lower_constraints(
    parsed: ParsedConstraints,
) -> tuple[PrimaryKeyConstraint | None, tuple[ForeignKeyConstraint, ...]]:
    primary_key = None
    if parsed.primary_key is not None:
        primary_key = PrimaryKeyConstraint(
            columns=parsed.primary_key.columns,
            constraint_name=parsed.primary_key.constraint_name,
        )
    foreign_keys = tuple(
        ForeignKeyConstraint(
            local_columns=fk.local_columns,
            referenced_table=QualifiedName(*fk.referenced_table),
            referenced_columns=fk.referenced_columns,
            constraint_name=fk.constraint_name,
        )
        for fk in parsed.foreign_keys
    )
    return primary_key, foreign_keys


def _casefolded_list(value: object) -> tuple[str, ...]:
    if not value:
        return ()
    if not isinstance(value, list):
        return ()
    return tuple(str(item).casefold() for item in value)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/databricks/sql/test_describe_json.py -q`
Expected: PASS (all Task 1 + Task 3 tests).

- [ ] **Step 5: Add a real-fixture regression test**

Create `tests/adapters/databricks/sql/fixtures/order_fact.json` — a strict-JSON copy of the `order_fact` sample in `docs/todo/fixtures-describe-json-2026-07-15.md` (convert single-quoted inner strings to double-quoted, drop trailing commas). Then append:

```python
# tests/adapters/databricks/sql/test_describe_json.py  (append)
from pathlib import Path

_FIXTURES = Path(__file__).parent / "fixtures"


def test_real_order_fact_fixture():
    text = (_FIXTURES / "order_fact.json").read_text()
    snap = parse_table_snapshot(text, QualifiedName("dev", "gold", "order_fact"))
    assert len(snap.columns) == 7
    assert snap.columns[0].name == "order_id"
    assert snap.columns[0].nullable is False
    assert snap.primary_key.columns == ("order_id",)
    [fk] = snap.foreign_keys
    assert fk.referenced_table == QualifiedName("dev", "gold", "product_dimension")
    assert dict(snap.properties) == {"delta.columnMapping.mode": "name"}
```

- [ ] **Step 6: Run and commit**

Run: `uv run pytest tests/adapters/databricks/sql/test_describe_json.py -q`
Expected: PASS.

```bash
git add src/delta_engine/adapters/databricks/sql/describe_json.py tests/adapters/databricks/sql/test_describe_json.py tests/adapters/databricks/sql/fixtures/order_fact.json
git commit -m "feat(reader): parse an AS JSON document into a table snapshot"
```

---

### Task 4: Shared assembly `observed_table_from_snapshot`

Attaches tags and inbound FKs (via information_schema) to a `TableSnapshot`, producing the domain `ObservedTable`. The read-side twin of `execute_statements`.

**Files:**

- Create: `src/delta_engine/adapters/databricks/read.py`
- Test: `tests/adapters/databricks/test_read.py`

**Interfaces:**

- Consumes: `TableSnapshot` (Task 3); existing `sql` exports `column_tags_query`, `table_tags_query`, `referencing_foreign_keys_query`, `column_tags_from_rows`, `table_tags_from_rows`, `referencing_foreign_keys_from_rows`.
- Produces: `observed_table_from_snapshot(snapshot: TableSnapshot, *, run_info_schema_query: Callable[[str], Sequence[Any]]) -> ObservedTable`.

- [ ] **Step 1: Write the failing test**

```python
# tests/adapters/databricks/test_read.py
from types import SimpleNamespace

from delta_engine.adapters.databricks.read import observed_table_from_snapshot
from delta_engine.adapters.databricks.sql import (
    column_tags_query, referencing_foreign_keys_query, table_tags_query,
)
from delta_engine.adapters.databricks.sql.describe_json import TableSnapshot
from delta_engine.domain.model import Integer, ObservedColumn, QualifiedName

QN = QualifiedName("cat", "sch", "tbl")


def _snapshot(**overrides):
    base = dict(
        qualified_name=QN,
        columns=(ObservedColumn("id", Integer(), nullable=False),),
        comment="", partitioned_by=(), clustered_by=(),
        properties={}, primary_key=None, foreign_keys=(),
    )
    base.update(overrides)
    return TableSnapshot(**base)


def _router(responses):
    return lambda query: responses.get(query, [])


def test_tags_and_inbound_fks_attached():
    responses = {
        table_tags_query(QN): [SimpleNamespace(tag_name="Owner", tag_value="Data")],
        column_tags_query(QN): [
            SimpleNamespace(column_name="ID", tag_name="pii", tag_value="low"),
        ],
        referencing_foreign_keys_query(QN): [
            SimpleNamespace(constraint_name="child_fk", referencing_catalog="cat",
                            referencing_schema="sch", referencing_table="child"),
        ],
    }
    observed = observed_table_from_snapshot(_snapshot(), run_info_schema_query=_router(responses))

    assert dict(observed.tags) == {"Owner": "Data"}
    assert dict(observed.columns[0].tags) == {"pii": "low"}
    assert observed.referencing_foreign_keys[0].referencing_table == QualifiedName("cat", "sch", "child")


def test_snapshot_fields_pass_through():
    observed = observed_table_from_snapshot(
        _snapshot(comment="orders", clustered_by=("id",)),
        run_info_schema_query=_router({}),
    )
    assert observed.comment == "orders"
    assert observed.clustered_by == ("id",)
    assert dict(observed.columns[0].tags) == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/databricks/test_read.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the implementation**

```python
# src/delta_engine/adapters/databricks/read.py
"""
Shared observed-table assembly for the Databricks backends.

Both backends parse one AS JSON document into a ``TableSnapshot`` (table-local
state) and then attach the metadata that is not in the JSON — Unity Catalog
tags and inbound foreign keys — read through information_schema. Only how a
query is physically run differs per backend, so it is injected as a callable:
the read-side twin of the runner ``execution.execute_statements`` injects on
the write side. This module stays PySpark-free.
"""

from collections.abc import Callable, Sequence
from dataclasses import replace
from types import MappingProxyType
from typing import Any

from delta_engine.adapters.databricks.sql import (
    column_tags_from_rows, column_tags_query, referencing_foreign_keys_from_rows,
    referencing_foreign_keys_query, table_tags_from_rows, table_tags_query,
)
from delta_engine.adapters.databricks.sql.describe_json import TableSnapshot
from delta_engine.domain.model import ObservedTable


def observed_table_from_snapshot(
    snapshot: TableSnapshot,
    *,
    run_info_schema_query: Callable[[str], Sequence[Any]],
) -> ObservedTable:
    """Assemble the domain ``ObservedTable`` from a snapshot plus information_schema."""
    qualified_name = snapshot.qualified_name
    column_tags = column_tags_from_rows(run_info_schema_query(column_tags_query(qualified_name)))
    tagged_columns = tuple(
        replace(column, tags=column_tags.get(column.name, MappingProxyType({})))
        for column in snapshot.columns
    )
    return ObservedTable(
        qualified_name=qualified_name,
        columns=tagged_columns,
        comment=snapshot.comment,
        properties=snapshot.properties,
        tags=table_tags_from_rows(run_info_schema_query(table_tags_query(qualified_name))),
        partitioned_by=snapshot.partitioned_by,
        clustered_by=snapshot.clustered_by,
        primary_key=snapshot.primary_key,
        foreign_keys=snapshot.foreign_keys,
        referencing_foreign_keys=referencing_foreign_keys_from_rows(
            run_info_schema_query(referencing_foreign_keys_query(qualified_name))
        ),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/databricks/test_read.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/adapters/databricks/read.py tests/adapters/databricks/test_read.py
git commit -m "feat(reader): assemble ObservedTable from a snapshot plus info_schema"
```

---

### Task 5: `describe_json_query` builder + missing-relation classification

Adds the primary query builder and a shared way to recognise a "table not found" error across both backends.

**Files:**

- Modify: `src/delta_engine/adapters/databricks/sql/queries.py`
- Modify: `src/delta_engine/adapters/databricks/sql/__init__.py` (export `describe_json_query`)
- Modify: `src/delta_engine/adapters/databricks/errors.py`
- Test: `tests/adapters/databricks/sql/test_queries.py` (extend), `tests/adapters/databricks/test_errors.py` (extend or create)

**Interfaces:**

- Produces: `describe_json_query(qualified_name: QualifiedName) -> str`; `is_missing_relation(exception: BaseException) -> bool`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/adapters/databricks/sql/test_queries.py  (append)
from delta_engine.adapters.databricks.sql import describe_json_query
from delta_engine.domain.model import QualifiedName


def test_describe_json_query_is_extended_and_backticked():
    query = describe_json_query(QualifiedName("cat", "sch", "tbl"))
    assert query == "DESCRIBE TABLE EXTENDED `cat`.`sch`.`tbl` AS JSON"
```

```python
# tests/adapters/databricks/test_errors.py  (create or append)
from delta_engine.adapters.databricks.errors import is_missing_relation


class _Analysis(Exception):
    def __init__(self, condition): self._condition = condition
    def getCondition(self): return self._condition


def test_missing_relation_from_spark_condition():
    assert is_missing_relation(_Analysis("TABLE_OR_VIEW_NOT_FOUND")) is True
    assert is_missing_relation(_Analysis("INSUFFICIENT_PERMISSIONS")) is False


def test_missing_relation_from_warehouse_message_prefix():
    assert is_missing_relation(RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] Table … not found")) is True
    assert is_missing_relation(RuntimeError("connection reset")) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/adapters/databricks/sql/test_queries.py::test_describe_json_query_is_extended_and_backticked tests/adapters/databricks/test_errors.py -q`
Expected: FAIL — names not defined.

- [ ] **Step 3: Add `describe_json_query`**

In `src/delta_engine/adapters/databricks/sql/queries.py`, add:

```python
def describe_json_query(qualified_name: QualifiedName) -> str:
    """
    Render ``DESCRIBE TABLE EXTENDED <table> AS JSON``.

    The one primary read: it returns columns (structured types), the table
    comment, partition and clustering columns, table properties, and the
    ``table_constraints`` string in a single JSON document. ``AS JSON``
    requires ``EXTENDED``. Requires DBR 16.2+ (constraints: 17.3+ or a SQL
    warehouse); older runtimes surface as ``ReadFailed``.
    """
    return f"DESCRIBE TABLE EXTENDED {backtick_qualified_name(qualified_name)} AS JSON"
```

Add `describe_json_query` to `sql/__init__.py`'s import block and `__all__`.

- [ ] **Step 4: Add `is_missing_relation`**

In `src/delta_engine/adapters/databricks/errors.py`, add:

```python
import re
from typing import Final

_MISSING_RELATION_CONDITIONS: Final[frozenset[str]] = frozenset(
    {"TABLE_OR_VIEW_NOT_FOUND", "SCHEMA_NOT_FOUND", "CATALOG_NOT_FOUND"}
)
_CONDITION_PREFIX: Final = re.compile(r"\s*\[([A-Z0-9_.]+)\]")


def _error_condition(exception: BaseException) -> str | None:
    """The catalog error condition, from getCondition() (Spark) or the [COND] message prefix."""
    getter = getattr(exception, "getCondition", None)
    if callable(getter):
        try:
            condition = getter()
        except Exception:  # noqa: BLE001 - a duck-typed getter that misbehaves is not our condition
            condition = None
        if isinstance(condition, str):
            return condition
    match = _CONDITION_PREFIX.match(exception_message(exception))
    return match.group(1) if match else None


def is_missing_relation(exception: BaseException) -> bool:
    """Whether ``exception`` reports that the described table/schema/catalog does not exist."""
    return _error_condition(exception) in _MISSING_RELATION_CONDITIONS
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/adapters/databricks/sql/test_queries.py::test_describe_json_query_is_extended_and_backticked tests/adapters/databricks/test_errors.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/adapters/databricks/sql/queries.py src/delta_engine/adapters/databricks/sql/__init__.py src/delta_engine/adapters/databricks/errors.py tests/adapters/databricks/sql/test_queries.py tests/adapters/databricks/test_errors.py
git commit -m "feat(reader): add AS JSON query builder and missing-relation classifier"
```

---

### Task 6: Rewrite `WarehouseReader` onto the shared path

**Files:**

- Modify: `src/delta_engine/adapters/databricks/warehouse/reader.py`
- Test: `tests/adapters/databricks/warehouse/test_reader.py` (rewrite)

**Interfaces:**

- Consumes: `describe_json_query`, `is_missing_relation` (Task 5); `parse_table_snapshot` (Task 3); `observed_table_from_snapshot` (Task 4).
- Produces: `WarehouseReader(connection).fetch_state(qn) -> CatalogState` (unchanged public contract).

- [ ] **Step 1: Rewrite the test**

```python
# tests/adapters/databricks/warehouse/test_reader.py
import json
from types import SimpleNamespace

from delta_engine.adapters.databricks.sql import (
    column_tags_query, describe_json_query, referencing_foreign_keys_query, table_tags_query,
)
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader
from delta_engine.application.ports import ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import Integer, QualifiedName, String

QN = QualifiedName("cat", "sch", "tbl")

_DOC = json.dumps({
    "table_name": "tbl", "catalog_name": "cat", "schema_name": "sch",
    "columns": [
        {"name": "id", "type": {"name": "int"}, "nullable": False, "comment": "pk"},
        {"name": "name", "type": {"name": "string"}, "nullable": True},
    ],
    "comment": "orders",
    "clustering_columns": ["id"],
    "table_properties": {"delta.columnMapping.mode": "name", "delta.minReaderVersion": "3"},
    "table_constraints": "[(pk_tbl,PRIMARY KEY (`id`))]",
})


class RoutedCursor:
    def __init__(self, responses): self._responses = responses; self.queries = []
    def __enter__(self): return self
    def __exit__(self, *exc): return False
    def execute(self, query):
        self.queries.append(query)
        value = self._responses.get(query)
        if isinstance(value, Exception):
            raise value
        self._current = value if value is not None else []
    def fetchone(self): return self._current[0] if self._current else None
    def fetchall(self): return list(self._current)


class RoutedConnection:
    def __init__(self, responses): self.cursor_fake = RoutedCursor(responses)
    def cursor(self): return self.cursor_fake


def _responses(describe=_DOC, **overrides):
    responses = {
        describe_json_query(QN): [(describe,)] if describe is not None else describe,
        table_tags_query(QN): [], column_tags_query(QN): [], referencing_foreign_keys_query(QN): [],
    }
    responses.update(overrides)
    return responses


def test_present_table_reads_via_as_json():
    connection = RoutedConnection(_responses())
    state = WarehouseReader(connection).fetch_state(QN)
    assert isinstance(state, TablePresent)
    observed = state.table
    assert [c.name for c in observed.columns] == ["id", "name"]
    assert observed.columns[0].data_type == Integer()
    assert observed.columns[1].data_type == String()
    assert observed.comment == "orders"
    assert observed.clustered_by == ("id",)
    assert dict(observed.properties) == {"delta.columnMapping.mode": "name"}
    assert observed.primary_key.columns == ("id",)


def test_present_table_uses_four_queries():
    connection = RoutedConnection(_responses())
    WarehouseReader(connection).fetch_state(QN)
    assert len(connection.cursor_fake.queries) == 4
    assert connection.cursor_fake.queries[0] == describe_json_query(QN)


def test_missing_table_is_absent_and_stops_after_describe():
    responses = {describe_json_query(QN): RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] nope")}
    connection = RoutedConnection(responses)
    assert isinstance(WarehouseReader(connection).fetch_state(QN), TableAbsent)
    assert connection.cursor_fake.queries == [describe_json_query(QN)]


def test_other_backend_error_is_read_failed():
    responses = {describe_json_query(QN): RuntimeError("warehouse gone")}
    state = WarehouseReader(RoutedConnection(responses)).fetch_state(QN)
    assert isinstance(state, ReadFailed)
    assert "warehouse gone" in state.failure.message
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/databricks/warehouse/test_reader.py -q`
Expected: FAIL — reader still uses the old queries / shapes.

- [ ] **Step 3: Rewrite the reader**

```python
# src/delta_engine/adapters/databricks/warehouse/reader.py
"""
Reader adapter for Databricks SQL warehouses.

Unity Catalog only. One ``DESCRIBE TABLE EXTENDED … AS JSON`` yields the
table-local state; three information_schema queries add tags and inbound
foreign keys. The connector is never imported at runtime: the connection is
duck-typed (``.cursor()`` context manager with ``execute``/``fetchone``/
``fetchall``), so this backend imports nothing beyond the shared adapter core.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from delta_engine.adapters.databricks.errors import (
    exception_message, exception_type_name, is_missing_relation,
)
from delta_engine.adapters.databricks.read import observed_table_from_snapshot
from delta_engine.adapters.databricks.sql import describe_json_query
from delta_engine.adapters.databricks.sql.describe_json import parse_table_snapshot
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import CatalogState, ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import QualifiedName

if TYPE_CHECKING:
    from databricks.sql.client import Connection


class WarehouseReader:
    """Catalog state reader backed by a Databricks SQL warehouse connection."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """Return ``TablePresent``, ``TableAbsent``, or ``ReadFailed`` — the boundary is total."""
        try:
            return self._read(qualified_name)
        except Exception as exception:  # noqa: BLE001 - total port boundary
            return ReadFailed(
                failure=ReadFailure(exception_type_name(exception), exception_message(exception))
            )

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        with self._connection.cursor() as cursor:
            try:
                cursor.execute(describe_json_query(qualified_name))
            except Exception as exception:  # noqa: BLE001 - re-raised unless it means "absent"
                if is_missing_relation(exception):
                    return TableAbsent()
                raise
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"DESCRIBE AS JSON returned no row for {qualified_name}")
            snapshot = parse_table_snapshot(row[0], qualified_name)
            observed = observed_table_from_snapshot(
                snapshot, run_info_schema_query=lambda query: _fetch_all(cursor, query)
            )
        return TablePresent(table=observed)


def _fetch_all(cursor: Any, query: str) -> list[Any]:
    cursor.execute(query)
    return cursor.fetchall()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/databricks/warehouse/test_reader.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/adapters/databricks/warehouse/reader.py tests/adapters/databricks/warehouse/test_reader.py
git commit -m "refactor(warehouse): read via AS JSON through the shared assembly"
```

---

### Task 7: Rewrite `SparkReader` onto the shared path

**Files:**

- Modify: `src/delta_engine/adapters/databricks/spark/reader.py`
- Test: `tests/adapters/databricks/spark/test_reader.py` (rewrite)

**Interfaces:**

- Consumes: same shared core as Task 6. Executes via `spark.sql(...)`.
- Produces: `SparkReader(spark).fetch_state(qn) -> CatalogState` (unchanged contract).

- [ ] **Step 1: Rewrite the test**

```python
# tests/adapters/databricks/spark/test_reader.py
from __future__ import annotations

import json
from types import SimpleNamespace

from delta_engine.adapters.databricks.sql import (
    column_tags_query, describe_json_query, referencing_foreign_keys_query, table_tags_query,
)
from delta_engine.adapters.databricks.spark.reader import SparkReader
from delta_engine.application.ports import ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import Integer, QualifiedName

QN = QualifiedName("cat", "sch", "tbl")

_DOC = json.dumps({
    "table_name": "tbl", "catalog_name": "cat", "schema_name": "sch",
    "columns": [{"name": "id", "type": {"name": "int"}, "nullable": False}],
    "comment": "", "table_properties": {},
})


class FakeAnalysisException(Exception):
    def __init__(self, condition): super().__init__(condition); self._condition = condition
    def getCondition(self): return self._condition


class FakeDataFrame:
    def __init__(self, rows): self._rows = rows
    def first(self): return self._rows[0] if self._rows else None
    def collect(self): return list(self._rows)


class FakeSpark:
    """Routes spark.sql() by exact query text; the AS JSON result is one 1-col row."""
    def __init__(self, responses): self._responses = responses; self.queries = []
    def sql(self, query):
        self.queries.append(query)
        value = self._responses.get(query, [])
        if isinstance(value, Exception):
            raise value
        return FakeDataFrame(value)


def _responses(describe=_DOC, **overrides):
    responses = {
        describe_json_query(QN): [(describe,)],
        table_tags_query(QN): [], column_tags_query(QN): [], referencing_foreign_keys_query(QN): [],
    }
    responses.update(overrides)
    return responses


def test_present_table_reads_via_as_json():
    spark = FakeSpark(_responses())
    state = SparkReader(spark).fetch_state(QN)
    assert isinstance(state, TablePresent)
    assert state.table.columns[0].data_type == Integer()


def test_present_table_uses_four_queries():
    spark = FakeSpark(_responses())
    SparkReader(spark).fetch_state(QN)
    assert len(spark.queries) == 4
    assert spark.queries[0] == describe_json_query(QN)


def test_missing_table_is_absent():
    spark = FakeSpark({describe_json_query(QN): FakeAnalysisException("TABLE_OR_VIEW_NOT_FOUND")})
    assert isinstance(SparkReader(spark).fetch_state(QN), TableAbsent)
    assert spark.queries == [describe_json_query(QN)]


def test_other_error_is_read_failed():
    spark = FakeSpark({describe_json_query(QN): FakeAnalysisException("INSUFFICIENT_PERMISSIONS")})
    assert isinstance(SparkReader(spark).fetch_state(QN), ReadFailed)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/adapters/databricks/spark/test_reader.py -q`
Expected: FAIL.

- [ ] **Step 3: Rewrite the reader**

```python
# src/delta_engine/adapters/databricks/spark/reader.py
"""
Reader adapter for Databricks Unity Catalog over a SparkSession.

Unity Catalog only. One ``DESCRIBE TABLE EXTENDED … AS JSON`` yields the
table-local state; three information_schema queries add tags and inbound
foreign keys. Shares all parsing and assembly with the warehouse backend;
only statement execution and error classification are backend-specific.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from delta_engine.adapters.databricks.errors import (
    exception_message, exception_type_name, is_missing_relation,
)
from delta_engine.adapters.databricks.read import observed_table_from_snapshot
from delta_engine.adapters.databricks.sql import describe_json_query
from delta_engine.adapters.databricks.sql.describe_json import parse_table_snapshot
from delta_engine.application.failures import ReadFailure
from delta_engine.application.ports import CatalogState, ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import QualifiedName


class SparkReader:
    """Catalog state reader backed by a Databricks/Spark session."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        """Return ``TablePresent``, ``TableAbsent``, or ``ReadFailed`` — the boundary is total."""
        try:
            return self._read(qualified_name)
        except Exception as exception:  # noqa: BLE001 - total port boundary
            return ReadFailed(
                failure=ReadFailure(exception_type_name(exception), exception_message(exception))
            )

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        try:
            row = self.spark.sql(describe_json_query(qualified_name)).first()
        except Exception as exception:  # noqa: BLE001 - re-raised unless it means "absent"
            if is_missing_relation(exception):
                return TableAbsent()
            raise
        if row is None:
            raise RuntimeError(f"DESCRIBE AS JSON returned no row for {qualified_name}")
        snapshot = parse_table_snapshot(row[0], qualified_name)
        observed = observed_table_from_snapshot(
            snapshot, run_info_schema_query=lambda query: self.spark.sql(query).collect()
        )
        return TablePresent(table=observed)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/adapters/databricks/spark/test_reader.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/delta_engine/adapters/databricks/spark/reader.py tests/adapters/databricks/spark/test_reader.py
git commit -m "refactor(spark): read via AS JSON through the shared assembly"
```

---

### Task 8: Native test reader + rewire engine e2e

OSS Spark rejects `AS JSON` for Delta tables, so the local `local_e2e` tests inject a test-only reader that reads native `spark.table().schema` + `DESCRIBE DETAIL` and feeds the same shared assembly.

**Files:**

- Create: `tests/adapters/databricks/native_reader.py`
- Create: `tests/adapters/databricks/test_native_reader.py`
- Modify: `tests/e2e/test_engine_e2e.py` (swap `SparkReader(spark)` → `NativeSparkReader(spark)`)

**Interfaces:**

- Consumes: `TableSnapshot`, `observed_table_from_snapshot`, domain types.
- Produces: `NativeSparkReader(spark).fetch_state(qn) -> CatalogState` for tests only (not shipped).

- [ ] **Step 1: Write the reader and its type mapper**

```python
# tests/adapters/databricks/native_reader.py
"""
Test-only Databricks reader for OSS Spark + Delta (no ``AS JSON`` support).

Reads columns from the native ``StructType`` and layout/properties from
``DESCRIBE DETAIL``, then feeds the shipped ``observed_table_from_snapshot``.
Used by the local engine e2e tests to keep a real read->diff->plan->execute
round-trip credential-free; production reads use ``SparkReader`` (AS JSON).
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any

import pyspark.sql.types as T

from delta_engine.adapters.databricks.errors import exception_message, exception_type_name
from delta_engine.adapters.databricks.read import observed_table_from_snapshot
from delta_engine.adapters.databricks.sql.describe_json import TableSnapshot
from delta_engine.application.failures import ReadFailure
from delta_engine.application.properties import DELTA_PROPERTY_POLICY
from delta_engine.application.ports import CatalogState, ReadFailed, TableAbsent, TablePresent
from delta_engine.domain.model import (
    Array, Binary, Boolean, DataType, Date, Decimal, Double, Float, Integer, Long, Map,
    ObservedColumn, QualifiedName, Short, String, Struct, StructField, Timestamp, TimestampNtz,
)

_SIMPLE: dict[type, DataType] = {
    T.IntegerType: Integer(), T.LongType: Long(), T.ShortType: Short(),
    T.FloatType: Float(), T.DoubleType: Double(), T.BooleanType: Boolean(),
    T.StringType: String(), T.DateType: Date(), T.TimestampType: Timestamp(),
    T.TimestampNTZType: TimestampNtz(), T.BinaryType: Binary(),
}


def _data_type(spark_type: T.DataType) -> DataType | None:
    simple = _SIMPLE.get(type(spark_type))
    if simple is not None:
        return simple
    if isinstance(spark_type, T.DecimalType):
        return Decimal(spark_type.precision, spark_type.scale)
    if isinstance(spark_type, T.ArrayType):
        element = _data_type(spark_type.elementType)
        return Array(element) if element is not None else None
    if isinstance(spark_type, T.MapType):
        key = _data_type(spark_type.keyType); value = _data_type(spark_type.valueType)
        return Map(key, value) if key and value else None
    if isinstance(spark_type, T.StructType):
        fields = [
            StructField(f.name.casefold(), _data_type(f.dataType))
            for f in spark_type.fields if _data_type(f.dataType) is not None
        ]
        return Struct(tuple(fields))
    return None


class NativeSparkReader:
    def __init__(self, spark: Any) -> None:
        self.spark = spark

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        try:
            return self._read(qualified_name)
        except Exception as exception:  # noqa: BLE001 - total port boundary
            return ReadFailed(
                failure=ReadFailure(exception_type_name(exception), exception_message(exception))
            )

    def _read(self, qualified_name: QualifiedName) -> CatalogState:
        fq = str(qualified_name)
        if not self.spark.catalog.tableExists(fq):
            return TableAbsent()
        struct = self.spark.table(fq).schema
        detail = self.spark.sql(f"DESCRIBE DETAIL {fq}").first()
        columns = tuple(
            ObservedColumn(
                name=field.name.casefold(),
                data_type=_data_type(field.dataType),
                nullable=field.nullable,
                comment=field.metadata.get("comment") or "",
            )
            for field in struct.fields
            if _data_type(field.dataType) is not None
        )
        properties = DELTA_PROPERTY_POLICY.project_observed(detail["properties"] or {})
        snapshot = TableSnapshot(
            qualified_name=qualified_name,
            columns=columns,
            comment=self.spark.catalog.getTable(fq).description or "",
            partitioned_by=tuple(c.casefold() for c in (detail["partitionColumns"] or [])),
            clustered_by=tuple(c.casefold() for c in (detail["clusteringColumns"] or [])),
            properties=properties,
            primary_key=None,
            foreign_keys=(),
        )
        observed = observed_table_from_snapshot(snapshot, run_info_schema_query=lambda query: [])
        return TablePresent(table=observed)
```

- [ ] **Step 2: Write a smoke test for the native reader**

```python
# tests/adapters/databricks/test_native_reader.py
import pytest

from tests.adapters.databricks.native_reader import NativeSparkReader
from delta_engine.application.ports import TableAbsent, TablePresent
from delta_engine.domain.model import Integer, QualifiedName
from tests.config import TEST_CATALOG

pytestmark = pytest.mark.local_e2e


def test_reads_columns_partitioning_and_clustering(spark, make_temp_table, temp_schema):
    fq = make_temp_table("native", "id INT NOT NULL, region STRING")
    name = fq.split(".")[-1]
    state = NativeSparkReader(spark).fetch_state(QualifiedName(TEST_CATALOG, temp_schema, name))
    assert isinstance(state, TablePresent)
    assert [c.name for c in state.table.columns] == ["id", "region"]
    assert state.table.columns[0].data_type == Integer()


def test_absent_table(spark, temp_schema):
    state = NativeSparkReader(spark).fetch_state(QualifiedName("x", temp_schema, "nope"))
    assert isinstance(state, TableAbsent)
```

- [ ] **Step 3: Rewire the engine e2e to inject the native reader**

In `tests/e2e/test_engine_e2e.py`: replace the import `from delta_engine.adapters.databricks.spark.reader import SparkReader` with `from tests.adapters.databricks.native_reader import NativeSparkReader`, and replace every `SparkReader(spark)` with `NativeSparkReader(spark)` (the file constructs `Engine(reader=..., executor=SparkExecutor(spark))` in ~9 places).

- [ ] **Step 4: Run the e2e + native reader tests**

Run: `uv run pytest tests/e2e/test_engine_e2e.py tests/adapters/databricks/test_native_reader.py -q`
Expected: PASS (the Spark session boot takes 30–60s on first run).

- [ ] **Step 5: Commit**

```bash
git add tests/adapters/databricks/native_reader.py tests/adapters/databricks/test_native_reader.py tests/e2e/test_engine_e2e.py
git commit -m "test(e2e): drive engine round-trips with a native OSS-Spark reader"
```

---

### Task 9: Delete the obsolete read path

Remove the per-aspect queries, their mappers, the DDL type parser, and their tests — now that nothing uses them.

**Files:**

- Modify: `src/delta_engine/adapters/databricks/sql/queries.py`, `sql/rows.py`, `sql/__init__.py`
- Delete: `src/delta_engine/adapters/databricks/sql/parse.py`, `tests/adapters/databricks/sql/test_parse.py`
- Modify: `tests/adapters/databricks/sql/test_queries.py`, `tests/adapters/databricks/sql/test_rows.py`

- [ ] **Step 1: Confirm nothing references the doomed symbols**

Run:

```bash
rg -n 'parse_data_type|column_from_catalog|primary_key_from_rows|foreign_keys_from_rows|managed_properties_from_detail_row|clustering_columns_from_detail_row|columns_query|primary_key_query|foreign_keys_query|table_row_query|describe_detail_query|information_schema_probe_query' src tests
```

Expected: matches only in the files to be deleted/edited in this task (definitions, `__init__` exports, and their own tests). If a match appears anywhere else, stop and fix that first.

- [ ] **Step 2: Delete the symbols**

- `sql/queries.py`: delete `columns_query`, `primary_key_query`, `foreign_keys_query`, `table_row_query`, `describe_detail_query`, `information_schema_probe_query`. Keep `describe_json_query`, `table_tags_query`, `column_tags_query`, `referencing_foreign_keys_query`.
- `sql/rows.py`: delete `column_from_catalog`, `primary_key_from_rows`, `foreign_keys_from_rows`, `managed_properties_from_detail_row`, `clustering_columns_from_detail_row`, and the now-unused `_properties_from_detail_row`. Keep `table_tags_from_rows`, `column_tags_from_rows`, `referencing_foreign_keys_from_rows`; drop the now-unused property-filter, `parse_data_type`, and `json` imports.
- Delete `sql/parse.py` and `tests/adapters/databricks/sql/test_parse.py`.
- `sql/__init__.py`: remove the deleted names from imports and `__all__`.

```bash
git rm src/delta_engine/adapters/databricks/sql/parse.py tests/adapters/databricks/sql/test_parse.py
```

- [ ] **Step 3: Prune the query/row tests**

In `tests/adapters/databricks/sql/test_queries.py` and `test_rows.py`, delete the tests that reference the removed builders/mappers. Keep tests for the retained tag/inbound-FK queries and mappers.

- [ ] **Step 4: Full local check**

Run:

```bash
uv run pytest -m "not local_e2e and not databricks_e2e" -q
uv run ruff check src tests
uv run mypy src
uv run lint-imports
```

Expected: all green. (Then run the full `uv run pytest` including e2e once.)

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(reader): remove the obsolete per-aspect read path and DDL type parser"
```

---

### Task 10: Documentation

**Files:**

- Modify: `docs/reference-limitations.md`, `docs/explanation-architecture.md`, `docs/how-to-implement-adapter.md`, `docs/todo/roadmap.md`, `docs/todo/todo.md`

- [ ] **Step 1: Update limitations and architecture**

- `reference-limitations.md`: state that both readers are Unity-Catalog-only via `DESCRIBE … AS JSON`; hive_metastore tables are no longer readable through the Spark backend (they surface as `ReadFailed`); constraint observation requires DBR 17.3+ or a SQL warehouse (documented, not preflighted).
- `explanation-architecture.md` / `how-to-implement-adapter.md`: describe the read path as one AS JSON call parsed into a `TableSnapshot`, then `observed_table_from_snapshot` — the read-side twin of `execution.execute_statements`; a reader supplies only statement execution + missing-relation classification.

- [ ] **Step 2: Update the roadmap/todo**

- `docs/todo/roadmap.md` #17: note the per-table read reduced from ~8 round-trips to 4 via AS JSON (per-catalog batching still deferred).
- `docs/todo/todo.md`: mark the struct special-character round-trip issue fixed on the parse side (structured JSON field names); record the R1 live-verification gate (property-source equivalence across the 6 policy-managed keys, especially the two retention defaults).

- [ ] **Step 3: Build docs and commit**

Run: `uv run --group docs sphinx-build -b html docs docs/_build/html -W`
Expected: build succeeds with no warnings.

```bash
git add docs
git commit -m "docs: reader now reads via AS JSON; UC-only, DBR 17.3+ for constraints"
```

---

## Self-Review

**Spec coverage:** Every spec section maps to a task — type mapping (T1), constraint string (T2), snapshot/parser incl. comment/clustering/properties/skip-raise (T3), assembly (T4), query + existence-from-error (T5), warehouse shell (T6), spark shell (T7), native test reader + e2e (T8), obsolete-code removal + hive_metastore drop + probe removal (T9), runtime floor + docs (T10). The 4-round-trip contract is asserted in T6/T7; R1 recorded in T10.

**Placeholder scan:** No "TBD"/"handle edge cases". The one inline-import note in T8 is called out with the clean replacement to write instead.

**Type consistency:** `TableSnapshot` fields (T3) are consumed unchanged by `observed_table_from_snapshot` (T4) and both readers (T6/T7) and the native reader (T8). `parse_table_constraints -> ParsedConstraints` (T2) is consumed by `_lower_constraints` (T3). `describe_json_query`/`is_missing_relation` (T5) are consumed by T6/T7. `data_type_from_json` (T1) is consumed by T3.

**Open follow-ups (not blocking):** the R1 live property-equivalence check runs in the separate live project before release; the warehouse missing-relation message-prefix classifier is the one fragile spot, with an explicit fallback (a cheap existence probe) noted in the design spec.
