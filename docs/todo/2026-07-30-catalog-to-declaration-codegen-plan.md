# Catalog-to-Declaration Codegen Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `delta-engine generate CATALOG.SCHEMA.TABLE`, which reads one live Unity Catalog table and prints an importable Python module declaring it as a `DeltaTable`.

**Architecture:** Two pure functions in the `api` layer — `raise_declaration` inverts `_lower_declaration` (`ObservedTable → DeltaTable`), and `render_declaration` emits the Python source for any `DeltaTable`. A thin CLI command reads the table through a new `build_sql_reader` facade and prints the result. Correctness is verified with machinery the engine already owns: a generated declaration is right if and only if `diff_table(generated.to_desired_table(), observed)` is empty.

**Tech Stack:** Python 3.12+, Typer (CLI extra), pytest, ruff, mypy, import-linter.

Design: [2026-07-30-catalog-to-declaration-codegen-design.md](2026-07-30-catalog-to-declaration-codegen-design.md).

## Global Constraints

- **Layering.** `api` must not import `adapters` — those are independent siblings in the `cli → databricks | schema | adapters | api → application → domain` contract (`pyproject.toml:205`). `api/codegen.py` and `api/declaration_source.py` stay backend-free. `lint-imports` enforces this.
- **Line length 100**, ruff format with double quotes, isort with `force-sort-within-sections = true` and `known-first-party = ["delta_engine"]`.
- **Docstrings required** (`D` rules are on) on every public module, class, and function.
- **Emitted names must come from `delta_engine.schema.__all__`** (23 names). The renderer spells `Column`, never the domain's `DesiredColumn`.
- **Deterministic output.** No timestamp, no version, no set iteration order in generated source. Regenerating an unchanged table must be byte-identical.
- **No new runtime dependencies.** The base package stays dependency-free.
- **Conventional commits.** Commitizen runs on `commit-msg`. The feature commits are `feat:`; test-only and doc-only commits are `test:` and `docs:`.

Gates for every task: `uv run pytest`, `ruff check`, `ruff format --check`, `mypy .`, `lint-imports`.

---

## File Structure

| File | Responsibility |
| ---- | -------------- |
| `src/delta_engine/api/declaration_source.py` | **New.** Text emission: a `DeltaTable` → the Python source that reconstructs it. Knows the public vocabulary; knows nothing about catalogs. |
| `src/delta_engine/api/codegen.py` | **New.** Semantic inversion: an `ObservedTable` → a `DeltaTable`, plus the `generate_module` use case that composes it with the renderer. |
| `src/delta_engine/api/delta_table.py` | Add a `scope` property so the public surface round-trips completely. |
| `src/delta_engine/databricks.py` | Add `build_sql_reader`. |
| `src/delta_engine/adapters/databricks/warehouse/factory.py` | Add `build_reader`. |
| `src/delta_engine/cli/app.py` | Add the `generate` command. |
| `tests/api/test_declaration_source.py` | **New.** Renderer behaviour and the vocabulary/exhaustiveness pins. |
| `tests/api/test_codegen.py` | **New.** The raise, the module assembly, and the round-trip oracle. |
| `tests/cli/test_app_generate.py` | **New.** Command behaviour, exit codes, stdout/stderr split. |

**Deviation from the design doc:** the design listed `cli/generate.py`. The command goes in `cli/app.py` instead — that is where the Typer app and its shared `_anticipated_errors` / `_engine_logging` helpers live, and `tests/cli/conftest.py` already monkeypatches names on `cli_app`. Splitting would force a second patch target for no benefit. Task 8 updates the design doc's file table to match.

---

## Task 1: `DeltaTable.scope`

`render_declaration` must emit `scope=` for a metadata- or tags-scoped declaration. Every other constructor parameter has a matching read-only property; `scope` does not, so the renderer cannot currently recover it. Without this, rendering a hand-written `scope="metadata"` declaration would silently produce `scope="full"` — a change in meaning.

**Files:**
- Modify: `src/delta_engine/api/delta_table.py`
- Test: `tests/api/test_delta_table.py`

**Interfaces:**
- Produces: `DeltaTable.scope -> ScopeName`

- [ ] **Step 1: Write the failing tests**

Append to `tests/api/test_delta_table.py`:

```python
def test_scope_defaults_to_full_and_is_readable():
    table = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))

    assert table.scope == "full"


def test_scope_reports_the_declared_value():
    table = DeltaTable(
        "dev",
        "silver",
        "orders",
        columns=(Column("id", String()),),
        scope="metadata",
    )

    assert table.scope == "metadata"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/api/test_delta_table.py -k scope -v`
Expected: FAIL, `AttributeError: 'DeltaTable' object has no attribute 'scope'`

- [ ] **Step 3: Store and expose the scope**

In `src/delta_engine/api/delta_table.py`, at the end of `DeltaTable.__init__` (after `self._foreign_key_declarations = ...`):

```python
        self._scope = scope
```

And add the property beside the others (after `foreign_keys`):

```python
    @property
    def scope(self) -> ScopeName:
        """What this declaration manages: ``"full"``, ``"metadata"``, or ``"tags"``."""
        return self._scope
```

`ScopeName` is already imported at the top of the module from `delta_engine.application.scopes`; no new import is needed. The value is stored verbatim rather than derived from `managed_aspects`, because `_normalize_declaration` has already passed it through `managed_aspects_for`, which rejects unknown names.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/api/test_delta_table.py -k scope -v`
Expected: 2 passed

- [ ] **Step 5: Run the full gates**

Run: `uv run pytest && ruff check && ruff format --check && uv run mypy . && uv run lint-imports`
Expected: all green

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/api/delta_table.py tests/api/test_delta_table.py
git commit -m "feat(api): expose the declared scope on DeltaTable"
```

---

## Task 2: Data type source rendering

**Files:**
- Create: `src/delta_engine/api/declaration_source.py`
- Test: `tests/api/test_declaration_source.py`

**Interfaces:**
- Produces:
  - `render_data_type_source(data_type: DataType) -> str`
  - `schema_names_for(data_type: DataType) -> set[str]` — every `delta_engine.schema` name the rendered expression mentions, used later to build the import line.

- [ ] **Step 1: Write the failing tests**

Create `tests/api/test_declaration_source.py`:

```python
"""Rendering a DeltaTable back into the Python source that reconstructs it."""

import pytest

from delta_engine.api.declaration_source import (
    render_data_type_source,
    schema_names_for,
)
import delta_engine.schema as schema
from delta_engine.domain.model import (
    Array,
    DataType,
    Decimal,
    Integer,
    Map,
    String,
    Struct,
    StructField,
)


def _concrete_data_types() -> set[type[DataType]]:
    """Every concrete DataType variant, found by walking the class tree."""
    found: set[type[DataType]] = set()
    pending = [DataType]
    while pending:
        for subclass in pending.pop().__subclasses__():
            if subclass not in found:
                found.add(subclass)
                pending.append(subclass)
    return found


@pytest.mark.parametrize(
    ("data_type", "expected"),
    [
        (Integer(), "Integer()"),
        (String(), "String()"),
        (Decimal(10, 2), "Decimal(10, 2)"),
        (Array(String()), "Array(String())"),
        (Map(String(), Integer()), "Map(String(), Integer())"),
        (
            Struct([StructField("a", String()), StructField("b", Integer())]),
            'Struct([StructField("a", String()), StructField("b", Integer())])',
        ),
        (Array(Struct([StructField("a", String())])), 'Array(Struct([StructField("a", String())]))'),
    ],
)
def test_renders_the_expression_that_reconstructs_the_type(data_type, expected):
    assert render_data_type_source(data_type) == expected


@pytest.mark.parametrize("data_type", [Integer(), Decimal(10, 2), Array(Map(String(), Integer()))])
def test_rendered_source_evaluates_back_to_an_equal_type(data_type):
    # Given the rendered expression evaluated against the public vocabulary
    reconstructed = eval(render_data_type_source(data_type), vars(schema).copy())  # noqa: S307

    # Then it is the type it came from
    assert reconstructed == data_type


_PARAMETERISED_SAMPLES = {
    Decimal: Decimal(10, 2),
    Array: Array(String()),
    Map: Map(String(), Integer()),
    Struct: Struct([StructField("a", String())]),
}


def test_every_concrete_data_type_variant_can_be_rendered():
    # Given every DataType the domain defines
    # Then none of them raises — a new variant must be handled explicitly
    for variant in _concrete_data_types():
        sample = _PARAMETERISED_SAMPLES.get(variant) or variant()
        assert render_data_type_source(sample).startswith(variant.__name__)


def test_every_name_the_renderer_emits_is_publicly_importable():
    # Given the names needed to reconstruct a deeply nested type
    names = schema_names_for(
        Map(String(), Array(Struct([StructField("a", Decimal(10, 2))])))
    )

    # Then all of them are exported from delta_engine.schema
    assert names == {"Map", "String", "Array", "Struct", "StructField", "Decimal"}
    assert names <= set(schema.__all__)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/api/test_declaration_source.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'delta_engine.api.declaration_source'`

- [ ] **Step 3: Write the renderer**

Create `src/delta_engine/api/declaration_source.py`:

```python
"""
Render a ``DeltaTable`` as the Python source that reconstructs it.

The textual half of catalog-to-declaration generation: given a declaration —
generated by :mod:`delta_engine.api.codegen` or hand-written — emit source
that imports from ``delta_engine.schema`` and evaluates back to an equal
declaration.

Every name emitted here must be one ``delta_engine.schema`` exports, which is
why columns render as ``Column`` rather than the domain's ``DesiredColumn``.
``tests/api/test_declaration_source.py`` pins that against ``schema.__all__``.

This module is backend-free by contract: it renders the public declaration
vocabulary, never SQL. The Databricks type renderer is the unrelated twin in
``adapters/databricks/sql/types.py`` — same shape, different target language,
deliberately not shared.
"""

from typing import Final

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
    Short,
    String,
    Struct,
    Timestamp,
    TimestampNtz,
    Variant,
)

# The variants whose source form is a bare no-argument constructor. Listed
# rather than inferred so a new parameterised variant cannot silently render
# as `NewType()` and lose its arguments.
_PARAMETERLESS_TYPES: Final[frozenset[type[DataType]]] = frozenset(
    {
        Binary,
        Boolean,
        Byte,
        Date,
        Double,
        Float,
        Integer,
        Long,
        Short,
        String,
        Timestamp,
        TimestampNtz,
        Variant,
    }
)


def render_data_type_source(data_type: DataType) -> str:
    """Return the Python expression that reconstructs ``data_type``."""
    match data_type:
        case Decimal(precision, scale):
            return f"Decimal({precision}, {scale})"
        case Array(element):
            return f"Array({render_data_type_source(element)})"
        case Map(key, value):
            return f"Map({render_data_type_source(key)}, {render_data_type_source(value)})"
        case Struct(fields):
            rendered = ", ".join(
                f"StructField({str(field.name)!r}, {render_data_type_source(field.data_type)})"
                for field in fields
            )
            return f"Struct([{rendered}])"
        case _ if type(data_type) in _PARAMETERLESS_TYPES:
            return f"{type(data_type).__name__}()"
        case _:
            raise TypeError(
                f"No declaration source for DataType variant {type(data_type).__name__}:"
                " add an explicit case above, or list it in _PARAMETERLESS_TYPES"
            )


def schema_names_for(data_type: DataType) -> set[str]:
    """Return every ``delta_engine.schema`` name the rendered type mentions."""
    names = {type(data_type).__name__}
    match data_type:
        case Array(element):
            names |= schema_names_for(element)
        case Map(key, value):
            names |= schema_names_for(key) | schema_names_for(value)
        case Struct(fields):
            names.add("StructField")
            for field in fields:
                names |= schema_names_for(field.data_type)
    return names
```

Note `ruff format` will use double quotes, and `{str(field.name)!r}` produces double quotes because Python's `repr` prefers them for strings without embedded double quotes. `str(...)` is applied first because `field.name` is an `Identifier`; the conversion keeps the output stable if `Identifier` ever gains its own `__repr__`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/api/test_declaration_source.py -v`
Expected: all passed

- [ ] **Step 5: Run the full gates**

Run: `uv run pytest && ruff check && ruff format --check && uv run mypy . && uv run lint-imports`
Expected: all green

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/api/declaration_source.py tests/api/test_declaration_source.py
git commit -m "feat(api): render domain data types as declaration source"
```

---

## Task 3: Render a whole declaration

**Files:**
- Modify: `src/delta_engine/api/declaration_source.py`
- Test: `tests/api/test_declaration_source.py`

**Interfaces:**
- Consumes: `render_data_type_source`, `schema_names_for` (Task 2); `DeltaTable.scope` (Task 1).
- Produces:
  - `render_declaration(table: DeltaTable, *, variable: str) -> str` — the `variable = DeltaTable(...)` statement, no imports.
  - `render_import_line(table: DeltaTable) -> str` — the `from delta_engine.schema import ...` line, wrapped in parentheses when it would exceed 100 characters.

- [ ] **Step 1: Write the failing tests**

Append to `tests/api/test_declaration_source.py`:

```python
from delta_engine.api.declaration_source import render_declaration, render_import_line
from delta_engine.schema import Column, DeltaTable


def test_renders_a_minimal_declaration_omitting_every_default():
    table = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))

    assert render_declaration(table, variable="orders") == (
        "orders = DeltaTable(\n"
        '    catalog="dev",\n'
        '    schema="silver",\n'
        '    name="orders",\n'
        "    columns=[\n"
        '        Column("id", String()),\n'
        "    ],\n"
        ")"
    )


def test_renders_every_non_default_argument():
    table = DeltaTable(
        "dev",
        "silver",
        "orders",
        columns=(
            Column("id", Integer(), nullable=False, comment="pk"),
            Column("region", String(), tags={"pii": "no"}),
        ),
        comment="Orders",
        properties={"delta.enableChangeDataFeed": "true"},
        tags={"team": "data"},
        partitioned_by=["region"],
        primary_key=["id"],
        scope="metadata",
    )

    source = render_declaration(table, variable="orders")

    assert '    comment="Orders",' in source
    assert '    properties={"delta.enableChangeDataFeed": "true"},' in source
    assert '    tags={"team": "data"},' in source
    assert '    partitioned_by=["region"],' in source
    assert '    primary_key=["id"],' in source
    assert '    scope="metadata",' in source
    assert '        Column("id", Integer(), nullable=False, comment="pk"),' in source
    assert '        Column("region", String(), tags={"pii": "no"}),' in source


def test_rendering_is_deterministic_for_mappings():
    table = DeltaTable(
        "dev",
        "silver",
        "orders",
        columns=(Column("id", String()),),
        tags={"z": "1", "a": "2"},
    )

    assert '    tags={"a": "2", "z": "1"},' in render_declaration(table, variable="orders")


def test_import_line_covers_exactly_the_names_used():
    table = DeltaTable(
        "dev",
        "silver",
        "orders",
        columns=(Column("id", Integer()), Column("payload", Array(String()))),
    )

    assert render_import_line(table) == (
        "from delta_engine.schema import Array, Column, DeltaTable, Integer, String"
    )


def test_long_import_lines_are_parenthesised():
    table = DeltaTable(
        "dev",
        "silver",
        "orders",
        columns=(
            Column("a", Integer()),
            Column("b", Decimal(10, 2)),
            Column("c", Map(String(), Array(Struct([StructField("x", TimestampNtz())])))),
        ),
    )

    line = render_import_line(table)

    assert line.startswith("from delta_engine.schema import (\n")
    assert line.endswith(")")
    assert all(len(part) <= 100 for part in line.splitlines())
```

Add `TimestampNtz` to the existing `delta_engine.domain.model` import at the top of the test file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/api/test_declaration_source.py -v`
Expected: FAIL, `ImportError: cannot import name 'render_declaration'`

- [ ] **Step 3: Write the declaration renderer**

Append to `src/delta_engine/api/declaration_source.py`. Add to the imports at the top:

```python
from collections.abc import Mapping, Sequence

from delta_engine.api.delta_table import DeltaTable
from delta_engine.domain.model import DesiredColumn
```

Then:

```python
_INDENT: Final[str] = "    "
_MAX_LINE_LENGTH: Final[int] = 100


def _render_string_list(values: Sequence[str]) -> str:
    """Render a list of identifiers as a source list literal, order preserved."""
    return "[" + ", ".join(repr(str(value)) for value in values) + "]"


def _render_mapping(mapping: Mapping[str, str | None]) -> str:
    """Render a mapping as a source dict literal, key-sorted so output is stable."""
    items = ", ".join(f"{key!r}: {value!r}" for key, value in sorted(mapping.items()))
    return "{" + items + "}"


def _render_column(column: DesiredColumn) -> str:
    """Render one column, omitting every argument still at its default."""
    parts = [repr(str(column.name)), render_data_type_source(column.data_type)]
    if not column.nullable:
        parts.append("nullable=False")
    if column.comment:
        parts.append(f"comment={column.comment!r}")
    if column.tags:
        parts.append(f"tags={_render_mapping(column.tags)}")
    if column.renamed_from is not None:
        parts.append(f"renamed_from={str(column.renamed_from)!r}")
    return f"Column({', '.join(parts)})"


def render_declaration(table: DeltaTable, *, variable: str) -> str:
    """
    Return the ``variable = DeltaTable(...)`` statement that reconstructs ``table``.

    Arguments still at their default are omitted, so the output reads like a
    declaration a person would write. Imports are not included; pair this with
    :func:`render_import_line`.
    """
    arguments = [
        f"catalog={table.catalog!r}",
        f"schema={table.schema!r}",
        f"name={table.name!r}",
    ]

    columns = "".join(
        f"{_INDENT * 2}{_render_column(column)},\n" for column in table.columns
    )
    arguments.append(f"columns=[\n{columns}{_INDENT}]")

    if table.comment:
        arguments.append(f"comment={table.comment!r}")
    if table.properties:
        arguments.append(f"properties={_render_mapping(table.properties)}")
    if table.tags:
        arguments.append(f"tags={_render_mapping(table.tags)}")
    if table.partitioned_by:
        arguments.append(f"partitioned_by={_render_string_list(table.partitioned_by)}")
    if table.clustered_by:
        arguments.append(f"clustered_by={_render_string_list(table.clustered_by)}")
    if table.primary_key:
        arguments.append(f"primary_key={_render_string_list(table.primary_key)}")
    if table.scope != "full":
        arguments.append(f"scope={table.scope!r}")

    body = "".join(f"{_INDENT}{argument},\n" for argument in arguments)
    return f"{variable} = DeltaTable(\n{body})"


def render_import_line(table: DeltaTable) -> str:
    """Return the ``delta_engine.schema`` import covering every name ``table`` renders."""
    names = {"Column", "DeltaTable"}
    for column in table.columns:
        names |= schema_names_for(column.data_type)
    sorted_names = sorted(names)

    single = f"from delta_engine.schema import {', '.join(sorted_names)}"
    if len(single) <= _MAX_LINE_LENGTH:
        return single
    wrapped = "".join(f"{_INDENT}{name},\n" for name in sorted_names)
    return f"from delta_engine.schema import (\n{wrapped})"
```

`foreign_keys` is deliberately absent from the argument list: a generated declaration never carries them (Task 5 emits a warning instead), and `render_declaration` has no way to name the referenced `DeltaTable` variable. This is stated in the docstring rather than silently omitted — see Step 3 of Task 5.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/api/test_declaration_source.py -v`
Expected: all passed

- [ ] **Step 5: Run the full gates**

Run: `uv run pytest && ruff check && ruff format --check && uv run mypy . && uv run lint-imports`
Expected: all green. `lint-imports` matters here — it confirms `api/declaration_source.py` reaches nothing it should not.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/api/declaration_source.py tests/api/test_declaration_source.py
git commit -m "feat(api): render a DeltaTable as declaration source"
```

---

## Task 4: Raise an observed table into a declaration

**Files:**
- Create: `src/delta_engine/api/codegen.py`
- Test: `tests/api/test_codegen.py`

**Interfaces:**
- Produces: `raise_declaration(observed: ObservedTable) -> DeltaTable`

- [ ] **Step 1: Write the failing tests**

Create `tests/api/test_codegen.py`:

```python
"""Turning observed catalog state back into a public declaration."""

import pytest

from delta_engine.api.codegen import raise_declaration
from delta_engine.domain.model import (
    ForeignKeyConstraint,
    Integer,
    ObservedColumn,
    ObservedTable,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
    TableKind,
)


def observed(**overrides) -> ObservedTable:
    """Build an observed dev.silver.orders, overriding any field."""
    fields = {
        "qualified_name": QualifiedName("dev", "silver", "orders"),
        "columns": (
            ObservedColumn("id", Integer(), nullable=False),
            ObservedColumn("region", String(), comment="iso code", tags={"pii": "no"}),
        ),
    }
    return ObservedTable(**(fields | overrides))


def test_columns_carry_every_observable_field():
    declaration = raise_declaration(observed())

    id_column, region_column = declaration.columns
    assert (id_column.name, id_column.nullable) == ("id", False)
    assert region_column.comment == "iso code"
    assert dict(region_column.tags) == {"pii": "no"}


def test_the_qualified_name_survives():
    declaration = raise_declaration(observed())

    assert (declaration.catalog, declaration.schema, declaration.name) == (
        "dev",
        "silver",
        "orders",
    )


def test_the_primary_key_becomes_a_column_name_list_dropping_the_constraint_name():
    declaration = raise_declaration(
        observed(primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"))
    )

    assert declaration.primary_key == ("id",)


def test_a_table_without_a_primary_key_declares_none():
    assert raise_declaration(observed()).primary_key == ()


def test_foreign_keys_are_never_carried():
    # Given an observed table owning a foreign key
    table = observed(
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("region",),
                referenced_table=QualifiedName("dev", "silver", "regions"),
                referenced_columns=("code",),
                constraint_name="orders_region_fk",
            ),
        ),
    )

    # Then the declaration declares none: ForeignKey needs an object it cannot have
    assert raise_declaration(table).foreign_keys == ()


def test_a_streaming_table_is_raised_at_tags_scope():
    table = observed(kind=TableKind.STREAMING_TABLE)

    assert raise_declaration(table).scope == "tags"


def test_an_ordinary_table_is_raised_at_full_scope():
    assert raise_declaration(observed()).scope == "full"


def test_layout_comment_properties_and_tags_survive():
    table = observed(
        comment="Orders",
        properties={"delta.enableChangeDataFeed": "true"},
        tags={"team": "data"},
        partitioned_by=("region",),
    )

    declaration = raise_declaration(table)

    assert declaration.comment == "Orders"
    assert dict(declaration.properties) == {"delta.enableChangeDataFeed": "true"}
    assert dict(declaration.tags) == {"team": "data"}
    assert declaration.partitioned_by == ("region",)


def test_an_undeclarable_observed_table_raises_the_declaration_error():
    # Given a legacy layout the domain admits but a declaration rejects:
    # a nullable primary key column
    table = observed(
        columns=(ObservedColumn("id", Integer(), nullable=True),),
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
    )

    # Then the raise does not work around it; the declaration rule speaks
    with pytest.raises(ValueError, match="Primary key column must be NOT NULL"):
        raise_declaration(table)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/api/test_codegen.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'delta_engine.api.codegen'`

- [ ] **Step 3: Write the raise**

Create `src/delta_engine/api/codegen.py`:

```python
"""
Generate a public declaration from observed catalog state.

The semantic half of catalog-to-declaration generation, and the inverse of
``delta_table._lower_declaration``: it recovers the declaration a table would
need in order to be reconciled to its current state. The textual half — turning
that declaration into source — is :mod:`delta_engine.api.declaration_source`.

Two things do not cross. Foreign keys cannot: ``ForeignKey`` references its
parent as a ``DeltaTable`` object, which a single-table declaration does not
have. And a table whose observed state no declaration can express — a nullable
primary key column, an unsupported column mapping mode — is not worked around;
``DeltaTable`` raises and the caller reports it. Both are deliberate; see the
design doc for the alternatives weighed.
"""

from typing import assert_never

from delta_engine.api.delta_table import DeltaTable
from delta_engine.application.scopes import ScopeName
from delta_engine.domain.model import DesiredColumn as Column, ObservedTable, TableKind


def _scope_for(kind: TableKind) -> ScopeName:
    """Return the widest scope a declaration may claim over this relation kind."""
    match kind:
        case TableKind.STREAMING_TABLE:
            # A streaming table's definition belongs to its owning pipeline;
            # validation rejects any wider scope.
            return "tags"
        case TableKind.TABLE:
            return "full"
        case _ as unreachable:
            assert_never(unreachable)


def raise_declaration(observed: ObservedTable) -> DeltaTable:
    """
    Return the declaration that reconciles to ``observed``.

    Foreign keys are never carried. Every other observable aspect crosses
    verbatim, so planning the result against the table it came from is a no-op.

    Raises:
        ValueError: The observed state cannot be expressed as a declaration.

    """
    return DeltaTable(
        catalog=observed.qualified_name.catalog,
        schema=observed.qualified_name.schema,
        name=observed.qualified_name.name,
        columns=[
            Column(
                name=str(column.name),
                data_type=column.data_type,
                nullable=column.nullable,
                comment=column.comment,
                tags=dict(column.tags),
            )
            for column in observed.columns
        ],
        comment=observed.comment,
        properties=dict(observed.properties),
        tags=dict(observed.tags),
        partitioned_by=[str(name) for name in observed.partitioned_by],
        clustered_by=[str(name) for name in observed.clustered_by],
        primary_key=(
            [str(name) for name in observed.primary_key_columns]
            if observed.primary_key is not None
            else None
        ),
        scope=_scope_for(observed.kind),
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/api/test_codegen.py -v`
Expected: all passed

- [ ] **Step 5: Run the full gates**

Run: `uv run pytest && ruff check && ruff format --check && uv run mypy . && uv run lint-imports`
Expected: all green

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/api/codegen.py tests/api/test_codegen.py
git commit -m "feat(api): raise observed catalog state into a declaration"
```

---

## Task 5: Assemble the module, and prove the round trip

This is the task that produces the deliverable, and the one carrying the correctness oracle. Do not skip Step 7.

**Files:**
- Modify: `src/delta_engine/api/codegen.py`
- Test: `tests/api/test_codegen.py`

**Interfaces:**
- Consumes: `raise_declaration` (Task 4); `render_declaration`, `render_import_line` (Task 3).
- Produces:
  - `GeneratedModule` — frozen dataclass with `source: str` and `warnings: tuple[str, ...]`.
  - `generate_module(observed: ObservedTable, *, variable: str | None = None) -> GeneratedModule`
  - `variable_name_for(table_name: str) -> str`

- [ ] **Step 1: Write the failing tests**

Append to `tests/api/test_codegen.py`:

```python
from delta_engine.api.codegen import GeneratedModule, generate_module, variable_name_for
from delta_engine.domain.plan import diff_table


@pytest.mark.parametrize(
    ("table_name", "expected"),
    [
        ("orders", "orders"),
        ("my-table", "my_table"),
        ("2024_data", "t_2024_data"),
        ("class", "class_"),
        ("a b.c", "a_b_c"),
    ],
)
def test_variable_names_are_valid_python_identifiers(table_name, expected):
    assert variable_name_for(table_name) == expected
    assert expected.isidentifier()


def test_the_module_is_importable_and_exposes_a_plan_able_collection():
    module = generate_module(observed())

    namespace: dict[str, object] = {}
    exec(compile(module.source, "<generated>", "exec"), namespace)  # noqa: S102

    assert namespace["tables"] == [namespace["orders"]]


def test_a_table_without_foreign_keys_warns_about_nothing():
    assert generate_module(observed()).warnings == ()


def test_foreign_keys_produce_a_warning_naming_the_constraint_that_would_drop():
    table = observed(
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("region",),
                referenced_table=QualifiedName("dev", "silver", "regions"),
                referenced_columns=("code",),
                constraint_name="orders_region_fk",
            ),
        ),
    )

    module = generate_module(table)

    assert len(module.warnings) == 1
    warning = module.warnings[0]
    assert "orders_region_fk" in warning
    assert "dev.silver.regions" in warning
    assert "DROP" in warning
    # And the same information reaches anyone who only reads the file
    assert "orders_region_fk" in module.source
    assert "# " in module.source


def test_output_is_byte_identical_when_regenerated():
    table = observed(tags={"z": "1", "a": "2"}, properties={"delta.enableTypeWidening": "true"})

    assert generate_module(table).source == generate_module(table).source


def test_generated_source_plans_as_a_no_op_against_the_table_it_came_from():
    # Given a table exercising every aspect that crosses into a declaration
    table = observed(
        columns=(
            ObservedColumn("id", Integer(), nullable=False, comment="pk"),
            ObservedColumn("region", String(), tags={"pii": "no"}),
            ObservedColumn("payload", Array(Struct([StructField("a", Decimal(10, 2))]))),
        ),
        comment="Orders",
        properties={"delta.enableChangeDataFeed": "true"},
        tags={"team": "data"},
        partitioned_by=("region",),
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
    )

    # When the generated module is imported and diffed against that same table
    namespace: dict[str, object] = {}
    exec(compile(generate_module(table).source, "<generated>", "exec"), namespace)  # noqa: S102
    declaration = namespace["orders"]
    drift = diff_table(declaration.to_desired_table(), table)

    # Then there is nothing to do and nothing unresolvable
    assert drift.actions == ()
    assert drift.unresolvable == ()


def test_a_generated_table_with_foreign_keys_plans_exactly_the_key_drops():
    # Given a table whose only undeclarable aspect is its foreign key
    table = observed(
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("region",),
                referenced_table=QualifiedName("dev", "silver", "regions"),
                referenced_columns=("code",),
                constraint_name="orders_region_fk",
            ),
        ),
    )

    namespace: dict[str, object] = {}
    exec(compile(generate_module(table).source, "<generated>", "exec"), namespace)  # noqa: S102
    drift = diff_table(namespace["orders"].to_desired_table(), table)

    # Then the accepted trap is exactly one key drop, and nothing else
    assert [type(action).__name__ for action in drift.actions] == ["DropForeignKey"]
    assert drift.unresolvable == ()
```

Add `Array`, `Decimal`, `Struct`, `StructField` to the `delta_engine.domain.model` import at the top of the test file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/api/test_codegen.py -v`
Expected: FAIL, `ImportError: cannot import name 'GeneratedModule'`

- [ ] **Step 3: Write the module assembly**

Append to `src/delta_engine/api/codegen.py`. Add to the imports at the top:

```python
from dataclasses import dataclass
import keyword
import re

from delta_engine.api.declaration_source import render_declaration, render_import_line
from delta_engine.domain.model import ForeignKeyConstraint
```

Then:

```python
@dataclass(frozen=True, slots=True)
class GeneratedModule:
    """
    One generated declaration module and anything the reader must be told.

    ``warnings`` is separate from ``source`` so a caller writing the source to
    a file still surfaces them: the CLI sends source to stdout and warnings to
    stderr, and a redirected stdout must not swallow them.
    """

    source: str
    warnings: tuple[str, ...] = ()


def variable_name_for(table_name: str) -> str:
    """Return a valid Python identifier naming a table's declaration."""
    candidate = re.sub(r"\W", "_", table_name)
    if not candidate or candidate[0].isdigit():
        candidate = f"t_{candidate}"
    if keyword.iskeyword(candidate):
        candidate = f"{candidate}_"
    return candidate


def _foreign_key_warning(observed: ObservedTable) -> str:
    """Warn that undeclared foreign keys will be dropped, and how to keep them."""
    keys = observed.foreign_keys
    plural = "" if len(keys) == 1 else "s"
    listed = "\n".join(
        f"    {key.constraint_name}"
        f"  ({', '.join(str(column) for column in key.local_columns)}"
        f" -> {key.referenced_table})"
        for key in keys
    )
    suggested = "\n".join(f"    {_suggested_foreign_key(key)}" for key in keys)
    return (
        f"{observed.qualified_name} has {len(keys)} foreign key{plural},"
        " not declared in the generated module.\n"
        "ForeignKey references its parent as a DeltaTable object, which a"
        " single-table module does not have.\n"
        "\n"
        f"Planning this declaration as written will DROP the constraint{plural}:\n"
        "\n"
        f"{listed}\n"
        "\n"
        "To keep them, declare or import the referenced tables, add ForeignKey to"
        " the delta_engine.schema import, and pass:\n"
        "\n"
        "    foreign_keys=[\n"
        f"{suggested}\n"
        "    ],"
    )


def _suggested_foreign_key(key: ForeignKeyConstraint) -> str:
    """Render the ForeignKey call that would restore one observed constraint."""
    columns = (
        repr(str(key.local_columns[0]))
        if len(key.local_columns) == 1
        else "[" + ", ".join(repr(str(column)) for column in key.local_columns) + "]"
    )
    return f"ForeignKey({columns}, references={variable_name_for(key.referenced_table.name)}),"


def _commented(text: str) -> str:
    """Prefix every line with a comment marker, leaving blank lines bare."""
    return "\n".join(f"# {line}".rstrip() for line in text.splitlines())


def generate_module(
    observed: ObservedTable,
    *,
    variable: str | None = None,
) -> GeneratedModule:
    """
    Return an importable module declaring ``observed``, plus any warnings.

    The module ends with a ``tables`` collection, so it can be planned directly
    with ``delta-engine plan <module>:tables``. Output is deterministic: the
    same observed table always produces byte-identical source.

    Raises:
        ValueError: The observed state cannot be expressed as a declaration.

    """
    declaration = raise_declaration(observed)
    name = variable if variable is not None else variable_name_for(observed.qualified_name.name)

    blocks = [
        f"# Generated by delta-engine from {observed.qualified_name}.",
        render_import_line(declaration),
        render_declaration(declaration, variable=name),
    ]
    warnings: list[str] = []
    if observed.foreign_keys:
        warning = _foreign_key_warning(observed)
        warnings.append(warning)
        blocks.append(_commented(f"WARNING: {warning}"))
    blocks.append(f"tables = [{name}]")

    return GeneratedModule(source="\n\n".join(blocks) + "\n", warnings=tuple(warnings))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/api/test_codegen.py -v`
Expected: all passed

- [ ] **Step 5: Eyeball one real output**

Run:

```bash
uv run python -c "
from delta_engine.api.codegen import generate_module
from delta_engine.domain.model import *
print(generate_module(ObservedTable(
    qualified_name=QualifiedName('dev','silver','orders'),
    columns=(ObservedColumn('id', Integer(), nullable=False),
             ObservedColumn('region', String(), comment='iso code')),
    comment='Orders',
    primary_key=PrimaryKeyConstraint(columns=('id',), constraint_name='orders_pk'),
    foreign_keys=(ForeignKeyConstraint(
        local_columns=('region',),
        referenced_table=QualifiedName('dev','silver','regions'),
        referenced_columns=('code',),
        constraint_name='orders_region_fk'),),
)).source)"
```

Confirm by eye: the import line is complete, the warning names `orders_region_fk`, and `tables = [orders]` is last. Then check the output is actually clean by ruff's standards:

```bash
uv run python -c "..." > /tmp/generated_check.py && ruff check --isolated /tmp/generated_check.py
```

Expected: no errors. If ruff complains, fix the renderer rather than the test — generated code that a formatter immediately rewrites is a defect.

- [ ] **Step 6: Run the full gates**

Run: `uv run pytest && ruff check && ruff format --check && uv run mypy . && uv run lint-imports`
Expected: all green

- [ ] **Step 7: Commit**

```bash
git add src/delta_engine/api/codegen.py tests/api/test_codegen.py
git commit -m "feat(api): assemble a plan-able declaration module from observed state"
```

---

## Task 6: The reader seam

**Files:**
- Modify: `src/delta_engine/adapters/databricks/warehouse/factory.py`
- Modify: `src/delta_engine/databricks.py`
- Test: `tests/adapters/databricks/warehouse/test_factory.py` (create if absent)

**Interfaces:**
- Produces:
  - `delta_engine.adapters.databricks.warehouse.factory.build_reader(connection) -> WarehouseReader`
  - `delta_engine.databricks.build_sql_reader(connection) -> CatalogStateReader`

- [ ] **Step 1: Write the failing test**

Create or append to `tests/adapters/databricks/warehouse/test_factory.py`:

```python
"""The warehouse factory builds engine and reader from one caller-owned connection."""

from delta_engine.adapters.databricks.warehouse.factory import build_engine, build_reader
from delta_engine.adapters.databricks.warehouse.reader import WarehouseReader


class _StubConnection:
    def close(self) -> None:
        pass


def test_build_reader_returns_a_reader_over_the_given_connection():
    assert isinstance(build_reader(_StubConnection()), WarehouseReader)


def test_build_engine_still_returns_a_usable_engine():
    assert build_engine(_StubConnection()) is not None
```

And in `tests/test_public_api.py`, append:

```python
def test_build_sql_reader_is_exported_from_the_databricks_facade():
    import delta_engine.databricks as databricks

    assert "build_sql_reader" in databricks.__all__
    assert callable(databricks.build_sql_reader)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/adapters/databricks/warehouse/test_factory.py tests/test_public_api.py -v`
Expected: FAIL, `ImportError: cannot import name 'build_reader'`

- [ ] **Step 3: Add both factories**

In `src/delta_engine/adapters/databricks/warehouse/factory.py`, append:

```python
def build_reader(connection: Connection) -> WarehouseReader:
    """
    Create a catalog-state reader for a Databricks SQL warehouse.

    The read half of :func:`build_engine`, for callers that inspect catalog
    state without syncing. The caller owns the connection either way.
    """
    return WarehouseReader(WarehouseSqlRunner(connection))
```

In `src/delta_engine/databricks.py`, add `"build_sql_reader"` to `__all__` (keeping it sorted) and append:

```python
def build_sql_reader(connection: Connection) -> CatalogStateReader:
    """
    Create a catalog-state reader backed by a Databricks SQL warehouse connection.

    The read-only counterpart to :func:`build_sql_engine`, for callers that
    inspect a table's current state rather than reconcile it — the CLI's
    ``generate`` command is one. The caller owns the connection.
    """
    from delta_engine.adapters.databricks.warehouse.factory import build_reader as _build_reader

    return _build_reader(connection)
```

`databricks.py` already has `from __future__ import annotations`, so the return annotation is never evaluated at runtime and the import belongs under `TYPE_CHECKING`. Extend the existing block:

```python
if TYPE_CHECKING:
    from databricks.sql.client import Connection
    from pyspark.sql import SparkSession

    from delta_engine.application.ports import CatalogStateReader
```

Keeping it there preserves the module's whole point: importing `delta_engine.databricks` must not drag in a backend.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/adapters/databricks/warehouse/test_factory.py tests/test_public_api.py -v`
Expected: all passed

- [ ] **Step 5: Run the full gates**

Run: `uv run pytest && ruff check && ruff format --check && uv run mypy . && uv run lint-imports`
Expected: all green. `lint-imports` confirms the lazy import keeps the facade's exemptions intact.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/databricks.py src/delta_engine/adapters/databricks/warehouse/factory.py tests/
git commit -m "feat(databricks): expose a catalog-state reader alongside the engine"
```

---

## Task 7: The `generate` command

**Files:**
- Modify: `src/delta_engine/cli/app.py`
- Modify: `tests/cli/conftest.py`
- Test: `tests/cli/test_app_generate.py`

**Interfaces:**
- Consumes: `generate_module`, `GeneratedModule` (Task 5); `build_sql_reader` (Task 6).

- [ ] **Step 1: Extend the CLI fixtures**

In `tests/cli/conftest.py`, append:

```python
@pytest.fixture
def fake_reader(monkeypatch):
    """Route the CLI's reader boundary to a fake; yield it to preload states."""
    reader = FakeReader()

    @contextmanager
    def fake_connection():
        yield (
            Target(
                host="https://test.cloud.databricks.com",
                warehouse_id="test-warehouse",
            ),
            _StubConnection(),
        )

    monkeypatch.setattr(cli_app, "open_connection", fake_connection)
    monkeypatch.setattr(cli_app, "build_sql_reader", lambda connection: reader)
    return reader
```

- [ ] **Step 2: Write the failing tests**

Create `tests/cli/test_app_generate.py`:

```python
"""Behaviour of the read-only ``delta-engine generate`` workflow."""

from delta_engine.application.errors import ReadError
from delta_engine.application.ports import TableAbsent, TablePresent
from delta_engine.cli.app import app
from delta_engine.domain.model import (
    ForeignKeyConstraint,
    ObservedColumn,
    ObservedTable,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
)
from tests.cli.conftest import observed_orders


def test_generates_an_importable_module_on_stdout_and_exits_zero(
    runner, fake_reader, databricks_env
):
    fake_reader.states["dev.silver.orders"] = observed_orders()

    result = runner.invoke(app, ["generate", "dev.silver.orders"])

    assert result.exit_code == 0
    assert "from delta_engine.schema import Column, DeltaTable, String" in result.stdout
    assert "orders = DeltaTable(" in result.stdout
    assert "tables = [orders]" in result.stdout
    compile(result.stdout, "<generated>", "exec")


def test_foreign_keys_warn_on_stderr_as_well_as_in_the_source(
    runner, fake_reader, databricks_env
):
    fake_reader.states["dev.silver.orders"] = TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName("dev", "silver", "orders"),
            columns=(ObservedColumn("id", String(), nullable=False),),
            primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
            foreign_keys=(
                ForeignKeyConstraint(
                    local_columns=("id",),
                    referenced_table=QualifiedName("dev", "silver", "regions"),
                    referenced_columns=("code",),
                    constraint_name="orders_id_fk",
                ),
            ),
        )
    )

    result = runner.invoke(app, ["generate", "dev.silver.orders"])

    assert result.exit_code == 0
    assert "orders_id_fk" in result.stderr
    assert "orders_id_fk" in result.stdout


def test_an_absent_table_reports_it_and_exits_one(runner, fake_reader, databricks_env):
    fake_reader.states["dev.silver.orders"] = TableAbsent()

    result = runner.invoke(app, ["generate", "dev.silver.orders"])

    assert result.exit_code == 1
    assert "dev.silver.orders does not exist" in result.stderr
    assert result.stdout == ""


def test_a_read_failure_reports_it_and_exits_one(runner, fake_reader, databricks_env):
    fake_reader.states["dev.silver.orders"] = ReadError(
        exception_type="AnalysisException", message="permission denied"
    )

    result = runner.invoke(app, ["generate", "dev.silver.orders"])

    assert result.exit_code == 1
    assert "permission denied" in result.stderr


def test_a_malformed_table_name_is_rejected_before_connecting(runner, databricks_env):
    result = runner.invoke(app, ["generate", "silver.orders"])

    assert result.exit_code == 1
    assert "CATALOG.SCHEMA.TABLE" in result.stderr
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/cli/test_app_generate.py -v`
Expected: FAIL — `generate` is not a command

- [ ] **Step 4: Add the command**

In `src/delta_engine/cli/app.py`, add to the imports:

```python
from delta_engine.api.codegen import generate_module
from delta_engine.application.errors import ReadError
from delta_engine.application.ports import TablePresent
from delta_engine.databricks import build_sql_reader
from delta_engine.domain.model import QualifiedName
```

Add the argument type beside `DeclarationArgument`:

```python
TableArgument = Annotated[
    str,
    typer.Argument(
        metavar="CATALOG.SCHEMA.TABLE",
        help="One fully qualified Unity Catalog table to generate a declaration for.",
    ),
]
```

Extend `_anticipated_errors` to cover the two new expected failures:

```python
    except (ConfigError, DuplicateTableDefinitionError) as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(code=_EXIT_FAILURE) from None
    except ReadError as error:
        typer.echo(f"error: {error}", err=True)
        raise typer.Exit(code=_EXIT_FAILURE) from None
```

Then the command:

```python
@app.command()
def generate(table: TableArgument) -> None:
    """Read one live table and print the declaration module that reproduces it."""
    with _anticipated_errors():
        qualified_name = _parse_qualified_name(table)
        with _engine_logging():
            with open_connection() as (_, connection):
                state = build_sql_reader(connection).fetch_state(qualified_name)
        if not isinstance(state, TablePresent):
            raise ConfigError(f"{qualified_name} does not exist")
        module = generate_module(state.table)
        for warning in module.warnings:
            typer.echo(f"warning: {warning}", err=True)
        typer.echo(module.source, nl=False)


def _parse_qualified_name(text: str) -> QualifiedName:
    """Parse exactly one three-part Unity Catalog table name."""
    parts = text.split(".")
    if len(parts) != 3 or not all(part.strip() for part in parts):
        raise ConfigError(
            f"malformed table name '{text}': expected CATALOG.SCHEMA.TABLE, "
            "such as dev.silver.orders"
        )
    return QualifiedName(*parts)
```

Unlike `plan`, this does **not** wrap the body in `redirect_stdout(sys.stderr)`. `plan` needs that because it imports user declaration modules that may print; `generate` imports no user code and must keep stdout clean for the source.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/cli/test_app_generate.py -v`
Expected: all passed

- [ ] **Step 6: Confirm `plan` is unaffected**

Run: `uv run pytest tests/cli -v`
Expected: all passed, including every existing `test_app_plan.py` case

- [ ] **Step 7: Run the full gates**

Run: `uv run pytest && ruff check && ruff format --check && uv run mypy . && uv run lint-imports`
Expected: all green

- [ ] **Step 8: Commit**

```bash
git add src/delta_engine/cli/app.py tests/cli/
git commit -m "feat(cli): add generate, printing a declaration for one live table"
```

---

## Task 8: Documentation

**Files:**
- Modify: `docs/reference-cli.md`
- Modify: `README.md`
- Modify: `docs/todo/2026-07-30-catalog-to-declaration-codegen-design.md`
- Modify: `docs/todo/todo.md`

- [ ] **Step 1: Update the CLI reference**

`docs/reference-cli.md` opens with *"The `delta-engine` command has one read-only workflow"*. Change it to two, and add a `generate` section after the `plan` one covering: the `CATALOG.SCHEMA.TABLE` argument, that source goes to stdout and warnings to stderr, that the output is a plan-able module ending in `tables = [...]`, that output is deterministic so `generate | diff` works as a drift check, and — stated plainly, not buried — that foreign keys are not declared and planning the output as written drops them.

- [ ] **Step 2: Update the README**

The CLI paragraph names only `plan`. Add one sentence for `generate` describing it as the adoption on-ramp.

- [ ] **Step 3: Correct the design doc's file table**

The design lists `cli/generate.py`; the command went into `cli/app.py`. Update the Files table and the module list in the "Decision" section so the design does not contradict the shipped code.

- [ ] **Step 4: Mark the backlog entry resolved**

In `docs/todo/todo.md`, change the codegen entry from `- [ ]` to `- [x]` and append what was actually built, what was deferred (schema-wide discovery, the Spark path), and the accepted FK trap.

- [ ] **Step 5: Verify the docs build**

Run: `uv run sphinx-build -W docs docs/_build`
Expected: no warnings. `docs/todo/` is excluded (`docs/conf.py:81`), so only `reference-cli.md` is built.

- [ ] **Step 6: Commit**

```bash
git add docs/ README.md
git commit -m "docs: document the generate command"
```

---

## Task 9: Live verification (credentialed)

Requires a real workspace. Run before considering the feature proven; it is the only test that exercises a real `DESCRIBE … AS JSON` document.

**Files:**
- Create: `tests/live/test_sql_warehouse_live_generate.py`

- [ ] **Step 1: Write the live test**

```python
"""
Generating a declaration from a real Unity Catalog table plans as a no-op.

The only test that runs the generator against a real ``DESCRIBE … AS JSON``
document rather than a hand-built ``ObservedTable``. Everything the unit
round-trip proves rests on the fixtures being faithful; this proves the
catalog agrees.
"""

import pytest

pytest.importorskip("databricks.sql")


from delta_engine.api.codegen import generate_module, variable_name_for
from delta_engine.databricks import build_sql_engine, build_sql_reader
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan import diff_table
from delta_engine.schema import Column, Decimal, DeltaTable, Integer, String
from tests.live.sql_warehouse_live_helpers import live_catalog, live_schema


def test_a_generated_declaration_plans_clean_against_the_table_it_came_from(
    live_connection, live_tables
):
    # Given a real table carrying columns, comments, tags, a property and a key
    name = live_tables("generate")
    build_sql_engine(live_connection).sync(
        DeltaTable(
            live_catalog(),
            live_schema(),
            name,
            columns=(
                Column("id", Integer(), nullable=False, comment="pk"),
                Column("region", String(), tags={"pii": "no"}),
                Column("amount", Decimal(10, 2)),
            ),
            comment="codegen live pin",
            properties={"delta.enableChangeDataFeed": "true"},
            tags={"team": "data"},
            primary_key=["id"],
        )
    )

    # When we read it back and generate a declaration from the observed state
    qualified_name = QualifiedName(live_catalog(), live_schema(), name)
    state = build_sql_reader(live_connection).fetch_state(qualified_name)
    namespace: dict[str, object] = {}
    exec(compile(generate_module(state.table).source, "<generated>", "exec"), namespace)  # noqa: S102

    # Then planning the generated declaration against that same state is a no-op
    declaration = namespace[variable_name_for(name)]
    drift = diff_table(declaration.to_desired_table(), state.table)
    assert drift.actions == ()
    assert drift.unresolvable == ()
```

Notes on the fixtures, which differ from the rest of the suite:

- `tests/live/conftest.py` marks every test in the directory `databricks_e2e` automatically via `pytest_collection_modifyitems`, so **no `pytestmark` line is needed** — adding one is harmless but redundant.
- `live_tables(label)` allocates a uniquely suffixed name and drops the table afterwards; never build names with `uuid4` directly.
- `live_catalog()` and `live_schema()` are plain functions from `sql_warehouse_live_helpers`, not fixtures. `qualified_table(name)` from the same module returns a **dotted string**, so it cannot be passed to `fetch_state` — build a `QualifiedName` as above.
- The `pytest.importorskip("databricks.sql")` before the package imports matches the sibling files.

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/live/test_sql_warehouse_live_generate.py -m databricks_e2e --no-cov -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/live/test_sql_warehouse_live_generate.py
git commit -m "test: pin generated declarations against a live workspace"
```

---

## Coverage notes against the design

The design's testing section lists a golden-file test. No separate golden file
is added: `test_renders_a_minimal_declaration_omitting_every_default` (Task 3)
already asserts the complete rendered text character-for-character, and
`test_output_is_byte_identical_when_regenerated` (Task 5) pins determinism.
Between them a format change fails a diff-reviewable assertion, which is what
the golden file was for. Task 5's Step 5 covers the remaining concern the
design raised — that the output survives a formatter untouched — by running
`ruff check --isolated` over real generated source.

Everything else in the design maps to a task: the raise and its inversion table
(Task 4), the renderer and the omit-defaults rule (Task 3), the foreign-key
warning and the stdout/stderr split (Tasks 5 and 7), the undeclarable cases
(Task 4's final test), the reader seam (Task 6), the CLI surface and its named
failure paths (Task 7), and the vocabulary pin (Task 2).

## Definition of done

- `delta-engine generate dev.silver.orders` prints a module that `delta-engine plan generated:tables` reports as no changes.
- Regenerating an unchanged table is byte-identical.
- A generated FK-bearing table plans exactly the expected `DropForeignKey` actions and nothing else.
- Every name the renderer emits is in `schema.__all__`; every `DataType` variant renders.
- All six gates green, live suite included.
