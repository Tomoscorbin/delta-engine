# Catalog-to-Declaration Codegen Implementation Plan

> **For implementers:** Execute the tasks in order and keep the checkbox
> (`- [ ]`) markers current. Each task establishes the seam consumed by the
> next one.

**Goal:** Add `delta-engine generate CATALOG.SCHEMA.TABLE`, which reads one live Unity Catalog table and prints an importable Python module declaring its supported state as a `DeltaTable`. V1 omits every outbound foreign key uniformly and emits one consequence-only warning.

**Architecture:** The CLI command is the only supported public surface.
Internally, `generate_module` and `GeneratedModule` form one deep codegen
boundary. Private pure helpers project the non-FK state of an `ObservedTable`
and render the generated subset; rendering source and discovering imports are
one traversal, not a general `DeltaTable` serialisation API. Neither codegen
module is re-exported through a public facade. The CLI composition root reads
through the warehouse adapter's internal `build_reader` factory and prints the
result. For an ordinary FK-free table, correctness is proved by importing the
complete generated `tables` collection and running
`Engine.sync(..., dry_run=True)` against the captured observed state: no
failures, no changes, and no execution. FK-bearing and streaming inputs are
explicit warned limitations with separately pinned consequences.

**Tech Stack:** Python 3.12+, Typer (CLI extra), pytest, ruff, mypy, import-linter.

Design: [2026-07-30-catalog-to-declaration-codegen-design.md](2026-07-30-catalog-to-declaration-codegen-design.md).

## Global Constraints

- **Target-base prerequisite.** Execute this plan only after the PR branch
  incorporates current `main`, including #310's `scope="annotations"` and
  `StreamingTableAnnotationsOnly`. The current PR head predates that change;
  mixing its checked-out tags-only names with the target branch will make the
  streaming tests internally inconsistent.
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
| `src/delta_engine/api/declaration_source.py` | **New.** Internal text emission for the FK-free, default-scope declaration subset produced by codegen. Knows the public vocabulary; knows nothing about catalogs. |
| `src/delta_engine/api/codegen.py` | **New.** The `generate_module` use case, its non-FK observed-state projection, warnings, and module assembly. |
| `src/delta_engine/adapters/databricks/warehouse/factory.py` | Add `build_reader`. |
| `src/delta_engine/cli/app.py` | Add the `generate` command. |
| `tests/api/test_declaration_source.py` | **New.** Renderer behaviour and the vocabulary/exhaustiveness pins. |
| `tests/api/test_codegen.py` | **New.** Projection, module assembly, the full-engine supported-path oracle, and warned-limitation pins. |
| `tests/adapters/databricks/warehouse/test_factory.py` | **New.** Internal reader construction. |
| `tests/cli/conftest.py` | Add the fake reader boundary. |
| `tests/cli/test_app_plan.py` | Keep shared help text aligned with both commands. |
| `tests/cli/test_app_generate.py` | **New.** Command behaviour, exit codes, stdout/stderr split. |
| `tests/live/test_sql_warehouse_live_generate.py` | **New.** Credentialed read/generate/full-dry-run proof. |

**CLI placement:** the command goes in `cli/app.py`, where the Typer app and its
shared `_anticipated_errors` / `_engine_logging` helpers already live.
`tests/cli/conftest.py` also monkeypatches names on `cli_app`; splitting one
command into `cli/generate.py` would add a pass-through module and a second
patch target without hiding any complexity.

---

## Task 1: Data type source rendering

**Files:**
- Create: `src/delta_engine/api/declaration_source.py`
- Test: `tests/api/test_declaration_source.py`

**Internal interfaces:**
- Produces:
  - `_SourceFragment` — frozen internal value carrying `source: str` and the
    `schema_names: frozenset[str]` that source requires.
  - `_render_data_type_source(data_type: DataType) -> _SourceFragment` — one
    recursive operation that renders the expression and discovers its imports
    together.

- [ ] **Step 1: Write the failing tests**

Create `tests/api/test_declaration_source.py`:

```python
"""Rendering a DeltaTable back into the Python source that reconstructs it."""

import pytest

from delta_engine.api.declaration_source import _render_data_type_source
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
import delta_engine.schema as schema


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
        (
            Array(Struct([StructField("a", String())])),
            'Array(Struct([StructField("a", String())]))',
        ),
    ],
)
def test_renders_the_expression_that_reconstructs_the_type(data_type, expected):
    assert _render_data_type_source(data_type).source == expected


@pytest.mark.parametrize(
    "data_type",
    [Integer(), Decimal(10, 2), Array(Map(String(), Integer()))],
)
def test_rendered_source_evaluates_back_to_an_equal_type(data_type):
    # Given the rendered expression evaluated against the public vocabulary
    source = _render_data_type_source(data_type).source
    reconstructed = eval(source, vars(schema).copy())  # noqa: S307

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
        rendered = _render_data_type_source(sample)
        assert rendered.source.startswith(variant.__name__)
        assert rendered.schema_names <= set(schema.__all__)


def test_every_name_the_renderer_emits_is_publicly_importable():
    # Given a deeply nested type rendered through the one recursive operation
    rendered = _render_data_type_source(
        Map(String(), Array(Struct([StructField("a", Decimal(10, 2))])))
    )

    # Then its source carries the complete import dependency set
    assert rendered.schema_names == frozenset(
        {"Map", "String", "Array", "Struct", "StructField", "Decimal"}
    )
    assert rendered.schema_names <= set(schema.__all__)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/api/test_declaration_source.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'delta_engine.api.declaration_source'`

- [ ] **Step 3: Write the renderer**

Create `src/delta_engine/api/declaration_source.py`:

```python
"""
Render the Python-source fragments used by catalog codegen.

The textual half of catalog-to-declaration generation: given the FK-free,
default-scope declaration projected by :mod:`delta_engine.api.codegen`, emit
source that imports from ``delta_engine.schema`` and evaluates back to an equal
declaration. This is an internal codegen renderer, not a normaliser for
hand-written declarations.

Every name emitted here must be one ``delta_engine.schema`` exports, which is
why columns render as ``Column`` rather than the domain's ``DesiredColumn``.
``tests/api/test_declaration_source.py`` pins that against ``schema.__all__``.

This module is backend-free by contract: it renders the public declaration
vocabulary, never SQL. The Databricks type renderer is the unrelated twin in
``adapters/databricks/sql/types.py`` — same shape, different target language,
deliberately not shared.
"""

from dataclasses import dataclass
import json
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


@dataclass(frozen=True, slots=True)
class _SourceFragment:
    """Python source and the public schema names needed to evaluate it."""

    source: str
    schema_names: frozenset[str]


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


def _render_string(value: str) -> str:
    """Render a stable, double-quoted Python string literal."""
    return json.dumps(str(value), ensure_ascii=False)


def _render_data_type_source(data_type: DataType) -> _SourceFragment:
    """Render ``data_type`` and collect its imports in the same traversal."""
    match data_type:
        case Decimal(precision, scale):
            return _SourceFragment(
                source=f"Decimal({precision}, {scale})",
                schema_names=frozenset({"Decimal"}),
            )
        case Array(element):
            rendered = _render_data_type_source(element)
            return _SourceFragment(
                source=f"Array({rendered.source})",
                schema_names=rendered.schema_names | {"Array"},
            )
        case Map(key, value):
            rendered_key = _render_data_type_source(key)
            rendered_value = _render_data_type_source(value)
            return _SourceFragment(
                source=f"Map({rendered_key.source}, {rendered_value.source})",
                schema_names=(
                    rendered_key.schema_names | rendered_value.schema_names | {"Map"}
                ),
            )
        case Struct(fields):
            field_sources: list[str] = []
            schema_names = {"Struct", "StructField"}
            for field in fields:
                rendered = _render_data_type_source(field.data_type)
                field_sources.append(
                    f"StructField({_render_string(field.name)}, {rendered.source})"
                )
                schema_names.update(rendered.schema_names)
            return _SourceFragment(
                source=f"Struct([{', '.join(field_sources)}])",
                schema_names=frozenset(schema_names),
            )
        case _ if type(data_type) in _PARAMETERLESS_TYPES:
            name = type(data_type).__name__
            return _SourceFragment(
                source=f"{name}()",
                schema_names=frozenset({name}),
            )
        case _:
            raise TypeError(
                f"No declaration source for DataType variant {type(data_type).__name__}:"
                " add an explicit case above, or list it in _PARAMETERLESS_TYPES"
            )
```

`_render_string` uses JSON's string escaping because a JSON string is also a
valid Python string expression and already uses the repository's double-quote
style. Converting with `str(...)` keeps the output stable if `Identifier` ever
gains its own `__repr__`.

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

## Task 2: Render a whole declaration

**Files:**
- Modify: `src/delta_engine/api/declaration_source.py`
- Test: `tests/api/test_declaration_source.py`

**Internal interfaces:**
- Consumes: `_SourceFragment`, `_render_data_type_source` (Task 1).
- Produces:
  - `_render_declaration(table: DeltaTable, *, variable: str) -> _SourceFragment`
    — the generated declaration statement and all names it imports.
  - `_render_import_line(schema_names: frozenset[str]) -> str` — the
    `from delta_engine.schema import ...` line, wrapped in parentheses when it
    would exceed 100 characters.

- [ ] **Step 1: Write the failing tests**

Merge `_render_declaration` and `_render_import_line` into the existing
`delta_engine.api.declaration_source` import, add `TimestampNtz` to the existing
domain-model import, and add this top-level import:

```python
from delta_engine.schema import Column, DeltaTable
```

Then append the tests, below all imports:

```python


def test_renders_a_minimal_declaration_omitting_every_default():
    table = DeltaTable("dev", "silver", "orders", columns=(Column("id", String()),))

    assert _render_declaration(table, variable="orders").source == (
        "orders = DeltaTable(\n"
        '    catalog="dev",\n'
        '    schema="silver",\n'
        '    name="orders",\n'
        "    columns=[\n"
        '        Column("id", String()),\n'
        "    ],\n"
        ")"
    )


def test_renders_every_supported_non_default_argument():
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
    )

    source = _render_declaration(table, variable="orders").source

    assert '    comment="Orders",' in source
    assert '    properties={"delta.enableChangeDataFeed": "true"},' in source
    assert '    tags={"team": "data"},' in source
    assert '    partitioned_by=["region"],' in source
    assert '    primary_key=["id"],' in source
    assert "scope" not in source
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

    source = _render_declaration(table, variable="orders").source
    assert '    tags={"a": "2", "z": "1"},' in source


def test_import_line_covers_exactly_the_names_used():
    table = DeltaTable(
        "dev",
        "silver",
        "orders",
        columns=(Column("id", Integer()), Column("payload", Array(String()))),
    )

    rendered = _render_declaration(table, variable="orders")

    assert _render_import_line(rendered.schema_names) == (
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

    rendered = _render_declaration(table, variable="orders")
    line = _render_import_line(rendered.schema_names)

    assert line.startswith("from delta_engine.schema import (\n")
    assert line.endswith(")")
    assert all(len(part) <= 100 for part in line.splitlines())
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/api/test_declaration_source.py -v`
Expected: FAIL, `ImportError: cannot import name '_render_declaration'`

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
    return "[" + ", ".join(_render_string(value) for value in values) + "]"


def _render_mapping(mapping: Mapping[str, str | None]) -> str:
    """Render a mapping as a source dict literal, key-sorted so output is stable."""
    items = ", ".join(
        f"{_render_string(key)}: "
        f"{_render_string(value) if value is not None else 'None'}"
        for key, value in sorted(mapping.items())
    )
    return "{" + items + "}"


def _render_column(column: DesiredColumn) -> _SourceFragment:
    """Render one column and the schema names its source requires."""
    rendered_type = _render_data_type_source(column.data_type)
    parts = [_render_string(column.name), rendered_type.source]
    if not column.nullable:
        parts.append("nullable=False")
    if column.comment:
        parts.append(f"comment={_render_string(column.comment)}")
    if column.tags:
        parts.append(f"tags={_render_mapping(column.tags)}")
    if column.renamed_from is not None:
        parts.append(f"renamed_from={_render_string(column.renamed_from)}")
    return _SourceFragment(
        source=f"Column({', '.join(parts)})",
        schema_names=rendered_type.schema_names | {"Column"},
    )


def _render_declaration(table: DeltaTable, *, variable: str) -> _SourceFragment:
    """
    Render one declaration produced by the v1 observed-state projection.

    Foreign keys and non-default scopes are outside this internal renderer's
    contract. Other arguments still at their default are omitted, so the output
    reads like a declaration a person would write. The returned dependency set
    feeds :func:`_render_import_line`; import discovery never walks the table a
    second time.
    """
    arguments = [
        f"catalog={_render_string(table.catalog)}",
        f"schema={_render_string(table.schema)}",
        f"name={_render_string(table.name)}",
    ]

    rendered_columns = tuple(_render_column(column) for column in table.columns)
    columns = "".join(
        f"{_INDENT * 2}{column.source},\n" for column in rendered_columns
    )
    arguments.append(f"columns=[\n{columns}{_INDENT}]")

    if table.comment:
        arguments.append(f"comment={_render_string(table.comment)}")
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

    body = "".join(f"{_INDENT}{argument},\n" for argument in arguments)
    schema_names = frozenset({"DeltaTable"}).union(
        *(column.schema_names for column in rendered_columns)
    )
    return _SourceFragment(
        source=f"{variable} = DeltaTable(\n{body})",
        schema_names=schema_names,
    )


def _render_import_line(schema_names: frozenset[str]) -> str:
    """Render the import for one already-rendered declaration."""
    sorted_names = sorted(schema_names)

    single = f"from delta_engine.schema import {', '.join(sorted_names)}"
    if len(single) <= _MAX_LINE_LENGTH:
        return single
    wrapped = "".join(f"{_INDENT}{name},\n" for name in sorted_names)
    return f"from delta_engine.schema import (\n{wrapped})"
```

`foreign_keys` is deliberately absent from the argument list. V1 omits every key uniformly, including self-references, and Task 4 emits one consequence-only warning. Rendering FK expressions or repair instructions is outside this internal renderer's contract.

`scope` is absent for a different reason, and the renderer could not emit it
anyway: `DeltaTable` exposes no `scope` property. Generated declarations always
take the `"full"` default, which is correct for an ordinary table and is the
whole point of omitting it — the output reads as a declaration a person would
write. For a streaming table it is *not* correct, because
`StreamingTableAnnotationsOnly` (`application/validation.py:604`) is an eligibility
check that cannot be suppressed via `rules`, and `"full"` claims more than
`ANNOTATION_ASPECTS`. This is a separate limitation from FK state drift: Task 4 emits
its own warning and commented block rather than widening the public surface.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/api/test_declaration_source.py -v`
Expected: all passed

- [ ] **Step 5: Run the full gates**

Run: `uv run pytest && ruff check && ruff format --check && uv run mypy . && uv run lint-imports`
Expected: all green. `lint-imports` matters here — it confirms `api/declaration_source.py` reaches nothing it should not.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/api/declaration_source.py tests/api/test_declaration_source.py
git commit -m "feat(api): render generated declaration source"
```

---

## Task 3: Raise an observed table into a declaration

**Files:**
- Create: `src/delta_engine/api/codegen.py`
- Test: `tests/api/test_codegen.py`

**Internal interface:**
- Produces: `_raise_declaration(observed: ObservedTable) -> DeltaTable`

- [ ] **Step 1: Write the failing tests**

Create `tests/api/test_codegen.py`:

```python
"""Projecting observed catalog state into generated declaration state."""

import pytest

from delta_engine.api.codegen import _raise_declaration
from delta_engine.domain.model import (
    ForeignKeyConstraint,
    Integer,
    ObservedColumn,
    ObservedTable,
    PrimaryKeyConstraint,
    QualifiedName,
    String,
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
    declaration = _raise_declaration(observed())

    id_column, region_column = declaration.columns
    assert (id_column.name, id_column.nullable) == ("id", False)
    assert region_column.comment == "iso code"
    assert dict(region_column.tags) == {"pii": "no"}


def test_the_qualified_name_survives():
    declaration = _raise_declaration(observed())

    assert (declaration.catalog, declaration.schema, declaration.name) == (
        "dev",
        "silver",
        "orders",
    )


def test_the_primary_key_becomes_a_column_name_list_dropping_the_constraint_name():
    declaration = _raise_declaration(
        observed(primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"))
    )

    assert declaration.primary_key == ("id",)


def test_a_table_without_a_primary_key_declares_none():
    assert _raise_declaration(observed()).primary_key == ()


@pytest.mark.parametrize(
    "referenced_table",
    [
        QualifiedName("dev", "silver", "regions"),
        QualifiedName("dev", "silver", "orders"),
    ],
)
def test_foreign_keys_are_omitted_uniformly(referenced_table):
    # Given either an external or self-referential observed foreign key
    table = observed(
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("id",),
                referenced_table=referenced_table,
                referenced_columns=("id",),
                constraint_name="orders_id_fk",
            ),
        ),
    )

    # Then the same v1 policy omits it; Task 4 owns the required warning
    assert _raise_declaration(table).foreign_keys == ()


def test_layout_comment_properties_and_tags_survive():
    table = observed(
        comment="Orders",
        properties={"delta.enableChangeDataFeed": "true"},
        tags={"team": "data"},
        partitioned_by=("region",),
    )

    declaration = _raise_declaration(table)

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
        _raise_declaration(table)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/api/test_codegen.py -v`
Expected: FAIL, `ModuleNotFoundError: No module named 'delta_engine.api.codegen'`

- [ ] **Step 3: Write the raise**

Create `src/delta_engine/api/codegen.py`:

```python
"""
Generate declaration source from observed catalog state.

The semantic half of catalog-to-declaration generation: it projects the
non-relationship state supported by v1 into a public declaration. The textual
half — turning that declaration into source — is
:mod:`delta_engine.api.declaration_source`.

Two things do not cross this internal projection. Every foreign key, including a
self-reference, is deliberately omitted under one v1 policy; :func:`generate_module`
owns the corresponding consequence-only warning. Ownership scope is not inferred
either — the declaration takes the ``"full"`` default, which is what an ordinary
table wants and what a streaming table must not have; :func:`generate_module`
warns rather than guessing. A table whose other observed state no declaration
can express — a nullable primary-key column or unsupported column-mapping mode —
is not worked around: ``DeltaTable`` raises and the caller reports it.
"""

from delta_engine.api.delta_table import DeltaTable
from delta_engine.domain.model import DesiredColumn as Column, ObservedTable


def _raise_declaration(observed: ObservedTable) -> DeltaTable:
    """
    Return the declaration that reconciles to ``observed``.

    Foreign keys are uniformly omitted, and the scope is always the ``"full"``
    default. Every other observable aspect crosses verbatim. An ordinary table
    without foreign keys therefore reconciles cleanly; Task 4 separately pins
    the known FK drops and streaming eligibility failure.

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

## Task 4: Assemble the module, and prove the supported path

This task produces the deliverable and carries the full-engine correctness oracle for ordinary FK-free tables. It also pins the exact consequences of the two warned limitations. Do not skip Step 7.

**Files:**
- Modify: `src/delta_engine/api/codegen.py`
- Test: `tests/api/test_codegen.py`

**Internal codegen boundary:**
- Consumes: `_raise_declaration` (Task 3); `_render_declaration`, `_render_import_line` (Task 2).
- Produces:
  - `GeneratedModule` — frozen dataclass with `source: str` and `warnings: tuple[str, ...]`.
  - `generate_module(observed: ObservedTable) -> GeneratedModule`
  - `_variable_name_for(table_name: str) -> str` — internal deterministic naming helper.

- [ ] **Step 1: Write the failing tests**

Merge these names into the existing top-level imports: `cast` from `typing`;
`generate_module` and `_variable_name_for` from codegen;
`Array`, `Decimal`, `Struct`, `StructField`, and `TableKind` from the domain
model. Add the following application and plan imports at the top as well:

```python
from delta_engine.application.engine import Engine
from delta_engine.application.ports import CatalogState, DesiredTableSource, TablePresent
from delta_engine.application.validation import validate_diff
from delta_engine.domain.plan import ActionPlan, DropForeignKey, diff_table
```

Then append the test support and tests, below all imports:

```python


class _SnapshotReader:
    """Serve the exact observed table used to generate the module."""

    def __init__(self, table: ObservedTable) -> None:
        self.table = table

    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        assert qualified_name == self.table.qualified_name
        return TablePresent(self.table)


class _RecordingExecutor:
    """Compile dry-run plans and fail the test if execution is attempted."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def compile(self, plan: ActionPlan) -> tuple[str, ...]:
        return ()

    def execute(self, statement: str) -> None:
        self.executed.append(statement)
        raise AssertionError("dry-run generation oracle attempted execution")


@pytest.mark.parametrize(
    ("table_name", "expected"),
    [
        ("orders", "orders"),
        ("my-table", "my_table"),
        ("2024_data", "t_2024_data"),
        ("class", "class_"),
        ("a b.c", "a_b_c"),
        ("orders²", "orders_"),
    ],
)
def test_variable_names_are_valid_python_identifiers(table_name, expected):
    assert _variable_name_for(table_name) == expected
    assert expected.isidentifier()


def test_the_module_is_importable_and_exposes_a_plan_able_collection():
    module = generate_module(observed())

    namespace: dict[str, object] = {}
    exec(  # noqa: S102
        compile(module.source, "<generated>", "exec"),
        namespace,
    )

    assert namespace["tables"] == [namespace["orders"]]


def test_an_ordinary_table_with_no_foreign_keys_warns_about_nothing():
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
    assert "(region) -> dev.silver.regions(code)" in warning
    assert "DROP" in warning
    # And the same information reaches anyone who only reads the file
    assert "orders_region_fk" in module.source
    assert "# " in module.source
    assert "ForeignKey(" not in module.source
    assert "references=" not in module.source


def test_external_and_self_referencing_keys_share_one_consequence_only_warning():
    # Given one external and one self-referential key
    table = observed(
        columns=(
            ObservedColumn("id", Integer(), nullable=False),
            ObservedColumn("parent_id", Integer()),
            ObservedColumn("region", String()),
            ObservedColumn("country", String()),
        ),
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("parent_id",),
                referenced_table=QualifiedName("dev", "silver", "orders"),
                referenced_columns=("id",),
                constraint_name="orders_parent_fk",
            ),
            ForeignKeyConstraint(
                local_columns=("region", "country"),
                referenced_table=QualifiedName("dev", "silver", "regions"),
                referenced_columns=("code", "country_code"),
                constraint_name="orders_region_fk",
            ),
        ),
    )

    module = generate_module(table)

    assert len(module.warnings) == 1
    warning = module.warnings[0]
    assert "orders_parent_fk: (parent_id) -> dev.silver.orders(id)" in warning
    assert (
        "orders_region_fk: (region, country)"
        " -> dev.silver.regions(code, country_code)"
    ) in warning
    assert "Self" not in warning
    assert "ForeignKey(" not in warning
    assert "references=" not in warning
    assert "restore" not in warning.lower()
    assert "declare or import" not in warning.lower()


def test_a_streaming_table_warns_that_the_default_scope_will_not_validate():
    module = generate_module(observed(kind=TableKind.STREAMING_TABLE))

    assert len(module.warnings) == 1
    warning = module.warnings[0]
    assert "streaming table" in warning
    assert 'scope="annotations"' in warning
    # And the same information reaches anyone who only reads the file
    assert 'scope="annotations"' in module.source


def test_streaming_and_foreign_key_warnings_do_not_overpromise_a_valid_plan():
    table = observed(
        kind=TableKind.STREAMING_TABLE,
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

    assert len(module.warnings) == 2
    assert any("StreamingTableAnnotationsOnly" in warning for warning in module.warnings)
    assert any("otherwise eligible" in warning for warning in module.warnings)


def test_output_is_byte_identical_when_regenerated():
    table = observed(
        tags={"z": "1", "a": "2"},
        properties={"delta.enableTypeWidening": "true"},
    )

    assert generate_module(table).source == generate_module(table).source


def test_supported_generated_source_passes_the_complete_dry_run_boundary():
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
        clustered_by=("region",),
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
    )

    # When the complete generated collection is planned against that snapshot
    namespace: dict[str, object] = {}
    exec(  # noqa: S102
        compile(generate_module(table).source, "<generated>", "exec"),
        namespace,
    )
    tables = cast(list[DesiredTableSource], namespace["tables"])
    executor = _RecordingExecutor()
    report = Engine(reader=_SnapshotReader(table), executor=executor).sync(
        *tables,
        dry_run=True,
    )

    # Then every planning phase accepts it, nothing changes, and nothing executes
    assert report.has_failures is False
    assert report.has_changes is False
    assert executor.executed == []


@pytest.mark.parametrize(
    "referenced_table",
    [
        QualifiedName("dev", "silver", "regions"),
        QualifiedName("dev", "silver", "orders"),
    ],
)
def test_a_generated_table_with_foreign_keys_plans_exactly_the_key_drops(
    referenced_table,
):
    # Given a table whose only undeclarable aspect is its foreign key
    table = observed(
        primary_key=PrimaryKeyConstraint(columns=("id",), constraint_name="orders_pk"),
        foreign_keys=(
            ForeignKeyConstraint(
                local_columns=("id",),
                referenced_table=referenced_table,
                referenced_columns=("id",),
                constraint_name="orders_id_fk",
            ),
        ),
    )

    namespace: dict[str, object] = {}
    exec(  # noqa: S102
        compile(generate_module(table).source, "<generated>", "exec"),
        namespace,
    )
    declaration = cast(DesiredTableSource, namespace["orders"])
    drift = diff_table(declaration.to_desired_table(), table)

    # Then the documented v1 limitation is exactly one key drop, and nothing else
    assert drift.actions == (DropForeignKey(constraint=table.foreign_keys[0]),)
    assert drift.unresolvable == ()


def test_a_generated_streaming_table_fails_the_streaming_eligibility_check():
    # Given a streaming table already in sync with its generated declaration
    table = observed(kind=TableKind.STREAMING_TABLE)

    namespace: dict[str, object] = {}
    exec(  # noqa: S102
        compile(generate_module(table).source, "<generated>", "exec"),
        namespace,
    )
    declaration = cast(DesiredTableSource, namespace["orders"])
    drift = diff_table(declaration.to_desired_table(), table)

    # Then the diff is clean — the trap is the claimed scope, not the state
    assert drift.actions == ()
    # And validation says so, naming the one check the warning told the user about
    assert [failure.rule_name for failure in validate_diff(drift)] == [
        "StreamingTableAnnotationsOnly"
    ]
```

`TableKind` is new in Task 4 rather than Task 3: Task 3 no longer distinguishes
relation kinds, so importing it there would leave an unused import and fail
`ruff check`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/api/test_codegen.py -v`
Expected: FAIL, `ImportError: cannot import name 'generate_module'`

- [ ] **Step 3: Write the module assembly**

Append to `src/delta_engine/api/codegen.py`. Add to the imports at the top:

```python
from dataclasses import dataclass
import keyword
import re
from typing import assert_never

from delta_engine.api.declaration_source import _render_declaration, _render_import_line
from delta_engine.domain.model import TableKind
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


def _variable_name_for(table_name: str) -> str:
    """Return a valid Python identifier naming a table's declaration."""
    candidate = re.sub(r"\W", "_", table_name, flags=re.ASCII)
    if not candidate or candidate[0].isdigit():
        candidate = f"t_{candidate}"
    if keyword.iskeyword(candidate):
        candidate = f"{candidate}_"
    return candidate


def _foreign_key_warning(observed: ObservedTable) -> str:
    """Name every FK omitted by v1 and its conditional apply consequence."""
    keys = observed.foreign_keys
    plural = "" if len(keys) == 1 else "s"
    listed = "\n".join(
        f"- {key.constraint_name}:"
        f" ({', '.join(str(column) for column in key.local_columns)})"
        f" -> {key.referenced_table}"
        f"({', '.join(str(column) for column in key.referenced_columns)})"
        for key in keys
    )
    return (
        f"{observed.qualified_name} has {len(keys)} foreign key{plural},"
        " not generated in v1.\n"
        "\n"
        "On an otherwise eligible table, applying this declaration will"
        f" DROP the constraint{plural}:\n"
        "\n"
        f"{listed}"
    )


def _streaming_scope_warning(observed: ObservedTable) -> str | None:
    """Warn that a generated streaming declaration claims an invalid scope."""
    match observed.kind:
        case TableKind.TABLE:
            return None
        case TableKind.STREAMING_TABLE:
            return (
                f"{observed.qualified_name} is a streaming table, and the generated"
                " module declares no scope — so it takes the default and claims"
                " ownership of the whole table.\n"
                "A streaming table's definition — schema, properties, and keys — is"
                " owned by its pipeline; only comments and Unity Catalog tags can be"
                " managed from outside it.\n"
                "\n"
                "Planning this declaration as written will fail"
                " StreamingTableAnnotationsOnly, an eligibility check that no rules="
                " setting can suppress.\n"
                "\n"
                "To correct this scope limitation, add:\n"
                "\n"
                '    scope="annotations",'
            )
        case _ as unreachable:
            assert_never(unreachable)


def _commented(text: str) -> str:
    """Prefix every line with a comment marker, leaving blank lines bare."""
    return "\n".join(f"# {line}".rstrip() for line in text.splitlines())


def generate_module(observed: ObservedTable) -> GeneratedModule:
    """
    Return an importable module declaring ``observed``, plus any warnings.

    The module ends with a ``tables`` collection, so it can be planned directly
    with ``delta-engine plan <module>:tables``. Output is deterministic: the
    same observed table always produces byte-identical source.

    V1 deliberately omits every foreign key under one policy and reports the
    resulting drops without suggesting repair code. A streaming table's missing
    restricted scope is a separate warned limitation. Each produces a matching
    commented block in the source, so the module still imports and the reader is
    told what planning it as written would do.

    Raises:
        ValueError: The observed state cannot be expressed as a declaration.

    """
    declaration = _raise_declaration(observed)
    name = _variable_name_for(observed.qualified_name.name)
    rendered = _render_declaration(declaration, variable=name)

    blocks = [
        f"# Generated by delta-engine from {observed.qualified_name}.",
        _render_import_line(rendered.schema_names),
        rendered.source,
    ]

    warnings: list[str] = []
    scope_warning = _streaming_scope_warning(observed)
    if scope_warning is not None:
        warnings.append(scope_warning)
    if observed.foreign_keys:
        warnings.append(_foreign_key_warning(observed))

    blocks.extend(_commented(f"WARNING: {warning}") for warning in warnings)
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

Confirm by eye: the import line is complete, the warning names
`orders_region_fk` and its referenced columns, no repair expression appears,
and `tables = [orders]` is last. Then lint and format-check this representative
output under the repository configuration:

```bash
uv run python -c "..." > /tmp/generated_check.py
ruff check --config pyproject.toml /tmp/generated_check.py
ruff format --check --config pyproject.toml /tmp/generated_check.py
```

Expected: no errors for this representative fixture. This is a smoke check, not
a promise that unusually long comments, tags, or nested types are already laid
out exactly as a formatter would choose; deterministic, importable output is the
v1 contract.

- [ ] **Step 6: Run the full gates**

Run: `uv run pytest && ruff check && ruff format --check && uv run mypy . && uv run lint-imports`
Expected: all green

- [ ] **Step 7: Commit**

```bash
git add src/delta_engine/api/codegen.py tests/api/test_codegen.py
git commit -m "feat(api): assemble a warned declaration module from observed state"
```

---

## Task 5: The reader seam

**Files:**
- Modify: `src/delta_engine/adapters/databricks/warehouse/factory.py`
- Test: `tests/adapters/databricks/warehouse/test_factory.py` (create if absent)

**Internal interface:**
- Produces:
  - `delta_engine.adapters.databricks.warehouse.factory.build_reader(connection) -> WarehouseReader`

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

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/adapters/databricks/warehouse/test_factory.py -v`
Expected: FAIL, `ImportError: cannot import name 'build_reader'`

- [ ] **Step 3: Add the reader factory**

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

Keep this factory internal to the warehouse adapter. The CLI is the composition
root and may import it directly under the existing layer contract; do not add a
reader returning internal application-port types to `delta_engine.databricks.__all__`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/adapters/databricks/warehouse/test_factory.py -v`
Expected: all passed

- [ ] **Step 5: Run the full gates**

Run: `uv run pytest && ruff check && ruff format --check && uv run mypy . && uv run lint-imports`
Expected: all green. `lint-imports` confirms the internal factory stays within
the existing adapter boundary.

- [ ] **Step 6: Commit**

```bash
git add src/delta_engine/adapters/databricks/warehouse/factory.py tests/adapters/databricks/warehouse/test_factory.py
git commit -m "feat(databricks): build an internal warehouse catalog reader"
```

---

## Task 6: The `generate` command

**Files:**
- Modify: `src/delta_engine/cli/app.py`
- Modify: `tests/cli/conftest.py`
- Modify: `tests/cli/test_app_plan.py`
- Test: `tests/cli/test_app_generate.py`

**Interfaces:**
- Consumes: `generate_module` (Task 4); internal `build_reader` (Task 5).

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
    # The first red test runs before the command imports this name into cli.app.
    monkeypatch.setattr(
        cli_app,
        "build_reader",
        lambda connection: reader,
        raising=False,
    )
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
    assert "ForeignKey(" not in result.stderr
    assert "ForeignKey(" not in result.stdout
    assert "references=" not in result.stderr
    assert "references=" not in result.stdout
    assert "Self" not in result.stderr
    assert "Self" not in result.stdout


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


def test_undeclarable_state_names_the_table_and_exits_one(
    runner, fake_reader, databricks_env
):
    fake_reader.states["dev.silver.orders"] = TablePresent(
        table=ObservedTable(
            qualified_name=QualifiedName("dev", "silver", "orders"),
            columns=(ObservedColumn("id", String(), nullable=True),),
            primary_key=PrimaryKeyConstraint(
                columns=("id",),
                constraint_name="orders_pk",
            ),
        )
    )

    result = runner.invoke(app, ["generate", "dev.silver.orders"])

    assert result.exit_code == 1
    assert "dev.silver.orders" in result.stderr
    assert "Primary key column must be NOT NULL" in result.stderr
    assert result.stdout == ""


def test_a_malformed_table_name_is_rejected_before_connecting(runner, databricks_env):
    result = runner.invoke(app, ["generate", "silver.orders"])

    assert result.exit_code == 1
    assert "CATALOG.SCHEMA.TABLE" in result.stderr
```

In `tests/cli/test_app_plan.py`, update the module docstring so it no longer
calls `plan` the single workflow. Rename
`test_help_and_version_keep_the_minimal_public_surface` to
`test_help_and_version_keep_the_read_only_public_surface` and add:

```python
    assert "generate" in help_result.stdout
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/cli/test_app_generate.py -v`
Expected: FAIL — `generate` is not a command

- [ ] **Step 4: Add the command**

Update `src/delta_engine/cli/app.py`'s module docstring to describe both
read-only workflows, and change the Typer app help to:

```python
help="Read-only planning and declaration generation for Delta Lake tables on Databricks."
```

In `src/delta_engine/cli/app.py`, add to the imports:

```python
from delta_engine.adapters.databricks.warehouse.factory import build_reader
from delta_engine.api.codegen import generate_module
from delta_engine.application.errors import ReadError
from delta_engine.application.ports import TablePresent
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
    """Print one table's supported declaration state and known limitations."""
    with _anticipated_errors():
        qualified_name = _parse_qualified_name(table)
        with _engine_logging():
            with open_connection() as (_, connection):
                state = build_reader(connection).fetch_state(qualified_name)
        if not isinstance(state, TablePresent):
            raise ConfigError(f"{qualified_name} does not exist")
        try:
            module = generate_module(state.table)
        except ValueError as error:
            raise ConfigError(f"cannot generate {qualified_name}: {error}") from None
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

## Task 7: Documentation

**Files:**
- Modify: `docs/reference-cli.md`
- Modify: `README.md`
- Modify: `docs/todo/2026-07-30-catalog-to-declaration-codegen-design.md`
- Modify: `docs/todo/todo.md`

- [ ] **Step 1: Update the CLI reference**

`docs/reference-cli.md` opens with *"The `delta-engine` command has one read-only
workflow"*. Change it to two, and add a `generate` section after the `plan` one
covering: the `CATALOG.SCHEMA.TABLE` argument, that source goes to stdout and
warnings to stderr, that the output is an importable module ending in
`tables = [...]`, and that output is deterministic so `generate | diff` works
as a drift check. State plainly, not buried, that v1 omits every foreign key,
including self-references; the warning lists each complete observed relationship
and states the conditional apply consequence; and no executable
repair expression is generated. Document streaming-table scope as a separate
limitation: a generated streaming table declares no scope, so it fails
`StreamingTableAnnotationsOnly` until `scope="annotations"` is added.

- [ ] **Step 2: Update the README**

The CLI paragraph names only `plan`. Add one sentence for `generate` describing it as the adoption on-ramp.

- [ ] **Step 3: Reconcile the design document**

Verify that the design's file table matches this plan
(`api/declaration_source.py`, `api/codegen.py`, `warehouse/factory.py`, and
`cli/app.py`) and that it
describes the CLI command as the only supported public operation, with
`generate_module` as its internal codegen boundary. Keep raising, rendering,
import discovery, and variable naming as private implementation details.

- [ ] **Step 4: Update the backlog entry, but keep it open**

In `docs/todo/todo.md`, record what was built and what was deferred (all
foreign-key source generation, relationship closure, schema-wide discovery,
and the Spark path). Record the uniform consequence-only FK warning and the
separate streaming-table scope limitation. Keep the entry at `- [ ]`: Task 8's
required live proof has not passed yet.

- [ ] **Step 5: Verify the docs build**

Run: `uv run --group docs sphinx-build -W docs docs/_build`
Expected: the complete included documentation tree builds without warnings.
`docs/todo/` is excluded (`docs/conf.py:81`); of the files changed in this task,
only `reference-cli.md` is part of the Sphinx build.

- [ ] **Step 6: Commit**

```bash
git add docs/ README.md
git commit -m "docs: document the generate command"
```

---

## Task 8: Live verification (credentialed)

Requires a real workspace. Run before considering the feature proven; it is the only test that exercises a real `DESCRIBE … AS JSON` document.

**Files:**
- Create: `tests/live/test_sql_warehouse_live_generate.py`
- Modify: `docs/todo/todo.md`

- [ ] **Step 1: Write the live test**

```python
"""
Generating a supported ordinary declaration from a real table plans as a no-op.

The only test that runs the generator against a real ``DESCRIBE … AS JSON``
document rather than a hand-built ``ObservedTable``. Everything the unit
supported-path tests prove rests on the fixtures being faithful; this proves the
catalog agrees.
"""

from typing import cast

import pytest

pytest.importorskip("databricks.sql")


from delta_engine.adapters.databricks.warehouse.factory import build_reader
from delta_engine.api.codegen import generate_module
from delta_engine.application.ports import DesiredTableSource, TablePresent
from delta_engine.databricks import build_sql_engine
from delta_engine.domain.model import QualifiedName
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
    state = build_reader(live_connection).fetch_state(qualified_name)
    assert isinstance(state, TablePresent)
    namespace: dict[str, object] = {}
    exec(  # noqa: S102
        compile(generate_module(state.table).source, "<generated>", "exec"),
        namespace,
    )

    # Then the complete dry-run pipeline accepts it and reports no changes
    generated_tables = cast(list[DesiredTableSource], namespace["tables"])
    report = build_sql_engine(live_connection).sync(*generated_tables, dry_run=True)
    assert report.has_failures is False
    assert report.has_changes is False
```

Notes on the fixtures, which differ from the rest of the suite:

- `tests/live/conftest.py` marks every test in the directory `databricks_e2e` automatically via `pytest_collection_modifyitems`, so **no `pytestmark` line is needed** — adding one is harmless but redundant.
- `live_tables(label)` allocates a uniquely suffixed name and drops the table afterwards; never build names with `uuid4` directly.
- `live_catalog()` and `live_schema()` are plain functions from `sql_warehouse_live_helpers`, not fixtures. `qualified_table(name)` from the same module returns a **dotted string**, so it cannot be passed to `fetch_state` — build a `QualifiedName` as above.
- The `pytest.importorskip("databricks.sql")` before the package imports matches the sibling files.

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/live/test_sql_warehouse_live_generate.py -m databricks_e2e --no-cov -v`
Expected: PASS

- [ ] **Step 3: Close the backlog only after the live proof passes**

Change the codegen entry in `docs/todo/todo.md` from `- [ ]` to `- [x]`. Do not
close it when the live test is skipped, unavailable, or failing.

- [ ] **Step 4: Commit**

```bash
git add tests/live/test_sql_warehouse_live_generate.py docs/todo/todo.md
git commit -m "test: pin generated declarations against a live workspace"
```

---

## Coverage notes against the design

The design's testing section lists a golden-file test. No separate golden file
is added: `test_renders_a_minimal_declaration_omitting_every_default` (Task 2)
already asserts the complete rendered text character-for-character, and
`test_output_is_byte_identical_when_regenerated` (Task 4) pins determinism.
Between them a format change fails a diff-reviewable assertion, which is what
the golden file was for. Task 4's Step 5 also lint- and format-checks one
representative generated module under the repository configuration; it is a
style smoke test, not a universal formatter-stability guarantee.

Everything else in the design maps to a task: the observed-state projection
(Task 3), the narrow renderer and omit-defaults rule (Task 2), uniform omission
of external and self-referential foreign keys, their consequence-only warning,
and the absence of generated repair source (Tasks 3, 4, and 6), the full-engine
supported-path oracle (Tasks 4 and 8), the remaining undeclarable cases (Task
3's final test), the reader seam (Task 5), the CLI surface and its named failure
paths (Task 6), and the vocabulary pin (Task 1).

No task adds a `scope` property to `DeltaTable`. Generated modules never pass
`scope` and take the `"full"` default, which is correct for an ordinary table
and wrong for a streaming table. That eligibility failure is a separate warned
limitation, not part of the foreign-key omission policy. The design records why
under *Rejected alternatives*.

## Definition of done

- For an ordinary table without foreign keys, `delta-engine generate dev.silver.orders` prints a module that passes the full dry-run pipeline with no failures and no changes.
- Regenerating an unchanged table is byte-identical.
- External and self-referential foreign keys are all omitted under one policy; one diagnostic warning lists their complete signatures without a repair expression, and direct diffing produces exactly the corresponding `DropForeignKey` actions.
- A generated FK-free streaming table diffs clean and fails exactly `StreamingTableAnnotationsOnly`, and its warning names the `scope="annotations"` fix.
- Every name the renderer emits is in `schema.__all__`; every `DataType` variant renders.
- All six gates green, live suite included.
