# Generate a `DeltaTable` declaration from a live Unity Catalog table

Date: 2026-07-30
Status: designed, approved for implementation

## Problem

The engine has no adoption on-ramp. Bringing an existing table under management
means hand-transcribing its catalog state into a `DeltaTable` declaration —
every column, type, nullability, comment, tag, property, and key.

Transcription errors are punished asymmetrically. The engine owns the full
column set and the full key set, so an *omission* is not a smaller declaration,
it is a destructive one: a column left out becomes `DropColumn`, a key left out
becomes `DropForeignKey` (`domain/plan/diff.py:488,518`). The safety rules catch
some of this — `ColumnMappingRequiredForDrop` blocks a column drop without
column mapping — but nothing blocks dropping a constraint. Hand transcription is
therefore most dangerous on exactly the tables that most need adopting: the
large, old, key-bearing ones.

A generator closes this, and it can be verified rather than trusted: a generated
declaration is correct if and only if planning it against the table it came from
is a no-op.

## Scope

`delta-engine generate CATALOG.SCHEMA.TABLE` prints one importable Python module
to stdout. One table per invocation. Schema-wide discovery is out of scope
(see below).

## What already exists

The translation this feature needs is half-built. `_lower_declaration`
(`api/delta_table.py:550`) is exactly `DeltaTable → DesiredTable`; nothing goes
the other way. And the public surface is close to round-trippable already: every
`DeltaTable.__init__` parameter except `scope` has a matching read-only
property, and `ObservedColumn`'s fields are a subset of `DesiredColumn`'s.

The correctness oracle also already exists. `diff_table(desired, observed)` is
the authority on whether two states agree, so every generated declaration can be
checked against the table it came from with machinery the engine already owns.

## Why there is no serialisation layer

This was considered explicitly and rejected. Recording it here because the
question recurs: the codebase has nine translation sites and they *look* like
they belong together.

| Site | Wire format | Domain end | Layer |
| ---- | ----------- | ---------- | ----- |
| `sql/describe.py` | DESCRIBE … AS JSON document | `ObservedColumn`, layout, properties | adapters |
| `sql/rows.py` | information_schema rows | `PrimaryKeyConstraint`, `ForeignKeyConstraint`, tags | adapters |
| `sql/types.py:115` | AS JSON type object | `DataType` | adapters |
| `sql/types.py:47` | Spark SQL type string | `DataType` | adapters |
| `sql/compile.py` | Databricks DDL | `ActionPlan` | adapters |
| `api/delta_table.py:550` | `DeltaTable` | `DesiredTable` | api |
| `report.py:203` | JSON dict | `SyncReport` | application |
| `application/rendering.py` | terminal text | `SyncReport` | application |
| `api/codegen.py` (new) | Python source | `ObservedTable` | api |

Four wire formats, five target languages. **No two sites share both endpoints**
— pairwise they share at most one. The single true inverse pair is
`render_data_type` / `data_type_from_json`, and those already live in one file
for precisely that reason. What the nine share is a verb, not logic.

### A `serialisation/` package has no legal position

The layer contract (`pyproject.toml:205`) is
`cli → databricks | schema | adapters | api → application → domain`, with
`exhaustive = true`. A shared serialisation package would need to be importable
by adapters, api, application and cli, which places it at or below `domain`. It
would then hold Databricks DDL rendering and AS JSON parsing — backend
specifics *below* the domain, inverting the hexagon that
`backend-imports-stay-in-adapters` and `shared-sql-core-is-backend-free` exist
to protect.

It would also worsen locality. Changing how Delta properties are managed today
touches `application/properties.py` and its callers; the policy and its meaning
sit together. Splitting the rendering out makes that change span two packages.

### `to_x` / `from_x` methods on domain classes are worse

Two independent objections.

They weld format vocabulary onto a deliberately format-neutral domain.
`render_data_type` emits *Databricks* syntax and `data_type_from_json` reads the
*Databricks* AS JSON shape; `DataType.to_sql()` puts both on a domain class.
That runs against the Iceberg direction (Iceberg tables lowering into the same
`DesiredTable` is the reason format vocabulary stays out of the domain) and
against the open todo wanting the *application* layer to become more
backend-agnostic, not the domain less so.

And the expression problem points the other way. `DataType` is a **closed set of
17**; the operations are what grows — SQL rendering, JSON parsing, Python-source
rendering, plus a second dialect if Iceberg lands. Methods mean each new format
edits all 17 classes. Free functions dispatching over the closed set mean a new
format is a new file and zero edits to existing ones. `sql/types.py:12-19`
already reasons about exactly this criterion, choosing `match` for the closed
`DataType` set and `singledispatch` for the open `Action` hierarchy.

### The rule the repo already follows

`diff_entries.py` exists because text rendering and `to_dict` were both
interpreting plan actions. What was extracted is the **meaning** — category,
operation, subject cells — not the emission. Its docstring says so: *"presentation
(grouping, grids, dict shapes) belongs to the consumers."*

> Extract when two sites share an **interpretation**. Leave them alone when they
> share only a **verb**.

Applied here: a Python-source type renderer shares only a verb with
`render_data_type` (same `match` shape, different target language), but it
shares an *interpretation* with `_lower_declaration` — that a
`PrimaryKeyConstraint` means `primary_key=[...]`, that `managed_aspects` means
`scope`. That is the extraction to make, and it belongs beside the lowering.

## Decision: raise, then render

Three modules, each with one job.

```
api/codegen.py       pure: ObservedTable → Python source. No I/O, no backend.
databricks.py        build_sql_reader(connection) — mirrors build_sql_engine.
cli/generate.py      wires them; owns stdout/stderr and exit codes.
```

```python
def raise_declaration(observed: ObservedTable) -> DeltaTable:
    """Invert _lower_declaration. Foreign keys are never carried."""


def render_declaration(table: DeltaTable, *, variable: str) -> str:
    """Render any DeltaTable as source — including hand-written ones."""


@dataclass(frozen=True, slots=True)
class GeneratedModule:
    source: str
    warnings: tuple[str, ...]


def generate_module(observed: ObservedTable, *, variable: str) -> GeneratedModule:
    """Header, imports, declaration, warnings, and the plan-able collection."""
```

`raise_declaration` validates by construction: if `DeltaTable(...)` builds, the
emitted source imports. `render_declaration` accepting any `DeltaTable` is what
makes it reusable past codegen — it normalises hand-written declaration modules
too.

`GeneratedModule` earns its place for one concrete reason: the CLI writes
`source` to stdout and `warnings` to stderr. Without that split, anyone
redirecting stdout to a file never sees the foreign-key warning until they open
the file — which defeats its purpose.

### The inversions

| Observed | Declared |
| -------- | -------- |
| `ObservedColumn` fields | `Column` fields one-for-one; `renamed_from` is declaration-only and never set |
| `PrimaryKeyConstraint` | `primary_key=[...]`; the generated constraint name is discarded |
| `TableKind.STREAMING_TABLE` | forced `scope="tags"` — the only scope validation admits for a streaming table |
| `TableKind.TABLE` | `scope="full"` (omitted, it is the default) |
| `properties` | direct copy; the reader has already projected to managed keys |
| `comment`, `tags`, `partitioned_by`, `clustered_by` | direct copy |
| `foreign_keys` | **never carried** — becomes a warning |
| `supported_features`, `referencing_foreign_keys` | not declarable; dropped silently |

`supported_features` and `referencing_foreign_keys` are dropped without a
warning because neither is declaration state: features are derived from the
column tree at planning time, and inbound keys are owned by other tables.

## Foreign keys

`ForeignKey.references` holds a `DeltaTable` **object**, not a name
(`api/delta_table.py:314`), so a single-table module cannot construct one. And
omission is not free: `_diff_foreign_keys` (`domain/plan/diff.py:518`) compares
observed against declared with no unmanaged branch, so an undeclared key becomes
`DropForeignKey`. No scope avoids this — `METADATA_ASPECTS` (`scope="metadata"`)
includes `FOREIGN_KEYS`, and out-of-scope drift fails validation rather than
being ignored.

**Decision: emit the keys as a commented block with a warning that names the
consequence.** The generated module stays importable and immediately plan-able;
the cost is that planning it as written drops the table's foreign keys until the
user wires them up.

This was chosen over the alternatives in *Rejected alternatives* below. The
mitigations are all three of: the comment block, the stderr warning, and a test
that pins exactly which actions a generated FK-bearing table produces.

The warning names the constraint **semantically, not as DDL**. `api` cannot
import `adapters` — layers separated by `|` are independent, which is why
`schema -> api.delta_table` needs an explicit `ignore_imports` exemption — so
reaching for `compile_plan` to quote the real `ALTER TABLE` text would be the
first crack in the boundary this design otherwise respects.

## Everything else that cannot be declared

The domain deliberately admits observed states the declaration rejects. Rather
than a residue type, these let `DeltaTable.__init__` raise and the CLI surfaces
its message, naming the table.

| Case | Where rejected | Realistic? |
| ---- | -------------- | ---------- |
| nullable primary-key column | `domain/model/table.py:230-242` | Databricks requires PK columns to be `NOT NULL`; **unverified**, treat as rare rather than impossible |
| `delta.columnMapping.mode = 'id'` | `application/properties.py:64` | possible; `id` mode exists in Delta |
| partition by every column | `api/delta_table.py:213-221` | possible on a legacy table |
| more than 50 tags on a securable | `api/delta_table.py:124` | Unity Catalog enforces the limit, so unobservable |
| unmodellable column type | fails the *read* already (`sql/describe.py:118-124`) | generate fails identically |

No rule duplication and no retreat-and-retry logic: the raise attempts one
construction, and a rejection is reported rather than worked around. A
structured `Undeclarable` residue was considered and rejected — see below.

## Output

```python
# Generated by delta-engine from dev.silver.orders.

from delta_engine.schema import Column, DeltaTable, Integer, String, Timestamp

orders = DeltaTable(
    catalog="dev",
    schema="silver",
    name="orders",
    columns=[
        Column("order_id", Integer(), nullable=False),
        Column("customer_id", Integer(), nullable=False),
        Column("placed_at", Timestamp(), comment="UTC"),
    ],
    comment="Customer orders",
    properties={"delta.columnMapping.mode": "name"},
    primary_key=["order_id"],
)

# WARNING: dev.silver.orders has 1 foreign key, not declared above.
# ForeignKey references its parent as a DeltaTable object, which a
# single-table module does not have.
#
# Planning this declaration as written will DROP the constraint
# `orders_customer_id_fk` (customer_id -> dev.silver.customers).
#
# To keep it, declare or import dev.silver.customers and add:
#     foreign_keys=[ForeignKey("customer_id", references=customers)],

tables = [orders]
```

Four properties of this output are deliberate.

**`tables = [orders]`** makes the module directly runnable through
`delta-engine plan generated:tables`, closing the round trip at the CLI. It also
satisfies `load_declarations`, which rejects a bare `DeltaTable` and requires a
non-empty ordered sequence.

**No timestamp or version in the header.** Regenerating an unchanged table
produces byte-identical output, which makes `generate | diff` a usable drift
check for free and keeps regeneration quiet in git.

**Default arguments are omitted.** `scope="full"`, empty comments, empty tag and
property mappings, and `nullable=True` do not appear. The generated module reads
like one a person would write.

**Imports are derived from what is rendered**, walking nested types so an
`Array(Struct(...))` column pulls in all three names, and sorted to match
ruff's isort so the output lands clean.

### Variable naming

The table name, sanitised to a Python identifier: non-identifier characters
become `_`, a leading digit gets a `t_` prefix, a Python keyword gets a `_`
suffix. Deterministic, with no override flag in v1. Unity Catalog stores
catalog, schema and table names lowercased, so this is mostly a no-op.

## The reader seam

The CLI needs one observed table, not an engine. `databricks.py` gains a
six-line `build_sql_reader(connection)` mirroring `build_sql_engine`, and
`warehouse/factory.py` gains the matching `build_reader`.

This is preferred over the CLI constructing `WarehouseReader(WarehouseSqlRunner(...))`
directly. That import is legal (`cli → adapters`), but public entry points go
through the facade, and the facade is where the Spark twin
(`build_spark_reader`) will slot in when the notebook path is wanted. Leaving
the seam costs nothing now.

`ObservedTable` is not newly exposed by this: `TableRunReport.read` is already
public and yields `TablePresent`, whose `.table` is an `ObservedTable`.

## CLI surface

```text
delta-engine generate CATALOG.SCHEMA.TABLE
```

- **stdout** — the module source, and nothing else.
- **stderr** — warnings, plus the existing invocation-scoped engine logging.
- **exit 0** on success; **exit 1** on config failure, read failure, an absent
  table, or a table that cannot be declared.

Reuses `_anticipated_errors`, `_engine_logging`, and `open_connection` from the
existing `plan` command. The dotted argument parses into a `QualifiedName`;
anything other than three parts is a `ConfigError` with the same register as
`DeclarationRef.parse`.

`plan` wraps its engine call in `redirect_stdout(sys.stderr)` (`cli/app.py:79`)
so that imported user declaration modules cannot pollute stdout. `generate`
imports no user code and must keep stdout clean for the source, so it does not
inherit that wrapper.

Named failure paths: an absent table reports `dev.silver.orders does not exist`
rather than emitting an empty module; a view or materialized view already fails
the read as `UnsupportedRelationError` (`adapters/databricks/read.py:129`) and
its message is surfaced.

## Testing

**The round-trip oracle is the primary test.** For each `ObservedTable` fixture:
`generate_module` → `exec` the source in a fresh namespace → take `tables[0]` →
`diff_table(table.to_desired_table(), observed)` → assert no actions and no
unresolvables.

For fixtures carrying foreign keys, assert the actions are **exactly** the
expected `DropForeignKey`s and nothing else. That pins the blast radius of the
accepted trap instead of merely documenting it, and it fails loudly if omitting
a key ever starts costing more than the key.

The repo already has Hypothesis wired up, so this generalises to a property test
over generated `ObservedTable`s.

**Vocabulary pin.** Every name codegen can emit is in `schema.__all__` (23
names), and every `DataType` subclass (17) has a source renderer. This is the
one place drift between codegen and the public surface would otherwise be
silent — the renderer must emit `Column`, never `DesiredColumn`.

**Golden file** for one representative table covering nested types, tags,
properties, partitioning and a key, so format changes are reviewable in diffs.

**Determinism**: generating the same fixture twice is byte-identical.

**Live** (`tests/live/`): create a table, generate, plan the generated module,
assert no changes. The real proof.

Gates: `uv run pytest`, `ruff check`, `ruff format --check`, `mypy .`,
`lint-imports`, `sphinx-build -W`.

## Files

**`src/` (5)**

| File | Change |
| ---- | ------ |
| `api/codegen.py` | new — the raise, the renderers, `GeneratedModule` |
| `cli/generate.py` | new — the command |
| `cli/app.py` | register `generate` on the Typer app |
| `databricks.py` | `build_sql_reader`, added to `__all__` |
| `adapters/databricks/warehouse/factory.py` | `build_reader` |

**`tests/` (4)**

`tests/api/test_codegen.py`, `tests/cli/test_generate.py`,
`tests/e2e/` round-trip oracle, `tests/live/` generate-then-plan.

**`docs/` (3)**

| File | Change |
| ---- | ------ |
| `reference-cli.md` | says "has one read-only workflow"; becomes two commands |
| `README.md` | CLI paragraph names only `plan` |
| `index.md` | if it enumerates CLI capability |

Release: `feat:`. New public API (`api/codegen.py`, `build_sql_reader`); nothing
existing moves, so no `BREAKING CHANGE:` footer.

## Out of scope

- **Schema-wide discovery.** Needs a table-listing read the engine does not
  have (`information_schema.tables`) and a new port method. It is also the only
  mode in which foreign keys could be generated properly, so it is the natural
  follow-on rather than a competing design.
- **The Spark/notebook path.** The reader port is already backend-agnostic;
  `build_spark_reader` is the same six lines whenever it is wanted.
- **`--output FILE`.** stdout redirects.
- **External table `LOCATION`.** Not declarable at all yet — separate open todo.
- **Struct field nullability.** Domain gap, separate open todo. Not a
  round-trip risk: the reader normalises it away on both sides.

## Risks

**The foreign-key trap is mitigated, not eliminated.** Someone who pipes stdout
to a file and ignores stderr can still plan a key drop. Three defences (comment,
stderr warning, pinned test); none stops a determined pipe. Accepted knowingly.

**Silent fidelity gaps are benign but worth naming.** `char(n)` and `varchar(n)`
read as `String()` and struct field nullability is unmodelled, so the generated
declaration is narrower than the physical table. Because the reader normalises
identically on the next read, the round trip still plans clean — the generated
declaration is faithful to what the *engine can see*, which is the contract
everywhere else in the system.

## Rejected alternatives

**A `serialisation/` package, or `to_x`/`from_x` on domain classes.** Covered
above: the package has no legal position in the layer graph and worsens
locality; the methods weld format vocabulary onto a format-neutral domain and
invert the expression-problem trade-off.

**Routing through the existing `to_dict` contract as a neutral IR.** Wrong
contract. `schema_version: 2` is a **run report** projection with its own
stability guarantee, and it carries no declaration vocabulary. Pure indirection.

**Rendering straight from `ObservedTable`, with no intermediate `DeltaTable`.**
One function, no failure mode. Rejected because the inversion logic still has to
exist — it just lives inline in a renderer instead of behind a name — and the
generated source loses its import guarantee. The `DeltaTable` gets constructed
in tests anyway to run the oracle, so the raise is paid for either way and this
just declines to ship it.

**Following the foreign-key references** — read the referenced tables too and
emit them in the same module, ordered so the object references resolve.
Produces a module that plans clean, and is the only option that renders real
`ForeignKey(...)` declarations. Rejected for v1 as scope: it turns "one table"
into "one table and its transitive closure", needs cycle handling (a mutual
A↔B reference is inexpressible in the declaration language at all, since
references are objects and one side must be defined first), and belongs with
schema-wide generation, where the closure is already being read.

**Refusing when foreign keys are present.** Never wrong, but pushes the ordering
problem onto the user exactly when the schema is most complex.

**Emitting every declaration with `scope="tags"`.** Safe by construction —
nothing outside tags is ever compared, so nothing is ever dropped — but the
result is close to useless as a starting point for managing the table.

**A structured `Undeclarable` residue type** carrying aspect, reason,
consequence and remedy, with foreign keys as one member. Attractive while it
looked like several cases shared a shape. Rejected once the cases were
enumerated: foreign keys are the only *designed* residue, and the others are
rare-to-unobservable and already produce a good error by raising. One type with
one member, whose second member may never arrive, is not worth the indirection
— and `GeneratedModule.warnings` already carries what the CLI needs.
