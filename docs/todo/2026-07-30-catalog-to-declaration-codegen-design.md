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
becomes `DropForeignKey` (`domain/plan/diff.py:400,518`). The safety rules catch
some of this — `ColumnMappingRequiredForDrop` blocks a column drop without
column mapping — but nothing blocks dropping a constraint. Hand transcription is
therefore most dangerous on exactly the tables that most need adopting: the
large, old, key-bearing ones.

A generator closes this, and it can be verified rather than trusted. For the v1
baseline — an ordinary table with no outbound foreign keys — importing the
generated module and running a full dry-run sync against the state it came from
must produce no failures and no changes.

Two inputs deliberately produce scaffolds rather than that clean round trip in
v1. Foreign keys are omitted uniformly and reported as known `DropForeignKey`
actions; streaming tables keep the separate scope warning described below. The
generator names both limitations in source and on stderr instead of silently
claiming that those outputs reproduce the whole table.

## Scope

`delta-engine generate CATALOG.SCHEMA.TABLE` prints one importable Python module
to stdout. One table per invocation. Schema-wide discovery is out of scope
(see below). Every outbound foreign key, including a self-reference, is omitted
uniformly in v1 and recorded in one consequence-only warning.

## What already exists

The building blocks this feature needs are half-built. `_lower_declaration`
(`api/delta_table.py:550`) defines `DeltaTable → DesiredTable`; generation
applies the deliberately narrower v1 projection in the other direction. The
public surface exposes nearly all supported state: every `DeltaTable.__init__`
parameter except `scope` has a matching read-only property, and
`ObservedColumn`'s fields are a subset of `DesiredColumn`'s.

The correctness machinery also already exists. `diff_table` establishes whether
desired and observed state agree, but relationship resolution and validation can
still reject a clean diff. The supported-path oracle therefore imports the
generated `tables` collection and runs `Engine.sync(..., dry_run=True)` against
the captured observed state, asserting no failures, no changes, and no execution.
Focused `diff_table` assertions remain useful for pinning the deliberately known
FK-drop limitation.

## Why there is no serialisation layer

This was considered explicitly and rejected. Recording it here because the
question recurs: the codebase has nine translation sites and they *look* like
they belong together.

| Site | External representation | Domain end | Layer |
| ---- | ----------------------- | ---------- | ----- |
| `sql/describe.py` | DESCRIBE … AS JSON document | `ObservedColumn`, layout, properties | adapters |
| `sql/rows.py` | information_schema rows | `PrimaryKeyConstraint`, `ForeignKeyConstraint`, tags | adapters |
| `sql/types.py:115` | AS JSON type object | `DataType` | adapters |
| `sql/types.py:47` | Spark SQL type string | `DataType` | adapters |
| `sql/compile.py` | Databricks DDL | `ActionPlan` | adapters |
| `api/delta_table.py:550` | `DeltaTable` | `DesiredTable` | api |
| `report.py:203` | JSON dict | `SyncReport` | application |
| `application/rendering.py` | terminal text | `SyncReport` | application |
| `api/declaration_source.py` (new) | Python source | `DeltaTable` | api |

These are distinct boundary contracts. **No two sites share both endpoints** —
pairwise they share at most one. Even `render_data_type` and
`data_type_from_json` are not inverses: one emits a Spark SQL string and the
other reads an AS JSON object. They are correctly co-located because both hide
Databricks type-mapping knowledge. What the nine sites share is a verb, not
logic.

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
`PrimaryKeyConstraint` means `primary_key=[...]` and observable column state
maps to `Column` fields. That is the extraction to make, and it belongs beside
the lowering. Scope is deliberately not inferred in v1.

There is one smaller duplication to remove locally. Rendering a Python type
expression and discovering the `delta_engine.schema` names it imports traverse
the same closed type tree for the same reason. The private renderer therefore
returns both `source` and `schema_names` in one `_SourceFragment`. This is a
cohesive result inside codegen, not a generic serialisation abstraction.

## Decision: raise, then render the supported subset

Four collaborating units, each with one job.

```
api/declaration_source.py       internal source rendering for generated state.
api/codegen.py                  pure: ObservedTable → Python source. No I/O.
warehouse/factory.py            constructs the internal catalog reader.
cli/app.py                      composition root; owns I/O and exit codes.
```

```python
def _raise_declaration(observed: ObservedTable) -> DeltaTable:
    """Internal mapping for the non-FK declaration state supported by v1."""


@dataclass(frozen=True, slots=True)
class _SourceFragment:
    source: str
    schema_names: frozenset[str]


def _render_declaration(table: DeltaTable, *, variable: str) -> _SourceFragment:
    """Internal one-pass renderer for declarations and their imports."""


@dataclass(frozen=True, slots=True)
class GeneratedModule:
    source: str
    warnings: tuple[str, ...]


def generate_module(observed: ObservedTable) -> GeneratedModule:
    """Header, imports, declaration, warnings, and the runnable collection."""
```

`generate_module` and `GeneratedModule` form the codegen module's internal
use-case boundary; the CLI command is the only supported public surface.
`_raise_declaration`, source rendering, and variable naming are private steps
split out to keep each implementation unit testable. Import discovery belongs
to the same recursive rendering pass, not a parallel walker. The renderer
deliberately supports the narrow FK-free, default-scope shape produced by the
generator; it is not a general normaliser for hand-written `DeltaTable`
declarations. The codegen modules are not re-exported through a public facade.

`GeneratedModule` earns its place for one concrete reason: the CLI writes
`source` to stdout and `warnings` to stderr. Without that split, anyone
redirecting stdout to a file never sees the warnings until they open the file —
which defeats their purpose.

### The v1 projection

| Observed | Declared |
| -------- | -------- |
| `ObservedColumn` fields | `Column` fields one-for-one; `renamed_from` is declaration-only and never set |
| `PrimaryKeyConstraint` | `primary_key=[...]`; the generated constraint name is discarded |
| `TableKind.STREAMING_TABLE` | **no scope declared** — the default is wrong here, and becomes a warning |
| `TableKind.TABLE` | no scope declared; the `"full"` default is already correct |
| `properties` | direct copy; the reader has already projected to managed keys |
| `comment`, `tags`, `partitioned_by`, `clustered_by` | direct copy |
| `foreign_keys` | all owned keys, including self-references, are omitted uniformly and recorded in one warning |
| `supported_features`, `referencing_foreign_keys` | not declarable; dropped silently |

`supported_features` and `referencing_foreign_keys` are dropped without a
warning because neither is declaration state: features are derived from the
column tree at planning time, and inbound keys are owned by other tables.

## Foreign keys: one uniform v1 warning

Foreign keys are deliberately outside the first implementation slice. A
self-reference is locally expressible through `Self`, while an external
relationship requires another declaration object; composite mappings add
another rendering branch. Implementing only the easy subset would create
several policies and a repair language before the basic adoption path exists.
V1 therefore treats every outbound foreign key the same way: it does not render
one and it does not suggest executable repair code.

Omission is not free. `_diff_foreign_keys` (`domain/plan/diff.py:518`) compares
observed against declared with no unmanaged branch, so every omitted key becomes
`DropForeignKey`. No scope avoids this — `METADATA_ASPECTS` (`scope="metadata"`)
includes `FOREIGN_KEYS`, and out-of-scope drift fails validation rather than
being ignored.

**Decision: emit one consequence-only warning as a commented block and on
stderr.** It lists each observed constraint and states that applying the module
as written drops it when the table is otherwise eligible. It does not emit
`ForeignKey(...)`, `Self`, imports, repair instructions, or
variable-name guesses. The module remains importable and directly inspectable,
but an FK-bearing module is explicitly an incomplete scaffold rather than a
clean round trip.

The test pins the limitation: every observed FK is omitted uniformly, the
warning names it, and the only resulting actions are the corresponding
`DropForeignKey`s.

The warning names the constraint **semantically, not as DDL**. `api` cannot
import `adapters` — layers separated by `|` are independent, which is why
`schema -> api.delta_table` needs an explicit `ignore_imports` exemption — so
reaching for `compile_plan` to quote the real `ALTER TABLE` text would be the
first crack in the boundary this design otherwise respects.

## Streaming tables: a separate limitation

Generated declarations never pass `scope`, so they take the `"full"` default.
For an ordinary table that is exactly right. For a streaming table it is not:
`StreamingTableAnnotationsOnly` (`application/validation.py:604`) is an
eligibility check, so it runs unconditionally, cannot be suppressed via
`rules`, and judges the declaration's *claimed aspects* against the observed
kind rather than the drift — meaning it fires even when the table is perfectly
in sync. A generated streaming-table module therefore diffs clean and then
fails validation.

**Decision: warn.** This is independent of FK omission: it is an eligibility
failure rather than known state drift. The warning names the check and gives the
one line that fixes it — `scope="annotations"`, the widest scope a streaming
table admits, and the one that keeps comment management rather than narrowing
to tags. It appears in source and on stderr, and a test pins that a generated
streaming table produces exactly that one validation failure.

The alternative — a `DeltaTable.scope` property, letting the generator emit the
correct scope per kind — is recorded under *Rejected alternatives*. It is the
only option needing no hand edit, and it was rejected on surface cost, not on
correctness.

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

# WARNING: dev.silver.orders has 1 foreign key, not generated in v1.
#
# On an otherwise eligible table, applying this declaration will DROP:
# - orders_customer_id_fk: (customer_id) -> dev.silver.customers(id)

tables = [orders]
```

Four properties of this output are deliberate.

**`tables = [orders]`** makes the module directly runnable through
`delta-engine plan generated:tables`. For the supported ordinary, FK-free case,
that closes the round trip at the CLI. For a documented limitation, the same
entry point exposes the warning's stated consequence. It also satisfies
`load_declarations`, which rejects a bare `DeltaTable` and requires a non-empty
ordered sequence.

**No timestamp or version in the header.** Regenerating an unchanged table
produces byte-identical output, which makes `generate | diff` a usable drift
check for free and keeps regeneration quiet in git.

**Default arguments are omitted.** `scope="full"`, empty comments, empty tag and
property mappings, and `nullable=True` do not appear. The generated module reads
like one a person would write.

**Imports are returned by the rendering pass**, so an `Array(Struct(...))`
column produces its expression and all three required names together. The
module assembly only sorts that dependency set for the import line; it does not
walk the type tree again.

### Variable naming

The table name, sanitised to an ASCII Python identifier: every character outside
`[A-Za-z0-9_]` becomes `_`, a leading digit gets a `t_` prefix, and a Python
keyword gets a `_` suffix. Restricting the generated variable to ASCII handles
Unicode word characters that Python does not accept in identifiers. The rule is
deterministic, with no override flag in v1.

## The reader seam stays internal

The CLI needs one observed table, not an engine. The existing warehouse factory
therefore gains `build_reader(connection)`, hiding the
`WarehouseReader(WarehouseSqlRunner(...))` assembly from the command.

The CLI is the composition root, so its direct `cli → adapters` dependency is
legal. `delta_engine.databricks.__all__` does **not** gain a reader builder:
doing so would expose the internal `CatalogStateReader`/`ObservedTable`
vocabulary solely for one CLI workflow and would create a shallow public seam.
If a real Python generation API is wanted later, it should expose curated
public inputs and results rather than leaking this internal port.

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

**The supported-path oracle is the primary test.** For an ordinary, FK-free
`ObservedTable`: `generate_module` → `exec` the source in a fresh namespace →
pass its complete `tables` collection to `Engine.sync(..., dry_run=True)` using
a reader backed by that exact observed state. The report has no failures and no
changes. The recording executor's `compile` method runs as part of the dry-run
pipeline; its `execute` method is never called.

For fixtures carrying foreign keys, assert the actions are **exactly** the
expected `DropForeignKey`s and nothing else. That pins the blast radius of the
accepted limitation instead of calling the scaffold a round trip. The warning
must name every omitted constraint and its consequence, and must contain no
`ForeignKey(...)`, `Self`, import instructions, or variable-name repair guesses.
One streaming-plus-FK fixture asserts that the two warnings remain independent:
the scope warning describes an eligibility failure, while the FK warning's
apply consequence is explicitly conditional on the table otherwise being
eligible.

The repo already has Hypothesis wired up, so this generalises to a property test
over generated `ObservedTable`s.

**Vocabulary pin.** Every name codegen can emit is in `schema.__all__` (23
names), and every `DataType` subclass (17) has a source renderer. This is the
one place drift between codegen and the public surface would otherwise be
silent — the renderer must emit `Column`, never `DesiredColumn`.

**Golden file** for one representative table covering nested types, tags,
properties, partitioning and a key, so format changes are reviewable in diffs.

**Determinism**: generating the same fixture twice is byte-identical.

**Live** (`tests/live/`): create an ordinary FK-free table, generate, import its
`tables` collection, and run a complete engine dry run; assert no failures and
no changes. The real proof.

Gates: `uv run pytest`, `ruff check`, `ruff format --check`, `mypy .`,
`lint-imports`, `sphinx-build -W`.

## Files

**`src/` (4)**

| File | Change |
| ---- | ------ |
| `api/declaration_source.py` | new — internal Python-source rendering for the supported subset |
| `api/codegen.py` | new — observed-state mapping and `GeneratedModule` assembly |
| `cli/app.py` | register and implement `generate` beside the shared CLI helpers |
| `adapters/databricks/warehouse/factory.py` | `build_reader` |

**`tests/` (7)**

`tests/api/test_declaration_source.py`, `tests/api/test_codegen.py`,
`tests/adapters/databricks/warehouse/test_factory.py`, `tests/cli/conftest.py`,
`tests/cli/test_app_plan.py`, `tests/cli/test_app_generate.py`, and
`tests/live/test_sql_warehouse_live_generate.py`.

**`docs/` (5)**

| File | Change |
| ---- | ------ |
| `reference-cli.md` | says "has one read-only workflow"; becomes two commands |
| `README.md` | CLI paragraph names only `plan` |
| this design and its implementation plan | keep the shipped boundary and task state aligned |
| `todo.md` | record the uniform FK omission and deferred FK feature |

Release: `feat:`. The CLI is additive; the reader factory and codegen modules
are implementation details. Nothing existing moves, so no `BREAKING CHANGE:`
footer.

## Out of scope

- **Foreign-key source generation.** Self references, explicit composite
  mappings, external-parent reads, dependency closure and ordering, variable
  allocation, unique-backed references, and cycle handling are all deferred.
  They need not ship together; v1 makes no sequencing commitment and emits
  facts and consequences rather than partial repair code.
- **Schema-wide discovery.** Needs a table-listing read the engine does not have
  (`information_schema.tables`) and a new port method. It may complement later
  FK work, but FK support can also follow referenced names directly; the two
  capabilities are not the same requirement.
- **The Spark/notebook path.** The reader port is already backend-agnostic, but
  Spark composition and any curated Python generation API are separate design
  work; v1 adds neither to a public facade.
- **`--output FILE`.** stdout redirects.
- **External table `LOCATION`.** Not declarable at all yet — separate open todo.
- **Struct field nullability.** Domain gap, separate open todo. Not a
  round-trip risk: the reader normalises it away on both sides.

## Risks

**Foreign keys are a hard v1 limitation.** The source comment and mirrored stderr
warning make the limitation visible, and the test pins its exact blast radius;
they do not make an FK-bearing scaffold safe to apply unchanged. The CLI
reference says so without embedding a partial repair recipe.

**Other fidelity gaps are benign but worth naming.** `char(n)` and `varchar(n)`
read as `String()` and struct field nullability is unmodelled, so the generated
declaration is narrower than the physical table. Because the reader normalises
identically on the next read, the supported ordinary FK-free path still plans
clean — the generated declaration is faithful to what the *engine can see*.

## Rejected alternatives

**A `serialisation/` package, or `to_x`/`from_x` on domain classes.** Covered
above: the package has no legal position in the layer graph and worsens
locality; the methods weld format vocabulary onto a format-neutral domain and
invert the expression-problem trade-off.

**Routing through the existing `to_dict` contract as a neutral IR.** Wrong
contract. `schema_version: 2` is a **run report** projection with its own
stability guarantee, and it carries no declaration vocabulary. Pure indirection.

**Rendering straight from `ObservedTable`, with no intermediate `DeltaTable`.**
One function, no failure mode. Rejected because the projection logic still has to
exist — it just lives inline in a renderer instead of behind a name — and the
generated source loses its import guarantee. The `DeltaTable` gets constructed
in tests anyway to run the oracle, so the raise is paid for either way and this
just declines to ship it.

**Rendering only self-referential foreign keys.** Expressible through `Self`, but
rejected for v1 because it creates two FK policies and an FK-specific rendering
surface before the basic generator exists. Future FK work may be staged, but v1
has one omission policy.

**Following external foreign-key references.** Reading the referenced names and
emitting their transitive closure can produce a clean module, but it introduces
dependency ordering, collision-free variable allocation, and cycle handling.
Rejected for the basic single-table slice; schema-wide listing is not required
for this approach and remains a separate capability.

**Refusing when foreign keys are present.** Never wrong, but pushes the ordering
problem onto the user exactly when the schema is most complex.

**Emitting every declaration with `scope="tags"`.** No out-of-scope action can
enter an accepted plan, but omitted FKs still appear as out-of-scope drift and
make the scaffold ineligible. Even without keys, tag-only output is close to
useless as a starting point for managing the table.

**Adding a `DeltaTable.scope` property so the generator can emit the right scope
per relation kind.** This is what an earlier draft of the plan did, and it is
the only option that makes a generated streaming-table module plan cleanly with
no hand edit. Rejected because it widens the public surface to serve one
relation kind: every ordinary table — the overwhelming majority, and the whole
point of the on-ramp — already wants the `"full"` default, so the property would
exist to be read back by one branch of one renderer. Streaming remains a
separate warned limitation: its default scope causes an eligibility failure
rather than an expected state-drift action.

**A structured `Undeclarable` residue type** carrying aspect, reason,
consequence and remedy, with foreign keys as one member. Attractive while it
looked like several cases shared a shape. Rejected once the cases were
enumerated: foreign keys are the only *designed* residue, and the others are
rare-to-unobservable and already produce a good error by raising. One type with
one member, whose second member may never arrive, is not worth the indirection
— and `GeneratedModule.warnings` already carries what the CLI needs.
