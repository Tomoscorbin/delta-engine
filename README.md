# delta-engine

Domain-driven library for describing Delta tables, planning safe schema changes, validating them, and executing them on Databricks/Spark.

## Project overview

- Problem: keeping table schemas in sync across environments is error‑prone. Schema changes can break things; hand-written DDL can drift across environments; failures can occur mid-run. Delta Engine solves this by offering declarative table management - teams define the desired schema once, and Delta Engine identifies risky operations, applies updates in a safe, predictable order, and reports the outcome with SQL previews.
- What this package does: you declare desired tables. The engine computes a plan (create/add/drop), validates that plan against rules, and then executes it. It reports results with clear statuses and short previews.
- Table validation: By validating plans before any SQL is executed, predictable breakages are caught early. This reduces the chance of broken or partially-built tables and avoids starting work that would fail.
- Where it fits: this focuses on DDL for Delta tables on Databricks. It complements ETL/ELT tools and schedulers. It does not move data, run transformations, or manage jobs. The design is adapter‑based, so other backends could be added; the provided adapter targets Databricks/Spark.

## Architecture/Design

High‑level flow:

```
                                                      
User defined table  ──▶  Registry  ──▶  Engine
                                           │
                                           ▼          
           ┌───────────────┬───────────────┬───────────────┬───────────────┐
           │ Read Catalog  │ Plan Actions  │ Validate Plan │ Execute Plan  │   ──▶  Report
           └───────────────┴───────────────┴───────────────┴───────────────┘
```

Key design choices:
- Clear separation: Domain models and planning are independent of any engine. Adapters implement small ports for reading catalog state and executing plans.
- Deterministic planning: Actions are ordered (create → adds → drops; subjects alphabetical) for stable, predictable diffs and runs.
- Early validation: Rules check a computed plan before execution (e.g., reject adding NOT NULL columns to existing tables without defaults).
- Single registry: All defined tables live in one registry so the set is managed as a single asset. Enforces unique fully-qualified names (no table collisions).
- Focused results: Each run produces a structured report summarizing the sync status of all tables. Failures in individual tables do not halt the sync of others; they’re captured in the report (with SQL/error previews) and surfaced collectively via a single SyncFailedError.
- Adapter-first: The Databricks/Spark adapter compiles plans to SQL and runs them via a SparkSession. Other adapters can be added behind the same ports.

Module outline:

- `delta_engine/schema`: user‑facing `DeltaTable`, `Column`, and data types.
- `delta_engine/application`: `Engine`, `Registry`, planning/ordering, validation, results/reporting.
- `delta_engine/adapters/databricks`: Databricks reader/executor, SQL compiler, and small helpers.
- `delta_engine/domain`: core models and diffing logic used by the application layer.

See the full architecture diagrams here: [docs/architecture.md](docs/architecture.md).

## Example usage

Requires an active `SparkSession` on Databricks or local Spark.

```python
from delta_engine import (
    Column,
    DeltaTable,
    Integer,
    Registry,
    String,
    build_databricks_engine,
    configure_logging,
)

configure_logging()  # optional: install the package's coloured log handler
engine = build_databricks_engine(spark)
registry = Registry()

customers = DeltaTable(
    catalog="dev",
    schema="silver",
    name="customers",
    columns=[
        Column("id", Integer()),
        Column("name", String()),
    ],
)

registry.register(customers)
engine.sync(registry)
```

Notes:

- If validation fails, no SQL is executed for that table and `SyncFailedError` is raised with a report.
- If the observed schema already matches the desired schema, the plan is a no‑op.
- The Databricks adapter compiles actions to Spark SQL (CREATE TABLE, ALTER TABLE ADD/DROP COLUMN) and executes them with `spark.sql`.
- Execution stops at the first failed action for a table: the engine is not transactional, so a failed statement leaves the actions before it applied and the rest unattempted. Re-running re-plans from the live state.

## Safe-change rules

Delta Engine reconciles schemas declaratively, but some changes cannot be made
safely in place. These are rejected at validation (before any SQL runs) so they
surface as a clear `SyncFailedError` rather than a partial migration:

- **Adding a `NOT NULL` column to an existing table** — existing rows have no
  value for it. Add it nullable, backfill, then tighten.
- **Tightening an existing column to `NOT NULL`** — fails if the column already
  holds NULLs. Keep it nullable, backfill the NULLs, then set `NOT NULL`.
- **Changing a column's data type** — type migrations are not supported; recreate
  the table to change a type. (Without this guard the change would silently do
  nothing, since the differ matches columns by name.)
- **Changing partitioning** — partitioning is fixed at creation; recreate the
  table to repartition.

## Supported column types

The engine maps the following Spark column types to and from its domain model:

| Domain type | Spark SQL type |
| --- | --- |
| `Integer` | `INT` |
| `Long` | `BIGINT` |
| `Float` | `FLOAT` |
| `Double` | `DOUBLE` |
| `Boolean` | `BOOLEAN` |
| `String` | `STRING` |
| `Date` | `DATE` |
| `Timestamp` | `TIMESTAMP` |
| `Decimal` | `DECIMAL(precision, scale)` |
| `Array` | `ARRAY<...>` (of any supported element type) |
| `Map` | `MAP<..., ...>` (of any supported key/value types) |

A column whose Spark type is outside this set (e.g. `STRUCT`, `BINARY`,
`VARIANT`, `TIMESTAMP_NTZ`) is **skipped, not failed**: it is left unmanaged and
a warning naming the column and its type is logged, while every other column on
the table is still managed normally. This mirrors the declared-subset handling
of properties below — the engine manages what it understands and leaves the rest
untouched. A table whose columns are *all* unmappable surfaces as a read failure
for that table alone.

## Non-goals and limitations

Delta Engine manages **DDL for Delta tables only**. It deliberately does not
cover, and the following are known boundaries to be aware of:

- **No data movement or transformation** — it changes schema, not rows. It
  complements ETL/ELT tools and schedulers rather than replacing them.
- **Column renames are not modelled and cause data loss.** A rename reads as
  "drop the old column, add a new one", because columns are matched by name.
  The drop is explicit in your declared schema and visible in the plan, but the
  old column's data is gone. To preserve data, rename out of band (e.g.
  `ALTER TABLE ... RENAME COLUMN`) and update the definition to match.
- **Dropping a column requires `delta.columnMapping.mode=name`** (the default).
  Overriding it to `none` while dropping a column will fail at execution time.
- **Properties are a declared subset.** The engine only reconciles property keys
  you declare; properties set on the table out of band (e.g. by Databricks) are
  left untouched, never unset.
- **Unmappable column types are skipped, not managed.** Columns whose Spark type
  is outside the [supported set](#supported-column-types) are left unmanaged with
  a logged warning; they are neither created, altered, nor dropped.
- **No support for views, constraints, generated/identity columns, or liquid
  clustering** — these are out of scope for the current DDL-only model.
- **Table creation is not race-safe against concurrent writers.** Creation is
  compiled as `CREATE TABLE IF NOT EXISTS`, so if another process creates the
  table between the engine's read and its execution, the statement no-ops and the
  run reports success without reconciling that table's schema. The next sync
  re-reads the live state and plans any drift. Run a single writer per table if
  you need create-time schema guarantees.
