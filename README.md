# delta-engine

Domain-driven library for describing Delta tables, planning safe schema changes, validating them, and executing them on Databricks/Spark.

## Project overview

- Problem: keeping table schemas in sync across environments is error‑prone. Schema changes can break things; hand-written DDL can drift across environments; failures can occur mid-run. Delta Engine solves this by offering declarative table management - teams define the desired schema once, and Delta Engine previews changes, identifies risky operations, and applies updates in a safe, predictable order.
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
- Deterministic naming: Constraint names are derived from a stable scheme to avoid collisions and churn across runs.
- Early validation: Rules check a computed plan before execution (e.g., reject adding NOT NULL columns to existing tables without defaults).
- Single registry: All defined tables live in one registry so the set is managed as a single asset. This centralizes inter-table dependencies (PK/FK, uniqueness, references) and performs guardrails:
  - Enforces unique fully-qualified names (no table collisions).
  - Detects duplicate logical definitions pointing at the same physical table.
  - Optionally enforces a declared namespace/schema policy.
- Focused results: Each run produces a structured report summarizing the sync status of all tables. Failures in individual tables do not halt the sync of others; they’re captured in the report (with SQL/error previews) and surfaced collectively via a single SyncFailedError.
- Adapter-first: The Databricks/Spark adapter compiles plans to SQL and runs them via a SparkSession. Other adapters can be added behind the same ports.

Module outline:

- `delta_engine/adapters/schema`: user‑facing `DeltaTable`, `Column`, and data types.
- `delta_engine/application`: `Engine`, `Registry`, planning/ordering, validation, results/reporting.
- `delta_engine/adapters/databricks`: Databricks reader/executor, SQL compiler, and small helpers.
- `delta_engine/domain`: core models and diffing logic used by the application layer.

See the full architecture diagrams here: [docs/architecture.md](docs/architecture.md).

## Example usage

Requires an active `SparkSession` on Databricks or local Spark.

```python
from delta_engine.adapters.databricks import build_databricks_engine
from delta_engine.application.registry import Registry
from delta_engine.schema import DeltaTable, Column, Integer, String

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
