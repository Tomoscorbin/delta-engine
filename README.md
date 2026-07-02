# delta-engine

Declarative schema management for Delta Lake tables on Databricks. Define the schema you want; the engine plans, validates, and applies the DDL.

## Quickstart

```python
from delta_engine import Column, DeltaTable, Integer, String, Registry, build_databricks_engine

customers = DeltaTable(
    catalog="dev",
    schema="silver",
    name="customers",
    columns=[
        Column("id", Integer()),
        Column("name", String()),
    ],
)

registry = Registry()
registry.register(customers)

engine = build_databricks_engine(spark)
engine.sync(registry)  # creates the table, or no-ops if it already matches
```

## How-to guides

- [Getting started](docs/tutorial-getting-started.md) — define a table and run your first sync
- [Handle sync failures](docs/how-to-handle-sync-failures.md) — inspect `SyncReport` and act on each status
- [Declare partitioned tables](docs/how-to-declare-partitioned-tables.md) — use `partitioned_by`
- [Configure table properties](docs/how-to-configure-properties.md) — `Property` enum, defaults, overrides

## Reference

- [Data types](docs/reference-data-types.md) — supported types and Spark SQL equivalents
- [Safe-change rules](docs/reference-safe-change-rules.md) — changes the engine blocks at validation
- [API reference](docs/reference-api.md) — full public API

## Advanced

- [Architecture](docs/explanation-architecture.md) — layers, design decisions, why hexagonal
- [Implement a custom adapter](docs/how-to-implement-adapter.md) — `CatalogStateReader` and `PlanExecutor` protocols
- [Add a new action type](docs/how-to-add-action-type.md) — extend `Action`, `ActionPhase`, and the compiler
- [Contributing](CONTRIBUTING.md) — setup, test, lint, PR workflow
