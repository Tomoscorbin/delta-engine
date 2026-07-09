# delta-engine

Declarative schema management for Delta Lake tables on Databricks. Define the schema you want; the engine plans, validates, and applies the DDL.

## Quickstart

```python
from delta_engine.databricks import build_engine
from delta_engine.schema import Column, DeltaTable, Integer, String

customers = DeltaTable(
    catalog="dev",
    schema="silver",
    name="customers",
    columns=[
        Column("id", Integer()),
        Column("name", String()),
    ],
)

engine = build_engine(spark)
engine.sync(customers)  # creates the table, or no-ops if it already matches
```

## How-to guides

- [Getting started](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/tutorial-getting-started.md) — define a table and run your first sync
- [Handle sync failures](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/how-to-handle-sync-failures.md) — inspect `SyncReport` and act on each status
- [Configure a table](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/how-to-configure-table.md) — properties, tags, comments, keys, and partitioning

## Reference

- [Data types](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/reference-data-types.md) — supported types and Spark SQL equivalents
- [Safe-change rules](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/reference-safe-change-rules.md) — changes the engine blocks at validation
- [API reference](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/reference-api.md) — full public API

## Developer

- [Architecture](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/explanation-architecture.md) — layers, design decisions, why hexagonal
- [Implement a custom adapter](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/how-to-implement-adapter.md) — `CatalogStateReader` and `PlanExecutor` protocols
- [Add a new action type](https://github.com/Tomoscorbin/delta-engine/blob/main/docs/how-to-add-action-type.md) — extend `Action`, `ActionPhase`, and the compiler

## Contributing

Commit messages and PR titles follow [Conventional Commits](https://www.conventionalcommits.org/); the PR title drives the automated version bump and changelog. To lint your commit messages locally, run this once after cloning:

```bash
uv run pre-commit install --hook-type commit-msg
```
