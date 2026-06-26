# delta-engine

Declarative schema management for Delta Lake tables on Databricks. Define the schema you want; the engine plans, validates, and applies the DDL.

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
