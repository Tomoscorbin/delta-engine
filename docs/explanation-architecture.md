---
tags:
  - explanation
---

# Architecture

delta-engine uses a hexagonal (ports and adapters) architecture layered over a domain-driven design. This keeps the core planning logic independent of any backend, so the Databricks adapter can be swapped or extended without touching the domain or application layers.

## The four layers

Dependencies point strictly inward: Adapters → Ports → Application → Domain.

```
Adapters  (databricks reader, executor, SQL compiler)
   │
Ports     (CatalogStateReader, PlanExecutor — Python Protocols)
   │
Application  (Engine, Registry, validate_plan, result types)
   │
Domain    (DeltaTable model, Column, ActionPlan, differ)
```

### Domain

Pure Python — zero imports outside the standard library. Defines immutable value objects (`Column`, `QualifiedName`, data types) and the planning logic (`compute_plan`, `ActionPlan`, `Action` subtypes). No knowledge of Spark, Delta, or any backend.

### Application

Orchestrates the sync loop: iterate the registry, fetch state via the reader port, compute a plan, validate it, execute via the executor port, collect results. Owns `Engine`, `Registry`, `validate_plan`, and all result/report types. No knowledge of Databricks.

### Ports

Two `Protocol` interfaces are the only seams the engine crosses:

- `CatalogStateReader.fetch_state(qualified_name)` — returns the table's current state or a `ReadFailed`.
- `PlanExecutor.execute(qualified_name, plan)` — executes the plan and returns an `ExecutionSummary`.

Both are **total**: implementations must catch all exceptions internally and return a typed failure rather than raising. This ensures a failure on one table never aborts the sync of others.

### Adapters

The `delta_engine.adapters.databricks` package implements both ports for Databricks/Spark. The SQL compiler uses `functools.singledispatch` — one registered handler per `Action` subtype. Type mapping uses structural `match`/`case` patterns.

## Planning and determinism

`compute_plan(desired, observed)` diffs the desired declaration against the observed catalog state and returns an `ActionPlan`. Actions are sorted by `ActionPhase` (an `IntEnum`) then alphabetically by subject, producing a stable, predictable sequence regardless of declaration order.

The phase ordering encodes dependency constraints. Each ordering below exists because Databricks rejects the operation otherwise:

- **Foreign keys are dropped first** (before primary key and column drops): a foreign key may reference a column or primary key that a later phase drops, and Databricks rejects dropping anything still referenced by an active foreign key constraint.
- **Primary key drops run before column mutations**, so no constraint references a column being dropped.
- **Primary key sets run after nullability changes**, so columns are guaranteed non-nullable before the constraint is applied.
- **Foreign keys are set last** (after the primary key is set): a foreign key references a primary or unique key, so that key must exist before the foreign key can point at it.

## Sentinel actions

`ColumnTypeChange` and `PartitioningChange` are actions that are never executed. They exist solely to make unsupported drift visible in the plan so the validation layer can reject it with a clear message. The SQL compiler raises `AssertionError` if either reaches compilation — encoding the invariant that validation always runs first.

## Validation

Each rule implements a `Rule` protocol: a `name` class variable and an `evaluate(plan)` method returning zero or more `ValidationFailure` objects. `validate_plan` runs all rules in `DEFAULT_RULES` and aggregates failures. Rules receive the full plan, so they can reason across actions (e.g. "does this plan add a NOT NULL column to a table that already exists?").

## Lazy pyspark import

`delta_engine.__init__` uses PEP 562 `__getattr__` to defer import of `build_databricks_engine` and `configure_logging` until first access. This means `import delta_engine` and table declarations work without a Spark install — useful for testing and schema-only environments.
