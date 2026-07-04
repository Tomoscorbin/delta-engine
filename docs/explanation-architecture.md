---
tags:
  - explanation
---

# Architecture

delta-engine uses a hexagonal (ports and adapters) architecture layered over a
small domain model. The goal is to keep the planning core independent of any
backend, so a Databricks implementation can be replaced or extended without
changing the domain or application orchestration.

## Package structure

Most code lives in one of four packages:

| Package | Responsibility | Examples |
|---|---|---|
| `delta_engine.api` | User-facing declarations and import surface | `DeltaTable`, `ForeignKey`, `Property` |
| `delta_engine.application` | Use-case orchestration, ports, failures, reports | `Engine`, `CatalogStateReader`, `PlanExecutor`, `validate_diff`, `resolve` |
| `delta_engine.domain` | Backend-free schema snapshots, action plans, and diffing | `DesiredTable`, `ObservedTable`, `ActionPlan` |
| `delta_engine.adapters` | Backend integration and translation | `DatabricksReader`, `DatabricksExecutor`, SQL compiler |

```mermaid
flowchart TB
    User[User code] --> API[api<br/>DeltaTable, ForeignKey]
    API --> Domain[domain<br/>DesiredTable, ObservedTable, ActionPlan]
    App[application<br/>Engine, validation, reports] --> Domain
    Adapters[adapters<br/>Databricks reader, executor, SQL compiler] --> Ports[application ports<br/>CatalogStateReader, PlanExecutor]
    App --> Ports
    Public[delta_engine.__init__<br/>curated public exports] --> API
    Public --> App
    Public -. lazy .-> Adapters
```

The arrows show dependencies. Backend-specific code depends inward on the
application ports and domain vocabulary; the domain does not depend on Spark,
Databricks, or adapter code. The top-level `delta_engine` package eagerly exposes
the pure-Python API and application surface, and lazily exposes Databricks helpers
so importing table declarations does not require PySpark.

## Hexagonal boundary

The application owns the ports. Adapters implement them; the engine only sees the
protocols and typed return values.

```mermaid
flowchart LR
    Engine[Engine] --> ReaderPort[CatalogStateReader<br/>fetch_state]
    Engine --> ExecutorPort[PlanExecutor<br/>execute]
    ReaderPort <|.. Reader[DatabricksReader]
    ExecutorPort <|.. Executor[DatabricksExecutor]
    Reader --> Catalog[Unity Catalog / Spark catalog]
    Executor --> Compiler[SQL compiler]
    Compiler --> Spark[Spark SQL]
```

`CatalogStateReader.fetch_state(qualified_name)` returns one of:

- `TablePresent(table=ObservedTable(...))`
- `TableAbsent()`
- `ReadFailed(failure=ReadFailure(...))`

`PlanExecutor.execute(qualified_name, plan)` returns an `ExecutionSummary` with
one result per attempted action. Both ports are **total**: implementations catch
backend exceptions and return typed failures instead of raising. That boundary is
what lets one table fail while the engine continues reading, planning, and
reporting the rest of the run.

## Sync lifecycle

`Engine.sync(...)` is a phase chain. Each table gets a private run object during
the sync; that run accumulates read state, a plan, failures, and execution
results before being frozen into a public `TableRunReport`.

```mermaid
sequenceDiagram
    participant User
    participant API as DeltaTable/API
    participant Engine
    participant Reader as CatalogStateReader
    participant Domain as Domain planner
    participant Validator
    participant Resolver
    participant Executor as PlanExecutor

    User->>Engine: sync(customers, orders)
    Engine->>API: to_desired_table()
    API-->>Engine: DesiredTable
    Engine->>Reader: fetch_state(qualified_name)
    Reader-->>Engine: TablePresent / TableAbsent / ReadFailed
    Engine->>Domain: diff_table(desired, observed)
    Domain-->>Engine: TableDiff
    Engine->>Validator: validate_diff(diff)
    Validator-->>Engine: ValidationResult
    Engine->>Domain: plan from diff.dimensions
    Domain-->>Engine: ActionPlan
    Engine->>Resolver: resolve(tables, blocked=failed_tables)
    Resolver-->>Engine: dependency order + FK failures
    Engine->>Executor: execute(qualified_name, plan)
    Executor-->>Engine: ExecutionSummary
    Engine-->>User: SyncReport or SyncFailedError(report)
```

The phases are:

1. **Prepare**: lower user-facing table declarations to `DesiredTable` values and
   reject duplicate qualified names.
2. **Read**: ask the reader port for the current catalog state of each table.
3. **Diff**: compute the typed `TableDiff` with `diff_table`.
4. **Validate**: judge the diff with `validate_diff`.
5. **Plan**: construct an `ActionPlan` by iterating `diff.dimensions` after validation.
6. **Resolve**: order tables by foreign-key dependency and block dependents of
   failed tables.
7. **Execute**: execute non-empty plans for tables that have no failures.
8. **Report**: return `SyncReport`, or raise `SyncFailedError` with the report on
   real runs that failed.

## Boundary data shapes

| Shape | Produced by | Consumed by | Purpose |
|---|---|---|---|
| `DeltaTable` | User code | Application preparation | Public declaration object |
| `DesiredTable` | API lowering | Domain planner, resolver, report | Target schema snapshot |
| `ObservedTable` | Reader adapter | Domain planner, report | Catalog schema snapshot |
| `TableDiff` | `diff_table` | Validation, Engine (dimensions) | Typed facts separating observed from desired |
| `ActionPlan` | Engine (from dimensions) | Executor, report | Ordered table-local changes |
| `CatalogState` | Reader port | Engine | Present, absent, or read-failed state |
| `ExecutionSummary` | Executor port | Engine, report | Attempted action outcomes |
| `SyncReport` | Engine | User code | Immutable run result |

## Planning and determinism

An `ActionPlan` is produced by iterating each dimension's `.actions()`; actions are sorted by `ActionPhase` (an `IntEnum`) then alphabetically by subject, producing a stable, predictable sequence regardless of declaration order.

The phase ordering encodes dependency constraints. Each ordering below exists because Databricks rejects the operation otherwise:

- **Foreign keys are dropped first** (before primary key and column drops): a foreign key may reference a column or primary key that a later phase drops, and Databricks rejects dropping anything still referenced by an active foreign key constraint.
- **Primary key drops run before column mutations**, so no constraint references a column being dropped.
- **Primary key sets run after nullability changes**, so columns are guaranteed non-nullable before the constraint is applied.
- **Foreign keys are set last** (after the primary key is set): a foreign key references a primary or unique key, so that key must exist before the foreign key can point at it.

## Diff-first planning

Planning is two pure stages connected by a typed diff. `diff_table(desired,
observed)` produces a `TableDiff` — `TableMissing` when the table does not
exist, else a `TableDrift` recording per-dimension facts (`Added`, `Removed`,
and `Changed` entries for columns, properties, tags, and keys; `Changed`
values for the comment and partitioning). The diff states facts only.
Each dimension in the drift owns its own lowering: `.actions()` returns the DDL
steps to reconcile that aspect. Whether a dimension's drift is permitted is
policy — `validate_diff` evaluates precondition rules against the dimension
tuple, and rules inspect dimension types directly (e.g.
`ColumnDataTypeChangeNotSupported` looks for `ColumnDataTypeChanged` entries;
`PartitioningChangeNotSupported` looks for `PartitioningDimension`). The engine
constructs the `ActionPlan` by iterating dimensions directly after validation —
there is no separate `lower_diff` step and no hidden dependency between lowering
and validation.

## Managed aspects

Every `DesiredTable` carries a `managed_aspects` field: a `frozenset[TableAspect]`
naming the dimensions the engine reconciles for that table. The differ
(`diff_table`) is scope-blind and always computes all dimensions. Scope awareness
lives in validation: the `UnmanagedDimensionDrift` rule fails the sync if any
unmanaged dimension has drifted from the declaration. If validation passes,
`TableDrift` contains only dimensions with actual drift — and since all unmanaged
dimensions were drift-free, those dimensions are absent from the tuple entirely.
`TableDrift.plan()` therefore naturally produces only the managed actions, with
no filtering logic needed.

The public API exposes named modes only: `DeltaTable(metadata_only=True)` maps
to the metadata aspects (comments, tags, key constraints). The `TableAspect`
enum stays internal.

## Constraint-name generation

A key constraint's name is a pure function of the table name and its columns (`{table}_pk` for primary keys, `{table}_{columns}_fk` for foreign keys). It is generated by the API layer when a `DeltaTable` is lowered to a `DesiredTable`, and read from the catalog for observed constraints. Either way, the name arrives as data on the constraint object — the differ and SQL compiler read it directly rather than deriving it. This means the name is set exactly once, at the boundary where the domain model is first populated, and nothing downstream needs to reason about naming policy.

## Foreign key references

A `ForeignKey` declares its target by passing the referenced `DeltaTable` object directly (or the `Self` sentinel for a self-reference), not a dotted `catalog.schema.table` string:

```python
customers = DeltaTable(catalog="dev", schema="silver", name="customers", columns=[...])
orders = DeltaTable(
    ...,
    foreign_keys=[ForeignKey(local_columns=("customer_id",), references=customers)],
)
```

This is a deliberate design choice. An object reference makes the dependency explicit and lets the engine infer the referenced columns from the referenced table's primary key, so the declaration never restates them. The cost is that the referenced table must be declared as a `DeltaTable` in scope: within a module you declare the parent before the child, and a cross-module reference becomes an import. Note this constrains only the *source* order in which tables are declared — the engine still topologically sorts the actual sync order during dependency resolution, so a referenced table is always synced before its dependents regardless of declaration order.

References by name are intentionally not supported in this iteration. If they are ever needed — for a table declared in another module, or one that exists in the catalog but is not managed here — the `references` union can be widened to also accept a `QualifiedName`. That is a backward-compatible addition: existing object-reference declarations keep working, and the new branch would require explicit referenced columns, since a bare name carries no primary key to infer from.

## Foreign key dependency resolution

Foreign keys affect table order, not just table-local SQL order. A referenced
table must be synced before a dependent table tries to apply its FK constraint.
Resolution also propagates failures: if a dependency cannot reach its desired
state in this run, every downstream table that depends on it is blocked.

```mermaid
flowchart LR
    Customers[customers<br/>validation failed] --> Orders[orders<br/>blocked by failed dependency]
    Orders --> Shipments[shipments<br/>blocked by failed dependency]
    Products[products<br/>success] --> OrderLines[order_lines<br/>success]
    Orders --> OrderLines
```

The resolver treats read failures, validation failures, FK failures, and
execution failures as table-level blockers for downstream FKs. It still includes
every registered table in the final report, so users can see the full blast
radius in one run.

## Validation

Each rule implements the `Rule` protocol: a `name` `ClassVar[str]` and an `evaluate(dimensions: tuple[Dimension, ...]) -> tuple[ValidationFailure, ...]` method. Rules inspect the dimensions directly — typically by scanning for a specific dimension type with `isinstance` — and return all violations at once, avoiding a fix-and-rerun cycle per failure.

`validate_diff` dispatches on the diff variant first: a `TableMissing` passes automatically — creating a table from its full declaration is always safe — so no rule ever sees a missing table. For a `TableDrift`, `validate_diff` calls every rule in `DEFAULT_RULES` and aggregates their failures into a `ValidationResult`.

## Lazy pyspark import

`delta_engine.__init__` uses PEP 562 `__getattr__` to defer import of `build_databricks_engine` and `configure_logging` until first access. This means `import delta_engine` and table declarations work without a Spark install — useful for testing and schema-only environments.

## Where to make changes

| Change | Main location | Notes |
|---|---|---|
| Add a new backend | `delta_engine.adapters` | Implement `CatalogStateReader` and `PlanExecutor`; keep backend exceptions inside the adapter. |
| Add a new dimension | `delta_engine.domain.plan.diff` | Add a dimension type with `.actions()`; `diff_table` constructs it. If the dimension represents currently-unsupported drift, add a rule to `validation.py`. No other files change. |
| Add a new action type | `delta_engine.domain.plan` and adapter compiler | Define the action and phase in `actions.py`, emit it from the relevant dimension type's `.actions()` method, then compile it in the backend adapter. |
| Add a safety rule | `delta_engine.application.validation` | Rules inspect the `TableDrift` facts and return `ValidationFailure` values. |
| Add a data type | `delta_engine.domain.model.data_type` and adapter type mapping | The domain type is backend-free; SQL names and Spark parsing live in the Databricks adapter. |
| Change public declarations | `delta_engine.api` | Lower public API choices into domain snapshots before the engine phases begin. |
| Change report output | `delta_engine.application.report` / `rendering` | Keep display formatting out of domain objects. |

## Architectural rules

- Keep PySpark and Databricks imports inside `delta_engine.adapters`.
- Keep the domain backend-free and deterministic.
- Put orchestration and failure policy in the application layer.
- Put backend normalization at adapter boundaries, such as lowercasing catalog
  identifiers or mapping Spark types to domain types.
- Return typed failures across ports instead of raising backend exceptions.
- Let `ActionPlan` own action ordering; callers should not sort plans manually.
