---
tags:
  - how-to
---

# How to implement a custom adapter

delta-engine ships with two backends today, both for Delta Lake on Databricks: a Spark session backend and a Databricks SQL warehouse backend. A backend is defined by two port protocols — `CatalogStateReader` and `PlanExecutor`, in `delta_engine.application.ports` — and this guide shows how to implement them. See [the hexagonal boundary](explanation-architecture.md#the-hexagonal-boundary) for how the ports fit.

Implementing the two ports is enough for a backend that shares Delta's semantics. A genuinely different table format needs more — see [Adding a genuinely different backend](#adding-a-genuinely-different-backend) below before you start.

## The two protocols

Both live in `delta_engine.application.ports`. You do not inherit from them; you implement the required methods and the engine accepts your class.

### CatalogStateReader

```python
from delta_engine.application.errors import ReadError
from delta_engine.application.ports import CatalogStateReader, CatalogState, TableAbsent, TablePresent
from delta_engine.domain.model import QualifiedName

class MyReader:
    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        try:
            # Return TableAbsent() if the table does not exist.
            # Return TablePresent(table=...) if it does.
            ...
        except Exception as exc:
            # Translate backend-specific failures into the port's error.
            raise ReadError(
                exception_type=type(exc).__name__,
                message=str(exc),
            ) from exc
```

Returning normally means the state is known: the table is either present or
absent. Raise `ReadError` when the adapter cannot determine either state. The
engine catches that specific error for the current table, records a
`ReadFailure`, and continues with independent tables. Backend-specific and
unexpected exceptions are outside the port contract and propagate.

### PlanExecutor

Execution is a two-stage boundary. `compile` turns a domain plan into the backend statements it lowers to, without touching the backend:

```python
from delta_engine.application.errors import ExecutionError
from delta_engine.application.ports import CompiledPlan, PlanExecutor
from delta_engine.domain.plan.actions import ActionPlan

class MyExecutor:
    def compile(self, plan: ActionPlan) -> CompiledPlan:
        return CompiledPlan(
            plan=plan,
            statements=tuple(
                self._render(plan.target, plan.kind, action)
                for action in plan.actions
            ),
        )
```

The engine calls `compile` only with the `ActionPlan` carried by a
`PlanningAccepted` result, on every dry or real run, and records the
compiled plan on the table's report. `CompiledPlan` pairs each statement with
the source-plan action at the same position and rejects missing or additional
statements. A rejected diff has no plan and
never reaches this port. `compile` is **not** total: compiling an accepted plan
is a pure, local operation that cannot fail against a backend, so it may raise
on a genuine programming error rather than swallowing it. The plan carries
both the qualified table target (`plan.target`) and the relation kind its
actions lower against (`plan.kind`). Backends whose DDL dialect differs by
kind read both facts from the plan; a backend with one dialect may ignore the
kind. Databricks, for example, uses `ALTER STREAMING TABLE` for annotation
actions on a streaming table. Eligibility validation ensures that wider
streaming-table declarations never produce an accepted plan.

The engine passes those same compiled statements to `execute` one at a time.
The table they target is already baked into each statement. Returning normally
means the statement succeeded; translate an expected backend failure into
`ExecutionError`:

```python
    def execute(self, statement: str) -> None:
        try:
            self._run(statement)
        except Exception as exc:
            raise ExecutionError(
                exception_type=type(exc).__name__,
                message=str(exc),
            ) from exc
```

The application owns statement indexes, constructs the `ExecutionResult`, and
stops after the first `ExecutionError`. It then records the partial result and
moves on to the next independent table. Exceptions other than
`ExecutionError` are port-contract or programming errors and propagate.

## Wire the engine

Pass your implementations to `Engine`, then sync your declared tables through it. `sync` is variadic — pass one or more `DeltaTable` definitions directly:

```python
from delta_engine import Engine

engine = Engine(reader=MyReader(), executor=MyExecutor())
engine.sync(customers, orders)  # your DeltaTable definitions
```

## Compile actions to statements

`plan.actions` is a sorted tuple containing only `Action` objects. Each action
has a `TableAspect`, a `subject`, and an `ActionPhase`; richer actions also keep
the desired/observed semantic state while exposing stable compiler-facing
properties such as `SetColumnComment.column_name`. You can inspect the action type
with `isinstance` or `match`/`case`:

```python
from delta_engine.domain.plan.actions import CreateTable, AddColumn, DropColumn

for action in plan.actions:
    match action:
        case CreateTable():
            ...
        case AddColumn():
            ...
        case DropColumn():
            ...
```

See `delta_engine/adapters/databricks/sql/compile.py` for a complete example using `functools.singledispatch`.

A backend that targets Databricks itself, over a SQL warehouse connection
instead of a Spark session, does not need to reimplement the SQL layer:
`delta_engine.adapters.databricks.sql` — the compiler, identifier quoting, and
`information_schema` queries — is PySpark-free by contract and is shared
between backends. `delta_engine.adapters.databricks.warehouse` is the live
example: its reader and executor are the only backend-specific code it adds,
built entirely on that shared core. The Spark backend in
`delta_engine.adapters.databricks.spark` is the other consumer, adding its own
reader, executor, and backend-specific error translation.

## Adding a genuinely different backend

The two ports are import-clean: the `domain` and `application` layers never import `pyspark` or `delta`, and your adapter adds no backend imports to them. But import purity is not the whole story. The application layer still encodes Delta-specific policy in ordinary Python — `DELTA_PROPERTY_POLICY` in `application/properties.py`, and validation rules such as `ColumnMappingRequiredForDrop` that exist only because of how Delta handles column drops.

The consequence for a new backend:

- **A Delta-compatible backend** — another Delta-on-Spark or Unity Catalog surface with the same property and safety semantics — can be added with the two ports alone.
- **A different table format**, such as Iceberg, would first need that Delta-specific policy lifted out of the application layer (or made selectable per backend) so its own property model and safety rules could take its place. Implementing the ports is necessary but not sufficient.

See [Import purity versus semantic coupling](explanation-architecture.md#import-purity-versus-semantic-coupling) for the full picture.
