---
tags:
  - how-to
---

# How to implement a custom adapter

delta-engine ships with a single backend today: Delta Lake on Databricks. A backend is defined by two port protocols — `CatalogStateReader` and `PlanExecutor`, in `delta_engine.application.ports` — and this guide shows how to implement them. See [the hexagonal boundary](explanation-architecture.md#the-hexagonal-boundary) for how the ports fit.

Implementing the two ports is enough for a backend that shares Delta's semantics. A genuinely different table format needs more — see [Adding a genuinely different backend](#adding-a-genuinely-different-backend) below before you start.

## The two protocols

Both live in `delta_engine.application.ports`. You do not inherit from them; you implement the required methods and the engine accepts your class.

### CatalogStateReader

```python
from delta_engine.application.ports import CatalogStateReader, CatalogState, ReadFailed, TableAbsent, TablePresent
from delta_engine.application.failures import ReadFailure
from delta_engine.domain.model import QualifiedName

class MyReader:
    def fetch_state(self, qualified_name: QualifiedName) -> CatalogState:
        try:
            # Return TableAbsent() if the table does not exist.
            # Return TablePresent(table=...) if it does.
            ...
        except Exception as exc:
            # MUST catch all exceptions and return ReadFailed — never raise.
            return ReadFailed(
                failure=ReadFailure(
                    exception_type=type(exc).__name__,
                    message=str(exc),
                ),
            )
```

`fetch_state` is **total**: it must never raise. Return `ReadFailed` for any error. A raised exception escapes the engine's per-table error boundary and aborts the entire sync.

### PlanExecutor

```python
from delta_engine.application.ports import PlanExecutor, ExecutionSummary, ExecutionSucceeded, ExecutionFailed
from delta_engine.application.failures import ExecutionFailure
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.plan.actions import ActionPlan

class MyExecutor:
    def execute(self, qualified_name: QualifiedName, plan: ActionPlan) -> ExecutionSummary:
        results = []
        for i, action in enumerate(plan.actions):
            try:
                self._run(action)
                results.append(ExecutionSucceeded(
                    action=repr(action),
                    action_index=i,
                    statement_preview=repr(action),
                ))
            except Exception as exc:
                results.append(ExecutionFailed(
                    action=repr(action),
                    failure=ExecutionFailure(
                        action_index=i,
                        exception_type=type(exc).__name__,
                        message=str(exc),
                        statement_preview=repr(action),
                    ),
                ))
                break  # stop at first failure — the engine is non-transactional
        return ExecutionSummary(results=tuple(results))
```

`execute` is also **total**. Stop at the first failure and return the summary — the engine records partial results and moves on to the next table.

## Wire the engine

Pass your implementations to `Engine`, then sync your declared tables through it. `sync` is variadic — pass one or more `DeltaTable` definitions directly:

```python
from delta_engine import Engine

engine = Engine(reader=MyReader(), executor=MyExecutor())
engine.sync(customers, orders)  # your DeltaTable definitions
```

## Compile actions to statements

`plan.actions` is a sorted tuple of `Action` objects. Each action has a `subject` (the column or table name it targets) and an `ActionPhase`. You can inspect the action type with `isinstance` or `match`/`case`:

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

## Adding a genuinely different backend

The two ports are import-clean: the `domain` and `application` layers never import `pyspark` or `delta`, and your adapter adds no backend imports to them. But import purity is not the whole story. The application layer still encodes Delta-specific policy in ordinary Python — the Delta table-property registry in `application/properties.py`, and validation rules such as `ColumnMappingRequiredForDrop` that exist only because of how Delta handles column drops.

The consequence for a new backend:

- **A Delta-compatible backend** — another Delta-on-Spark or Unity Catalog surface with the same property and safety semantics — can be added with the two ports alone.
- **A different table format**, such as Iceberg, would first need that Delta-specific policy lifted out of the application layer (or made selectable per backend) so its own property model and safety rules could take its place. Implementing the ports is necessary but not sufficient.

See [Import purity versus semantic coupling](explanation-architecture.md#import-purity-versus-semantic-coupling) for the full picture.
