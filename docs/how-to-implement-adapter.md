---
tags:
  - how-to
---

# How to implement a custom adapter

delta-engine ships with a Databricks adapter. To target a different backend, implement the two port protocols: `CatalogStateReader` and `PlanExecutor`.

## The two protocols

Both live in `delta_engine.application.ports`. Both are `runtime_checkable` Protocols — you do not inherit from them; you implement the required methods and the engine accepts your class.

### CatalogStateReader

```python
from delta_engine.application.ports import CatalogStateReader
from delta_engine.application.results import CatalogState, ReadFailed, TableAbsent, TablePresent, ReadFailure
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
from delta_engine.application.ports import PlanExecutor
from delta_engine.application.results import ExecutionSummary, ExecutionSucceeded, ExecutionFailed, ExecutionFailure
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
                    action_index=i,
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

Pass your implementations to `Engine` directly:

```python
from delta_engine import Engine, Registry

engine = Engine(reader=MyReader(), executor=MyExecutor())
registry = Registry()
# ... register tables ...
engine.sync(registry)
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
