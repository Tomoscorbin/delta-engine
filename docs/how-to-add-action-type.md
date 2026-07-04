---
tags:
  - how-to
---

# How to add a new action type

This guide walks through the steps to add a new `Action` subtype to the engine — for example, `UpdateComment` to alter a table or column comment.

## 1. Define the action in the domain

Add a frozen dataclass to `src/delta_engine/domain/plan/actions.py`:

```python
@dataclass(frozen=True, slots=True)
class UpdateComment(Action):
    """Change the comment on a column or table."""
    column_name: str
    new_comment: str

    phase: ClassVar[ActionPhase] = ActionPhase.SET_COLUMN_COMMENT

    @property
    def subject(self) -> str:
        return self.column_name
```

> Note: `SetColumnComment` is already implemented — this is a hypothetical example showing the pattern.

`Action.subject` determines alphabetical sort order within a phase. `ActionPhase` is an `IntEnum` — lower values run first.

## 2. Add a phase constant if needed

If the action belongs to a new execution phase, add it to the `ActionPhase` enum in the same file:

```python
class ActionPhase(IntEnum):
    CREATE_TABLE = auto()
    SET_PROPERTY = auto()
    DROP_FOREIGN_KEY = auto()
    DROP_PRIMARY_KEY = auto()
    ADD_COLUMN = auto()
    DROP_COLUMN = auto()
    SET_COLUMN_COMMENT = auto()
    SET_TABLE_COMMENT = auto()
    SET_COLUMN_NULLABILITY = auto()
    SET_PRIMARY_KEY = auto()
    SET_FOREIGN_KEY = auto()
    # ADD_YOUR_NEW_PHASE = auto()
```

`ActionPlan` sorts by phase then subject automatically — no changes needed there.

## 3. Add a lowering case

In `src/delta_engine/domain/plan/diff.py`, add the action emission inside the relevant drift fact's `actions()` method. For example, if `UpdateComment` is produced by `TableCommentChanged`:

```python
def actions(self) -> tuple[Action, ...]:
    return (UpdateComment(new_comment=self.desired_comment),)
```

If the action belongs to a new kind of difference, add a new drift fact dataclass (with an `aspect` `ClassVar[TableAspect]` and an `actions()` method), add it to the `DriftFact` union, and emit it from the relevant `_diff_*` helper in `diff_table`.

## 4. Register a SQL compiler

In `src/delta_engine/adapters/databricks/sql/compile.py`, register a `singledispatch` handler:

```python
@_compile_action.register
def _(action: UpdateComment, backticked_table_name: str) -> str:
    col = backtick(action.column_name)
    comment = quote_literal(action.new_comment)
    return f"ALTER TABLE {backticked_table_name} ALTER COLUMN {col} COMMENT {comment}"
```

Each handler receives the `backticked_table_name` and renders SQL. A constraint action carries its own name (generated when the `DesiredTable` was built, or read from the catalog for an observed one), so the handler renders `action.constraint_name` directly rather than computing it.

Use `backtick` for identifiers and `quote_literal` for string literals (both in `delta_engine/adapters/databricks/sql/dialect.py`).

## 5. Add a validation rule if needed

If the new action type can be unsafe or is not yet supported, add a rule in `src/delta_engine/application/validation.py`. Rules receive the drift's flat fact tuple and its managed aspects, and match fact types directly:

```python
from typing import ClassVar
from delta_engine.application.failures import ValidationFailure
from delta_engine.domain.model.table_aspect import TableAspect
from delta_engine.domain.plan.diff import DriftFact, TableCommentChanged


class NoUnsafeCommentChange:
    name: ClassVar[str] = "NoUnsafeCommentChange"

    def evaluate(
        self, facts: tuple[DriftFact, ...], managed_aspects: frozenset[TableAspect]
    ) -> tuple[ValidationFailure, ...]:
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=f"Operation not allowed: ...",
            )
            for fact in facts
            if isinstance(fact, TableCommentChanged) and <condition>
        )
```

Add it to `DEFAULT_RULES` in the same file.

## 6. Write tests

Add tests in:
- `tests/domain/plan/test_diff.py` — does the relevant drift fact's `actions()` produce `UpdateComment`?
- `tests/adapters/databricks/sql/test_compile.py` — does the compiler produce the correct SQL?
- `tests/application/test_validation.py` — if you added a rule, does it fire correctly?

Run:
```bash
uv run pytest tests/ -v
```

Expected: all tests pass, coverage above 90%.
