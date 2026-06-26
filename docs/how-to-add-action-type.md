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

`Action.subject` determines alphabetical sort order within a phase. `ActionPhase` is an `IntEnum` — lower values run first.

## 2. Add a phase constant if needed

If the action belongs to a new execution phase, add it to the `ActionPhase` enum in the same file:

```python
class ActionPhase(IntEnum):
    CREATE_TABLE = auto()
    SET_PROPERTY = auto()
    ADD_COLUMN = auto()
    DROP_COLUMN = auto()
    SET_COLUMN_COMMENT = auto()
    SET_TABLE_COMMENT = auto()
    SET_COLUMN_NULLABILITY = auto()
    COLUMN_TYPE_CHANGE = auto()
    PARTITIONING_CHANGE = auto()
    # ADD_YOUR_NEW_PHASE = auto()
```

`ActionPlan` sorts by phase then subject automatically — no changes needed there.

## 3. Add a differ case

In `src/delta_engine/domain/plan/differ.py`, add the condition that produces your new action. `compute_plan(desired, observed)` is the entry point.

## 4. Register a SQL compiler

In `src/delta_engine/adapters/databricks/sql/compile.py`, register a `singledispatch` handler:

```python
@_compile_action.register
def _(action: UpdateComment, backticked_table_name: str) -> str:
    col = backtick(action.column_name)
    comment = quote_literal(action.new_comment)
    return f"ALTER TABLE {backticked_table_name} ALTER COLUMN {col} COMMENT {comment}"
```

Use `backtick` for identifiers and `quote_literal` for string literals (both in `delta_engine/adapters/databricks/sql/dialect.py`).

## 5. Add a validation rule if needed

If the new action type can be unsafe, add a rule in `src/delta_engine/application/validation.py`:

```python
class NoUnsafeCommentChange:
    name: ClassVar[str] = "NoUnsafeCommentChange"

    def evaluate(self, plan: ActionPlan) -> tuple[ValidationFailure, ...]:
        ...
```

Add it to `DEFAULT_RULES` in the same file.

## 6. Write tests

Add tests in:
- `tests/domain/plan/test_differ.py` — does `compute_plan` produce `UpdateComment` in the right cases?
- `tests/adapters/databricks/sql/test_compile.py` — does the compiler produce the correct SQL?
- `tests/application/test_validation.py` — if you added a rule, does it fire correctly?

Run:
```bash
uv run pytest tests/ -v
```

Expected: all tests pass, coverage above 90%.
