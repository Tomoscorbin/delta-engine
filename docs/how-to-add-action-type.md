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
    UNSET_PROPERTY = auto()
    SET_TABLE_TAG = auto()
    UNSET_TABLE_TAG = auto()
    DROP_FOREIGN_KEY = auto()
    DROP_PRIMARY_KEY = auto()
    ADD_COLUMN = auto()
    DROP_COLUMN = auto()
    SET_COLUMN_TAG = auto()
    UNSET_COLUMN_TAG = auto()
    SET_COLUMN_COMMENT = auto()
    SET_TABLE_COMMENT = auto()
    SET_COLUMN_NULLABILITY = auto()
    SET_PRIMARY_KEY = auto()
    SET_FOREIGN_KEY = auto()
    # ADD_YOUR_NEW_PHASE = auto()
```

`ActionPlan` sorts by phase then subject automatically — no changes needed there.

## 3. Add a lowering case

In `src/delta_engine/domain/plan/changes.py`, add the action emission inside the relevant change type's `actions()` method. For example, if `UpdateComment` is produced by `ColumnCommentChanged`:

```python
def actions(self) -> tuple[Action, ...]:
    return (UpdateComment(column_name=self.column_name, new_comment=self.desired_comment),)
```

If the action belongs to a new kind of difference, add a new change dataclass in `changes.py` (with an `aspect` `ClassVar[TableAspect]` and an `actions()` method), add it to the `Change` union there, and emit it from the relevant `_diff_*` helper in `src/delta_engine/domain/plan/diff.py`.

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

## 5. Register a diff rendering arm

In `src/delta_engine/application/rendering.py`, register a `singledispatch` arm on `_action_entries` so the action shows up in `render_diff`. Return one or more `DiffEntry` values — each tags the line with a `DiffCategory` (columns, keys, properties, tags, comments), a `+`/`-`/`~` symbol, and its aligned cells:

```python
@_action_entries.register
def _(action: UpdateComment) -> tuple[DiffEntry, ...]:
    text = f"column {action.column_name}: '{action.new_comment}'"
    return (DiffEntry(DiffCategory.COMMENTS, "~", (text,)),)
```

An action may emit several entries across categories (`CreateTable` lists its columns and its primary key), and category grouping in the diff is display-only — it never changes execution order. `test_every_action_type_has_registered_diff_entries` fails if an action has no arm.

## 6. Add a validation rule if needed

If the new action type can be unsafe or is not yet supported, add a rule in `src/delta_engine/application/validation.py`. Rules receive the self-contained drift and usually match concrete change types from `drift.managed_changes`:

```python
from typing import ClassVar
from delta_engine.application.failures import ValidationFailure
from delta_engine.domain.plan.changes import TableCommentChanged
from delta_engine.domain.plan.diff import TableDrift


class NoUnsafeCommentChange:
    name: ClassVar[str] = "NoUnsafeCommentChange"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=f"Operation not allowed: ...",
            )
            for change in drift.managed_changes
            if isinstance(change, TableCommentChanged) and <condition>
        )
```

Add it to `DEFAULT_RULES` in the same file.

## 7. Write tests

Add tests in:

- `tests/domain/plan/test_diff.py` — does the relevant change's `actions()` produce `UpdateComment`?
- `tests/adapters/databricks/sql/test_compile.py` — does the compiler produce the correct SQL?
- `tests/application/test_rendering.py` — does the action render its expected diff entries?
- `tests/application/test_validation.py` — if you added a rule, does it fire correctly?

Run:

```bash
uv run pytest tests/ -v
```

Expected: all tests pass and coverage stays above the configured threshold.
