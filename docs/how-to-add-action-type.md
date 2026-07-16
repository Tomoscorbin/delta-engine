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
    desired_comment: str
    observed_comment: str

    aspect: ClassVar[TableAspect] = TableAspect.COLUMN_COMMENTS
    phase: ClassVar[ActionPhase] = ActionPhase.SET_COLUMN_COMMENT

    def __post_init__(self) -> None:
        if self.desired_comment == self.observed_comment:
            raise ValueError("UpdateComment carries no difference")

    @property
    def subject(self) -> str:
        return self.column_name
```

> Note: `SetColumnComment` is already implemented — this is a hypothetical example showing the pattern.

Every action declares its `TableAspect`, carries enough desired and observed
state for validation and reporting, and rejects a no-op payload when that is
representable. Name each field once, semantically (`desired_*` / `observed_*`
for transition state); compilers and renderers read those names directly.
`Action.subject` determines alphabetical sort order within a phase;
`ActionPhase` is an `IntEnum` — lower values run first.

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
    RENAME_COLUMN = auto()
    ADD_COLUMN = auto()
    ALTER_COLUMN_TYPE = auto()
    SET_CLUSTERING = auto()
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

When choosing a phase, mind the rename boundary: a plan may rename columns,
and `RENAME_COLUMN` is the point where the table's column names switch from
observed to desired. An action phased before it may reference only observed
column names (or constraint names, which the rename does not touch); an
action phased after it must use desired names.

## 3. Emit the action directly from the differ

Update the relevant helper in `src/delta_engine/domain/plan/diff.py`. For
example, a matched column comment pair emits the action itself:

```python
if desired.comment != observed.comment:
    actions.append(
        UpdateComment(
            column_name=desired.name,
            desired_comment=desired.comment,
            observed_comment=observed.comment,
        )
    )
```

Actions join the diff's `actions` tuple, so no union edit is needed. If the
comparison cannot be represented as an action, add a frozen difference type in
`unresolvable.py`, name it in `Unresolvable`, and emit it into the diff's
`unresolvable` tuple from `diff.py`. Decide whether it is accepted or
rejected in application validation; the current three unresolvable
differences are rejected by the default policy. Successful `plan_diff`
results must contain actions only.

## 4. Register a SQL compiler

In `src/delta_engine/adapters/databricks/sql/compile.py`, register a `singledispatch` handler:

```python
@_compile_action.register
def _(action: UpdateComment, target: _SqlTarget) -> str:
    col = backtick(action.column_name)
    comment = quote_literal(action.desired_comment)
    return f"{target.alter_table} ALTER COLUMN {col} COMMENT {comment}"
```

Each handler receives a `_SqlTarget` carrying the backticked table name (`target.name`) and the kind-correct ALTER clause (`target.alter_table` — `ALTER TABLE ...` or `ALTER STREAMING TABLE ...`); ALTER-family statements start from `target.alter_table` so every action follows the observed relation kind. A constraint action carries its complete constraint (named when the `DesiredTable` was built, or read from the catalog for an observed one), so the handler renders `action.constraint.constraint_name` directly rather than computing it.

Use `backtick` for identifiers and `quote_literal` for string literals (both in `delta_engine/adapters/databricks/sql/dialect.py`).

## 5. Register a diff rendering arm

In `src/delta_engine/application/diff_entries.py`, register a `singledispatch`
arm on `action_entries` so the action shows up in reports. Return one or more
`DiffEntry` values — each tags the line with a `DiffCategory` (columns, keys,
properties, tags, comments), a `+`/`-`/`~` symbol, and its aligned cells:

```python
@action_entries.register
def _(action: UpdateComment) -> tuple[DiffEntry, ...]:
    text = f"column {action.column_name}: '{action.desired_comment}'"
    return (DiffEntry(DiffCategory.COMMENTS, "~", (text,)),)
```

An action may emit several entries across categories (`CreateTable` lists its columns and its primary key), and category grouping in the diff is display-only — it never changes execution order. `test_every_action_type_has_registered_diff_entries` fails if an action has no arm.

## 6. Add a validation rule if needed

If the new action can be unsafe, add a rule in
`src/delta_engine/application/validation.py`. Rules receive the self-contained
drift and match concrete types from `drift.actions` (or `drift.unresolvable`
when judging unresolvable differences). The scope gate runs before any rule
and short-circuits on out-of-scope drift, so a rule only ever sees
differences the declaration manages — declare the correct `TableAspect` on
the action (step 1) and no scope filtering is needed here:

```python
from typing import ClassVar
from delta_engine.application.failures import ValidationFailure
from delta_engine.domain.plan.actions import UpdateComment
from delta_engine.domain.plan.diff import TableDrift


class NoUnsafeCommentChange:
    name: ClassVar[str] = "NoUnsafeCommentChange"

    def evaluate(self, drift: TableDrift) -> tuple[ValidationFailure, ...]:
        return tuple(
            ValidationFailure(
                rule_name=self.name,
                message=f"Operation not allowed: ...",
            )
            for change in drift.actions
            if isinstance(change, UpdateComment) and <condition>
        )
```

Add it to `DEFAULT_RULES` in the same file. `plan_diff` deliberately exposes
no rules parameter: accepted plans always use this default policy.

## 7. Write tests

Add tests in:

- `tests/domain/plan/test_diff.py` — does `diff_table` emit `UpdateComment` directly with both states?
- `tests/domain/plan/test_actions.py` — does the action declare its aspect, invariants, phase, and compiler properties?
- `tests/application/test_planning.py` — is the action accepted or rejected at the total planning boundary?
- `tests/adapters/databricks/sql/test_compile.py` — does the compiler produce the correct SQL?
- `tests/application/test_rendering.py` — does the action render its expected diff entries?
- `tests/application/test_validation.py` — if you added a rule, does it fire correctly?

Run:

```bash
uv run pytest tests/ -v
```

Expected: all tests pass and coverage stays above the configured threshold.
