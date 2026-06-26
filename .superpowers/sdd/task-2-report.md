## Status
DONE_WITH_CONCERNS

## Commits
ac5b219 feat: add DropPrimaryKey and SetPrimaryKey action types with DROP_PRIMARY_KEY and SET_PRIMARY_KEY phases

## Tests
191 passed (7 new in test_actions.py, 2 new in test_compile.py). Pre-existing coverage threshold failure at 89.82% vs 90% — was 89.58% before this task; the gap is in unrelated pre-existing files (executor.py, log_config.py, preview.py, types.py).

## Concerns
**Out-of-brief scope addition:** The brief specified changes to `actions.py`, `__init__.py`, and `test_actions.py` only. However, `test_every_action_type_has_a_registered_compiler` in `test_compile.py` is a hard guard that immediately fails when any new `Action` subclass lacks a registered SQL compiler. Without also registering compilers, the full suite had 1 failure.

To keep the suite green, I added:
- SQL compilers for `DropPrimaryKey` and `SetPrimaryKey` to `src/delta_engine/adapters/databricks/sql/compile.py`
- Corresponding tests to `tests/adapters/databricks/sql/test_compile.py`

SQL emitted:
- `DropPrimaryKey` → `ALTER TABLE <tbl> DROP PRIMARY KEY`
- `SetPrimaryKey` → `ALTER TABLE <tbl> ADD CONSTRAINT <name> PRIMARY KEY (<col1>, <col2>, ...)`

If a follow-on task was intended to own SQL compilation for these types, those files will need revision. The implementations are correct Databricks SQL syntax and consistent with the rest of the compiler module.

---

## Bug Fix (follow-on)

### What changed
**Fix 1 — `DropPrimaryKey` compiler** (`compile.py` line 139):
- `DROP PRIMARY KEY` → `DROP PRIMARY KEY IF EXISTS` (idempotent; avoids error when no PK exists)

**Fix 2 — `SetPrimaryKey` compiler** (`compile.py` line 146):
- Removed `backtick()` wrapping from `constraint_name`; constraint names are named identifiers, not column names, and should not be backtick-quoted.

**Fix 3 — `test_drop_primary_key_renders_alter_drop_primary_key`** (`test_compile.py` line 269):
- Assertion updated to expect `DROP PRIMARY KEY IF EXISTS`

**Fix 4 — `test_set_primary_key_renders_add_constraint_primary_key`** (`test_compile.py` line 288):
- Assertion updated: `orders_pk` (unquoted) instead of `` `orders_pk` ``

### Test command run
```
uv run pytest tests/adapters/databricks/sql/test_compile.py -v -k "primary_key"
```

### Test output
```
tests/adapters/databricks/sql/test_compile.py::test_drop_primary_key_renders_alter_drop_primary_key PASSED
tests/adapters/databricks/sql/test_compile.py::test_set_primary_key_renders_add_constraint_primary_key PASSED
2 passed, 19 deselected
```
(Coverage failure is a pre-existing issue from running only 2/21 tests via `-k` filter, not introduced by this fix.)
