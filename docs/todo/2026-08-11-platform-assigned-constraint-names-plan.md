# Platform-assigned constraint names

## Goal

Stop synthesizing primary- and foreign-key constraint names. When a user omits
the name, preserve that omission through desired state and SQL so Databricks
allocates the physical name.

The governing rule for existing constraints is:

```text
same definition
```

This gives names one narrow meaning:

- no name: Databricks chooses the physical name when creating the constraint;
- explicit name: request that physical name when creating the constraint.

Databricks-generated names are opaque. The engine observes them so it can drop
the corresponding physical constraint later, but it never predicts, persists,
or reproduces them.

## Design

### Desired and observed state

`PrimaryKeyConstraint` and `ForeignKeyConstraint` are declarations. Their
optional `name` is a creation preference and is excluded from equality and
hashing; constraint identity is its relational definition.

`ObservedPrimaryKeyConstraint` and `ObservedForeignKeyConstraint` refine that
model with a required catalog name. The information-schema reader constructs
those concrete types, so planning can use observed names for drops without
casts, assertions, or repeated `None` checks.

### Declaration lowering

The public API passes user intent through unchanged:

- `primary_key_name=None` remains `None`;
- `ForeignKey(name=None)` remains `None`;
- explicit names retain their supplied spelling.

There is no fallback generator. `DeltaTable.primary_key_name` exposes only the
explicitly declared name and therefore returns `None` for an unnamed key.

### Reconciliation

Primary keys use ordinary equality. Any matching column set is already
converged, regardless of the requested and observed names. A definition change
becomes a drop followed by a set.

Foreign keys likewise match by ordinary structural equality. Each declaration
claims one matching observation; unmatched observations are dropped and
unmatched declarations are added, with all drops ordered before additions.
There is no name reservation or asymmetric comparison protocol.

### SQL compilation

The compiler owns the optional Databricks grammar. Private PK and FK renderers
each produce a complete constraint definition, containing the optional name,
identifier quoting, columns, and references. They are used by:

- inline primary keys in `CREATE TABLE`;
- `ALTER TABLE ... ADD PRIMARY KEY`;
- `ALTER TABLE ... ADD FOREIGN KEY`.

Drop actions carry only the concrete observed name they need.

### Reporting

Named additions are identified by name. Unnamed additions are identified by
their columns, and foreign-key detail continues to include the referenced
table. Reports never render `None` or guess the eventual Databricks name.

## Compatibility

Existing tables created with the old `{table}_pk` and
`{table}_{local_columns}_fk` conventions do not churn when declarations omit
names: their structural definitions satisfy the unnamed desired constraints.

Fresh unnamed constraints receive Databricks-generated names. Supplying
`primary_key_name` or `ForeignKey(name=...)` requests a predictable name when
the constraint is first created or later recreated because its definition
changed. Changing only that preference does not rename an existing constraint.

An explicit name collision during creation remains a Databricks execution error
because the namespace spans the schema, including constraints the engine may
not manage.

## Verification

Unit coverage must prove:

- API omission and explicit-name preservation;
- unnamed and named SQL for create/add operations;
- desired and observed constraints compare by definition;
- name-only differences produce no actions;
- multiple foreign keys are matched one-to-one by definition;
- observed constraints reject missing physical names;
- reporting identifies unnamed keys without displaying `None`;
- existing dependency and primary-key safety behavior remains unchanged.

Live coverage must prove:

- Databricks assigns non-empty names to engine-created unnamed keys;
- a second sync produces no changes;
- legacy explicitly named constraints are adopted by matching declarations;
- changing only a requested name leaves an existing constraint untouched.

Validation order:

```bash
uv run pytest <focused files> -q
uv run mypy src
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
uv run --group docs myst-docutils-html \
  docs/todo/2026-08-11-platform-assigned-constraint-names-plan.md
git diff --check
```
