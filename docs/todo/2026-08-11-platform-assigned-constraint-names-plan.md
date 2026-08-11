# Platform-assigned constraint names

## Goal

Stop synthesizing primary- and foreign-key constraint names. When a user omits
the name, preserve that omission through desired state and SQL so Databricks
allocates the physical name.

The governing rule is:

```text
same definition
and (desired name is absent or desired name equals observed name)
```

This gives the public options precise meanings:

- no name: the physical name is not desired state;
- explicit name: the physical name is managed state.

Databricks-generated names are opaque. The engine observes them so it can drop
the corresponding physical constraint later, but it never predicts, persists,
or reproduces them.

## Design

### Desired and observed state

`PrimaryKeyConstraint` and `ForeignKeyConstraint` carry `name: str | None`.

- Desired constraints may omit the name.
- Observed constraints must have a name because catalog constraints always have
  a physical identity.
- Exact equality includes the optional name.
- `is_satisfied_by` implements the asymmetric desired-to-observed rule.

This keeps one compact constraint model without weakening exact value equality.

### Declaration lowering

The public API passes user intent through unchanged:

- `primary_key_name=None` remains `None`;
- `ForeignKey(name=None)` remains `None`;
- explicit names retain their supplied spelling.

There is no fallback generator. `DeltaTable.primary_key_name` exposes only the
explicitly declared name and therefore returns `None` for an unnamed key.

### Reconciliation

Primary keys apply `is_satisfied_by` directly. A structurally matching observed
key satisfies an unnamed declaration. An explicit name mismatch, or any
definition mismatch, becomes a drop followed by a set.

Foreign keys reconcile in two stages:

1. Match and reserve explicitly named declarations by physical name.
2. Match unnamed declarations against the remaining observations by definition.

Unmatched observations are dropped and unmatched declarations are added, with
all drops ordered before additions. Reserving explicit names first prevents an
unnamed declaration from adopting an observed constraint whose physical name a
different declaration explicitly requests.

No generic matching framework is introduced; this policy remains inside the
foreign-key differ.

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

Fresh unnamed constraints receive Databricks-generated names. Code that needs
a stable physical name must supply `primary_key_name` or `ForeignKey(name=...)`.
An explicit name collision remains a Databricks execution error because the
namespace spans the schema, including constraints the engine may not manage.

Databricks has no direct constraint rename. Changing an explicit name therefore
requires dropping and recreating the constraint. Existing primary-key reference
safety validation continues to block that replacement while foreign keys depend
on the key.

## Verification

Unit coverage must prove:

- API omission and explicit-name preservation;
- unnamed and named SQL for create/add operations;
- unnamed adoption for matching primary and foreign keys;
- explicit name mismatch produces drop-and-recreate;
- explicit FK names are reserved before unnamed adoption;
- observed constraints reject missing physical names;
- reporting identifies unnamed keys without displaying `None`;
- existing dependency and primary-key safety behavior remains unchanged.

Live coverage must prove:

- Databricks assigns non-empty names to engine-created unnamed keys;
- a second sync produces no changes;
- legacy explicitly named constraints are adopted by unnamed declarations;
- explicitly managed name changes still replace the constraint.

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
