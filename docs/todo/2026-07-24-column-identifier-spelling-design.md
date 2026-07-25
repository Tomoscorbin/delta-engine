# Preserve column identifier spelling — design

Date: 2026-07-24
Status: accepted for implementation (2026-07-25); dated decision notes below
Branch: `fix/preserve-column-identifier-case`

## Summary

Delta Engine currently lowercases column-like identifiers when domain objects
are constructed. That makes case-insensitive comparison simple, but it also
destroys the spelling that Databricks returns and that some Databricks DDL
paths require.

A live SQL warehouse reproduction proved the concrete failure:

```sql
ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (`requestid`)
```

fails when the catalog column is stored as `requestId`, while the same
statement using `requestId` succeeds. Ordinary `ALTER COLUMN` statements still
resolve the lowercase spelling, so Databricks is not consistent across DDL
paths.

The proposed change separates three concepts that the engine currently
conflates:

1. **Spelling** — the exact identifier declared by the user or observed in the
   catalog.
2. **Identity key** — a derived lowercase string used only where Databricks
   treats column identifiers case-insensitively.
3. **Execution spelling** — the exact post-sync physical spelling an action
   must send back to Databricks.

Column-like identifier spelling will be preserved throughout the model.
Lowercasing will remain only at explicit identity boundaries and for qualified
object names, which Unity Catalog itself stores lowercase. Before an accepted
plan becomes executable, its column references will be bound to the table's
post-sync physical schema. The SQL compiler will continue to quote and emit
the names carried by the plan without performing catalog lookups or
normalization.

## Problem

### Reported failure

After a cluster-mode change, a sync reported five failures:

- Three bronze tables failed planning with `UnmanagedAspectDrift` for column
  structure, column comments, and table comment.
- Two silver tables reached execution but failed while adding primary keys:

```text
[COLUMN_NOT_FOUND_IN_TABLE] Column 'requestid' not found in table ...

ALTER TABLE ...bedrock_message
ADD CONSTRAINT `bedrock_message_pk`
PRIMARY KEY (`requestid`, `message_index`)
```

and:

```text
[COLUMN_NOT_FOUND_IN_TABLE] Column 'requestid' not found in table ...

ALTER TABLE ...bedrock_request_invocation
ADD CONSTRAINT `bedrock_request_invocation_pk`
PRIMARY KEY (`requestid`)
```

The live catalog columns were spelled `requestId`. The declaration also used
`requestId`, but `DesiredColumn`, `ObservedColumn`, and
`PrimaryKeyConstraint` each lowercased their identifiers during construction.
By compilation time, the engine no longer had either the declared or catalog
spelling and could emit only `requestid`.

The cluster-mode change exposed the defect; it did not create the underlying
identifier behavior. The bronze `UnmanagedAspectDrift` failures are also
separate declaration-versus-live-state mismatches. Preserving identifier
spelling fixes the silver constraint failures but does not suppress real
unmanaged drift.

### Live evidence

The live reproduction module is
`tests/live/test_sql_warehouse_live_column_case_repro.py`.

The original evidence commits were:

- `9fd9d78` — added the primary-key reproduction.
- `9d34cae` — pinned the exact-case workaround and convergent resync.

They are carried on this branch as `9721ab3` and `fea3d75`, together with the
two earlier commits that introduced the reproduction module and its control
case.

The relevant GitHub Actions runs were:

- Failing reproduction:
  <https://github.com/Tomoscorbin/delta-engine/actions/runs/30104882066>
- Passing workaround pin:
  <https://github.com/Tomoscorbin/delta-engine/actions/runs/30105095992>

Together the live tests establish four facts:

1. A table created with `requestId` reports that exact display spelling.
2. An ordinary `ALTER COLUMN requestid COMMENT ...` resolves to
   `requestId`.
3. `ADD CONSTRAINT ... PRIMARY KEY (requestid)` fails with
   `COLUMN_NOT_FOUND_IN_TABLE`.
4. Reissuing the same constraint statement with `requestId` succeeds, and
   the engine then observes the constraint and converges.

The failure reproduces through the SQL warehouse backend. It is therefore not
specific to Spark Connect, an all-purpose cluster, or the cluster-mode change.
The failing stack enters the Unity Catalog `createTableConstraint` path, which
is stricter about physical column spelling than ordinary column alteration.

## Cause

The engine chose canonical lowercase storage because Databricks resolves
column identifiers case-insensitively. That decision made comparison easy:
plain Python string equality, set membership, dictionary lookup, and dataclass
equality all appeared to model identifier identity correctly.

The implementation went further than comparison, however. It replaced the
authoritative spelling at construction boundaries:

```python
object.__setattr__(self, "name", self.name.lower())
```

The resulting flow is:

```text
declaration requestId ─┐
                      ├─► model requestid ─► diff requestid
catalog requestId ─────┘                    ─► action requestid
                                           ─► SQL `requestid`
```

This is lossy normalization. Once both sides have become `requestid`, no later
layer can choose the catalog spelling for an existing column or the declared
spelling for a new column.

Case-insensitive identity and exact execution spelling are both real platform
requirements. They cannot be represented by one destructively normalized
string.

## Goals

1. Preserve declared and observed spelling for every column-like identifier.
2. Continue treating identifiers that differ only by case as the same column.
3. Never report case-only column spelling as schema drift.
4. Emit catalog spelling when an action addresses an existing physical
   column.
5. Emit declared spelling when an action creates or renames a column.
6. Bind primary-key, foreign-key, partition, and clustering references to the
   exact columns present after the plan executes.
7. Keep action plans self-contained and SQL compilation mechanical.
8. Keep generated constraint names deterministic across declaration casing.
9. Retain current Unicode normalization semantics by using `str.lower`, not
   `str.casefold`.

## Non-goals

- Managing column display case as drift.
- Supporting case-only column renames. They name the same Databricks
  identifier and remain invalid as `renamed_from` declarations.
- Changing catalog, schema, or table-name normalization. Unity Catalog stores
  those object names lowercase, and the existing live test pins Python
  `str.lower` as the stored form.
- Changing tag-key or tag-value behavior. Unity Catalog tag keys are
  case-sensitive and already remain verbatim.
- Fixing the three reported bronze `UnmanagedAspectDrift` failures.
- Changing type-name, enum-label, report-label, property-value, or
  configuration parsing that happens to call `lower()` or `casefold()`.
- Adding backend-specific retries or fallback SQL after a failed constraint
  statement. The plan should be correct before execution.

## Identifier vocabulary

### Spelling

The exact string at a state boundary:

- A desired spelling comes from the declaration.
- An observed spelling comes from Databricks.

Spelling is retained in domain values and public reports. It is also the
source used when creating a new physical identifier.

### Identity key

A derived key used only for case-insensitive identity:

```python
def identifier_key(name: str) -> str:
    """Return the Databricks identity key without changing stored spelling."""
    return name.lower()
```

The helper should live in a small domain identifier module and be the only
way column-like names are canonicalized. `casefold()` is deliberately not
used. The current live object-name pin distinguishes Python lowercasing from
casefolding (`GRÖßE`.lower() differs from `.casefold()`), and this change
should not silently introduce new Unicode identity semantics.

An identifier index is a dictionary keyed by `identifier_key(name)` whose
values retain the original object:

```python
columns_by_key = {
    identifier_key(column.name): column
    for column in columns
}
```

Any reusable index builder must reject duplicate keys rather than silently
letting the later value win.

### Execution spelling

The spelling that will physically exist at the moment a statement runs:

| Column transition | Execution spelling |
| --- | --- |
| Existing, matched column | Observed spelling |
| Newly added column | Desired spelling |
| Rename source | Observed old spelling |
| Rename target | Desired new spelling |
| Column in a newly created table | Desired spelling |

References in keys and layout declarations do not independently choose
physical spelling. They resolve by identity to one of the columns above.

## Normalization policy

| Value | Stored form | Identity/comparison | SQL form |
| --- | --- | --- | --- |
| Desired top-level column | Declared spelling | `identifier_key` | Desired for create/add/rename |
| Observed top-level column | Catalog spelling | `identifier_key` | Observed for existing operations |
| `renamed_from` | Declared spelling | `identifier_key` | Observed rename source |
| Struct field | Declared/catalog spelling | Recursive semantic type key | Desired when rendering a new type |
| Partition/clustering reference | Original spelling | Resolve by `identifier_key` | Bound post-sync column spelling |
| PK/FK column reference | Original spelling | Canonical signature | Bound post-sync column spelling |
| Observed constraint name | Catalog spelling | `identifier_key` where identity matters | Exact observed spelling |
| Generated constraint name | Deterministic lowercase | `identifier_key` | Generated spelling |
| Catalog/schema/table part | Lowercase | Stored value | Lowercase |
| Tag key/value | Verbatim | Exact, case-sensitive | Verbatim |

## Where lowercase identity is necessary

Lowercasing is not limited to the desired-versus-observed diff. Identifier
identity is judged at several earlier and later boundaries.

### Domain construction and validation

Use identity keys for:

- duplicate top-level column detection;
- duplicate nested struct-field detection;
- partition and clustering reference resolution and duplicate detection;
- primary-key column existence and duplicates;
- foreign-key local and referenced column duplicates;
- nullable primary-key column lookup;
- duplicate foreign keys over the same local column set;
- generated constraint-name collision checks;
- `renamed_from` source existence, uniqueness, and self-rename checks.

The stored values remain unchanged after these validations.

### Public declaration lowering

Use identity indexes for:

- layout validation;
- primary-key resolution;
- same-name foreign-key pairing;
- explicit foreign-key mapping validation;
- local and referenced type lookup;
- CDF reserved-column checks where Databricks treats those identifiers
  case-insensitively.

`ForeignKey.__post_init__` should freeze caller-owned sequences and mappings,
but should not lowercase their contents.

### Diff

Use identity keys for:

- desired/observed column alignment;
- rename-source and rename-target lookup;
- rename-conflict detection;
- projection of partition and clustering references through renames;
- partitioning and clustering equality;
- primary-key signatures;
- foreign-key signatures.

A matched pair keeps both spellings. Any action addressing the existing
column must eventually use the observed spelling, even when the desired
spelling differs only by case.

### Dependency resolution

Use identity-keyed type indexes for both local and referenced columns.
Primary-key and foreign-key signatures must also contain identity keys. This
keeps FK validation case-insensitive without rewriting the constraint's
stored spelling.

### Catalog assembly

Column tag rows and described columns can carry the same identifier with
different display casing. The tag join should key both sides with
`identifier_key`; it should not lowercase the `ObservedColumn.name` itself.

### Action ordering

If deterministic action ordering is intended to be case-insensitive, its sort
key should use `identifier_key(action.subject)` with the original subject as
a deterministic tie-breaker. Ordering must not depend on the old side effect
of constructors lowercasing every action subject.

## Aspects that must preserve spelling

### Columns and rename hints

`DesiredColumn.name`, `ObservedColumn.name`, and
`DesiredColumn.renamed_from` preserve their inputs. A case-only rename remains
invalid because:

```python
identifier_key(column.name) == identifier_key(column.renamed_from)
```

### Constraints

`PrimaryKeyConstraint`, `ForeignKeyConstraint`, and
`ForeignKeyReference` preserve column and constraint-name spelling.

Constraint content identity remains case-insensitive:

- A primary-key signature is the frozenset of column identity keys.
- A foreign-key signature contains canonical local/referenced pairs plus the
  already-canonical `QualifiedName`.
- Foreign-key pairs remain sorted for deterministic identity and DDL, but
  sorting uses the local column's identity key while retaining both original
  spellings.

Observed constraint names must remain exact so a later
`DROP CONSTRAINT <name>` can send the catalog spelling back to Databricks.
Generated names should continue to use canonical table/local-column keys so a
case-only declaration edit does not rename a generated constraint.

### Physical layout

`partitioned_by` and `clustered_by` preserve the spelling supplied by their
boundary. Comparison resolves their elements by identity. Executable layout
actions bind desired references to post-sync physical columns before
compilation.

### Struct fields and type equality

Preserving `StructField.name` makes raw dataclass equality case-sensitive.
The engine must not compensate by restoring constructor lowercasing.

Instead, introduce a semantic data-type identity function that recursively
uses identifier keys for struct-field names:

```text
STRUCT<requestId: STRING>
STRUCT<requestid: STRING>
```

are the same managed type, while their spelling remains available for
rendering and reporting. The semantic type identity must be used by:

- desired/observed column type comparison;
- foreign-key type validation;
- any action invariant that checks whether desired and observed types differ.

Primitive types, decimal parameters, array element types, and map key/value
types retain their existing structural identity.

**Decision (2026-07-25, implementation review):** semantic type identity is
realized as `canonical_data_type(data_type) -> DataType`, which returns the
same type shape with identity-keyed struct-field names, so semantic
comparison remains plain equality on canonical forms. `key_signature`
likewise canonicalizes through `identifier_key` at its single definition
site, making every primary- and foreign-key signature case-insensitive at
one stroke.

## Binding plans to physical names

### Why binding is required

Simply deleting constructor `.lower()` calls fixes only declarations whose
spelling happens to match the existing catalog.

For example:

```text
desired column:  requestid
observed column: requestId
```

These are the same identifier and should produce no column drift. If the
table is missing a primary key, however, compiling the desired constraint
verbatim would still emit `requestid` and reproduce the live failure.

The engine therefore needs one explicit conversion from semantic references
to execution spelling.

### Resulting schema index

After all table diffs are available, derive a resulting-column index for each
registered table:

```text
QualifiedName
  └── identifier_key(column name)
        └── exact spelling after successful execution of this table's plan
```

For a present table:

1. Index observed columns by identity key.
2. Identify unambiguous `RenameColumn` actions.
3. For each desired column:
   - an applied rename uses the desired new spelling;
   - an identity match not being renamed uses the observed spelling;
   - no identity match uses the desired spelling.

For an absent table, every resulting spelling is the desired column spelling.

Removed observed columns need not appear in the resulting index. Drop actions
already carry their exact observed column object.

If a referenced table failed to read or plan, its dependents will later be
blocked by dependency resolution and will not execute. A desired-spelling
fallback can still support a useful dry-run preview, but no executable child
may rely on an unavailable parent binding.

### Planning boundary

`TableDiff.actions` are semantic differences and may still carry desired-side
references. `ActionPlan` is documented as validated and executable, so it
should contain bound execution spelling.

The planning boundary should therefore receive the resulting-schema index and
bind accepted actions while constructing each `ActionPlan`. This preserves the
existing useful invariant:

```text
ActionPlan → exact SQL preview → the same SQL is executed
```

It also avoids adding lookup behavior to the adapter compiler or making
`PlanExecutor.compile` depend on mutable catalog state.

The engine already computes every diff before planning any table. It can build
the cross-table resulting-schema index once between those two phases, then
pass the read-only index into planning. This is necessary for foreign keys:
the child diff knows the referenced table name and logical column references,
but only the full sync snapshot knows the parent's observed physical spelling.

If implementation review finds that a separate binding operation reads more
clearly, it may remain a distinct internal step, but compilation must accept
only a fully bound, self-contained plan. There must not be two plan variants
that the compiler can accidentally confuse.

**Decision (2026-07-25, implementation review):** the conversion is split by
where the information lives. Column-addressing actions are born with their
physical column's spelling at diff time: the rename-projected observed frame
already carries the resulting spelling for every matched column (observed
spelling in place, desired spelling after an applied rename), and add, drop,
and rename actions already carry their true side. The planning binder
resolves only symbolic references — `SetPrimaryKey`, `SetForeignKey` (both
sides, cross-table), `AlterClustering`, and `CreateTable`'s internal
primary-key and layout references — through the resulting-schema index.
Failure scoping follows who guarantees resolution. Own-table references
(primary-key columns, clustering references, `CreateTable`-internal
references, and a foreign key's local columns) are guaranteed resolvable by
domain validation, so a miss there is an engine invariant violation and
fails loudly. The foreign key's referenced side is not guaranteed: an
unregistered, read-failed, or divergent parent legitimately cannot bind, so
any referenced-side miss — a missing parent entry, or a missing column
within a present entry — falls back to the declared spelling. That keeps
compilation-before-resolution intact: the child still compiles preview SQL,
and dependency resolution retains sole ownership of classifying the
foreign-key failure and blocking execution. Removed columns never appear in
the index, and no bound action needs to look one up.

### Action binding rules

| Action | Binding |
| --- | --- |
| `CreateTable` | Resolve PK/layout/FK references to desired column spellings |
| `AddColumn` | Preserve desired spelling |
| `DropColumn` | Preserve observed spelling |
| `RenameColumn` | Observed source, desired target |
| Existing column type/nullability/comment/tag action | Target resulting spelling, normally observed |
| `AlterClustering` | Target table's resulting spelling |
| `SetPrimaryKey` | Target table's resulting spelling |
| `DropPrimaryKey` | No column reference emitted; preserve observed state for reporting |
| `SetForeignKey` local columns | Child table's resulting spelling |
| `SetForeignKey` referenced columns | Parent table's resulting spelling |
| `DropForeignKey` | Preserve observed constraint name |

The binding operation should fail loudly if an accepted action's own-table
reference names no resulting column. Declaration validation makes that
impossible short of an engine defect; silently retaining an unresolved
own-table spelling would recreate the current class of execution failure.
A foreign key's referenced side is the deliberate exception: it falls back
to declared spelling when the parent cannot bind, and dependency
resolution — which runs after compilation by design — owns classifying that
failure and blocking execution.

## Compiler

The compiler should not call `lower()`, perform case-insensitive lookup, read
catalog state, or choose between desired and observed names. It should quote
and emit the exact identifiers carried by its `ActionPlan`.

This keeps the adapter boundary small:

```text
semantic diff
  → validation and physical-name binding
  → self-contained executable plan
  → mechanical SQL rendering
```

No compiler-specific workaround should inspect `SetPrimaryKey` and substitute
camelCase names. Primary keys exposed the model defect, but column comments,
tags, clustering, foreign keys, and future DDL all benefit from the same
correct execution spelling.

## Proposed file map

### New identifier policy module

`src/delta_engine/domain/model/identifier.py`

- `identifier_key(name: str) -> str`
- optionally one small duplicate-detecting index helper if repeated call sites
  justify it;
- no generic public identifier wrapper unless implementation shows raw strings
  cannot maintain the invariants.

A wrapper is not proposed initially. Replacing every public string with a new
value type would expand the API and serialization surface without removing
the need to choose desired versus observed execution spelling.

### Domain model

`src/delta_engine/domain/model/column.py`

- preserve desired/observed names and `renamed_from`;
- use identity keys for a case-only self-rename check.

`src/delta_engine/domain/model/data_type.py`

- preserve struct-field spelling;
- reject duplicate field identity keys;
- add or consume recursive semantic type identity.

`src/delta_engine/domain/model/constraints.py`

- preserve constraint column/name spelling;
- canonicalize only signatures, duplicate checks, sorting keys, and generated
  identity.

`src/delta_engine/domain/model/table.py`

- preserve layout spelling;
- convert structural validation and desired-only invariants to identity-keyed
  indexes.

### API

`src/delta_engine/api/delta_table.py`

- freeze declarations without lowercasing them;
- resolve layout, primary-key, and foreign-key references via identity indexes;
- keep generated constraint names canonical and stable;
- update public documentation that currently promises lowercase return values.

### Diff and planning

`src/delta_engine/domain/plan/diff.py`

- align and compare identifiers by key;
- retain both desired and observed spelling in matched pairs;
- emit matched-column actions with the projected observed (resulting)
  spelling;
- use semantic data-type identity;
- ensure rename projection retains the correct exact spelling.

`src/delta_engine/domain/plan/actions.py`

- review action invariants and deterministic ordering for assumptions that
  subjects were already lowercase.

`src/delta_engine/application/planning.py`

- bind accepted actions through the resulting-schema index before constructing
  executable plans.

`src/delta_engine/application/engine.py`

- construct the read-only cross-table index after diffing and make it available
  to planning without moving catalog knowledge into the compiler.

### Dependency resolution and reader

`src/delta_engine/application/dependency_resolution.py`

- key column-type dictionaries by identifier identity;
- compare FK and PK signatures canonically.

`src/delta_engine/adapters/databricks/sql/rows.py`

- preserve names returned by information schema;
- key the temporary column-tag grouping through `identifier_key`.

`src/delta_engine/adapters/databricks/read.py`

- attach tags using identity keys while retaining `ObservedColumn.name`.

### Compiler

`src/delta_engine/adapters/databricks/sql/compile.py`

- no identifier normalization;
- continue quoting plan values exactly;
- update tests to prove plans arrive with physical spelling.

## Tests

### Domain and API

- Mixed-case desired and observed columns preserve their spelling.
- `requestId` and `requestid` are rejected as duplicate identifiers in one
  schema.
- Mixed-case layout, PK, FK, and rename references resolve to their columns.
- Duplicate PK/FK/layout/struct references are rejected case-insensitively.
- A case-only `renamed_from` remains invalid.
- Observed constraint names preserve catalog spelling.
- Generated constraint names are identical across declaration casing.
- Public accessors return preserved spelling; release notes call out the
  behavior change.

### Diff

- Desired `requestid` and observed `requestId` align as one column.
- Case-only spelling produces no column, layout, key, or nested-type drift.
- A genuine difference such as `request_id` versus `requestId` still produces
  structural drift.
- Existing column actions retain or bind observed spelling.
- Adds and rename targets retain desired spelling.
- Rename sources use observed spelling even when the hint casing differs.

### Planning and compilation

- Adding a PK to observed `requestId` emits `requestId` even if the declaration
  says `requestid`.
- Creating a table whose column is `requestId` but whose PK reference is
  `REQUESTID` emits the column definition and inline PK reference as
  `requestId`.
- Existing comment, tag, nullability, type, and clustering actions use
  observed physical spelling.
- FK local columns use the child's resulting physical spelling.
- FK referenced columns use the parent's resulting physical spelling.
- FK binding covers an existing parent, a new parent, a renamed parent key,
  and a self-reference.
- Drops use exact observed constraint names.
- Dry-run SQL is byte-for-byte the SQL later passed to execution.

### Catalog assembly

- Tags returned for `requestid` attach to an observed `requestId` column
  without changing `ObservedColumn.name`.
- Constraint rows preserve column and constraint spelling.

### Live Databricks

The carried reproduction currently expects the engine's PK addition to fail
and then applies the exact-case SQL manually. During implementation, invert
that test:

1. `engine.sync(declaration)` succeeds directly.
2. Planned SQL contains ``PRIMARY KEY (`requestId`)``.
3. The catalog reports the PK on `requestId`.
4. A second sync is a no-op.

Retain the raw lowercase `ALTER COLUMN` test as a platform fact and the
`request_id` control as proof that genuine structural mismatch is still
detected.

Add a parallel live foreign-key reproduction before declaring the work
complete. It should independently vary the child and parent display casing so
both sides of constraint binding are exercised.

Run focused tests first, then the complete local suite, then the opt-in Live
Databricks Tests workflow.

## Compatibility

The public declaration currently exposes normalized lowercase values through
properties such as:

- `Column.name`;
- `DeltaTable.partitioned_by`;
- `DeltaTable.clustered_by`;
- `DeltaTable.primary_key`;
- `ForeignKey.columns`.

After this change they will expose preserved declaration spelling. That is an
intentional behavior correction and may affect callers or snapshots that
assert lowercase values. It should be included in release notes.

Identifier identity does not change: declarations that differ only in case
still name the same Databricks column, still collide within one schema, and
still compare as the same desired/observed state.

Reports become more truthful because desired and observed values can retain
their boundary spelling. Any report consumer that previously relied on
lowercase column names should apply its own presentation policy rather than
depending on lossy engine storage.

## Risks and mitigations

### Missed exact-string lookup

Removing constructor normalization without converting every lookup would
cause false missing-column errors or false drift.

Mitigation: inventory all name-keyed sets and dictionaries; add mixed-case
tests at construction, diff, dependency, reader, and planning boundaries.

### Unbound plan reaches compilation

If an action keeps a desired reference instead of execution spelling, the
original PK failure can recur.

Mitigation: keep physical binding at the single planning boundary and test
exact compiled SQL. The compiler must never guess.

### Cross-table foreign-key spelling

The child table alone cannot know the parent's observed spelling.

Mitigation: build the resulting-schema index from the complete sync after all
diffs exist, not independently inside each table's diff.

### Nested type equality becomes case-sensitive

Preserving `StructField.name` changes raw dataclass equality.

Mitigation: introduce and consistently use recursive semantic type identity
where the engine judges drift or FK compatibility.

### Constraint-name churn

Preserving desired casing in generated names could make a case-only
declaration edit look like a constraint rename.

Mitigation: keep generated constraint names based on canonical identity keys;
preserve only catalog-observed or explicitly supplied physical names.

### Accidental case management

Once both spellings are visible, a raw string comparison could report display
case as drift.

Mitigation: tests must explicitly prove case-only declaration/catalog
differences converge with no action.

## Implementation sequence

1. Add the identifier-key and semantic data-type identity helpers with focused
   unit tests.
2. Preserve spelling in column, struct, constraint, table, and public API
   constructors.
3. Convert domain/API validation and generated-name logic to explicit identity
   keys.
4. Convert diff alignment, rename projection, layout comparison, and
   constraint signatures.
5. Convert dependency type lookup and catalog tag assembly.
6. Build the cross-table resulting-schema index.
7. Bind accepted actions while constructing executable plans.
8. Update compiler and report expectations without adding normalization there.
9. Invert the carried PK live reproduction and add the FK live reproduction.
10. Update user documentation and release notes that currently promise
    lowercase column-like identifiers.
11. Run focused, full local, and Live Databricks test suites.

Each step should leave duplicate detection and case-insensitive resolution
covered. Do not land an intermediate state that merely removes `.lower()`
while exact-string lookups still depend on it.

## Acceptance criteria

- No column-like model constructor destructively lowercases spelling.
- All case-insensitive identity operations use one explicit helper.
- Qualified object-name normalization remains unchanged and live-pinned.
- Case-only desired/observed differences are no-ops.
- Existing-column actions emit observed physical spelling.
- New and renamed columns emit desired physical spelling.
- PK and FK actions emit post-sync physical spelling on every side.
- The live PK reproduction succeeds without manual SQL and converges.
- A live FK reproduction proves local and referenced exact-case binding.
- The real-name mismatch control still reports structural drift.
- The full local suite and Live Databricks Tests workflow pass.
