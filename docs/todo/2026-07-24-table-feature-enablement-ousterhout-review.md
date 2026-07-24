---
tags:
  - todo
  - architecture
  - review
---

# Table-feature enablement design review

**Status:** Resolved in the PR branch; live Databricks verification still required

**Review date:** 2026-07-24

**Reviewed PR:** [#280 — plan required Delta table-feature enablement](https://github.com/Tomoscorbin/delta-engine/pull/280)

**Reviewed revision:** `6b2f694`

## Final design

When a declaration contains `TimestampNtz` or `Variant`, a new table needs no
separate feature action: CREATE establishes the schema-implied feature. For an
existing table, the domain differ derives the features required by the desired
column trees, subtracts the features observed on the table, and emits one
`EnableTableFeature` action for each missing member.

```text
required_features(desired.columns) - observed.supported_features
    -> EnableTableFeature(...)
    -> dependent column actions
```

The engine remains an orchestration component. It receives no
`DeltaTablePolicy` or feature policy; it continues to pass the raw domain diff
to the application `plan_diff` boundary, which validates rather than rewriting
the completed diff.

## Resolution of the review findings

### 1. Remove parallel desired feature state

The original implementation stored `DesiredTable.implied_features`, populated
by the public `DeltaTable` lowering path. That made a derived fact optional:
another `DesiredTableSource` could return a `TimestampNtz` column without the
matching feature name, and the accepted plan would omit the required upgrade.

The resolved design removes `DesiredTable.implied_features`. Desired columns
are the only source of truth. The domain differ contains one explicit mapping:

```python
TimestampNtz -> TableFeature.TIMESTAMP_NTZ
Variant      -> TableFeature.VARIANT
```

It walks complete nested type trees while diffing an existing table.

### 2. Make observed feature support factual and authoritative

`ObservedTable.supported_features` remains observed catalog state, represented
by the closed `TableFeature` enum rather than unconstrained strings. Unknown
platform-managed features are ignored because the engine never enables or
disables them.

The production reader obtains support from the synthesized
`delta.feature.* = supported` entries already present in
`DESCRIBE ... AS JSON.table_properties`. It extracts them before the property
policy projects user-managed properties, avoiding a separate `DESCRIBE DETAIL`
round trip. The test-only native Spark reader still populates the feature set
from its existing `DESCRIBE DETAIL` row because OSS Spark cannot run the AS JSON
command.

The Databricks adapter keeps catalog recognition and enablement spelling in one
feature definition module. Both readers and the SQL compiler consume that
module, so `variantType-preview` is declared once as the enablement spelling
and is automatically accepted alongside the canonical `variantType` identity.

### 3. Put reconciliation in the domain differ

For `TableDrift`, `diff_table` adds missing feature enablements alongside the
other desired-versus-observed discrepancies. For `TableMissing`, it adds
nothing because CREATE establishes the schema-implied features. `plan_diff`
then validates the completed diff without rewriting it.

This placement has three useful properties:

- the engine receives no policy object;
- every desired-table source follows the same derivation path; and
- validation sees the feature action and can reject it when column structure
  is outside the declaration's managed scope.

`ActionPlan` phase ordering keeps `EnableTableFeature` before `AddColumn` and
`AlterColumnType`.

### 4. Keep feature identity constrained

`TableFeature` is a small domain identity enum used by
`ObservedTable.supported_features` and `EnableTableFeature.feature`. It prevents
an unknown string from passing planning and failing later during compilation.

The enum deliberately carries no type mapping, catalog aliases, or SQL
properties:

- the domain differ owns type-to-feature requirements;
- the reader owns external catalog aliases; and
- the Databricks compiler owns the exceptional VARIANT enable-key spelling.

This keeps the common path direct while isolating verified compatibility
exceptions.

### 5. Complete the report contract

Feature enablement remains a distinct, visible, permanent action even though
its Databricks SQL representation is `SET TBLPROPERTIES`. The public report
contract and action-authoring guide now include
`changes[].kind == "features"`, with a serialization contract test.

## Properties and features

Properties and features should not be unified into a `DeltaTablePolicy`.

They share a physical SQL mechanism but not semantics:

| Concern | Managed property | Schema-required feature |
| --- | --- | --- |
| Intent | Explicitly declared | Derived from desired columns |
| Reconciliation | Exact value or asserted absence | Add missing support only |
| Removal | Sometimes permitted | Never planned |
| Action | `SetProperty` / `UnsetProperty` | `EnableTableFeature` |
| Risk | Property-specific | Permanent protocol upgrade |

Combining them would introduce mode flags and optional fields without removing
work from callers. The implementation therefore keeps the existing property
policy and uses a small feature mapping plus a distinct action.

## Verification

- Full configured suite: `1027 passed, 67 deselected` with 96.71% coverage.
- Static typing: `uv run mypy src` passed.
- Credentialed Databricks tests remain the final compatibility pin for
  create, enable, observe, and resync, including the VARIANT preview enable
  key.
