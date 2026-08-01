# Fail closed on unsupported string collations

**Status:** Implemented and locally validated; live Databricks evidence remains

**Date:** 2026-08-01

## Goal

Prevent catalog columns with a non-default string collation from being observed as
ordinary `String()` values. Until collation is modeled as declared state, the reader
must fail the table read rather than erase a semantic difference and potentially report
the table as synchronized.

## Scope

This is a contained read-adapter correction:

- inspect collation while mapping Databricks structured type metadata;
- accept the absent/default `UTF8_BINARY` collation;
- reject every other or malformed collation as an unsupported type; and
- preserve the existing recursive failure behavior for arrays, maps, and structs.

The change does not add declarable collations, change the `String` domain value, alter
SQL compilation, or introduce a new read-error variant. Bounded `CHAR` and `VARCHAR`
types continue to normalize to `String()` when their collation is supported.

## Existing boundary

`data_type_from_json` already returns `None` for an unsupported type. Recursive type
mapping propagates that result, and table-description parsing turns it into a
`MetadataParseError`; the shared catalog reader then reports the table read as failed.
The implementation should use that existing path rather than add another exception or
duplicate integration coverage.

## Implementation

### 1. Check collation before normalizing string-like types

In `src/delta_engine/adapters/databricks/sql/types.py`:

1. Remove `string` from `_SIMPLE_TYPES` so it cannot bypass collation inspection.
2. Handle `string`, `char`, `varchar`, and `character` in one explicit branch.
3. Treat an absent collation as the supported default `UTF8_BINARY`.
4. Return `None` for a non-default or non-string collation value.
5. Return `String()` otherwise, retaining the existing bounded-string normalization.

Keep this logic local to the structured type mapper. Do not add collation state to the
domain or make the general parser aware of Databricks collation names.

### 2. Replace the permissive collation test

In `tests/adapters/databricks/sql/test_types.py`:

1. Preserve coverage that an absent collation maps to `String()`.
2. Preserve coverage that explicit `UTF8_BINARY` maps to `String()`.
3. Add representative `string` and bounded-string cases with a non-default collation
   that return `None`.
4. Add a malformed collation value that returns `None`.
5. Add one nested string case that returns `None`, proving the existing recursive
   propagation remains connected.

Prefer a small parameterized matrix. Do not repeat array, map, and struct propagation
tests when one nested example plus the existing recursive-type tests establishes the
behavior.

## Validation

Run the narrow test first:

```bash
uv run pytest tests/adapters/databricks/sql/test_types.py --no-cov -q
```

Then validate the affected adapter boundary and normal static checks:

```bash
uv run pytest tests/adapters/databricks/sql tests/adapters/databricks/test_read.py \
  --no-cov -q
uv run ruff check src/delta_engine/adapters/databricks/sql/types.py \
  tests/adapters/databricks/sql/test_types.py
uv run mypy src
git diff --check
```

## Live evidence

Separately, verify on a credentialed Databricks environment that `DESCRIBE TABLE
EXTENDED ... AS JSON` exposes a non-default collation on a test column and that the
engine reports a read failure. Record the observed metadata spelling and result in the
correctness review. Do not make that credentialed verification a prerequisite for
keeping the local implementation contained.

## Completion criteria

- No non-default or malformed string collation maps to `String()` at any nesting depth.
- Absent and explicit `UTF8_BINARY` collations retain current behavior.
- `CHAR` and `VARCHAR` length normalization is unchanged for supported collations.
- Production changes remain confined to the structured type mapper.
- Focused tests, adapter tests, Ruff, mypy, and `git diff --check` pass.
