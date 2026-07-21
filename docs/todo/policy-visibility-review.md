---
tags:
  - todo
  - architecture
---

# Policy visibility review

This review is about code organization and discoverability, not adding
capabilities or changing the underlying policy. The question is whether a
reader can quickly find where policy decisions are made.

## Overall assessment

The code already has several good policy homes:

- `ActionPhase` is the visible owner of action precedence.
- `MANDATORY_SCOPE_GATES` and `DEFAULT_SAFETY_RULES` together expose the
  complete validation policy.
- `Engine` owns run ordering, dependency blocking, and failure propagation.
- `diff.py` owns desired/observed comparison semantics.
- `sql/compile.py` owns backend statement lowering.
- `adapters/databricks/read.py` is the correct shared production boundary for
  relation/provider admission.

The main remaining issue is policy that emerges from several modules rather
than one named owner.

## Consolidation opportunities

### Property ownership — consolidated

Property decisions are applied at four lifecycle points:

- Declaration admission and value validation in `api/delta_table.py`.
- Observation filtering in `adapters/databricks/read.py`.
- Exact-declaration comparison in `domain/plan/diff.py`.
- Transition and undeclared-property enforcement in
  `application/validation.py`.

Those locations now delegate ownership judgments to the deep
`DELTA_PROPERTY_POLICY` boundary in `application/properties.py` through:

- `validate_declaration(properties)`
- `project_observed(properties)`
- `permits_transition(name, observed, desired)`
- `permits_removal(name, observed)`

The read assembly remains in `read.py`, while the ownership decision is
explicit:

```python
properties=DELTA_PROPERTY_POLICY.project_observed(description.table_properties)
```

Property diff production can remain in `diff.py`; that is comparison logic,
not property-policy ownership.

### Validation composition — consolidated

Validation composition is now explicit in `application/validation.py` through
two adjacent values:

- `MANDATORY_SCOPE_GATES` lists the scope checks that always run; and
- `DEFAULT_SAFETY_RULES` lists the safety rules callers may replace at the
  lower-level validation boundary.

Every scope gate implements `ScopeGate` over the complete `TableDiff` union,
returning no failures for an irrelevant diff arm. `validate_diff` evaluates
all mandatory gates in declaration order and returns their accumulated
failures before any safety rule runs. Once the gates pass, a `TableMissing`
needs no further judgement, while a `TableDrift` is evaluated by every
configured `SafetyRule`. The adjacent tuples therefore show every validation
mechanism and its deterministic evaluation order in one short block.

### Named scopes

Scope semantics are spread across:

- scope-to-aspect mapping in `api/delta_table.py`;
- streaming-table allowed aspects in `application/validation.py`;
- the property exception in `domain/plan/diff.py`; and
- managed foreign-key filtering in `application/dependency_resolution.py`.

The latter two need to act locally, but the definitions of `full`, `metadata`,
and `tags` should have one owner. An `application/scopes.py` module could hold
the named aspect sets and their mapping. The API would translate public scope
names there, and streaming validation would reuse the same tag-aspect set
instead of restating it.

The domain should continue to receive only `managed_aspects`; it should not
import application policy.

### Execution sequencing

`adapters/databricks/execution.py` owns the stop-on-first-failure loop, even
though both Databricks executors share it and adapter documentation describes
the behavior as a general adapter contract.

If stop-on-first-failure is engine-wide policy, raise it to the application
boundary. One possible shape is for adapters to execute and translate one
statement, while application code owns iteration and stopping. If the policy
is intentionally adapter-specific, the port documentation should say so
explicitly.

### Constraint naming

The documentation describes constraint names as API-generated, but
`PrimaryKeyConstraint.generate()` and `ForeignKeyConstraint.generate()` live
in the domain model. Naming physical catalog objects is deployment policy, not
a domain invariant.

Move the generation helpers beside `DeltaTable`/`ForeignKey` lowering. The
domain constraint should validate a supplied name; the API lowering should
choose it.

### Declaration validation boundary

The declaration checks are in the right broad layer: `api/delta_table.py` owns
what a public declaration may express. The constructor currently sequences
normalization, property checks, layout checks, tag checks, and lowering
directly.

Make that boundary visually explicit with a sequence such as:

```python
normalized = _normalize_declaration(...)
_validate_declaration(normalized)
self._desired_table = _lower_declaration(normalized)
```

The focused validators can remain in the same module. This is primarily a
readability improvement, not a reason to create pass-through abstractions.
Normalization should precede validation so the policy and stored values use
the same representation.

## Policy that is already in an appropriate place

These should not be moved merely to achieve more modules:

- relation/provider admission and catalog read assembly in `read.py`;
- backend type representation and normalization in `sql/types.py` and
  `sql/describe.py`;
- desired/observed diff meaning in `domain/plan/diff.py`;
- action precedence in `domain/plan/actions.py`;
- run-level ordering and failure propagation in `application/engine.py`; and
- backend SQL lowering in `sql/compile.py`.

The goal is not to move every validation function upward. It is to make each
cross-cutting policy have one obvious owner, while keeping local invariants
close to the value or representation they validate.
