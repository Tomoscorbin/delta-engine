---

paths:

- "src/delta_engine/domain/**/*.py"
- "src/delta_engine/application/**/*.py"
- "tests/domain/**/*.py"
- "tests/application/**/*.py"

---

# Domain and application architecture rules

Keep the domain backend-free, deterministic, and independent of infrastructure.

`delta_engine.domain` must not import Spark, Databricks, Delta Lake, Py4J, adapters, API declarations, schema entrypoints, application code, or infrastructure libraries.

`delta_engine.application` may depend on the domain and on application ports. It must not import concrete backend adapters.

Do not solve adapter or execution problems by leaking backend concepts inward.

## Responsibilities

Domain code owns:

* table state models
* backend-free value objects
* typed drift facts
* action types
* deterministic action plans
* action ordering concepts

Application code owns:

* `Engine.sync` orchestration
* application ports
* validation policy
* dependency resolution
* failure propagation
* sync reports
* output rendering

Adapter code owns:

* Spark and Databricks integration
* SQL compilation
* Spark DDL parsing
* identifier quoting
* backend metadata normalization
* backend exception translation

## Sync lifecycle

Preserve the lifecycle:

```text
prepare -> read -> diff -> validate -> plan -> resolve -> execute -> report
```

Do not bypass the lifecycle by making API declarations, adapters, or domain objects directly execute changes.

A table that fails an early phase should keep that failure in its report and be skipped by later mutating phases.

One table's failure should not abort the whole sync unless the engine contract is deliberately changed.

## Diff, validation, and planning

Diff code states facts. It should not decide whether a change is safe.

Validation code decides safety. Add safety policy to `delta_engine.application.validation`, not to diff code.

Planning should happen from validated drift facts.

Do not introduce a second hidden lowering stage unless the design is being deliberately changed.

`ActionPlan` owns action ordering. Do not manually sort actions elsewhere.

Keep failure handling explicit and typed.

## Ports

Application ports should be total from the engine's perspective.

`CatalogStateReader.fetch_state(...)` should return a typed read result rather than leaking backend exceptions.

`PlanExecutor.execute(...)` should return an execution summary rather than leaking backend exceptions.

Adapters should catch backend exceptions and convert them into typed failures at the boundary.

## Layer boundaries

Shared domain vocabulary is fine across layers.

What must not cross inward are:

* Spark types
* Databricks exceptions
* Py4J exceptions
* SQL strings
* backend-specific metadata shapes
* persistence models
* execution details
* infrastructure-specific naming assumptions

When adding a new concept, place it by responsibility:

* Facts about desired or observed table state: domain model.
* Facts about drift: domain diff.
* Safety policy: application validation.
* Use-case orchestration: application engine.
* Backend SQL or Spark behaviour: adapter.
* Public user declaration syntax: API/schema layer.
