# Databricks runtime compatibility audit

**Reviewed and recorded:** 2026-07-29
**Status:** Review record and focused implementation backlog. No tested Spark-runtime
claim exists until the proposed live smoke tests pass.

This is a focused follow-up to the
[distribution, versioning, and runtime review](2026-07-23-distribution-versioning-runtime-review.md).
It reviews what Delta Engine actually depends on inside a Databricks notebook and
proposes the smallest compatibility policy justified by that dependency surface.

## Executive conclusion

Delta Engine should not grow a general Databricks compatibility framework. Its
production Spark integration is deliberately small:

- the base wheel is pure Python and has no runtime dependencies;
- Databricks supplies its own mutually compatible PySpark and Delta packages;
- Delta Engine does not use the production `delta-spark` Python API;
- the Spark adapter primarily passes SQL through `SparkSession.sql()`;
- the production reader parses Databricks SQL metadata rather than traversing Spark or
  Delta internals.

The practical risks are therefore narrower than a DBR-by-Spark-by-Delta matrix would
suggest:

1. the notebook's Python version may be below Delta Engine's declared floor;
2. Delta Engine's baseline metadata SQL may not exist on an old runtime;
3. a user may request an optional Databricks or Delta feature that needs a newer
   runtime;
4. Databricks may change SQL behaviour or metadata that Delta Engine reads;
5. a compute mode may behave differently, as demonstrated by the current
   Dedicated/`SINGLE_USER` failure.

Each existing layer can own one of those concerns. Python packaging should reject an
old interpreter, feature documentation should tell users when they need a newer DBR,
Databricks should provide the Spark/Delta pair, and a thin live notebook smoke test
should catch platform regressions. A new runtime should remain allowed unless a concrete
incompatibility is found.

The intended baseline contract is:

> The Spark backend requires Python 3.12 or later, Databricks Runtime 16.2 or later,
> Unity Catalog, and the PySpark and Delta libraries supplied by Databricks. Delta
> Engine places no maximum on the runtime version. Individual features may require a
> newer runtime. Tested environments and known exceptions are recorded separately.

Dedicated access mode (`data_security_mode` value `SINGLE_USER`) is currently one such
known exception. It remains unsupported until the observed failure is reproduced and
understood.

## Responsibility model

| Concern | Natural owner | Delta Engine policy |
| --- | --- | --- |
| Python too old | Package metadata and `pip` | Declare the real lower bound with `requires-python` |
| Newer Python | Minimum/latest Python CI | Test the supported endpoints without a speculative upper bound |
| Spark/Delta package pairing | Databricks Runtime | Never install or pin replacements on Databricks compute |
| Baseline metadata API | Spark adapter contract | Document DBR 16.2 as the technical floor |
| Optional platform feature | User plus feature documentation | State a minimum only for the feature that needs it |
| Platform SQL or metadata regression | Thin live notebook smoke test | Test representative real runtimes using the exact wheel |
| Known environment-specific defect | Targeted investigation and regression | Fix or document the narrow incompatibility |

This model intentionally does not try to discover every runtime capability before a
sync. Add a targeted preflight only when a demonstrated feature failure would otherwise
be confusing or could occur after earlier statements have already changed a table.

## Evidence available today

| Area | Current evidence | What it proves |
| --- | --- | --- |
| Base distribution | Installed-wheel and lazy-import tests | Declarations and planning import without backend packages |
| Python | CI runs Python 3.12 | The declared floor works; the latest stable endpoint is not continuously verified |
| Local Spark/Delta | OSS Spark and Delta with a test-only native reader | Engine lifecycle and adapter internals, not production Databricks metadata reads |
| SQL warehouse | Weekly credentialed suite against one configured endpoint | Shared SQL core and warehouse adapter behaviour on that endpoint |
| Numbered DBR Spark | None | No numbered runtime can currently be called live-tested |
| Dedicated (`SINGLE_USER`) Spark | User-observed failure; exact cause not yet pinned | Treat this access mode as unsupported pending investigation |

Relevant implementation boundaries:

- [`pyproject.toml`](../../pyproject.toml) declares Python 3.12 or later, leaves base
  dependencies empty, and keeps PySpark and Delta development-only.
- [`delta_engine.databricks`](../../src/delta_engine/databricks.py) loads the Spark
  backend lazily.
- The production Spark shell imports `SparkSession` and `DataFrame`, then delegates
  physical work to `spark.sql()`.
- The production reader depends on
  [`DESCRIBE TABLE EXTENDED ... AS JSON`](../../src/delta_engine/adapters/databricks/sql/queries.py),
  which establishes the DBR 16.2 technical floor.
- The [live workflow](../../.github/workflows/live.yaml) exercises the SQL warehouse
  backend, not `build_spark_engine()`.
- The [local native reader](../../tests/adapters/databricks/native_reader.py) replaces
  the production reader because open-source Spark cannot execute the Databricks
  metadata command.

The implementation has a low compatibility risk, but the current test evidence does
not yet establish Spark notebook support.

## Findings

### 1. Python compatibility is already mostly a packaging concern

The base wheel is pure Python (`py3-none-any`) and has no runtime dependencies.
`requires-python = ">=3.12"` gives an old notebook interpreter a standard, early
installation failure instead of allowing code with unsupported syntax or standard
library assumptions to run. The floor is not arbitrary: production modules use the
[`type` alias statement](https://peps.python.org/pep-0695/), which was introduced in
Python 3.12.

Keep the lower bound aligned with the oldest Python version the project is prepared to
test. Do not add an upper bound merely because a new interpreter has not yet appeared in
a DBR. The
[Python packaging guidance](https://packaging.python.org/en/latest/guides/dropping-older-python-versions/)
warns against upper Python bounds because they create resolver conflicts and prevent
likely-compatible installations.

Python has an explicit
[backwards-compatibility policy](https://peps.python.org/pep-0387/), and Delta Engine has
no Python-version branches, compiled extensions, or base dependencies. Test the minimum
supported Python and the latest stable Python rather than every intermediate minor. At
this review date those endpoints are 3.12 and 3.14. Python 3.13 remains inside the
supported range without needing a dedicated job. Add an intermediate version only when
version-specific code, dependency resolution, or a reproduced defect creates a distinct
path.

Keep linting and type checking targeted at the minimum version so newer-only syntax and
APIs cannot enter accidentally. Build one universal wheel and smoke-test that exact
artifact once per build on the Python floor. The full suite supplies the minimum/latest
Python evidence, so repeating the same packaging smoke at both endpoints would not prove
a distinct property. Spark compatibility remains a separate live DBR concern, using the
Python and PySpark combination supplied by Databricks.

The optional SQL and CLI dependencies have their own resolver constraints, but they are
not part of the normal base-wheel Spark notebook installation.

#### When the Python floor should rise

Raising the floor is a product compatibility decision, not routine housekeeping. Do it
only when at least one concrete need exists:

- production code needs newer syntax or a standard-library API and a compatibility
  implementation is not worthwhile;
- a required core dependency drops the old interpreter;
- the old Python reaches end of life and retaining it creates a security or material
  tooling burden;
- supporting it requires significant version branches, backports, or duplicated code;
- an interpreter defect prevents correct or safe behaviour.

Also confirm that no Databricks environment the project intends to support still needs
the old Python. A new Python release, a development-tool requirement, an optional
dependency, or an optional DBR feature does not by itself justify raising the global
floor.

When the floor does rise:

1. announce the compatibility change;
2. identify the final Delta Engine release supporting the old Python;
3. keep production installation examples pinned;
4. update `requires-python`, static-tool targets, CI endpoints, classifiers, and release
   notes together.

An unpinned installer can select the last package release compatible with an old
interpreter, which is another reason production notebooks should use exact Delta Engine
versions.

### 2. New Databricks and Delta features should be handled feature by feature

A user who needs a feature introduced by a newer DBR is responsible for selecting a
runtime that provides it. Delta Engine is responsible only for the features it chooses
to expose or manage.

When Delta Engine adds such a feature:

1. model the feature and its safety implications;
2. document its minimum DBR or protocol requirement beside the feature;
3. add compiler, planning, and failure tests;
4. keep the package-wide DBR floor unchanged unless every baseline operation now needs
   the newer API;
5. preserve the original structured Databricks error when the user's environment
   cannot execute it.

The existing approach described in
[runtime features](../reference-limitations.md#runtime-features) is reasonable:
Databricks normally enforces its feature requirements. A targeted preflight is justified
only when waiting for execution creates a poor or unsafe result, especially when a
multi-statement plan could be partially applied.

A newly announced platform feature needs no Delta Engine work when the package neither
models nor changes it.

### 3. A Spark-by-Delta dependency matrix would not represent the real risk

Databricks owns and tests the Spark and Delta versions installed together in a runtime.
Installing another `pyspark` or `delta-spark` version into a notebook would create the
very mismatch the package should avoid.

Delta Engine's production Spark dependency is limited to stable shell types and
`spark.sql()`. It does not import or call the production `delta-spark` API. The
development dependencies on PySpark and Delta 4.x are therefore test tooling, not a
statement that a notebook must contain those versions.

Do not publish:

- a Spark-by-Delta compatibility table;
- a `spark` extra that installs either platform package;
- one Delta Engine wheel or release line per DBR;
- separate minimum and maximum dependency matrices for the notebook backend.

One real-runtime smoke test is more relevant than several local combinations because the
metadata SQL itself is Databricks-specific.

### 4. SQL and metadata evolution are the important forward-compatibility risks

Basic PySpark calls are unlikely to be the source of a future break. The more credible
risks are:

- a change to `DESCRIBE TABLE EXTENDED ... AS JSON`;
- a change to `information_schema` data or permissions;
- a structured exception changing shape;
- a known type gaining semantics outside Delta Engine's model;
- a DDL statement changing behaviour.

The reader is directionally safe: malformed and unknown data types fail the read, and
unknown relation kinds are rejected. Irrelevant additive top-level JSON fields can be
ignored without coupling the parser to every platform release.

Known semantic gaps still need explicit decisions:

- string collation is ignored;
- `CHAR(n)` and `VARCHAR(n)` are normalized to unbounded `String`;
- nested struct-field nullability is available in metadata but absent from the domain.

For new metadata, use a simple rule:

1. model and compare it when Delta Engine owns it;
2. ignore it only when Delta Engine demonstrably preserves it and it is outside the
   package's ownership;
3. otherwise fail the read rather than incorrectly reporting convergence.

This parser policy and a live smoke test are more valuable than a general runtime
capability registry.

### 5. Dedicated (`SINGLE_USER`) is a concrete defect to investigate

A user-observed Spark deployment fails when compute uses the `data_security_mode` value
`SINGLE_USER`. Databricks now calls this Dedicated access mode; `SINGLE_USER` remains the
API and system-table value. See the
[Dedicated compute overview](https://docs.databricks.com/aws/en/compute/dedicated-overview)
and
[access-mode value reference](https://docs.databricks.com/aws/en/admin/system-tables/compute#access-mode-reference).

This should not motivate a general access-mode framework before the failure is
understood. The focused investigation is:

1. install the same Delta Engine wheel on Standard and Dedicated compute using the same
   DBR and equivalent Unity Catalog permissions;
2. locate the first differing operation: import, engine construction, metadata SQL,
   information-schema query, session configuration, or DDL;
3. retain the complete structured Databricks condition and traceback;
4. fix the specific boundary or document the narrow platform limitation;
5. add the minimal reproduction as a live regression before changing the support
   statement.

Until then, document Dedicated (`SINGLE_USER`) as unsupported. A Standard result neither
explains nor clears this failure.

### 6. The missing assurance is a thin exact-wheel notebook smoke test

The local Spark suite remains useful, but its alternate reader cannot prove the
production notebook path. The SQL warehouse live suite also cannot prove that
`build_spark_engine()` works.

The missing test should install the same base wheel a notebook user receives. A minimal
job should:

1. install one exact candidate or published wheel without installing PySpark or Delta;
2. restart Python when required by the notebook installation mechanism;
3. import the public API and build the production Spark engine;
4. create a disposable Unity Catalog Delta table through the engine;
5. read it through the production JSON metadata reader;
6. prove a second sync is a no-op;
7. plan and apply one representative safe alteration;
8. verify one representative failure retains an actionable Databricks condition;
9. drop the disposable objects in cleanup.

Record only the information needed to reproduce a failure: Delta Engine version and
artifact digest, DBR identifier, Python and Spark versions, access mode, test date, and
the build values returned by `current_version()` when readily available. Do not build a
public environment-profile subsystem merely to collect CI metadata.

## Recommended support and verification policy

### User-facing contract

- Require Python 3.12 or later through package metadata.
- Require DBR 16.2 or later for the Spark backend because the production reader needs
  the JSON metadata command.
- Require Unity Catalog.
- Use Databricks-supplied PySpark and Delta packages.
- Place no maximum on Python or DBR versions without a reproduced incompatibility.
- Document newer requirements beside individual optional features.
- List live-tested environments as evidence, not as a hard runtime allowlist.
- List concrete known incompatibilities separately.

Newer DBRs should be expected to work because the integration surface is small. They
remain allowed before they are added to the tested list.

Classic compute, serverless Spark, and SQL warehouses are different execution surfaces.
Do not imply one from another, but add coverage only when the project intends to claim
that surface. There is no need to build the full Cartesian product of cloud, runtime,
access mode, Photon setting, and table feature.

### Minimal live matrix

The following is deliberately representative rather than exhaustive:

| Notebook environment | Purpose |
| --- | --- |
| Oldest maintained LTS on Standard | Protect the backward compatibility boundary |
| Current LTS on Standard | Cover the common current notebook environment |
| Current LTS on Dedicated | Cover the other claimed access mode after the known failure is fixed |
| Newest DBR on Standard, optional and non-blocking | Give early warning of an upcoming regression |

If multiple active LTS releases become important to users, add them because demand
justifies the cost, not because every possible runtime combination must exist in a
framework.

Run the candidate wheel before a Delta Engine release. Periodically run the latest
published wheel to detect a Databricks service or runtime-build change affecting
existing users. Baseline smoke steps must not silently skip.

### When a new DBR is released

1. Do not change package metadata or add a maximum runtime.
2. Run the published wheel through the notebook smoke test.
3. If it passes, record the dated result. Do not release Delta Engine solely to announce
   compatibility.
4. If it fails, reproduce the narrow boundary, document the concrete incompatibility,
   and add a regression.
5. Release a compatibility fix only when Delta Engine code actually needs to change.

No automatic runtime discovery service is required initially. A scheduled or manually
triggered canary is enough for the expected release rate and risk.

### When a new Python version appears in DBR

1. Leave the open-ended `>=3.12` requirement in place.
2. Advance the explicit latest-stable CI endpoint; do not add a permanent job for every
   intervening minor.
3. Add another endpoint only for version-specific code, dependency resolution, or a
   reproduced defect.
4. Run the real notebook smoke test when a DBR adopts the interpreter.
5. If the endpoint passes, add its classifier without requiring a Delta Engine release
   solely for compatibility.
6. If it fails, fix or narrowly document the incompatibility rather than adding a
   speculative upper bound.

### When a new Delta or Databricks feature appears

1. Do nothing until Delta Engine intentionally supports or manages it.
2. When support is added, document the feature's own runtime/protocol floor.
3. Let the user select an appropriate runtime.
4. Prefer the original Databricks error unless a targeted early validation materially
   improves safety or clarity.
5. Do not raise the global DBR floor for optional functionality.

## Focused backlog

### P0 — establish the missing evidence

- [ ] Run the core suite on the minimum supported and latest stable Python versions
      (currently 3.12 and 3.14); keep intermediate 3.13 supported without a dedicated
      job while it has no distinct compatibility path. Smoke-test each exact built wheel
      once on the Python floor.
- [ ] Reproduce the Dedicated (`SINGLE_USER`) failure against Standard on the same DBR,
      identify the first differing operation, and retain a live regression.
- [ ] Add the exact-wheel production Spark smoke on the oldest maintained and current
      LTS runtimes using Standard access mode.
- [ ] Add Dedicated coverage on the current LTS after the known failure is fixed.
- [ ] Report the exact artifact and minimal sanitized runtime identity in smoke output.
- [ ] Keep user-facing language clear that DBR 16.2 is a technical floor, while the
      tested list is evidence and not an allowlist.

### P1 — strengthen the real compatibility boundaries

- [ ] Decide whether to model, preserve, or reject collation, bounded strings, and
      nested struct nullability instead of silently erasing their semantics.
- [ ] Capture sanitized production JSON metadata fixtures when a live runtime exposes a
      meaningful new shape or reproduces a parser defect.
- [ ] Add a non-blocking newest-DBR canary if its operating cost proves worthwhile.
- [ ] Add serverless Spark or additional access-mode/runtime combinations only when the
      project intends to support them.
- [ ] Add a targeted feature preflight only when a demonstrated execution-time failure
      is confusing or risks partial application.

## Decisions to preserve

- Do not publish or install PySpark or `delta-spark` on Databricks compute.
- Do not create a Spark-by-Delta compatibility matrix.
- Do not create a runtime allowlist or hard maximum.
- Do not create one wheel or Delta Engine release line per DBR.
- Do not test every intermediate Python minor unless it has a distinct compatibility
  path; test the minimum and latest stable endpoints.
- Do not build a general platform profile, tri-state capability registry, or public
  environment diagnostic without a demonstrated need.
- Do not infer production Spark support from the local alternate reader or SQL
  warehouse suite.
- Do not infer Dedicated support from a Standard-mode result.
- Do not raise the package-wide runtime floor for an optional feature.
- Do preserve unknown platform state or fail closed when ignoring it could make a sync
  unsafe.
