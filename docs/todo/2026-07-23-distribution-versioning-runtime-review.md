# Distribution, versioning, and Databricks runtime review

**Reviewed and recorded:** 2026-07-23
**Status:** Review record. Recommendations below are not implemented unless described as
existing behaviour.

**Distribution decision, 2026-07-23:** The published `spark` extra was removed. Shipped
code never imports `delta-spark`, and the production Spark reader requires Databricks
Runtime and Unity Catalog features that local open-source Spark does not provide.
PySpark and `delta-spark` remain development dependencies for the repository's
credential-free local integration suite; that suite substitutes a test-only reader and
is not a supported consumer installation path.

**Dependency decision, 2026-07-23:** Published optional dependencies now have
upper-major review gates. Dependency validation remains lean: the normal suite uses the
lock, the existing focused CLI smoke verifies its minimum direct dependencies, and
periodic lock refreshes reuse the normal suite instead of introducing a version matrix.

## Conclusions

The distribution design is sound: the base wheel is pure Python, has no runtime
dependencies, and can expose the declaration and planning APIs without importing a
backend. The main risks are narrower:

1. The published optional dependencies are limited to reviewed major lines. Releases
   within those lines can still introduce incompatibilities, so bounds and periodic lock
   refreshes have complementary roles.
2. The original `typer>=0.12` floor was too low for the current Click ecosystem. It has
   since been raised to the first version verified by the installed-wheel compatibility
   test.
3. The former `spark` extra described an unsupported local consumer workflow and could
   replace the Spark and Delta packages supplied by Databricks Runtime. It has been
   removed; those packages are now development-only.
4. Current live coverage exercises a SQL warehouse, not the Spark backend on a matrix of
   Databricks Runtimes.
5. Import Linter enforces architectural dependency direction, but it cannot distinguish a
   lazy function-local import from an eager module-level import.
6. The project has good PyPI metadata and release machinery, but the release job does not
   itself validate the artifacts from a clean consumer environment before publishing them.
7. The supported installation paths and their intended environments need to be prominent:
   especially the difference between a SQL warehouse and Databricks compute.

## Overall product policy

Treat distribution, compatibility, and usability as one product contract:

1. Keep the base package small, pure Python, and dependency-free.
2. Put client integrations behind explicit extras and lazy backend construction; use
   platform-supplied libraries for the Databricks Runtime backend.
3. Document a distinct installation path for each kind of user.
4. Validate the built wheel and sdist, and exercise the exact wheel that consumers
   install rather than only the editable development checkout.
5. Advertise compatibility only where there is test evidence, while allowing newer,
   unknown environments to run unless a concrete incompatibility is known.
6. Make dependency, Python, and Databricks Runtime changes visible in the changelog and
   support matrix.
7. Prefer a feature-level requirement over raising the whole package's environment floor.

The goal is not to predict and prohibit every future incompatibility through package
bounds. It is to make installation unsurprising, find incompatibilities early, and give
users an exact, documented version combination they can keep running.

## Current distribution and versioning contract

- `requires-python = ">=3.12"` has no upper bound.
- The base distribution has no unconditional runtime dependencies.
- Published extras are:

  | Extra | Current requirements | Intended environment |
  | --- | --- | --- |
  | `sql` | `databricks-sql-connector>=4.0.0,<5` | Plain Python process using a SQL warehouse |
  | `cli` | `typer>=0.15.4,<1`, `databricks-sdk>=0.70.0,<1`, `databricks-sql-connector>=4.0.0,<5` | Read-only CLI using a SQL warehouse |

- PySpark and `delta-spark` are development dependencies only. They support the local
  integration suite and are not part of the published consumer contract.
- Hatch obtains the package version from a `vMAJOR.MINOR.PATCH` VCS tag. Commitizen owns
  the conventional-commit bump and changelog workflow.
- The release workflow builds both an sdist and wheel, publishes through PyPI trusted
  publishing, and creates the corresponding GitHub release.
- The wheel build target contains only `src/delta_engine`, so the resulting wheel is
  platform-independent.
- CI proves that a base-only installation imports the public declaration surface without
  development dependencies. The packaging tests also inspect installed metadata and the
  console-script target.
- CI currently uses Python 3.12 only. The proposed policy tests the minimum supported
  and latest stable Python endpoints rather than every intermediate minor.
- `uv.lock` makes development and release automation reproducible, but it does not
  constrain what downstream pip users resolve from the published lower bounds.

The VCS version fallback is `0.0.0`. Tagged release builds are safe because the release
workflow fetches full history and tags. Building from a source archive without `.git`
metadata may produce the fallback version; decide whether source-archive builds are a
supported distribution path before changing this.

## Downloadability, installation, and usability

### What is already sound

The package has the metadata expected for a usable PyPI project:

- a description and README;
- an MIT licence;
- Python, development-status, operating-system, topic, and typing classifiers;
- search keywords;
- homepage, repository, documentation, and issue-tracker links;
- a `py.typed` marker;
- a `delta-engine` console-script entry point.

The wheel contains only `src/delta_engine`, is platform-independent, and does not force
large backend dependencies on users of the declaration and planning APIs. The release
workflow publishes a wheel and sdist through PyPI trusted publishing, and TestPyPI
publishing is available as a manual rehearsal.

### Installation paths to document

The README, PyPI page, and installation documentation should present the choices by user
environment instead of showing the extras as interchangeable:

| User | Installation | Notes |
| --- | --- | --- |
| Pure declaration/planning library | `pip install delta-engine` | No Spark or Databricks client dependencies |
| SQL warehouse integration | `pip install "delta-engine[sql]"` | Installs the Databricks SQL connector |
| Command-line interface | `pip install "delta-engine[cli]"` | Installs the CLI, SDK, and SQL connector |
| Databricks compute | `%pip install "delta-engine==X.Y.Z"` | Use the runtime's Spark and Delta packages, then restart Python |

The first screen of the README/PyPI description should say what Delta Engine does, show
the simplest successful example, state the supported Python versions, and lead each user
to the correct installation path. The base-only Databricks instruction and the absence of
a consumer Spark extra should be difficult to miss.

Backend construction should fail with an actionable optional-dependency message when the
relevant SQL or CLI extra is absent. Spark construction should identify a missing
Databricks Runtime environment without telling the user to install or replace
runtime-supplied PySpark. These checks must remain inside the backend boundary so
importing the base package continues to work without any extra.

### Validate what is actually downloaded

Repository tests and `uv.lock` do not prove that an ordinary pip consumer can use the
published distribution. A release candidate should be exercised as built artifacts:

The implemented release gate is recorded in the
[release artifact validation plan](2026-07-23-release-artifact-validation-plan.md).

1. Build both the wheel and sdist once.
2. Use Twine's standard metadata and README validation.
3. Build the wheel from the sdist, then install that wheel into a blank environment and
   run the dependency-free public API, lazy-import, version, and base console-shim smoke
   tests.
4. Pass those files through a CI artifact boundary to a separate OIDC publishing job.
5. Publish only the already-validated artifacts rather than rebuilding them.

Do not duplicate the build backend and publisher with a custom archive parser. Checks for
CLI, SQL, dependency minima, and newest compatible versions belong in a separate
compatibility matrix: an upstream optional-dependency release should not make basic
archive validation non-deterministic. The internal local Spark suite also does not prove
Databricks Runtime compatibility.

The release workflow now runs this focused gate before pushing a new release commit/tag,
then transfers the exact files to an isolated Trusted Publishing job. TestPyPI remains
useful for checking downloadability, but its separately built files are not proof that a
later PyPI build is identical.

Release documentation should include:

- the package version and user-visible changelog;
- supported Python versions;
- the tested Databricks Runtime matrix;
- dependency or environment compatibility changes;
- deprecation notices before dropping a Python or runtime version;
- the final delta-engine version supporting a retired environment;
- whether that older release line receives critical fixes, and for how long.

## Dependency range findings

### CLI minimum

A clean environment resolving the declared `typer==0.12` minimum with a current Click
release did not provide a working CLI: `--version` failed and `--help` crashed. Resolving
Typer 0.26 succeeded. The verified boundary is Typer 0.15.4: 0.15.3 fails `--help`,
while 0.15.4 passes both `--help` and `--version`. The declared floor is now 0.15.4,
and CI exercises the exact minimum direct CLI dependency set from an installed wheel.

### Upper bounds

Do not add an upper Python bound merely because a future Python has not yet been tested.
An upper bound would prevent installation on a new Databricks Runtime even when the pure
Python package works and can cause installers to choose an older distribution instead.
Fix or narrowly document a demonstrated incompatibility rather than using an upper bound
as the normal response.

Published backend and CLI dependencies describe reviewed major lines rather than
accepting every future major:

```toml
sql = ["databricks-sql-connector>=4.0.0,<5"]
cli = [
  "typer>=0.15.4,<1",
  "databricks-sdk>=0.70.0,<1",
  "databricks-sql-connector>=4.0.0,<5",
]
```

These are compatibility boundaries, not substitutes for testing. The Databricks SDK is
still pre-1.0, so `<1` cannot guarantee that every intermediate release is
backwards-compatible. Applications and deployments should pin or lock a complete
environment even when the library metadata uses ranges.

PySpark and `delta-spark` do not need published bounds because they are not consumer
dependencies. Their compatible pair is owned by the development lock and local
integration suite.

Dependency validation is deliberately not a Cartesian compatibility matrix:

- the normal pull-request suite exercises the reproducible locked environment;
- the focused installed-wheel CLI smoke verifies the declared minimum direct CLI set;
- periodic lock refreshes bring in the newest versions inside the declared ranges and
  use the existing suite rather than a separate newest-version job;
- there is no maximum-version job: the exclusive upper bounds are metadata review gates.

Minimum/latest CPython coverage and live Databricks Runtime coverage remain separate
environment-compatibility concerns. Intermediate Python minors need a dedicated job only
when version-specific code, dependency resolution, or a reproduced defect creates a
distinct path.

## Databricks Runtime compatibility

The current reader requires `DESCRIBE TABLE EXTENDED ... AS JSON`, establishing a
Databricks Runtime floor of 16.2 for Spark compute. SQL warehouses support the same
operation independently of a numbered cluster runtime.

The relevant Databricks Runtime snapshot at review time is:

| Runtime | Python | Apache Spark | Delta Lake | Proposed status |
| --- | --- | --- | --- | --- |
| 16.4 LTS | 3.12.3 | 3.5.2 | 3.3.1 | Supported and tested |
| 17.3 LTS | 3.12.3 | 4.0.0 | 4.0.0 | Supported and tested |
| 18 | 3.12.3 | 4.1.0 | 4.2.0 | Supported and tested |
| 19 Beta | 3.12.3 | 4.2.0 | 4.2.0 | Experimental until a live smoke test passes |

This is a dated snapshot, not a permanent promise. Track the official
[Databricks Runtime compatibility table](https://docs.databricks.com/aws/en/release-notes/runtime)
when changing the support matrix.

The implementation uses a small PySpark surface and delegates catalog operations to SQL,
so a Spark major upgrade is less risky than it would be for a DataFrame-heavy library.
It is not risk-free: SQL syntax, metadata JSON, `information_schema`, catalog behaviour,
and PySpark exception behaviour can all change across runtimes.

The existing weekly live suite uses the Databricks SQL connector against a SQL warehouse.
It detects Databricks service-side drift for the shared reader/compiler and warehouse
adapter, but it does not prove that the Spark adapter works on 16.4, 17.3, 18, or later.

### Installation contract on Databricks

Install an exact base-package version:

```python
%pip install "delta-engine==X.Y.Z"
dbutils.library.restartPython()
```

For jobs, declare the same exact base version as a job or compute library. Put all
session-scoped installs at the beginning of a notebook and restart Python after changing
them. See Databricks'
[notebook-scoped library guidance](https://docs.databricks.com/aws/en/libraries/notebooks-python-libraries).

Do **not** install any of these on Databricks compute:

```text
pyspark
delta-spark
```

Databricks supplies mutually tested Spark and Delta versions. Notebook-installed packages
take precedence over runtime libraries, so notebook-installing `pyspark` or `delta-spark`
can shadow, upgrade, or downgrade the runtime's packages. The current package therefore
does not publish a `spark` extra. See the documented
[library precedence](https://docs.databricks.com/aws/en/libraries/) and
[Spark migration guidance](https://docs.databricks.com/aws/en/migration/spark).

Pip environment markers cannot select packages based on a Databricks Runtime number.
Leaving Spark and Delta out of the on-cluster installation is therefore more reliable
than attempting to encode every runtime combination in the wheel metadata.

Local open-source Spark/Delta tests remain useful internal adapter tests, but their
test-only reader means they are neither a supported installation path nor a replacement
for tests on Databricks Runtime.

### Runtime assurance

Publish a tested-runtime matrix and run a thin scheduled Spark smoke suite on:

1. the oldest supported LTS;
2. each supported current LTS;
3. the newest GA runtime;
4. the newest beta as a non-blocking early warning.

The smoke suite should install the exact released base wheel, import the public surfaces,
build a Spark engine, read an existing table, plan a no-op and a change, and safely
create/read/drop an isolated table. A beta failure should change the beta's documented
status, not break supported releases.

### Forward-compatibility and support policy

A passing runtime matrix is evidence about specific releases, not a guarantee about every
future Databricks Runtime. Treat compatibility as an explicit, versioned product contract
with four states:

| State | Meaning | Behaviour |
| --- | --- | --- |
| Minimum supported | Oldest runtime on which the package's baseline operations are maintained | Fail clearly below it |
| Tested | Runtimes exercised by the live suite for this package release | Fully supported |
| Newer, untested | A runtime newer than the tested ceiling with no known incompatibility | Allow it to run; do not claim tested support |
| Known incompatible | A runtime for which a concrete failure is known | Fail early or publish a constrained package recommendation |

Do not encode the newest tested runtime as a hard upper limit. That would turn every
compatible Databricks release into an unnecessary outage. Detect and reject known-old or
known-bad environments, while scheduled beta/current-runtime tests provide early warning
about forward incompatibilities.

For example, if `delta-engine 0.8` is tested on DBR 16.4, 17.3, and 18 when DBR
19 appears:

1. Test the already-released `delta-engine 0.8` wheel on DBR 19.
2. If it passes, add DBR 19 to the tested matrix. A matching delta-engine release is not
   required.
3. If it needs a small compatibility change, release `0.8.1` with both the old and new
   paths so it supports DBR 16.4 through 19.
4. If only a new optional feature requires DBR 19, release it without changing the global
   floor; attempts to use that feature on DBR 16.4 fail clearly, while ordinary operations
   continue to work.
5. Only if supporting both generations is impractical should a breaking delta-engine line
   raise the minimum runtime. DBR 16.4 users then pin the final compatible package line.

There is no intended one-to-one mapping such as “DBR 19 requires delta-engine 19.” Each
delta-engine release supports a range of runtimes, and a new runtime first gets tested
against the package that already exists.

Databricks gives LTS runtimes three years of support and recommends the latest LTS for
stable workloads. A reasonable delta-engine policy is to support the current and previous
Databricks LTS while Databricks supports them, test the current GA release, and treat beta
as non-blocking preview coverage. Announce the removal of an old LTS before dropping it.
See the
[Databricks Runtime support lifecycle](https://docs.databricks.com/aws/en/release-notes/runtime/databricks-runtime-ver).

New functionality should not automatically raise the package-wide runtime floor:

| Change | Compatibility treatment |
| --- | --- |
| New declaration or DDL supported only on a newer runtime | Keep baseline operations compatible; validate or fail clearly only when that feature is declared |
| Newer API offers an optimisation | Keep the old path as a fallback and select the fast path by capability |
| New metadata shape/API is required for every table read | Retain a bounded old-reader implementation if practical; otherwise raise the baseline in a breaking package release |
| New PySpark API is required only by the Spark adapter | Keep the dependency and check adapter-private; the SQL warehouse and pure-Python surfaces remain usable |
| Databricks removes or changes an existing API | Add a compatibility implementation, or mark the affected runtime known-incompatible until one is released |
| Supporting an old LTS prevents a necessary architectural improvement | Deprecate it, release one final compatible line, then drop it in a breaking release |

When a feature is optional, prefer a feature-level requirement such as “liquid clustering
conversion requires DBR 18.1+” over “delta-engine requires DBR 18.1+”. Users on 16.4 can
then continue using every operation that 16.4 supports.

If a new dependency is needed only to develop or test a new feature, updating the
internal local Spark environment does not raise the Databricks Runtime floor. On-cluster
users install the dependency-free base wheel and continue using the runtime's own
PySpark. A global runtime-floor increase is warranted only when the production code path
genuinely cannot operate on the older runtime.

If the global floor must rise, make the package boundary explicit:

```text
delta-engine old release line  -> DBR 16.4 and later
delta-engine new breaking line -> DBR 18 and later
```

Old-runtime deployments must pin the old compatible package line. Python package metadata
cannot make pip choose a delta-engine version from the Databricks Runtime version, so an
unpinned `pip install delta-engine` cannot provide this guarantee automatically. The
package should detect an unsupported baseline when the Databricks engine is constructed
and report the newest compatible delta-engine range, while production jobs pin exact
versions.

Before 1.0, a dropped runtime may technically be represented by a minor version under
SemVer, but users still need an explicit compatibility promise. Once the public contract
is stable, use a major release to raise the baseline. In either case:

1. announce the deprecation at least one release ahead;
2. document the final delta-engine version compatible with the old LTS;
3. keep that release installable and its documentation discoverable;
4. decide and publish whether the old line receives critical fixes, and for how long.

Runtime checks belong at backend construction or feature validation, never at
`import delta_engine`. The pure-Python surface must remain importable anywhere. Prefer
checking the actual required capability; where a stable capability probe is unavailable,
Databricks exposes the runtime through `current_version().dbr_version`. Keep version
parsing and policy inside the Databricks adapter. A Spark version alone is insufficient
for Databricks-only SQL, Delta protocol, Unity Catalog, or SQL warehouse capabilities.

Avoid building a large table of speculative version gates. Preflight:

- the baseline required to read and operate safely;
- feature requirements that can be determined reliably;
- requirements whose late failure could leave a multi-statement sync partially applied.

For other platform-specific failures, preserve the current policy of surfacing the
structured Databricks execution error and documenting the feature floor.

## Lazy loading and Import Linter

The current Import Linter contracts provide useful architectural guarantees:

- schema, API, application, and domain code cannot import `delta` or `pyspark`;
- CLI code cannot acquire Spark/Delta through a direct or indirect dependency;
- the shared SQL core and warehouse backend cannot import `delta`, `pyspark`, or `py4j`;
- Typer, Click, and Rich stay inside the CLI package.

The facade intentionally has two ignored static edges:

- `delta_engine.databricks -> delta_engine.adapters.databricks.spark.factory`;
- `delta_engine.databricks -> pyspark`.

The first is a function-local import used to construct the Spark backend. The second is
inside `TYPE_CHECKING` for the `SparkSession` annotation. Grimp's static graph sees both
as ordinary import edges and cannot tell whether an import executes at module load time.
Moving the factory import from inside `build_spark_engine()` to module scope would
therefore retain the same graph edge and could still pass Import Linter.

Import Linter protects **where dependencies may flow**, not **when imports execute**.

The behavioural lazy-loading protection is instead:

- `tests/test_packaging.py`, which imports the base package, CLI shim, and Databricks
  facade in a subprocess and checks that optional packages did not load;
- `tests/test_public_imports.py`, which makes PySpark and Databricks packages unavailable
  and imports the public pure-Python surfaces;
- the SQL-engine test, which proves that constructing the warehouse backend does not
  require PySpark or the SQL connector to be imported by the package.

At review time all seven Import Linter contracts and all ten focused packaging/public
import tests passed.

Strengthen the runtime guard by:

- adding `delta` and `py4j` to the subprocess's forbidden eager-module list;
- keeping the black-box test as the authoritative lazy-loading contract;
- running the same check against the built base wheel in a clean environment;
- avoiding an AST test that encodes the exact indentation or location of imports unless
  a runtime behavioural test cannot express the requirement.

## Action list

### Before the next release

- [x] Correct the Typer floor and add minimum-version CLI smoke tests.
- [x] Add tested upper-major boundaries to the published optional dependencies.
- [x] Remove the unsupported consumer `spark` extra while retaining the locked local
      Spark/Delta integration environment for repository tests.
- [x] Add a concise installation chooser for base, SQL, CLI, and Databricks compute to
      the README/PyPI description and installation documentation.
- [x] State prominently that Databricks users install a pinned base wheel and use the
      runtime's Spark and Delta libraries.
- [x] Keep missing CLI dependencies pointed at `[cli]`; make missing PySpark at the
      Spark factory explain the supported Databricks Runtime requirement. The SQL
      factory accepts a caller-owned connector connection and therefore does not import
      or diagnose `[sql]` itself.
- [x] Add `delta` and `py4j` to the lazy-import regression test.
- [x] Build the sdist and wheel once, with the wheel built from the sdist; metadata-check
      both and smoke-test the exact wheel before publishing the artifacts unchanged.
- [x] Separate artifact construction from the OIDC publishing job.
- [x] Require successful checks for the exact release commit/tag, or add a focused
      validation gate to the release workflow before PyPI publication.

### Compatibility infrastructure

- [x] Test the minimum supported and latest stable Python versions in CI (currently 3.12
      and 3.14); add intermediate minors only for a distinct compatibility path.
      Implemented in PR #294.
- [ ] Refresh the dependency lock periodically and validate updates with the normal
      suite; do not add separate minimum, maximum, or newest-compatible matrices.
- [x] Keep local Spark/Delta coverage as an internal locked integration suite rather than
      a published compatibility promise.
- [ ] Add live Spark-backend smoke tests for the documented Databricks Runtime matrix.
- [ ] Publish supported, tested, and experimental runtime statuses in user-facing docs.
- [ ] Define the supported-LTS window, deprecation notice, and maintenance period for the
      final package line supporting a retired runtime.
- [ ] Add an adapter-private runtime context and a clear baseline incompatibility error;
      keep it out of base-package import paths.
- [ ] Define feature-level runtime requirements where a reliable preflight prevents
      partial execution; do not raise the global baseline for optional features.
- [ ] Review each new Databricks Runtime before claiming support.

### Later decisions

- [ ] Decide whether builds from VCS-free source archives must preserve the real version
      instead of falling back to `0.0.0`.
- [ ] Decide whether the release flow should promote an artifact rehearsed on TestPyPI or
      use another build-once mechanism; avoid treating separate builds as identical.
- [ ] Revisit bounds when a dependency major or Databricks Runtime changes; do not let
      old caps become permanent without evidence.
