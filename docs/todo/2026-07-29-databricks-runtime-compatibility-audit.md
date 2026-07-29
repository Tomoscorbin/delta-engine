# Databricks platform compatibility and runtime assurance audit

**Reviewed and recorded:** 2026-07-29
**Status:** Review record and implementation backlog. No support status proposed here is
implemented until the corresponding live evidence exists.

This is a focused follow-up to the
[distribution, versioning, and runtime review](2026-07-23-distribution-versioning-runtime-review.md).
That review establishes the right packaging and forward-compatibility policy. This audit
checks the current implementation and test evidence against that policy, expands the
scope beyond numbered Databricks Runtimes, and turns the remaining gaps into an
actionable assurance plan.

## Executive conclusion

Delta Engine has the right foundation for working across Databricks releases:

- the base wheel is pure Python and has no runtime dependencies;
- the Spark and SQL backends load lazily;
- Databricks compute supplies its own mutually compatible PySpark and Delta packages;
- the production integration uses a deliberately small PySpark surface and delegates
  catalog work to SQL;
- unknown relation kinds and data types generally fail closed rather than being guessed.

The missing piece is evidence. Databricks Runtime 16.2 is a technical syntax floor for
the production Spark reader, not a tested-support statement. The recurring live suite
uses one SQL warehouse, while the local Spark suite substitutes a test-only reader for
the production Databricks reader. No current job proves that the released wheel works
through `build_spark_engine()` on any numbered Databricks Runtime.

Until live Spark coverage exists, the honest contract is:

1. DBR 16.2 is the earliest runtime on which the production reader can technically run.
2. The Spark backend has no published tested-runtime matrix.
3. Newer runtimes are allowed but unverified unless a concrete incompatibility is known.
4. SQL warehouse evidence is separate from Spark-runtime evidence.
5. Dedicated access mode (`data_security_mode` value `SINGLE_USER`) has an observed failure
   and is currently unsupported while its cause and affected runtime range are unknown.
6. Serverless Spark, warehouse release channels, clouds, and access modes need explicit
   scope; none is implied by a numbered-runtime result.

The first supported Spark floor should be DBR 16.4 LTS, after it passes the exact-wheel
smoke suite. Keep 16.2 documented as the technical floor, but do not spend assurance
budget supporting a non-LTS minor as the oldest maintained environment.

## Evidence available today

| Area | Current evidence | What it proves |
| --- | --- | --- |
| Base distribution | Installed-wheel and lazy-import tests | Declarations and planning import without backend packages |
| Local Spark/Delta | OSS Spark and Delta with a test-only native reader | Engine lifecycle and adapter internals, not production Databricks reads |
| SQL warehouse | Weekly credentialed suite against one configured endpoint | Shared SQL core and warehouse adapter behaviour on that endpoint |
| Numbered DBR Spark | None | No runtime can currently be called tested |
| Dedicated (`SINGLE_USER`) Spark | User-observed failure; exact cause and environment not yet pinned | Treat this access mode as unsupported pending investigation |
| Serverless Spark | None | Spark Connect and rolling server behaviour are unverified |
| Python | CI runs 3.12 | The advertised 3.13 classifier is not continuously verified |

Relevant implementation boundaries:

- [`pyproject.toml`](../../pyproject.toml) deliberately leaves base dependencies empty
  and keeps PySpark and Delta development-only.
- [`delta_engine.databricks`](../../src/delta_engine/databricks.py) constructs the Spark
  backend lazily but checks only whether PySpark can be imported.
- The production reader depends on
  [`DESCRIBE TABLE EXTENDED ... AS JSON`](../../src/delta_engine/adapters/databricks/sql/queries.py).
- The [live workflow](../../.github/workflows/live.yaml) runs only the SQL warehouse
  suite from a repository checkout and locked development environment.
- The [local native reader](../../tests/adapters/databricks/native_reader.py) explicitly
  bypasses the production reader because OSS Spark cannot execute the required metadata
  command.

The [installation guide](../installation.md#requirements) currently presents DBR 16.2+
as a requirement. The [limitations reference](../reference-limitations.md#runtime-features)
more accurately says there is no complete tested Spark-backend matrix. User-facing
support language should consistently distinguish those two facts.

## Prioritized findings

### 1. Technical minimum and tested support are conflated

`DESCRIBE TABLE EXTENDED ... AS JSON` establishes a DBR 16.2 floor. It does not prove
that every later runtime preserves the metadata schema, information-schema behaviour,
SQL syntax, session configuration, exception shape, or Delta protocol operations used
by the package.

The distinction must be explicit:

- **technical floor:** the earliest environment containing the baseline API;
- **tested:** an environment exercised by a live suite for a specific Delta Engine
  artifact;
- **supported:** a tested environment covered by the maintenance policy;
- **newer, untested:** permitted, with no tested-support claim;
- **known incompatible:** a reproduced failure with an actionable recommendation.

The existing runtime review's dated table uses proposed statuses, not evidence. Its
snapshot already illustrates the maintenance problem: it records DBR 19 as Beta and DBR
18 as non-LTS, while the official table on 2026-07-29 records DBR 19 as GA and DBR 18 as
LTS. A manually edited runtime list is useful as a dated review record but must not be
the source of truth for current support.

See the official
[Databricks Runtime compatibility table](https://docs.databricks.com/aws/en/release-notes/runtime)
and
[runtime support lifecycle](https://docs.databricks.com/aws/en/release-notes/runtime/databricks-runtime-ver).

### 2. Compatibility has several independent platform axes

A single `DBR x.y` result cannot represent every supported deployment:

| Axis | Relevant states |
| --- | --- |
| Backend | Spark session, SQL warehouse connection |
| Spark compute | Classic numbered runtime, serverless Spark |
| Warehouse release channel | Current, Preview |
| Runtime lifecycle | LTS, GA, Beta |
| Runtime identity | Version family, Databricks build hashes, test date |
| Workspace | Cloud, region, Unity Catalog configuration |
| Compute policy | Standard or dedicated access mode, Photon where relevant |
| Client environment | Python version, SQL connector version, serverless environment |
| Table state | Delta protocol and enabled table features |

Do not build the full Cartesian product. Pick representative supported combinations,
record the exact environment, and use feature-level tests for differences. However,
documentation must say which axes were actually tested.

Serverless Spark needs a separate contract. Databricks automatically upgrades its server
runtime, while the Python/Spark Connect client environment has its own version. A classic
DBR result therefore does not establish serverless support. See
[serverless compute](https://docs.databricks.com/aws/en/compute/serverless) and
[serverless environment versions](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version).

SQL warehouses likewise expose Current and Preview channels. Preview is valuable early
warning but should not be a production support promise. See
[SQL warehouse release channels](https://docs.databricks.com/aws/en/compute/sql-warehouse/create).

### 3. Dedicated (`SINGLE_USER`) access mode currently fails

A user-observed Spark deployment has established that Delta Engine does not currently
work when compute uses the `data_security_mode` value `SINGLE_USER`. Databricks now
calls this Dedicated access mode; `SINGLE_USER` remains the API and system-table value.
See the
[Dedicated compute overview](https://docs.databricks.com/aws/en/compute/dedicated-overview)
and
[access-mode value reference](https://docs.databricks.com/aws/en/admin/system-tables/compute#access-mode-reference).

The root cause, first failing operation, runtime/build range, and other environment axes
have not yet been isolated. Do not generalize the observation into a Databricks platform
explanation without that evidence. Until the investigation is complete:

- state that Dedicated (`SINGLE_USER`) compute is unsupported;
- scope every classic-runtime result to its tested access mode;
- run the initial blocking runtime matrix on Standard access mode;
- do not treat a Standard result as evidence that the Dedicated failure is fixed;
- do not treat this failure as evidence that Standard mode is supported before its own
  matrix passes.

The investigation should capture:

1. the exact Delta Engine artifact, `current_version()` result, Spark/Python versions,
   cloud, Photon setting, single-node setting, Unity Catalog context, and executing
   principal;
2. the first failing boundary: import, Spark-engine construction, `current_version()`,
   `DESCRIBE TABLE EXTENDED ... AS JSON`, `information_schema`, session configuration,
   or DDL execution;
3. the complete structured Databricks condition, message, and traceback;
4. the same minimal operation on Standard mode with the runtime, identity, and table
   held constant;
5. a sanitized reproduction and live regression before changing the support status.

The `spark.sql.variable.substitute` guard, production JSON reader, information-schema
queries, Unity Catalog identity, and exception translation are useful isolation
boundaries, not assumed causes.

### 4. A runtime family is not a reproducible environment

Databricks can update a runtime line without changing the headline DBR version. DBR 19's
unified release model makes this especially visible: clusters receive continuing updates
when restarted. A report saying only "DBR 19 passed" loses the identity needed to
reproduce a later failure.

See the [DBR 19 release notes](https://docs.databricks.com/aws/en/release-notes/runtime/19).

Every live result should record:

- Delta Engine version and artifact digest;
- backend kind;
- `dbr_version` or `dbsql_version`;
- `u_build_hash` and `r_build_hash`;
- Spark and Python versions;
- serverless environment version, when applicable;
- warehouse channel/type or compute access mode, where discoverable;
- cloud and test timestamp;
- required capability results and skips.

Databricks exposes runtime and build identity through
[`current_version()`](https://docs.databricks.com/aws/en/sql/language-manual/functions/current_version).
Do not record credentials, workspace hostnames, catalog contents, or other secrets in
public artifacts.

### 5. Runtime and capability failures arrive too late

The Spark factory currently diagnoses only a missing PySpark import. The runtime and
Delta protocol are not inspected before a read or write. Unsupported operations therefore
surface as adapter read or execution errors, and a capability failure in a
multi-statement plan can occur after earlier DDL has succeeded.

Add one adapter-owned platform profile, created lazily at backend construction or first
operation rather than package import. It should support:

- a typed baseline incompatibility error below the technical floor;
- known-incompatible rules for reproduced platform defects;
- an informational newer/unknown state rather than a hard maximum;
- feature-level capability decisions where late failure risks partial application;
- environment provenance in `SyncReport` or an associated diagnostic result.

Capabilities should be tri-state: supported, unsupported, or unknown. Unknown must not
be silently treated as unsupported, and a permission failure must not be misclassified
as a platform capability failure.

Keep platform capabilities separate from table capabilities. For example, a runtime may
understand `VARIANT` while an existing table still requires a permanent protocol feature
upgrade. Runtime version alone cannot answer both questions.

Avoid a large speculative version table. Preflight only:

1. the baseline needed to read and operate safely;
2. reliable feature requirements;
3. requirements whose late failure could leave a partially applied sync.

### 6. Metadata evolution can create silent semantic loss

The JSON reader is directionally safe: malformed and unknown data types fail the whole
read, and unknown relation kinds are rejected. Additive, irrelevant top-level JSON fields
can be tolerated without coupling the parser to every platform release.

The weak point is a known type acquiring semantics outside the domain model:

- string collation is ignored;
- `CHAR(n)` and `VARCHAR(n)` are normalized to unbounded `String`;
- nested struct-field nullability is present in metadata but absent from the domain;
- future governance or generation metadata may affect whether an operation is safe even
  when Delta Engine does not manage that feature.

The current string parser and its tests explicitly assert lossy collation and length
handling in
[`sql/types.py`](../../src/delta_engine/adapters/databricks/sql/types.py) and
[`test_types.py`](../../tests/adapters/databricks/sql/test_types.py).

The structured response evolves over time; Databricks documents fields introduced in
later versions in
[`DESCRIBE TABLE`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table).
For every semantic addition, choose one explicit policy:

1. model and compare it;
2. prove it is outside Delta Engine's ownership and operations preserve it;
3. fail the read with an actionable unsupported-state error.

Do not silently report convergence after discarding state that can change the meaning or
safety of the declared table.

Capture sanitized real `AS JSON` documents from each matrix runtime as parser contract
fixtures. These complement live tests by making metadata-shape changes reviewable and
reproducible without credentials.

### 7. Compatibility jobs must test the consumer artifact

The current live workflow installs the repository's locked development environment. That
does not prove the exact wheel a user installs works on the platform.

Two artifacts answer different questions:

- **latest published wheel:** does a new or rolling Databricks environment remain safe
  for existing users?
- **candidate wheel built from the current commit:** will the next Delta Engine release
  preserve the supported matrix?

The scheduled new-runtime canary must test the published wheel first. Pull-request or
release validation should test the candidate wheel. Neither job should accidentally
import the checkout instead of the installed artifact.

Baseline compatibility tests must not skip. Optional feature tests may report an explicit
unsupported capability, but the summary must preserve and count every skip so a green
job cannot silently lose coverage.

### 8. Current local Spark coverage cannot represent the runtime range

The locked development environment exercises one OSS Spark/Delta pair and uses an
alternate metadata reader. It remains valuable for fast lifecycle regression tests but
cannot substitute for Databricks Runtime coverage.

In particular, the development requirements begin at PySpark and Delta 4.x, while the
oldest candidate LTS uses Spark 3.5 and Delta 3.x. The production PySpark surface is
small, which lowers the compatibility risk; it does not eliminate the need to execute
that surface on the oldest supported runtime.

The Spark SQL runner's temporary change to `spark.sql.variable.substitute` is one
specific hotspot to include in classic and serverless tests. Spark Connect or restricted
compute may expose different session-configuration behaviour. PySpark exception
conditions and information-schema result shapes are other adapter boundaries that need
real-runtime coverage.

## What this package should guarantee

A declarative schema-management package should:

1. install without replacing platform-managed Spark or Delta dependencies;
2. observe every piece of state it claims to manage faithfully, or refuse the read;
3. tolerate irrelevant additive platform metadata without guessing new semantics;
4. produce deterministic, inspectable plans before making changes;
5. identify baseline and high-risk feature incompatibilities before mutation;
6. preserve structured platform errors and enough environment provenance to reproduce
   failures;
7. state support as dated test evidence rather than a broad version assumption;
8. continuously test the released consumer artifact against supported and upcoming
   environments.

Delta Engine already has strong foundations for installation, deterministic planning,
and explicit failure values. The assurance matrix, environment diagnostics, capability
boundary, and semantic metadata policy are the material gaps.

## Recommended support contract

Use four externally visible states:

| State | Meaning | Behaviour |
| --- | --- | --- |
| Technical minimum | Earliest platform with the baseline metadata API | Fail clearly below it |
| Tested and supported | Live matrix passed for the released artifact | Normal support |
| Newer, untested | No known incompatibility and no passing evidence yet | Allow; expose the unverified profile |
| Known incompatible | A concrete failure is reproduced | Fail early or publish a constrained version recommendation |

Never encode the newest tested runtime as a hard upper bound. Doing so would convert each
compatible Databricks release into an unnecessary outage. There should be no one-to-one
mapping between Delta Engine and DBR version numbers.

### Initial assurance matrix

The following is a proposed test matrix, not a current support claim:

| Surface | Blocking coverage after implementation | Non-blocking early warning |
| --- | --- | --- |
| Classic Spark — Standard | DBR 16.4 LTS, 17.3 LTS, 18 LTS, and 19 GA | Newest Beta on Standard |
| Classic Spark — Dedicated (`SINGLE_USER`) | No support claim until the observed failure is reproduced, understood, and fixed | Retest the failing environment and newest GA |
| SQL warehouse | Current channel | Preview channel |
| Serverless Spark | Latest environment plus oldest maintained Python-3.12 environment | Newly released environment |
| Python client | 3.12 and advertised 3.13 for pure-Python/SQL surfaces | Next Python prerelease when useful |
| SQL connector | Declared minimum and current supported 4.x | Next major during compatibility review |

DBR 16.2 remains the technical floor. DBR 16.2 and 16.3 may be technically eligible but
are not proposed maintained environments. Every runtime below 16.2, including DBR 15.4,
is explicitly unsupported because it does not provide the metadata command required by
the production reader.

Testing every currently supported LTS at or above the floor is initially affordable
because the runtime suite should be thin. If that becomes materially expensive, choose
and publish a narrower window such as current plus previous LTS; do not silently stop
testing an older claimed runtime.

Run the initial classic matrix on Standard access mode and label the cloud and exact
mode precisely. Dedicated (`SINGLE_USER`) remains unsupported until its observed failure
is reproduced and resolved. Other clouds or modes may be expected to work; they are not
tested until a job records them.

## Thin Spark-runtime smoke suite

Keep the full behavioural suite on the SQL warehouse and make the numbered-runtime
matrix deliberately small. Each job should:

1. provision an isolated Unity Catalog namespace on an exact runtime identifier and
   explicit access mode;
2. install an exact base wheel without installing `pyspark` or `delta-spark`;
3. restart Python where the installation mechanism requires it;
4. record the sanitized platform profile before testing;
5. import public pure-Python surfaces and construct the production Spark engine;
6. create an isolated Delta table through the engine;
7. read it through the production `AS JSON` reader;
8. prove a second sync is a no-op;
9. dry-run and apply one representative safe change;
10. exercise tags, properties, constraints, and required table-feature enablement in
    focused cases;
11. verify missing-table and execution-error normalization;
12. round-trip text containing `${...}` through the Spark SQL runner;
13. drop all isolated objects in guaranteed cleanup;
14. publish the profile, pass/fail/skip counts, timings, and artifact identity.

Use required baseline tests for the blocking support decision. Feature cases may be
separated so a new optional feature can have a higher runtime floor without raising the
whole package baseline.

## Procedure when Databricks releases a runtime

1. **Discovery:** compare the workspace's available runtime identifiers with the
   maintained matrix and alert when a new Beta, GA, LTS, serverless environment, or
   warehouse channel state appears.
2. **Beta/Preview:** run the latest published wheel as a non-blocking canary. Record
   failures and metadata changes without breaking supported releases.
3. **GA:** test the already-published wheel first. This answers whether current users can
   move to the new runtime without upgrading Delta Engine.
4. **Passing GA:** add dated evidence to the matrix. Do not release a matching Delta
   Engine version merely to change a compatibility label.
5. **Failing GA:** mark it known-incompatible, preserve the profile and sanitized
   metadata, isolate the adapter boundary, add a regression fixture, and release the
   smallest compatibility patch when code must change.
6. **LTS promotion:** make the runtime blocking once it passes and enters the maintained
   support window.
7. **Retirement:** announce removal at least one Delta Engine release ahead, identify the
   final compatible package line, and state whether that line receives critical fixes.
8. **Rolling verification:** rerun supported environments weekly because service and
   runtime builds can change without a new headline version.

Optional functionality available only on a new runtime should use feature-level
capability validation. It should not raise the package-wide floor unless the production
reader or every safe baseline operation genuinely requires the new API.

Every runtime result remains scoped to its access mode. A passing Standard job must not
silently clear or hide the Dedicated (`SINGLE_USER`) limitation.

## Implementation backlog

### P0 — establish honest support evidence

- [ ] Make user-facing documentation call 16.2 the technical Spark floor rather than a
      blanket tested-support claim.
- [ ] Publish Dedicated (`SINGLE_USER`) as a current unsupported access mode while the
      observed failure remains unexplained.
- [ ] Reproduce the Dedicated failure with a sanitized environment profile, identify the
      first failing boundary, compare the same operation on Standard, and retain the
      failure as a regression test.
- [ ] Add exact-wheel, production-reader Spark smoke jobs for DBR 16.4 LTS, 17.3 LTS,
      18 LTS, and 19 GA on explicit Standard access-mode compute.
- [ ] Add the newest Beta as a non-blocking early-warning job.
- [ ] Test the latest published wheel when assessing a new runtime and the candidate
      wheel before a Delta Engine release.
- [ ] Publish a generated matrix with artifact version, runtime/build identity, backend,
      environment scope, last-tested date, and result.
- [ ] Make baseline skips fail and preserve all optional-feature skips in the published
      summary.

### P1 — diagnose and contain compatibility failures

- [ ] Add one adapter-private platform profile and a public diagnostic/report projection
      without making package import perform I/O.
- [ ] Add a typed baseline incompatibility error and known-incompatible rules; allow
      newer unknown environments.
- [ ] Introduce tri-state feature capabilities only where reliable preflight prevents
      confusing or partially applied execution.
- [ ] Keep runtime, table-protocol, permission, and declaration-policy decisions
      separate.
- [ ] Capture sanitized production `AS JSON` fixtures from every tested runtime.
- [ ] Model, preserve, or reject non-default collation, bounded strings, and nested
      struct nullability instead of silently erasing semantics.
- [ ] Include stable Databricks conditions and the sanitized platform profile in failure
      diagnostics where the backend exposes them.

### P2 — broaden explicitly supported surfaces

- [ ] Decide whether serverless Spark is in scope; if so, test the oldest maintained and
      latest environments separately from classic compute.
- [ ] Exercise SQL warehouse Current as blocking and Preview as non-blocking.
- [ ] Test advertised Python 3.13 support for pure-Python and SQL surfaces.
- [ ] Exercise the declared minimum and current supported SQL connector releases.
- [ ] Record the tested cloud and access mode, then add additional environments only when
      product demand justifies their cost.
- [ ] Define the supported-LTS window and maintenance period for a retired runtime's
      final Delta Engine release line.

## Decisions to preserve

- Do not publish or install PySpark or `delta-spark` on Databricks compute; use the
  runtime's compatible pair.
- Do not create one wheel, extra, or Delta Engine version line per DBR release.
- Do not add a hard maximum DBR version.
- Do not infer production-reader support from the local OSS Spark suite.
- Do not infer Spark-runtime or serverless support from a SQL warehouse result.
- Do not infer support for one compute access mode from a result on another.
- Do not scatter runtime comparisons throughout the domain or application layers; keep
  platform policy at the Databricks adapter boundary.
- Do not create a speculative gate for every feature. Prefer live evidence, stable
  capability checks, and clear execution errors where preflight would be unreliable.
