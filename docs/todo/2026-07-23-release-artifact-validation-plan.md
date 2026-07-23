# Release artifact validation — implementation plan

**Goal:** Prove that the wheel and source distribution consumers download are complete,
installable, correctly versioned, and lazy about optional backends, then publish those
exact validated files unchanged.

**Implementation status:** Implemented and locally validated on 2026-07-23. The manual
TestPyPI publication/download rehearsal remains to be run after review; its workflow gate
and exact-download probe are implemented here.

**Scope:** This plan covers artifact construction, inspection, clean-environment smoke
tests, and CI/release integration. It does not choose new dependency bounds, establish
minimum dependency versions, or add the Databricks Runtime matrix; those are subsequent
sections of the
[distribution/versioning/runtime review](2026-07-23-distribution-versioning-runtime-review.md).

**Build decision:** Keep Hatchling and `hatch-vcs` as the PEP 517 backend and use
`uv build` as the build command. `uv build` invokes the configured backend; they are
complementary. By default, `uv build` builds the sdist first and then builds the wheel
from that sdist, which is the consumer path this plan needs to validate. The validator
rejects stale duplicate artifacts; the repository's uv 0.9.x does not provide
`uv build --clear`, and GitHub jobs start from clean checkouts.

## Agreed artifact contract

- Both the wheel and sdist are supported, published artifacts.
- A release contains exactly one wheel and one sdist for `delta-engine`.
- The wheel remains pure Python and platform-independent (`py3-none-any`).
- The base wheel has no unconditional third-party runtime dependencies.
- Base imports work without Spark, Delta, Py4J, Typer, or Databricks clients installed.
- The wheel contains `delta_engine`, `py.typed`, package metadata, the MIT licence, and
  the `delta-engine = delta_engine.cli:main` console-script entry point.
- The sdist contains the source package and the files needed to rebuild the same project:
  at minimum `pyproject.toml`, `README.md`, `LICENSE`, and `src/delta_engine`.
- The base, `sql`, `cli`, and local-only `spark` installation paths are tested in
  separate clean environments so one extra cannot mask another's missing dependency.
- Pull-request builds may have a VCS-derived development version. Release validation
  must receive an expected version and reject `0.0.0`, a development version, or any
  filename/metadata mismatch.
- The release workflow builds once, validates those files, verifies their checksums did
  not change, and passes the same `dist/` directory to PyPI publishing without another
  build.

## Validation layers

| Layer | Purpose | Environment |
| --- | --- | --- |
| Source tests | Fast regression coverage for metadata and lazy imports during development | Locked development environment |
| Artifact inspection | Verify archive names, metadata, contents, entry point, and wheel tag | Built wheel and sdist, without installing the checkout |
| Clean-install probes | Exercise the package as a consumer sees it | Temporary environment per installation path |
| Pull-request artifact job | Run the complete artifact contract before merge | CI, Python 3.12 initially |
| Release gate | Check the exact release version and exact files before publication | Release job, after the local version bump/tag and before any push/publish |
| TestPyPI gate | Prevent publishing an unvalidated rehearsal build | TestPyPI job, using the same validator |

The Python 3.13 matrix belongs to the later Python-compatibility task. This first change
establishes one authoritative artifact path on Python 3.12; the matrix can then reuse it.

## Proposed files

| File | Responsibility |
| --- | --- |
| `scripts/validate_distribution.py` | Locate and inspect the two artifacts, create isolated environments, install exact local artifacts, run probes, and optionally enforce an expected release version |
| `tests/distribution/probe_base.py` | Exercise public base imports and assert optional modules were not loaded |
| `tests/distribution/probe_sql.py` | Import and minimally construct the SQL-warehouse surface without making a network call |
| `tests/distribution/probe_cli.py` | Exercise the installed console script's `--help` and `--version` |
| `tests/distribution/probe_spark.py` | Import and minimally construct the local Spark surface without contacting Databricks |
| `tests/test_packaging.py` | Retain fast source-environment metadata checks and extend the eager-import forbidden list |
| `.github/workflows/ci.yaml` | Replace the build-only job with build, validate, and upload-on-failure diagnostics |
| `.github/workflows/release.yaml` | Build and validate before pushing the release commit/tag or publishing |
| `.github/workflows/publish-testpypi.yaml` | Validate the rehearsal artifacts before TestPyPI publication |

The probes must use only the standard library plus the installed distribution and its
selected extra. They must run with the temporary directory as their working directory
and an isolated interpreter so the repository checkout and development environment
cannot satisfy imports accidentally.

## Task 1: Pin the source-level lazy-import contract

**Files:**

- Modify: `tests/test_packaging.py`

- [ ] Add `delta` and `py4j` to the optional modules forbidden after importing
      `delta_engine`, `delta_engine.cli`, and `delta_engine.databricks`.
- [ ] Keep the subprocess test behavioural. Do not encode the physical location or
      indentation of an import with an AST test.
- [ ] Confirm the current public imports pass with all development dependencies present;
      this proves they were not merely absent from the environment.

Validate:

```bash
uv run pytest tests/test_packaging.py -q --no-cov
```

Expected: all packaging tests pass and the subprocess reports no eagerly loaded optional
module.

## Task 2: Implement archive inspection and the base-wheel probe

**Files:**

- Create: `scripts/validate_distribution.py`
- Create: `tests/distribution/probe_base.py`

- [ ] Give the validator an explicit `--dist-dir` argument and an optional
      `--expected-version`.
- [ ] Require exactly one `delta_engine-*.whl` and one `delta_engine-*.tar.gz`; fail on
      stale or additional matching artifacts.
- [ ] Read wheel and sdist contents with the standard library rather than importing from
      the checkout.
- [ ] Verify the distribution name and version agree across filenames and metadata.
- [ ] In release mode, require the version to equal `--expected-version` exactly and
      reject `0.0.0` and development/local versions.
- [ ] Verify the wheel is tagged `py3-none-any`, contains `delta_engine/py.typed`, the
      licence, and the expected console entry point, and has no unconditional
      `Requires-Dist`.
- [ ] Verify every dependency is attached to a declared extra and that the declared
      extras are exactly `spark`, `sql`, and `cli`. Exact dependency versions remain
      covered by the later dependency-policy work.
- [ ] Verify the sdist contains the required build inputs and package source.
- [ ] Create a temporary base environment with `uv venv`, install the exact wheel by
      absolute path with `uv pip install --python ...`, and run the probe through that
      environment's interpreter.
- [ ] Run the probe outside the repository with isolated Python. Assert the imported
      module path is inside the temporary environment, exercise the public declaration
      surface, and confirm none of `pyspark`, `delta`, `py4j`, `typer`,
      `databricks.sdk`, or `databricks.sql` was loaded.
- [ ] Invoke the base console-script shim without CLI dependencies and verify it fails
      with the documented actionable `delta-engine[cli]` installation message, rather
      than a raw `ModuleNotFoundError`.
- [ ] Calculate SHA-256 digests before the install probes and verify them again when all
      probes finish.

Validate:

```bash
uv build
uv run python scripts/validate_distribution.py --dist-dir dist
```

Expected: one sdist and one wheel are inspected, the base wheel is installed without
dependencies, all base probes pass, and both final checksums equal their initial values.

## Task 3: Add independent extra probes

**Files:**

- Modify: `scripts/validate_distribution.py`
- Create: `tests/distribution/probe_sql.py`
- Create: `tests/distribution/probe_cli.py`
- Create: `tests/distribution/probe_spark.py`

For every extra, install the exact local wheel with that extra selected. Do not install
`delta-engine` by package name from an index.

- [ ] `sql`: install only the `sql` extra; assert the SQL connector is present, the
      warehouse-facing public surface imports, and no Spark/Delta module is loaded. Use
      dummy configuration only and make no network call.
- [ ] `cli`: install only the `cli` extra; execute the installed `delta-engine --help`
      and `delta-engine --version` commands and require exit code zero. Confirm the
      reported version equals the wheel metadata.
- [ ] `spark`: install only the `spark` extra; assert PySpark, Delta Lake, and the Spark
      adapter import together. Call `build_spark_engine` with a lightweight stand-in;
      the factory only stores the session on its adapters, so starting Java and a real
      Spark session would add cost without exercising more packaging behaviour. Do not
      exercise a Databricks connection.
- [ ] Assert the SQL and CLI environments did not receive PySpark or Delta, and the Spark
      environment did not receive the SQL connector merely because another probe used
      it.
- [ ] Keep these as latest-compatible resolution smoke tests. Testing the lowest declared
      versions is the next dependency-policy task, especially because the current Typer
      floor is already known to be wrong.

Validate:

```bash
uv build
uv run python scripts/validate_distribution.py --dist-dir dist
```

Expected: base, SQL, CLI, and local Spark probes each pass in their own temporary
environment.

## Task 4: Make artifact validation a pull-request gate

**Files:**

- Modify: `.github/workflows/ci.yaml`

- [ ] Give the existing build job full Git history/tags so `hatch-vcs` produces a
      meaningful development version.
- [ ] Rename it from a build-only job to an artifact-validation job.
- [ ] Build once with `uv build`.
- [ ] Run the validator against `dist/`; do not run a second build for the probes.
- [ ] Upload the wheel, sdist, validator output, and checksums when the job fails so the
      failure can be diagnosed. If artifacts are retained on success, label them clearly
      as CI artifacts that are not approved for PyPI publication.
- [ ] Keep the existing source tests. Artifact tests complement rather than replace lint,
      type checking, unit tests, or the fast source-level packaging tests.

Validate locally:

```bash
uv run ruff check scripts tests/distribution tests/test_packaging.py
uv run mypy scripts tests/distribution
uv build
uv run python scripts/validate_distribution.py --dist-dir dist
```

Then verify the pull-request workflow completes with the new artifact job green.

## Task 5: Make the release operation build once and fail before publication

**Files:**

- Modify: `.github/workflows/release.yaml`

The current new-release path pushes the bump commit and tag before building. Change the
order so a packaging failure does not publish a tag that has no usable artifacts:

```text
create bump commit and tag locally
        ↓
resolve expected version
        ↓
uv build once
        ↓
validate exact wheel + sdist and verify checksums
        ↓
push the already-validated commit and tag
        ↓
publish the unchanged dist/ files
        ↓
create the GitHub release
```

- [ ] Split the existing "bump and push" step into a local bump step and a later push
      step.
- [ ] Pass the resolved project version to
      `scripts/validate_distribution.py --expected-version`.
- [ ] Require the built version, both filenames, and both metadata records to match that
      version before any push or publish.
- [ ] Recheck artifact digests directly before the PyPI action.
- [ ] Do not invoke `uv build` again after validation.
- [ ] Ensure the existing-tag recovery path checks out the exact tag, derives the
      expected version from it, and uses the same validation gate.
- [ ] Keep dry-run mode non-mutating and non-publishing.
- [ ] Preserve trusted publishing and feed the existing `dist/` directory directly to
      `pypa/gh-action-pypi-publish`.
- [ ] Make a failed validator stop the workflow before the push, PyPI, and GitHub Release
      steps.

The artifact gate does not replace the normal CI suite. A separate compatibility task
should either verify that the exact release SHA already passed required CI or rerun the
required checks in the release workflow.

Validate safely without publishing:

- exercise the workflow's dry-run path;
- exercise the build and validator commands locally with a development version;
- review the workflow conditions for new-release, existing-tag, and dry-run modes;
- use TestPyPI for the first end-to-end OIDC publication rehearsal.

## Task 6: Apply the same gate to TestPyPI

**Files:**

- Modify: `.github/workflows/publish-testpypi.yaml`

- [ ] Build once with `uv build`.
- [ ] Validate that job's exact wheel and sdist before invoking the TestPyPI publisher.
- [ ] Publish only those validated files.
- [ ] Download the published version from TestPyPI into a fresh environment and run the
      base import/version probe as a separate post-publication downloadability check.
      Use the exact version and configure the main PyPI index for third-party
      dependencies when probing extras; TestPyPI is not a complete dependency index.
- [ ] Treat the TestPyPI files as a rehearsal build, not as proof that a separately built
      future PyPI release is byte-identical.

Promoting the same files from TestPyPI to PyPI would require an explicit artifact-retention
and approval design. That remains a later decision; this plan guarantees build-once
behaviour independently inside each publication job.

## Task 7: Final verification and handoff

- [ ] Run the focused packaging tests.
- [ ] Run the complete artifact validator from a clean `dist/`.
- [ ] Run Ruff and mypy on the new tooling and probes.
- [ ] Run the full local unit suite.
- [ ] Run Import Linter to ensure the probes/tooling did not alter package architecture.
- [ ] Review the final workflow diff specifically for ordering: no release push or
      publish may occur before artifact validation.
- [ ] Manually dispatch TestPyPI and verify a fresh exact-version download passes.
- [ ] Record the successful TestPyPI version/run in the pull request.

Commands:

```bash
uv run pytest tests/test_packaging.py -q --no-cov
uv build
uv run python scripts/validate_distribution.py --dist-dir dist
uv run ruff check .
uv run mypy .
uv run pytest -q
uv run lint-imports
git diff --check
```

## Completion criteria

This section is complete when:

1. a pull request cannot pass if either artifact is malformed or any supported
   installation path fails its clean-environment smoke test;
2. the release workflow rejects a version mismatch or `0.0.0`;
3. the release workflow publishes the same wheel and sdist bytes that passed validation;
4. TestPyPI refuses unvalidated artifacts and an exact published version can be
   downloaded and imported in a fresh environment;
5. base imports are behaviourally protected from eager Spark, Delta, Py4J, CLI, and
   Databricks client imports; and
6. no dependency-bound or Databricks Runtime claim has been silently expanded by this
   work.
