# Release artifact validation

**Status:** Implemented and locally validated on 2026-07-23. A manual TestPyPI
publication remains an optional end-to-end rehearsal.

## Decisions

- Keep Hatchling and `hatch-vcs` as the PEP 517 backend.
- Use `uv build` as the frontend. It builds both the sdist and wheel, with the wheel
  built from the sdist by default.
- Publish both artifacts.
- Test the wheel as an isolated consumer, without importing the repository checkout.
- Publish the exact files that passed validation; do not rebuild in the publishing job.
- Keep dependency compatibility and Databricks Runtime coverage separate from artifact
  validation.

## Artifact contract

The release gate proves the small set of behaviours that matter to consumers:

1. `uv build` produces an sdist and a pure-Python wheel.
2. `twine check --strict` accepts both artifacts and their rendered metadata.
3. The wheel is built from the sdist and installs in a clean Python 3.12 environment.
4. The installed version matches the release version.
5. The dependency-free public API works.
6. Importing the base package, CLI shim, and Databricks facade does not eagerly load
   Spark, Delta, Py4J, Typer, or the Databricks clients.
7. Invoking the base console-script shim gives the actionable `delta-engine[cli]`
   installation message.

These are covered by one black-box script:
`tests/distribution/smoke_test.py`.

The release gate deliberately does not parse wheel or tar archives itself. Hatchling,
`uv`, Twine, and the PyPI publishing action already validate standard archive and
metadata structure. Package-specific behaviour is more reliably protected by installing
the artifacts and exercising them.

## Workflow structure

Pull-request CI:

```text
uv build
    -> twine check wheel + sdist
    -> isolated wheel smoke test
```

The pull-request artifact smoke runs once on the Python floor. The normal test matrix
provides minimum/latest Python compatibility evidence, so repeating the same
pure-Python wheel smoke in the build job would be redundant.

Release:

```text
create version commit and tag locally
    -> build once
    -> metadata and consumer smoke tests
    -> atomically push the validated commit and tag
    -> upload the validated artifacts
    -> isolated OIDC publishing job downloads and publishes them
    -> separate job creates the GitHub release
```

For a new release, the consumer smoke also runs on the latest stable Python as a cheap
final compatibility gate. This does not imply that the universal wheel needs a build
matrix.

The build job can write the release commit and tag but has no PyPI identity token. The
publishing job has `id-token: write`, but it cannot modify the repository and does not
build or test a different copy. Passing the artifacts between jobs is the provenance
boundary; an additional checksum written and checked inside one job would not improve it.

The TestPyPI workflow uses the same build/validate/publish separation, followed by an
exact-version download and base smoke test.

## Checks kept outside this gate

Optional dependencies are compatibility concerns, not archive-format concerns. Test
them without turning the basic artifact job into a version matrix:

- the normal suite exercises the locked environment;
- the existing installed-wheel CLI smoke verifies the minimum direct CLI set;
- periodic lock refreshes exercise newer versions inside the declared ranges through
  the normal suite;
- The minimum supported and latest stable Python versions, and supported Databricks
  Runtime versions, remain separate environment-compatibility checks. Intermediate
  Python minors need a dedicated job only for a distinct compatibility path.

The repository's local Spark/Delta suite is an internal locked test
environment, not a published optional-dependency compatibility promise.

The source-level test in `tests/test_packaging.py` remains the fast regression check for
the lazy-loading rule. Import Linter continues to enforce dependency direction; it does
not replace the behavioural eager-import check.

## Verification

```bash
uv build
uvx twine check --strict dist/*
uv run --isolated --no-project --python 3.12 --with ./dist/*.whl \
  python -I tests/distribution/smoke_test.py
uv run pytest tests/test_packaging.py -q --no-cov
uv run ruff check .
uv run mypy .
uv run pytest -q
uv run lint-imports
git diff --check
```

Completion means both artifacts pass standard metadata validation, the exact wheel passes
the consumer smoke test with its release version enforced, and only the validated
artifacts cross into the privileged publishing job.
