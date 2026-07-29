---
tags:
  - explanation
---

# Runtime compatibility

This page records how maintainers handle Python, Databricks Runtime, Spark, and
Delta compatibility. The policy is intentionally small: the base wheel is pure
Python, Databricks supplies the Spark and Delta pair, and delta-engine uses a
small PySpark surface around SQL-driven catalog operations.

Supported floors and tested environments are different things. A floor is a
requirement; a tested environment is evidence. The tested list must not become
an allowlist that rejects compatible newer environments.

## Current contract

| Surface | Contract |
| ------- | -------- |
| Python | Python 3.12 or later, with no upper bound |
| Spark backend | Databricks Runtime 16.2 or later, Unity Catalog, and the PySpark and Delta libraries supplied by the runtime |
| SQL warehouse backend | A Unity Catalog-enabled SQL warehouse; no PySpark or numbered Databricks Runtime required |
| Dedicated access mode | Unsupported; the Spark backend does not currently work with `data_security_mode=SINGLE_USER` |

Databricks Runtime 16.2 is the Spark backend's technical floor because the
reader requires `DESCRIBE TABLE EXTENDED … AS JSON`. It is not a claim that
every later numbered runtime has been tested.

## Python versions

Package metadata expresses the minimum supported Python version and deliberately
has no upper bound. A speculative upper bound would reject likely-compatible
new Python releases and can make installers select an older delta-engine
release instead.

CI runs the full suite on:

- the minimum supported Python version; and
- the latest stable Python version.

Intermediate minor versions remain supported without a dedicated job unless
they introduce a distinct compatibility path, such as version-specific code,
dependency resolution, or a reproduced defect. The exact built wheel is
smoke-tested once on the Python floor; repeating the same packaging check on
every Python minor would not prove a different property.

When a new stable Python version appears:

1. add it as the latest CI endpoint;
2. keep the minimum endpoint;
3. fix or narrowly document any demonstrated incompatibility; and
4. test the Spark path separately when a Databricks Runtime adopts that
   interpreter.

Raise the Python floor only when production code needs newer language or
standard-library functionality, a required dependency drops the old version,
or maintaining the old version has a concrete cost that outweighs its user
value. Do not raise it merely because a new Python version exists or an
internal development tool wants a newer interpreter.

A floor increase is a deliberate compatibility change. Announce it, update
`requires-python`, classifiers, CI, the lock, and documentation together, and
identify the final delta-engine release that works on the retired Python
version.

## Databricks Runtime, Spark, and Delta

Do not publish, install, or select `pyspark` or `delta-spark` for Databricks
users. Databricks owns the compatible Spark and Delta combination in each
runtime; shadowing either package from a notebook can create the mismatch the
runtime is designed to avoid.

delta-engine therefore does not maintain a Spark-by-Delta dependency matrix,
one wheel per runtime, a runtime allowlist, or a maximum Databricks Runtime.
The local Spark suite checks delta-engine against the repository's locked
development environment, but it is not evidence that the Spark adapter works
on a numbered Databricks Runtime.

When a new Databricks Runtime appears:

1. make no package-metadata change by default;
2. test the existing exact wheel on that runtime before claiming it as tested;
3. if it passes, record the environment as evidence without requiring a new
   delta-engine release; and
4. if it fails, reproduce the narrow boundary, then fix it or document that
   concrete incompatibility.

Any published test evidence should identify the exact delta-engine artifact,
Databricks Runtime, and access mode. A runtime absent from that evidence remains
allowed unless a concrete incompatibility is known.

## New Databricks or Delta features

A new platform feature does not change delta-engine's contract until the
project intentionally adopts it. When it does:

- document the feature's own Databricks Runtime or Delta protocol requirement;
- let the user choose a runtime that provides the feature;
- keep baseline operations available on older supported runtimes; and
- prefer the original Databricks error unless a targeted preflight materially
  improves safety or avoids partial execution.

An optional feature must not raise the package-wide runtime floor. Raise the
global floor only when a baseline operation genuinely requires a newer
platform and retaining the old path is impractical.

## What each test proves

| Evidence | What it supports |
| -------- | ---------------- |
| Minimum/latest Python CI | Pure-Python and locked integration behaviour at the supported interpreter endpoints |
| Installed-wheel smoke | The built wheel installs and its base public surface works on the Python floor |
| Local Spark suite | Delta-engine behaviour against the locked development Spark/Delta pair |
| Live SQL warehouse suite | Shared SQL/catalog behaviour and the SQL warehouse adapter |
| Live Spark notebook smoke | The exact wheel and Spark adapter work on the named runtime and access mode |

Only the final row establishes evidence for a numbered Databricks Runtime.
Until that coverage exists, describe 16.2 as the technical floor and avoid
calling any numbered runtime live-tested.

See [Installation](installation.md) for the consumer setup and
[Capabilities and limitations](reference-limitations.md#runtime-features) for
current user-visible restrictions.
