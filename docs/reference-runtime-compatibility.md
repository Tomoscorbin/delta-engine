---
tags:
  - reference
---

# Databricks runtime compatibility

## Support contract

The Spark backend checks the running Databricks Runtime when
`build_spark_engine(spark)` is called. Delta Engine 0.6.x supports:

```text
Databricks Runtime >=16.4,<19
```

The check uses `current_version().dbr_version`, so it covers classic compute
and serverless Spark compute when they report a numbered runtime family. It
accepts Databricks values such as `16.4.x-scala2.12`, `17.3.x-scala2.13`, and
`18.x-aarch64-photon-scala2.13`.

The factory refuses to construct an engine when the runtime is below the
minimum, at or above the maximum, cannot be read, or cannot be parsed. This is
intentional: a newly released DBR remains unavailable to that Delta Engine
release until compatibility has been reviewed.

| Delta Engine | Spark backend DBR | Behaviour outside the range |
| --- | --- | --- |
| 0.6.x | `>=16.4,<19` | Fails when the Spark engine is constructed |
| 0.5.x and earlier | No enforced package range | Compatibility is not preflighted |

The base package remains importable outside Databricks because the check occurs
only when the Spark backend is constructed. The SQL warehouse backend is not
governed by a DBR number: Databricks SQL reports a separate `dbsql_version`,
and `build_sql_engine(connection)` performs no DBR check.

## Live evidence

Supported and live-tested are separate claims. Free Edition provides
serverless compute only, so it can validate the Spark backend on the current
serverless runtime but cannot select classic DBR 16.4 or 17.3.

Before each release, the [release checklist](https://github.com/Tomoscorbin/delta-engine/blob/main/.github/RELEASE_CHECKLIST.md)
requires the candidate wheel to pass the focused Free Edition Spark notebook
and the SQL warehouse live suite. Record completed checks here; do not label a
classic runtime live-tested until the candidate wheel has run on that runtime.

| Candidate | Environment | Reported version | Commit and date | Result |
| --- | --- | --- | --- | --- |
| 0.6.x | Free Edition serverless Spark | `18.x-aarch64-photon-scala2.13` | Version probe observed 2026-07-23; candidate smoke pending | Pending |
| 0.6.x | Free Edition serverless SQL warehouse | DBSQL `2026.20` | Version probe observed 2026-07-23; candidate suite pending | Pending |

Databricks Runtime 18 and later use a unified release model: Databricks can
ship dated updates without changing the major runtime family. A `<19` gate
blocks DBR 19, but it cannot distinguish two dated DBR 18 images. The
pre-release live checks provide evidence for the image tested on that date,
not a guarantee against every later platform update.

## Changing the range

When a new DBR major family is released:

1. Test a candidate Delta Engine wheel against it.
2. Make any required compatibility changes.
3. Increase the exclusive maximum and publish a patch release.
4. Update this page and let the release workflow publish the new range.

Raising the minimum removes a previously supported environment. Before 1.0,
treat that as a breaking minor release, announce it in advance, and document
the final Delta Engine line compatible with the retired runtime. Deployments
on an older DBR must pin that older Delta Engine release explicitly; Python
package metadata cannot choose a Delta Engine version from the surrounding
DBR version.

Feature-specific requirements remain separate. A declaration may use a
Databricks feature introduced after the global floor; Delta Engine does not
preflight every feature or Delta table protocol. Unsupported feature use still
surfaces as a normal read, planning, or execution failure with the original
Databricks error.
