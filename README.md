# delta-engine

[![Live Databricks Tests](https://github.com/Tomoscorbin/delta-engine/actions/workflows/live.yaml/badge.svg?branch=main)](https://github.com/Tomoscorbin/delta-engine/actions/workflows/live.yaml)

Declarative, safety-first schema and metadata management for Delta Lake tables on Databricks.

Delta Engine gives each table an explicit, version-controlled definition of its intended schema, metadata, layout, and constraints. The declaration captures the table as it should exist today, making the contract readable, reviewable, and machine-usable. Delta Engine reads the table's current Unity Catalog state, calculates the semantic difference, validates whether the transition is safe, and derives only the DDL required to reconcile the two.

Delta Engine is a reconciler, not a data pipeline or migration ledger. Existing PySpark jobs, Databricks Declarative Pipelines, dbt models, and other systems continue to produce and write table data. Delta Engine manages the schema and catalog metadata around that data, with scoped ownership available when another team or system remains responsible for part of the table.

## Why use Delta Engine?

1. **Declarative table management**

   Define what your table should look like, not the sequence of operations required to get it there. Delta Engine reads the table’s current Unity Catalog state, compares it with the declared desired state, validates the differences, and derives the DDL required to reconcile them. The same declaration works whether the table is missing, has drifted, needs updating, or already matches and requires no changes.

2. **A single table contract in Python**

   Keep the schema, metadata, layout, and constraints a table should have today together in one version-controlled Python declaration. The declaration is a self-documenting, machine-usable contract: engineers can read it directly, changes are visible in code review, and the same definition can be imported by pipeline code, tests, and other tooling. The intended table state is explicit in one place rather than having to be reconstructed from historical DDL, deployment scripts, or pipeline behaviour.

3. **Safe, controlled schema evolution**

   Delta Engine protects the table at two stages. Construction-time guards reject declarations that are internally invalid regardless of catalog state (e.g., a primary key containing a nullable column) so these mistakes can be caught during development or unit testing without connecting to Databricks.

   For valid declarations, Delta Engine then compares the desired state with the live table and validates the complete proposed transition before the first DDL statement runs. If any part of the plan is unsafe, unsupported, or structurally invalid, the table is blocked rather than applying only the statements that happen to be valid or executing DDL that will fail.

4. **Preview and gate table changes in CI**

   Run a complete dry run against live Unity Catalog state as part of a pull request or deployment pipeline. Delta Engine performs the same reading, semantic diffing, safety validation, dependency resolution, and SQL compilation as a real sync, but executes no DDL. Reviewers can see both what would change and the exact SQL produced for the current live state, while unreadable or unsafe plans can be rejected by CI before deployment.

5. **Manage only the parts of a table you own**

   A declaration does not have to take responsibility for the entire table. Delta Engine’s `full`, `metadata`, `annotations`, and `tags` scopes let a team manage everything from the complete table definition down to tags alone, while enforcing that boundary during reconciliation. This allows one team or tool to manage comments, tags, or constraints around a table whose schema and data lifecycle are owned elsewhere, without risking changes outside its responsibility.

6. **Built for Delta and Unity Catalog**

   Delta Engine models Databricks table-management concepts directly rather than treating changes as arbitrary SQL strings. Column mapping, explicit renames, safe type widening, Delta table-feature requirements, partitioning, liquid clustering, properties, comments, tags, and key constraints all participate in the same diff, validation, planning, and reporting model. These features are planned and validated together as part of the table’s desired state rather than being managed as unrelated pieces of DDL.

7. **Reconcile related tables together**

   Delta Engine can synchronise a set of related tables in the same run rather than treating each one in isolation. Primary- and foreign-key relationships are validated before execution, parent tables are ordered before their dependants, and downstream tables are blocked when a dependency cannot reach its desired state. The engine can therefore reason about whether a related set of table definitions can converge together, not merely whether each table contains individually valid DDL.

8. **Run through Spark or a SQL warehouse**

   Reconcile tables through the Spark session already available on Databricks compute, or through a Databricks SQL warehouse from a conventional Python environment. The same declarations and reconciliation model can be used inside a data pipeline, a deployment job, or a lightweight CI workflow without installing PySpark or starting a Spark cluster.

9. **Structured results for automation**

   Every sync returns a structured `SyncReport` describing each table’s semantic changes, rejected changes, planned SQL, failures, and execution progress. The report has a stable, versioned, JSON-serialisable representation, so the same information can drive CI gates, structured logging, audit history, dashboards, and other automation rather than existing only as console output.

## Install

```bash
pip install delta-engine
```

The base package is pure Python with no runtime dependencies: declaring and
planning schemas needs no PySpark. Running a sync needs either a Spark
session supplied by Databricks Runtime or a Databricks SQL warehouse
connection. On Databricks compute, install only the
base package and use the runtime's Spark and Delta libraries. Outside
Databricks compute, install the `[sql]` extra to sync through a SQL warehouse
— for example, schema sync from CI without a Spark session or cluster. See the
[installation guide](https://tomoscorbin.github.io/delta-engine/installation.html)
for the supported installation paths and their requirements.

Install `delta-engine[cli]` to run
`delta-engine plan MODULE:ATTRIBUTE` through any standard Databricks
unified-auth configuration. The CLI always shows the semantic diff, report,
and planned SQL and never applies the generated plan. See the
[CLI reference](https://tomoscorbin.github.io/delta-engine/reference-cli.html).

## Quickstart

```python
from delta_engine.databricks import build_spark_engine
from delta_engine.schema import Column, DeltaTable, Integer, String

customers = DeltaTable(
    catalog="dev",
    schema="silver",
    name="customers",
    columns=[
        Column("id", Integer(), nullable=False),
        Column("name", String()),
    ],
)

engine = build_spark_engine(spark)
report = engine.sync(customers)     # creates the table, or no-ops if it already matches
print(report.render())
```

A first sync that creates this table renders like this:

```text
SYNC REPORT
===========

TABLE                 STATUS   STATEMENTS  DETAIL
dev.silver.customers  SUCCESS  1/1         2 columns

1 table: 1 applied (0.0s)
```

`sync` returns the structured `SyncReport`; it does not print this text itself.
Use `report.render()` for status, `report.render_diff()` for semantic changes,
`report.planned_sql_statements` for exact DDL, or `report.to_dict()` for JSON-safe
automation data. The [getting-started tutorial](https://tomoscorbin.github.io/delta-engine/tutorial-getting-started.html)
shows all four and explains where declarations fit in a pipeline.

Validation happens before execution. When a table contains an unsafe change, Delta Engine does not execute a partially valid plan for that table.

## Documentation

Start with [how a sync works](https://tomoscorbin.github.io/delta-engine/explanation-sync-lifecycle.html)
for the model, or jump to what you need:

**Getting started**

- [Installation](https://tomoscorbin.github.io/delta-engine/installation.html)
- [Getting started tutorial](https://tomoscorbin.github.io/delta-engine/tutorial-getting-started.html) — define a table and run your first sync

**Concepts**

- [How a sync works](https://tomoscorbin.github.io/delta-engine/explanation-sync-lifecycle.html) — the phases between calling `sync` and getting a report
- [The safety model](https://tomoscorbin.github.io/delta-engine/explanation-safety-model.html) — what the engine blocks, and why

**How-to guides**

- [Configure a table](https://tomoscorbin.github.io/delta-engine/how-to-configure-table.html) — properties, tags, comments, keys, and partitioning
- [Deploy metadata only](https://tomoscorbin.github.io/delta-engine/how-to-deploy-metadata-only.html) — roll out governance metadata with no schema change
- [Preview changes with a dry run](https://tomoscorbin.github.io/delta-engine/how-to-preview-changes.html)
- [Gate schema changes in CI](https://tomoscorbin.github.io/delta-engine/how-to-gate-changes-in-ci.html) — report planned changes and fail unreadable or unsafe plans
- [Handle sync failures](https://tomoscorbin.github.io/delta-engine/how-to-handle-sync-failures.html) — inspect `SyncReport` and act on each status

**Reference**

- [CLI](https://tomoscorbin.github.io/delta-engine/reference-cli.html) — the read-only plan command, connection contract, output, and exit codes
- [Capabilities and limitations](https://tomoscorbin.github.io/delta-engine/reference-limitations.html) — what the engine can and cannot manage
- [Data types](https://tomoscorbin.github.io/delta-engine/reference-data-types.html) — supported types and Spark SQL equivalents
- [Safe-change rules](https://tomoscorbin.github.io/delta-engine/reference-safe-change-rules.html) — changes the engine blocks at validation
- [Run report schema](https://tomoscorbin.github.io/delta-engine/reference-run-report.html) — the `to_dict()` payload, field by field
- [API reference](https://tomoscorbin.github.io/delta-engine/reference-api.html)

**Architecture**

- [Architecture](https://tomoscorbin.github.io/delta-engine/explanation-architecture.html) — layers, ports and adapters, design decisions
- [Implement a custom adapter](https://tomoscorbin.github.io/delta-engine/how-to-implement-adapter.html) — the `CatalogStateReader` and `PlanExecutor` ports
- [Add a new action type](https://tomoscorbin.github.io/delta-engine/how-to-add-action-type.html) — extend `Action`, `ActionPhase`, and the compiler
