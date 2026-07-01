# Notebooks

## delta_engine_walkthrough.py

A worked example and manual integration test for delta-engine. It walks the full
lifecycle — define, sync, change, resync — and proves each sync by inspecting the
live Unity Catalog and asserting the result. The embedded `assert` statements are
the test suite; running the notebook on a cluster is how you run them.

### Requirements

- Databricks Runtime 13.3 LTS or later
- Unity Catalog enabled (for primary keys, foreign keys, and tags)
- `APPLY TAG` privilege on the target schema, plus permission to create and drop
  tables there
- delta-engine installed on the cluster (`pip install delta-engine`)

### How to run

1. Import both `delta_engine_walkthrough.py` and `catalog_inspector.py` into the
   same workspace folder (Repos / Git folder, or Workspace import). The notebook
   imports `CatalogInspector` from its sibling module, so they must live together.
2. Set the `catalog` and `schema` widgets at the top to a sandbox you can write to
   (defaults: `main` / `delta_engine_demo`).
3. Run all cells top to bottom. The notebook is idempotent — it drops its demo
   tables at the start and end, so it is safe to re-run.

`catalog_inspector.py` reads the active `SparkSession` from `databricks.sdk.runtime`,
so it works when imported as a module (a bare `spark` global is only injected into
notebooks, not into imported files).

A successful run prints a verification line after each act and raises nothing. If
any `assert` fails, that act's expectation was not met — read the printed report
above the failure.
