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

1. Import `delta_engine_walkthrough.py` into your workspace (Repos / Git folder, or
   Workspace import). It is a Databricks source file and imports as a notebook.
2. Set the `catalog` and `schema` widgets at the top to a sandbox you can write to
   (defaults: `main` / `delta_engine_demo`).
3. Run all cells top to bottom. The notebook is idempotent — it drops its demo
   tables at the start and end, so it is safe to re-run.

A successful run prints a verification line after each act and raises nothing. If
any `assert` fails, that act's expectation was not met — read the printed report
above the failure.
