# Release checklist

Complete the live checks against the candidate code commit. Record that SHA in
the evidence table; an evidence-only documentation commit may follow without
rerunning the live checks. The release workflow then adds only generated
version and changelog data before building the final artifacts.

- [ ] Required CI checks pass on the final `main` commit intended for release.
- [ ] Build the candidate wheel with `uv build`.
- [ ] Upload that wheel to Databricks Free Edition and install it in a serverless
      notebook without installing or replacing PySpark or Delta.
- [ ] Run `notebooks/release_smoke_test.py` against a disposable Unity Catalog
      schema and record the reported DBR version.
- [ ] Manually dispatch `.github/workflows/live.yaml` against the serverless SQL
      warehouse and record its DBSQL version.
- [ ] Update the live-evidence table in
      `docs/reference-runtime-compatibility.md` with the commit, date, versions,
      and results.
- [ ] Confirm the documented and generated Spark requirement matches the intended
      range before dispatching `.github/workflows/release.yaml`.
