# Open questions and decisions

- [ ] Decide whether to remove `ENABLE_DELETION_VECTORS` from the default properties applied to every `DeltaTable`
- [ ] Determine whether Delta table properties and/or liquid clustering need to be tied to specific Delta / DBR versions (compatibility matrix)
- [ ] Decide whether to create an enum for `Property` values as well as keys (currently only keys are enumerated)
- [ ] Figure out how to add existing tables (tables that already exist in the catalog but are not yet declared in the registry)
- [ ] Add support for foreign keys
- [ ] Add support for tags
- [ ] Add support for clustering
- [ ] make partitioned_by a Column-level thing
- [ ] add unique columns: ALTER TABLE U ADD CONSTRAINT u_uq_email UNIQUE(email);
- [ ] add scripts/example.ipynb for example notebook
- [ ] where does backticked_table_name belong? Should it be constructed inside the compiler dispatches?
- [ ] should all of the sql statements live in a dedicated file and be imported into reader/exeecutor?
- [ ] should databricks rules live in validator?
- [ ] add clarifying comments to differ functions