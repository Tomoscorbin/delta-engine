# Open questions and decisions

- [ ] Decide whether to remove `ENABLE_DELETION_VECTORS` from the default properties applied to every `DeltaTable`
- [ ] Determine whether Delta table properties and/or liquid clustering need to be tied to specific Delta / DBR versions (compatibility matrix)
- [ ] Decide whether to create an enum for `Property` values as well as keys (currently only keys are enumerated)
- [ ] Figure out how to add existing tables (tables that already exist in the catalog but are not yet declared in the registry)
