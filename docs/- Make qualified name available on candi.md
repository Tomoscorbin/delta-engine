# PR review checklist

- [x] Make qualified name available on candidate rather than candidate.table.qualified_name
- [x] remove variable names like foreign_key_tuple. We shouldn't write the type in the name
- [x] remove _utc_now()
- [x] TableRunReport.execution should be Optional, rather than passing ExecutionSummary()
- [ ] ~~should execution failures be bundled up with validation failures as one failures attribute?~~ **DEFERRED** — involves a redesign
- [ ] differ doesnt need to order foreign keys??? **→ folded into the _diff_foreign_keys rework (points 10–12); ActionPlan owns ordering**
- [x] make sure we use full names and not fk (unless in a listcomp or similar)
- [x] Don't think I like resolve_foreign_key_constraint_name()
- [ ] remove comments from ActionPhase. Order of actions should be documented properly
- [ ] the tuple comprehension FK differ is hard to read and doesnt match the established pattern
- [ ] review _fk_content_equal and make it easier to understand what the logic is logic
- [ ] review foreign key differ in totality
- [ ] should we remove the PK and FK AnalysisException catch and instead change the tests?
- [ ] Can we simplify or improve the fk graph & dependency logic?
- [ ] is there a clearer, simpler, more natural way to do this bit in engine: `if isinstance(state, ReadFailed) and not external_failures.get(qn): ...` Like do we need external_failures
- [ ] should we do the same thing as candidate.table.qualified_name with catalog_state? we are doing things like catalog_state.failure.exception_type
- [ ] Can we simplify _fetch_foreign_keys? seems quite complicated
- [ ] update documentation
