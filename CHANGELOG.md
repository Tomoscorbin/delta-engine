## v0.7.0 (2026-07-31)

### BREAKING CHANGE

- ValidationFailure.rule_name now reports 'StreamingTableAnnotationsOnly' where it previously reported 'StreamingTableTagsOnly'. Consumers matching on that value in a run report's to_dict() projection must update. schema_version stays at 2: the key and its type are unchanged.
- a declaration must name existing columns with the
catalog's exact case. Declarations that previously synced against
differently-cased catalog columns now fail validation with
ColumnSpellingMustMatchCatalog, or REFERENCED_COLUMN_CASE_MISMATCH for a
foreign key's referenced columns. Run DESCRIBE TABLE and copy the
spelling it reports.
- TableRunReport.execution_outcome is renamed to
TableRunReport.execution, and the execution pass-through property is
removed. Code constructing or reading that field by name must rename it.
The to_dict() payload already spelled the key "execution" and does not
change, so the schema_version 2 contract is unaffected.

### Feat

- annotations scope on streaming tables (#310)
- rework relationship resolution and dependency blocking (#290)

### Refactor

- eligibility checks name their role, not their membership (#307)
- a diff entry names its operation, not a bare symbol (#305)
- the accepted plan narrows once; execution drops its alias (#304)

## v0.6.0 (2026-07-28)

### Feat

- bind symbolic plan references to post-sync spelling
- derive post-sync column spellings from a table diff
- add recursive semantic data-type identity
- add the identifier identity-key policy module
- observe enabled delta table features at the read boundary
- plan required table-feature enablement in the differ
- add EnableTableFeature action with compiler and diff rendering
- add table-feature domain vocabulary and observed feature state

### Fix

- explain missing Databricks Spark runtime
- bound optional dependency majors
- remove unsupported spark extra
- verify supported CLI dependencies
- preserve column identifier spelling in the model and API
- read an unrecognized feature value as unsupported
- reject feature policies whose names resolve ambiguously
- report all state established by table creation
- stale docs and tests
- fix covereage

### Refactor

- adopt catalog spellings before diffing
- add catalog-spelling adoption pass
- select identifier spelling during diffing
- bind references to column identifiers
- simplify identifier execution spelling
- simplify Spark runtime diagnostic
- streamline distribution validation
- simplify distribution validation
- keep feature policy in table diff
- trust observed table features
- tighten table feature ownership
- drop Identifier.key and .spelling properties
- delete identifier_key helpers superseded by Identifier
- key catalog column tags by Identifier
- wrap raw declaration input at the Identifier boundary
- bind plans through Identifier-keyed resulting schemas
- diff through plain Identifier collections
- wrap action name fields as Identifiers
- wrap table layout references as Identifiers
- store constraint columns and names as Identifiers
- replace canonical_data_type with Identifier-aware type equality
- store column and struct-field names as Identifiers
- add case-insensitive Identifier str type
- key dependency types and tag joins by identity
- resolve declaration references through identifier keys
- align and order the diff through identifier keys
- validate table structure through identifier keys
- judge key signatures and generated names by identity key
- centralize Databricks feature policy
- simplify table feature reconciliation
- make feature property keys explicit
- redesign
- keep the feature policy an ordinary frozen value
- build and keep the feature-policy lookups
- make the definitions the feature vocabulary
- name the two kinds of table-feature requirement
- consolidate the table-feature policy into one module
- move table-feature vocabulary into the application layer
- remove unnecessary postponed annotations
- WarehouseSqlRunner added
- introduced SparkSqlRunner
- make phase outcomes the run source of truth
- constraint identity consolidated
- make diff.py more readable
- added desired and observed values to SetColumn/TableTag
- consolidate column diffing
- tighten comparison boundary invariants
- make action plans self-contained
- keep execution summaries passive
- keep run reports passive
- expose report construction
- simplify run outcome ownership
- consolidate table run outcomes
- consolidate declaration validation
- keep foreign key declarations public
- make constraint naming explicit
- separate port errors from failures
- consolidate execution sequencing
- consolidate named scopes
- make validation composition explicit
- rejig policy validation

## v0.5.0 (2026-07-20)

### Feat

- read streaming tables as observed state
- thread the observed relation kind to the compiler
- compile the ALTER dialect per relation kind
- gate streaming tables to tag-only declarations
- carry the observed relation kind on table drift
- model the observed relation kind in the domain
- read only managed and external Delta tables as engine state
- **reader**: add AS JSON query builder and missing-relation classifier
- **reader**: assemble ObservedTable from a snapshot plus info_schema
- **reader**: parse an AS JSON document into a table snapshot
- **reader**: parse the table_constraints string into structured keys
- **reader**: map structured AS JSON types to domain types

### Fix

- normalize identifier case to lowercase instead of rejecting mixed case
- reject clustering keys and Map-of-Map declarations Databricks cannot deploy
- validate foreign-key types against the registered parent declaration
- fail the read on a decimal without precision and scale
- **reader**: confirm the schema exists before reading a missing table as absent
- **reader**: a missing schema or catalog fails the read instead of reading as absent
- clean up planning
- **reader**: fail the read on unmappable column types
- **reader**: fail the read on malformed layout and type shapes
- report schema version fix
- walkthrough fix
- vocab change - validation failed -> planning failed
- **reader**: parse_table_snapshot raises MetadataParseError on malformed constraints
- **reader**: constraint parser raises ConstraintParseError on malformed input and ignores unmanaged constraint types
- **reader**: data_type_from_json returns None instead of raising on malformed types
- write the live test summary once, not once per xdist worker
- remove false-success race from table creation

### Refactor

- normalize observed identifiers once, in the domain constructors
- make SQL compilation an explicit phase
- rename the compiler statement target to _Target
- derive the unsupported-relation message from the admit tables
- carry the relation kind on the diff and plan, not the port
- judge managed properties in the shared read
- judge relation acceptance in the shared read
- **domain**: rename Finding to Unresolvable and extract into its own module
- **reader**: one deep read function per information_schema aspect
- **reader**: package the describe as one absence-aware step
- **reader**: name catalog rows and fold parsing into assembly
- **reader**: own the describe result shape in the describe mapper
- **reader**: name the injected query-runner type
- **reader**: expose one shared read entry point
- simplify column and primary-key diff helpers
- removed redundant match case in planning.py
- sync() logging cleaned up
- inline scope-gate checks, TODO to revisit class form
- make scope a short-circuiting validation gate
- share observed-table assembly across the Databricks readers
- **reader**: source primary and foreign keys from information_schema
- **reader**: drop the unmappable-partition-column special case
- **reader**: rename TableSnapshot -> TableDescription
- **sql**: parse constraints to domain objects, colocate type mapping, rename describe module
- **reader**: unify both backends behind one shared read_catalog_state
- clean up engine
- **reader**: remove the obsolete per-aspect read path and DDL type parser
- **spark**: read via AS JSON; drive local e2e with a native OSS-Spark reader
- **warehouse**: read via AS JSON through the shared assembly

## v0.4.0 (2026-07-14)

### BREAKING CHANGE

- public result/failure types lose statement_preview in
favour of statement; failure text renders 'SQL:' instead of 'SQL preview:'.
- install with delta-engine[spark] instead of
delta-engine[databricks]; import build_spark_engine instead of build_engine
from delta_engine.databricks.
- ExecutionFailure.statement_index replaces
action_index; the run report's execution.total now counts planned
statements.
- SyncReport.any_failures is now SyncReport.has_failures,
mirroring TableRunReport.has_failures.

### Feat

- render column renames in sync reports
- compile RenameColumn to ALTER TABLE ... RENAME COLUMN
- reject ambiguous declared renames; point guard at renamed_from
- relabel observed columns through declared renames in diff_table
- add ColumnRenamed change and RenameColumn action
- require column mapping at construction for rename hints
- enforce rename-hint coherence on DesiredTable
- add renamed_from declaration hint to Column
- adopt unified auth and explicit declaration selection in the CLI
- add DuplicateTableDefinitionError and extract prepare_desired_tables
- wire the delta-engine console script with graceful degradation
- add delta-engine apply command
- add delta-engine plan command
- add render_planned_sql for planned-statement previews
- add CLI warehouse connection resolution
- add CLI declaration loading for module[:attr] specs
- expose build_sql_engine for PySpark-free warehouse syncs
- add warehouse plan executor
- add warehouse catalog state reader
- add information_schema table-row and columns query builders
- add PySpark-free DDL type-string parser to the shared sql package
- export TableRunReport and concrete failure types
- add to_dict projection on sync and table run reports
- record compiled SQL statements on every table run report
- add PlanExecutor.compile returning the plan's SQL statements
- add has_changes and rename SyncReport.any_failures to has_failures
- adopt the full Delta type-widening matrix
- permit safe type widenings gated on delta.enableTypeWidening
- lower column type changes to AlterColumnType
- add AlterColumnType action with SQL compilation and diff rendering
- register delta.enableTypeWidening as a managed property

### Fix

- Update verification table header in conftest.py
- enforce Databricks 256-character tag key and value limits
- harden correctness edge cases
- compile empty column comments as COMMENT '' instead of UNSET COMMENT
- register the parent in the FK-rename live sync
- harden declarative column rename planning
- filter lazy connector warning
- reject path-like declaration references and honour module __getattr__
- suppress irrelevant PyArrow warning
- close a false-positive gap in the CLI PySpark-free contract
- compile empty column comments as COMMENT '' instead of UNSET COMMENT
- make WarehouseExecutor.execute total across cursor lifecycle
- treat pathological nesting depth as unmappable in the type parser
- lint

### Refactor

- plan explicit constraint drops around column renames
- rename domain Column to DesiredColumn
- drop compiler-facing alias properties from actions
- split table drift into actions and findings
- remove primary-key replacement correlation
- clarify planning domain boundaries
- collapse changes into canonical actions
- single-source the duplicate-table rule in engine preparation
- delegate CLI authentication to Databricks SDK
- narrow CLI to read-only OIDC plans
- fail fast on a missing HTTP path and pin cwd import precedence
- remove TableSnapshot in favour of standalone table types
- readers build ObservedColumn for catalog state
- split observed catalog columns into ObservedColumn
- fold desired-table preparation into the engine, move DesiredTableSource to ports
- route reader imports through the sql facade, widen rows.py framing
- acquire the warehouse cursor lazily inside the execution loop
- share exception naming via duck-typed py4j probe, delete the injection seam
- read Spark catalog types through the shared DDL parser, delete spark/types.py
- move the shared per-column read policy behind column_from_catalog
- fuse DESCRIBE DETAIL properties mapping into one boundary function
- share quoted-token reading between backtick names and string literals
- narrow the exception seam to type naming, delete errors.py
- share DESCRIBE DETAIL row mapping, tolerant of both shapes
- rename sql_type_for_data_type to render_data_type
- record exact statements and exception messages on failures
- align execution and error-translation vocabulary
- move information_schema row mappers into the shared sql package
- extract shared statement-execution loop from the spark executor
- extract shared exception-summarising core from the spark backend
- rename the databricks extra to spark and build_engine to build_spark_engine
- enforce the PySpark-free shared sql core with import-linter
- rename Databricks reader and executor to Spark names
- split Spark type parsing out of the shared sql package
- move Spark-coupled adapter code into a spark subpackage
- denominate execution results in statements
- polish the CI report contract before it ships
- execute compiled statements instead of recompiling the plan
- fold compile back into the plan phase
- extract compile into its own sync phase
- compile plans unconditionally, dropping the empty-plan guard
- extract action interpretation into diff_entries module
- reintroduce _SelfReference class in delta_table.py
- state the FK name-collision fact without API name-generation advice
- dedupe partition/clustering key-list validation in TableSnapshot
- name the resolved referenced side of a foreign key
- merge partition and clustering validators into _validate_layout

## v0.3.0 (2026-07-10)

### BREAKING CHANGE

- DeltaTable(metadata_only=True) is replaced by
DeltaTable(scope="metadata"). The metadata_only parameter is removed.

### Feat

- **api**: declare sync scope with a scope parameter
- declare foreign key pairings as an explicit mapping
- declare primary keys at table level
- canonicalize foreign key pair order in the domain
- derive clustering from Column.cluster_key in DeltaTable
- read observed clustering from DESCRIBE DETAIL clusteringColumns
- diff clustering by set and emit AlterClustering
- render clustering changes with an OPTIMIZE FULL hint
- compile CLUSTER BY for create and in-place clustering
- add AlterClustering action and SET_CLUSTERING phase
- carry clustered_by on table snapshots with invariants
- add CLUSTERING aspect and Column.cluster_key flag

### Fix

- reject a bare string primary_key at declaration
- **notebooks**: migrate walkthrough to the scope parameter
- update runtime-import CI smoke test to table-level primary_key
- drop OPTIMIZE FULL hint from clustering removal; note clustering in metadata-only docs
- reconcile clustering before dropping columns
- **test**: make DESCRIBE DETAIL reader fakes support asDict()

### Refactor

- **api**: clean up scope resolution and structure-gated validation
- wrap ForeignKey.columns in a read-only view; drop stale inference wording
- drop redundant clustering guard and trim docstrings
- declare clustering with table-level clustered_by, not a per-column flag

## v0.2.0 (2026-07-09)

### Feat

- expand walkthrough notebook with live checks and construction-time guards
- expose DeltaTable declaration as read-only accessors
- build wheel in CI

### Fix

- restore test body dropped in merge conflict resolution
- lint
- spacing in validation for primary and foreign key constraints
- reject duplicate foreign key columns at construction
- read primary key columns in key order
- only treat a missing information_schema as unavailable in the probe
- reject colliding generated foreign key constraint names at declaration
- exclude unique-backed foreign keys from the inbound PK-reference query
- reject cross-catalog foreign keys at declaration

### Refactor

- **application**: tighten constants and aliases
- **plan**: extract change vocabulary
- **domain**: tighten table model boundaries
- drop metadata_only accessor from DeltaTable
- store metadata_only flag instead of deriving it

## v0.1.0 (2026-07-08)

### Feat

- **api**: accept any sequence or mapping in public declarations, store tuples internally
- **validation**: block primary key drops while foreign keys reference the key
- **diff**: carry inbound foreign key references on primary key changes
- **reader**: observe foreign keys referencing each table
- **domain**: model inbound foreign key references on observed tables
- **api**: reject foreign keys whose column types do not match the referenced primary key
- **api**: enforce Unity Catalog tag count and value-length limits at declaration time
- **api**: reject CDF-reserved column names when change data feed is declared
- **api**: gate nested struct field names behind column mapping
- **api**: require column mapping for special-character column names
- **properties**: validate declared property values at declaration time
- **domain**: reject unpartitionable column types and all-column partitioning on desired tables
- **sql**: backtick struct field names in compiled DDL
- **types**: add Struct type with name+type fields
- **types**: map TINYINT, SMALLINT, BINARY, TIMESTAMP_NTZ, VARIANT; normalise CHAR/VARCHAR to String on read
- **domain**: cap Decimal precision at the Delta/Spark limit of 38
- shorten grid DETAIL to failure headline; underline failures section header
- title report and diff output (SYNC REPORT / DIFF headings)
- add failures section and dry-run banner to report
- show execution progress and humanized detail in report grid
- group diff output by category with richer action lines
- record dry_run on SyncReport
- **adapter**: probe information_schema availability; fail reads loudly on UC
- exact property declarations — None assertions, no default injection, metadata-only fast-fail
- property guards — transition, must-declare, unset-forbidden rules and drop precondition
- exact-declaration properties diff with policy parameter and PROPERTIES gate
- UnsetProperty action; SetProperty observed_value; None-filtered CREATE properties
- DesiredTable properties accept None absence assertions
- property policy — domain mechanism, application definitions
- add metadata_only mode to DeltaTable
- add UnmanagedDimensionDrift rule and wire desired into validate_diff
- add managed_aspects scope field to DesiredTable
- add TableAspect enum for per-aspect table management
- add remaining five dimension types to diff.py
- add ColumnsDimension to diff.py
- add UnhandledFact, Dimension protocol, and Changed guard to diff.py
- lower table diffs into action plans
- compute typed table diffs from desired and observed state
- add typed diff vocabulary for table planning
- declare foreign keys by table reference
- add QualifiedName.parse boundary constructor
- add desired definition to TableRunReport as the pipeline record
- **notebooks**: add display tables for schema, tags, and properties
- **notebooks**: add column_tags_of accessor to CatalogInspector
- **results**: render column tag changes in the plan diff
- **reader**: observe column tags from information_schema to close the round-trip
- **sql**: compile SetColumnTag/UnsetColumnTag to ALTER COLUMN SET/UNSET TAGS
- **plan**: reconcile column tags with full-state ownership
- **plan**: add SetColumnTag/UnsetColumnTag actions and ordering
- **domain**: add case-sensitive tags field to Column
- **notebooks**: add teardown, README, and finalize walkthrough
- **notebooks**: add foreign-key failure and dry-run acts
- **notebooks**: add validation-block acts
- **notebooks**: add safe evolution and drift-management acts
- **notebooks**: add define, first sync, and idempotent resync acts
- **notebooks**: scaffold walkthrough notebook with setup and helpers
- **results**: render table tag changes in the plan diff
- **api**: accept free-form tags on DeltaTable
- **reader**: observe table tags from information_schema to close the round-trip
- **plan**: reconcile table tags with full-state ownership
- **plan**: add SetTableTag/UnsetTableTag actions and their SQL compilation
- **domain**: add tags field to TableSnapshot
- move constraint-name derivation into the SQL compiler
- add diff_by_key match-by-identity matcher
- reject foreign keys that do not reference the parent's primary key
- add human-readable __str__ and diff() to report types
- add dry_run flag to Engine.sync
- extend resolve() to accept external_failures for cross-failure-type propagation
- foreign keys are all-or-nothing with a narrow resolver interface
- add foreign-key failure result types (fail-closed vocabulary)
- wire FK planning into Engine.sync() via phased architecture
- add SkipReason, SkippedForeignKey, ForeignKeyValidationReport to results
- add foreign_key_planning module (resolve FK dependencies)
- expose foreign_keys parameter on DeltaTable; export ForeignKey alias
- read foreign key constraints from Unity Catalog information_schema
- compile DropForeignKey and SetForeignKey to SQL
- add FK differ — compute DropForeignKey/SetForeignKey actions
- add DropForeignKey and SetForeignKey action types
- add foreign_keys field to TableSnapshot
- add ForeignKeyConstraint domain value object
- add primary_key field to Column and primary_key/primary_key_constraint_name to DeltaTable
- add _fetch_primary_key to DatabricksReader; populate ObservedTable.primary_key
- add DropPrimaryKey and SetPrimaryKey SQL compiler handlers; inline PK in CreateTable
- add PrimaryKeyColumnsNullable validation rule
- add _diff_primary_key to differ — emits DropPrimaryKey/SetPrimaryKey on PK drift
- add DropPrimaryKey and SetPrimaryKey action types with DROP_PRIMARY_KEY and SET_PRIMARY_KEY phases
- add primary_key field and primary_key_constraint_name to TableSnapshot/DesiredTable
- export Property from public API so users can discover managed keys
- add curated top-level delta_engine namespace (lazy Databricks factory)
- close consumer import-surface gaps and fix test package collision (C3)
- declare application public API (C2 Q1a)

### Fix

- keep __version__ out of __all__ runtime surface
- **reader**: fail reads when a partition column type is unmappable
- **properties**: restrict integer property values to canonical digits
- **types**: treat casefold-colliding struct field names as unmappable
- lint
- walkthrough notebook updated
- classify CYCLE only for FKs inside the table's own cycle
- block FK dependents when a parent fails during execution
- notebook fix
- lint
- walkthrough notebook update
- e2e tests updated
- pytest fail under 70%
- dependency resolution tests updated
- validation tests updated
- lint
- updated plan domain tests
- deleted old test file
- removed runtime_checkable from protocols
- address review findings — doc staleness, test gaps
- address review findings — rename PropertySet, close test gaps, fix stale docs
- resolve ruff line-length and docstring violations
- lint
- add missing docstrings and fix line lengths to satisfy ruff
- skip planning validation-failed runs; fix message prefix; update stale docs
- suppress column actions when data_type change is present in ColumnsDimension
- remove _log_output and inline log
- fail loud when DESCRIBE DETAIL returns no rows for a present table
- line spaces added to engine sync()
- Clean up properties by removing commented lines
- reference self.references in FK type error, not unbound name
- lint
- match case to use _
- walkthrough notebook updated + logging improved
- **notebooks**: wrap long lines in display_tags and display_properties
- create schema statement dropped
- resolve PK constraint name via the value object in the compiler
- lowercase observed foreign-key constraint names in reader
- align composite foreign-key columns in reader query
- pass None instead of {} to FakeSpark in no-fk reader test
- propagate read failures to FK dependents; fix FakeSpark; update stale docstring
- casefold references string from catalog metadata in _fetch_foreign_keys
- widen _classify_failures return type and add FakeSpark safety fallback
- use specific tuple[Failure, ...] type hint in test_errors helper
- correct FK sync docstring, type the record closure, add diamond propagation test
- treat self-referential foreign keys as applicable, not cycles
- reject foreign keys that resolve to a duplicate constraint name
- validate references is lowercase, non-blank, and constraint_name is non-blank
- match foreign keys by content so external constraint names stay idempotent
- never strip DropForeignKey when suppressing skipped constraints
- only skip FKs of true cycle members, not tables blocked by a cycle
- drop foreign keys before the columns and keys they reference
- remove unused import and rename fk abbreviations in test_engine
- make FK differ idempotent across catalog round-trips; validate references format
- guard FK query in FakeSpark test helpers
- remove unused imports in test_actions.py
- replace qn abbreviation with qualified_name in sync() comprehensions
- handle missing information_schema in _fetch_primary_key
- todo added
- address final review findings — backtick constraint names, duplicate PK validation, SQL injection, abbreviations
- correct DropPrimaryKey SQL (add IF EXISTS) and remove backtick from constraint name
- tidy reader import order and document supported column types
- skip unmappable columns rather than failing the whole table
- break long line in SetColumnComment compiler (E501)
- validate DeltaTable at construction time rather than deferring to to_desired_table()
- lint
- move statement_preview into ExecutionFailure so format_lines is self-contained
- lint
- validation rules report all violations in a single pass
- remove logging.raiseExceptions = False from configure_logging
- use exc_type_name in executor to surface Java class for Py4J errors
- enforce lowercase validation on partition column names
- lint
- lint
- write tests isolated
- lint
- decouple engine from registry
- reject blank names and duplicate partition columns in the domain
- render column comments in CREATE TABLE (lost on first sync)
- lint
- lint
- make logging opt-in instead of a factory side effect (B6)
- flag column type changes as a validation failure (B1)
- guard nullability tightening and stop execution at first failure (B3)
- contain all reader failures so one bad table can't abort the sync (B2)
- add missing docstrings to __post_init__ methods to satisfy ruff D105
- enforce case-insensitivity in Column and QualifiedName at construction time
- lint
- lint
- lint
- partition columns once in diff_columns to eliminate redundant actions for added columns

### Refactor

- **errors**: avoid shadowing report in the failed-tables comprehension
- **api**: rename table module to delta_table
- honest AbstractSet annotations and drop redundant guards
- replace single-letter variables and tighten rendering types
- **reader**: name loop variables and hoist the partition flag
- **engine**: make _TableRun a dataclass and fix phase-chain docstring
- **diff**: use dict.get lookups and keyword action construction
- **diff**: require referencing_foreign_keys on primary key changes; pin the exemption's phase-order dependency
- **api**: resolve the FK reference in one place and drop the dead referenced-side skip
- **properties**: deepen PropertyDefinition with declaration and transition judgments
- keep `from __future__ import annotations` only where needed
- **report**: make reports pure data behind a render seam
- route internal imports through domain package facades
- **adapter**: fold sql_preview into the executor
- **adapter**: rename clause helpers; honest warnings and logging docs
- **adapter**: single lazy layer on the public Databricks path
- **adapter**: move exception summarizing out of the sql package
- **adapter**: compile_plan returns action-statement pairs
- **adapter**: single information_schema seam; exact-query test fakes
- **adapter**: extract pure row mappers from the reader
- **adapter**: extract pure information_schema query builders
- consolidate PK/FK domain constraints into constraints.py
- group FK rows with itertools.groupby in reader
- extract blocking_failures helper in dependency resolution
- Refactor parse method in QualifiedName class
- dissolve api/properties.py — import Property and the policy directly
- extract MissingTableUnmanaged into a named invariant class
- rename PropertyUndeclared; uniform (desired, observed) diff-helper signatures
- scope check becomes UnmanagedAspectDrift with the Rule interface
- Rule.evaluate(drift); column-drop precondition becomes a Rule; single-source Property enum
- TableDrift carries desired; fold column-drop precondition into validate_diff
- metadata-only carries properties; reader filters observed keys; transitions-to-absence
- rename DriftFact to Change; facts to changes
- uniform _diff_* helpers in diff_table; readability spacing
- make unmanaged-aspect drift a scope invariant; scope rules to managed facts
- replace dimension containers with a flat aspect-tagged DriftFact model
- remove stale ColumnsDimension reference in comment
- split ColumnsDimension into structure, comments, and tags dimensions
- move blocking policy from domain to validation; delete unhandled()
- replace ColumnChanged with specific column entry types; unify ColumnsDimension interface
- move diff() construction into each dimension type; simplify diff_table
- push plan() onto TableDiff types; remove engine._plan_missing
- fix abbreviations, collapse duplicate SetColumnTag arm, remove redundant dict copies
- delete lower.py; move TableMissing planning into engine; update __init__ exports
- update validation to use dimension protocol; remove DisallowPartitioningChange and UnsupportedColumnTypeChange
- rewrite TableDrift and diff_table to use dimension objects; remove match_by_key
- delete sentinel actions dissolved by diff-first planning
- replace differ with diff-then-lower composition
- validate the table diff instead of the action plan
- fold empty-plan display branches in rendering, drop zero-table sentinel
- drop empty-collection guards in TableSnapshot.__post_init__
- model Tarjan's on-stack membership as a set, not a dict
- add TableSnapshot.primary_key_columns, remove repeated PK unwrap
- collapse the shallow domain_type_from_ddl wrapper
- split Spark type mapping by input shape
- eliminate dead optional-widening and unused parameters
- require a named constraint and delete the is-named guards
- generate constraint names when lowering the public API
- read the primary key's catalog constraint name
- add constraint generate() factory and tolerate-named shim
- replace public Registry with variadic engine.sync
- dispatch FK reference lowering with a match statement
- merge FK declaration into table module and simplify lowering
- make SetForeignKey carry compiler-ready fields
- structure foreign key references as QualifiedName
- replace _TableRun dataclass with plain accumulator class
- rename trimmed results.py to report.py
- move ValidationResult into application/validation.py
- move adapter boundary vocabularies into application/ports.py
- move the failure family into application/failures.py
- thread a mutable _TableRun through sync phases; log FK resolution failures
- chain Engine.sync over TableRunReport records; resolve returns ResolveResult
- use plain set for never-mutated local failure sets
- move report rendering into its own module
- collapse TableRunReport to one phase-ordered failures stream
- single failure accumulator in sync; resolve takes blocked names, returns FK failures only
- drop redundant action_index from ExecutionFailed carrier
- tag failures with a FailurePhase and fold FK reason detail into its enum
- **notebooks**: extract CatalogInspector to a sibling module
- **api**: drop redundant effective_tags accessor
- derive observed FKs in the branch, not via a second None test
- simplify the differ — structural matcher, deep column reconciler, PK invariant in the domain
- split UnsupportedChange into descriptive ColumnTypeChange + PartitioningChange
- generate constraint names on DesiredTable, not in the compiler
- merge sentinel actions into a single UnsupportedChange
- reframe duplicate-FK guard as a desired-only content invariant
- remove constraint naming from the domain and public API
- drop derived names from FK/PK actions and narrow the differ helpers
- describe foreign-key failures by content, not derived name
- express column and property diffs via diff_by_key
- unify foreign-key planning across create and migrate paths
- make SyncCandidate a frozen value object with a tuple of failures
- model primary key as a domain value object
- clarify FK-check priority and tighten Phase B tests
- expose planned actions on TableRunReport for dry runs
- rename foreign_key_planning to dependency_resolution
- use plain set for membership-only primary key comparisons
- match FKs by a signature property, aligning the differ with _diff_columns
- drop DesiredTable.resolve_foreign_key_constraint_name wrapper, call FK directly
- expand fk to foreign_key outside comprehensions; rename SetForeignKey.fk field
- make TableRunReport.execution Optional to distinguish never-executed from empty run
- inline datetime.now(UTC), removing the _utc_now wrapper
- rename type-suffixed variables to intent-revealing names
- expose qualified_name on SyncCandidate, replacing candidate.table.qualified_name
- move resolve() after validate so validation failures propagate to FK dependents
- lift inline imports to module level in FK planning tests
- replace _validate with _apply_validation, surfacing all pre-execution failures together
- replace validation/foreign_key_failures with pre_execution_failures on TableRunReport
- widen SyncCandidate.failures to list[Failure] and rename blocked to can_execute
- update engine to iterate SyncCandidate, removing ForeignKeyResolution queries
- replace ForeignKeyResolution with SyncCandidate in foreign_key_planning
- remove unused soft-skip foreign-key result types
- validate all plans and skip executing empty ones
- rename noun-named FK methods to verbs
- simplify Engine phase methods and TableRunReport
- restructure Engine.sync() into four explicit phases
- rename schema package to api
- state the all-unmappable-columns failure in the reader
- collapse type mapping to one Optional-returning function
- push unrecognised-type skip logic into _to_column_mapping
- move type-mapping knowledge out of reader into types module
- apply Priority 2 APoSD quick wins (no behaviour change except UNSET COMMENT)
- apply R2-R6 APoSD review polish (no behaviour change)
- replace double-pass column scan in reader with single _ColumnMapping pass
- push type-change and partition-change detection into the differ
- simplify the databricks SQL layer (catalog.tableExists, package surface)
- reshape adapters/ -- promote schema, dissolve catalog, home log_config
- rename diff_tables to compute_plan
- delete the plan_table wrapper; engine calls diff_tables directly
- dissolve domain/services into domain/plan
- one name for the qualified-name concept; thread the object, stringify at edges
- model read as CatalogState sum; give execution an ExecutionSummary
- dissolve PlanValidator class into validate_plan function
- PlanValidator.validate returns ValidationResult, not raw failures
- replace PlanContext with plan_table; validator takes explicit args
- ActionPlan orders its own actions on construction
- remove ActionPlan.target; pass table identity through execute(target, plan)
- split ExecutionResult into ExecutionSucceeded | ExecutionFailed (C2 Q1b)
- small correctness and consistency fixes (A6)
- fold format_failure_detail into errors.py as a private helper
- drop _AppliedStep, build ExecutionResult directly in executor
- fold ordering.py into plan.py and drop the sort_key injection seam
- collapse column_diff and table_diff into differ.py as private helpers
- give failure types a format_line() method
- model ReadResult as a ReadSucceeded | ReadFailed sum type
- inline Engine pass-through methods into _sync_table
- move DesiredTableSource from ports.py to registry.py
- consolidate per-table logging in _sync_table; drop FQN parameter from phase helpers
- delete TableFormat one-member enum; render USING delta literally
- delete PartitionBy action; rewrite DisallowPartitioningChange to use field comparison
- remove stale TODO comments and fix misleading diff_partition_columns docstring
- drop pass-through schema Column wrapper; re-export the domain type
- extract shared failure-rendering helper; delete dead format_sync_report
- adopt declared-subset property diff; delete UnsetProperty and read-time allowlist
- define property allowlist once in properties.py; derive both consumers from it
- call listColumns once in fetch_state; remove _fetch_columns and _fetch_partition_columns
- validate rather than silently normalise case in Column and QualifiedName
- name intermediate action tuples in diff_columns for readability
- move action ordering onto the actions themselves
- return SyncReport from sync; stop configuring logging at import
- build TableRunReport once in _sync_table
- push table conversion into DeltaTable, narrow registry port
