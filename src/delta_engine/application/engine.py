"""
High-level orchestration of planning, validation, and execution.

`Engine.sync` reads current catalog state, computes a plan (schema diff +
deterministic ordering), validates it against rules, executes it via provided
adapters, and aggregates results into a `SyncReport`. If any table fails,
`SyncFailedError` is raised with a formatted summary.

The sync runs four phases across all tables in sequence:
  1. Read     — fetch current catalog state for every table
  2. Plan     — compute action plans from desired vs observed state
  3. Validate — run validation rules against each plan
  4. Execute  — run passing plans against the catalog

A table that fails in an early phase is carried forward as a partial result
and skipped in later phases, so all tables are attempted and the report is
always complete.
"""

from __future__ import annotations

from datetime import UTC, datetime
import logging

from delta_engine.application.errors import SyncFailedError
from delta_engine.application.foreign_key_planning import resolve
from delta_engine.application.ports import CatalogStateReader, PlanExecutor
from delta_engine.application.registry import Registry
from delta_engine.application.results import (
    CatalogState,
    ExecutionSummary,
    ForeignKeyValidationReport,
    ReadFailed,
    SyncReport,
    TablePresent,
    TableRunReport,
    ValidationResult,
)
from delta_engine.application.validation import validate_plan
from delta_engine.domain.model import QualifiedName
from delta_engine.domain.model.table import DesiredTable
from delta_engine.domain.plan.actions import ActionPlan, DropForeignKey, SetForeignKey
from delta_engine.domain.plan.differ import compute_plan

logger = logging.getLogger(__name__)


def _strip_foreign_key_actions(
    plan: ActionPlan,
    names_to_skip: frozenset[str],
) -> ActionPlan:
    """Return a copy of the plan with skipped FK actions removed."""
    if not names_to_skip:
        return plan
    return ActionPlan(
        tuple(
            action
            for action in plan
            if not (
                isinstance(action, (DropForeignKey, SetForeignKey))
                and action.constraint_name in names_to_skip
            )
        )
    )


def _utc_now() -> datetime:
    """Return current UTC time as a timezone-aware datetime."""
    return datetime.now(UTC)


class Engine:
    """
    High-level orchestrator to plan, validate, and execute changes.

    The engine coordinates reading current state from a catalog, computing a
    plan to reach desired state, validating that plan, and executing it using
    the provided adapter implementations.
    """

    def __init__(
        self,
        reader: CatalogStateReader,
        executor: PlanExecutor,
    ) -> None:
        """Initialize the engine with the catalog adapters it orchestrates."""
        self.reader = reader
        self.executor = executor

    def sync(self, registry: Registry) -> SyncReport:
        """
        Synchronize all registered tables to their desired state.

        Runs four phases across all tables in sequence:
        read → plan → validate → execute. A table that fails in an early
        phase is skipped in later phases; its partial result is included in
        the final report.

        Before the phases, FK dependencies are resolved: tables are reordered
        so referenced tables sync first, and FK actions for cycles or
        unresolvable references are stripped from plans before validation.

        Returns:
            The aggregate :class:`SyncReport` for the run.

        Raises:
            SyncFailedError: If any table fails to read, validate, or execute.
                The report is available on the exception's ``report`` attribute.

        """
        run_started = _utc_now()
        logger.info("Starting sync for %d table(s)", len(registry))

        tables = tuple(registry)
        foreign_key_plan = resolve(tables)

        catalog_states = self._read(foreign_key_plan.ordered_tables)
        plans = self._plan(foreign_key_plan.ordered_tables, catalog_states)
        plans = {
            qualified_name: _strip_foreign_key_actions(
                plan,
                foreign_key_plan.skipped_names_by_table.get(str(qualified_name), frozenset()),
            )
            for qualified_name, plan in plans.items()
        }

        plans_to_validate = {
            qualified_name: plan for qualified_name, plan in plans.items() if plan
        }
        validations = self._validate(plans_to_validate)
        plans_to_execute = {
            qualified_name: plans_to_validate[qualified_name]
            for qualified_name, validation in validations.items()
            if not validation.failed
        }
        executions = self._execute(plans_to_execute)

        table_reports = tuple(
            TableRunReport(
                qualified_name=table.qualified_name,
                read=catalog_states[table.qualified_name],
                validation=validations.get(table.qualified_name, ValidationResult()),
                execution=executions.get(table.qualified_name, ExecutionSummary()),
            )
            for table in foreign_key_plan.ordered_tables
        )

        report = SyncReport(
            started_at=run_started,
            ended_at=_utc_now(),
            table_reports=table_reports,
            foreign_key_validation=ForeignKeyValidationReport(
                skipped=foreign_key_plan.skipped_foreign_keys
            ),
        )

        if report.any_failures:
            raise SyncFailedError(report)

        logger.info("Sync completed successfully for %d table(s)", len(report.table_reports))
        return report

    def _read(
        self,
        tables: tuple[DesiredTable, ...],
    ) -> dict[QualifiedName, CatalogState]:
        """Fetch current catalog state for every table."""
        catalog_states: dict[QualifiedName, CatalogState] = {}
        for table in tables:
            catalog_state = self.reader.fetch_state(table.qualified_name)
            catalog_states[table.qualified_name] = catalog_state
            if isinstance(catalog_state, ReadFailed):
                logger.error(
                    "Read failed for %s: %s - %s",
                    table.qualified_name,
                    catalog_state.failure.exception_type,
                    catalog_state.failure.message,
                )
            else:
                logger.info(
                    "Read state for %s: %s",
                    table.qualified_name,
                    "present" if isinstance(catalog_state, TablePresent) else "absent",
                )
        return catalog_states

    def _plan(
        self,
        tables: tuple[DesiredTable, ...],
        catalog_states: dict[QualifiedName, CatalogState],
    ) -> dict[QualifiedName, ActionPlan]:
        """
        Compute an action plan for each table.

        Tables that failed to read get an empty plan.
        """
        plans: dict[QualifiedName, ActionPlan] = {}
        for table in tables:
            catalog_state = catalog_states[table.qualified_name]
            if isinstance(catalog_state, ReadFailed):
                plans[table.qualified_name] = ActionPlan()
                continue

            observed = catalog_state.table if isinstance(catalog_state, TablePresent) else None
            plan = compute_plan(desired=table, observed=observed)
            plans[table.qualified_name] = plan
            logger.info("Planned %d action(s) for %s", len(plan), table.qualified_name)

        return plans

    def _validate(
        self,
        plans: dict[QualifiedName, ActionPlan],
    ) -> dict[QualifiedName, ValidationResult]:
        """Validate every plan in the given dict."""
        validations: dict[QualifiedName, ValidationResult] = {}
        for qualified_name, plan in plans.items():
            validation = validate_plan(plan)
            validations[qualified_name] = validation
            if validation.failed:
                logger.error(
                    "Validation failed for %s (%d failure(s))",
                    qualified_name,
                    len(validation.failures),
                )
            else:
                logger.info("Validation passed for %s", qualified_name)

        return validations

    def _execute(
        self,
        plans: dict[QualifiedName, ActionPlan],
    ) -> dict[QualifiedName, ExecutionSummary]:
        """Execute every plan in the given dict."""
        executions: dict[QualifiedName, ExecutionSummary] = {}
        for qualified_name, plan in plans.items():
            execution = self.executor.execute(qualified_name, plan)
            executions[qualified_name] = execution
            logger.info(
                "Executed %d action(s) for %s (%d failed)",
                len(execution.results),
                qualified_name,
                execution.failed_count,
            )
        return executions
