"""
Post-sync column spellings derived from a table's diff.

The resulting schema of a table maps each column's identity key to the
exact spelling the column will have after the table's plan executes:
observed spelling for a matched column, the declared spelling for an added
column or a rename target, and the desired spelling for every column of a
table being created. Removed columns do not appear — drop-path actions
carry their observed column verbatim and never resolve through this index.
"""

from typing import assert_never

from delta_engine.domain.model import identifier_key
from delta_engine.domain.plan.actions import RenameColumn
from delta_engine.domain.plan.diff import TableDiff, TableDrift, TableMissing


def resulting_column_spellings(diff: TableDiff) -> dict[str, str]:
    """Map each column's identity key to its exact post-sync spelling."""
    match diff:
        case TableMissing(desired=desired):
            return {identifier_key(column.name): column.name for column in desired.columns}
        case TableDrift() as drift:
            return _drift_spellings(drift)
        case _ as unreachable:
            assert_never(unreachable)


def _drift_spellings(drift: TableDrift) -> dict[str, str]:
    """Resolve matched columns to observed spelling, renames and adds to desired."""
    observed_by_key = {
        identifier_key(column.name): column.name for column in drift.observed.columns
    }
    rename_target_keys = {
        identifier_key(action.new_name)
        for action in drift.actions
        if isinstance(action, RenameColumn)
    }

    spellings: dict[str, str] = {}
    for column in drift.desired.columns:
        key = identifier_key(column.name)
        if key in rename_target_keys or key not in observed_by_key:
            spellings[key] = column.name
        else:
            spellings[key] = observed_by_key[key]
    return spellings
