"""Utilities for diffing table property mappings."""

from __future__ import annotations

from collections.abc import Mapping

from delta_engine.domain.model import DesiredTable, ObservedTable
from delta_engine.domain.plan.actions import (
    PartitionBy,
    SetProperty,
    SetTableComment,
)


def diff_properties(
    desired: Mapping[str, str],
    observed: Mapping[str, str],
) -> tuple[SetProperty, ...]:
    """
    Return the `SetProperty` actions needed to align observed with desired.

    Properties are a declared subset, not a complete desired state: the engine
    only manages keys the user declared. Observed-only keys (e.g. properties
    Databricks sets autonomously) are left untouched — they are never unset.
    """
    return tuple(
        SetProperty(name=name, value=value)
        for name, value in desired.items()
        if observed.get(name) != value
    )


def diff_table_comments(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[SetTableComment, ...]:
    """Return a comment update action when the desired table comment differs."""
    # Returns an empty tuple when comments are equal to keep plan composition
    # consistent with other diff functions that return tuples of actions.
    if desired.comment == observed.comment:
        return ()
    else:
        return (SetTableComment(comment=desired.comment),)


def diff_partition_columns(
    desired: DesiredTable, observed: ObservedTable
) -> tuple[PartitionBy, ...]:
    """Return the desired partition columns if different from observed."""
    if desired.partitioned_by == observed.partitioned_by:
        return ()
    else:
        return (PartitionBy(desired.partitioned_by),)
