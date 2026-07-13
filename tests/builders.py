"""Shared test builders for observed catalog state."""

from collections.abc import Iterable

from delta_engine.domain.model import Column, ObservedColumn


def as_observed_columns(
    columns: Iterable[Column | ObservedColumn],
) -> tuple[ObservedColumn, ...]:
    """
    Project columns to their observed catalog image.

    Mirrors what a catalog read-back reports: the observable fields only.
    Declaration-only syntax on ``Column`` does not survive the projection.
    """
    return tuple(
        ObservedColumn(
            name=column.name,
            data_type=column.data_type,
            nullable=column.nullable,
            comment=column.comment,
            tags=column.tags,
        )
        for column in columns
    )
