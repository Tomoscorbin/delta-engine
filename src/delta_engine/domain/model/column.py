"""Domain model for table columns."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from delta_engine.domain.model.data_type import DataType
from delta_engine.domain.model.identifier import identifier_key


def _validate_column_fields(name: str, tags: Mapping[str, str]) -> None:
    """Invariants shared by declared and observed columns."""
    if not name.strip():
        raise ValueError(f"Column name must not be blank: {name!r}")
    for tag_key in tags:
        if not tag_key.strip():
            raise ValueError(f"Tag key must not be blank: {tag_key!r}")


@dataclass(frozen=True, slots=True)
class DesiredColumn:
    """
    Immutable column declaration with a canonical-lowercase name.

    Exposed to users as ``Column`` through ``delta_engine.schema``; the
    domain name states the desired/observed side explicitly, mirroring
    :class:`ObservedColumn`.

    Attributes:
        name: Column name, lowercased on construction. Identifiers are
            case-insensitive on the platform, so two names differing only in
            case are the same column.
        data_type: Logical data type of the column.
        nullable: Whether the column accepts ``NULL`` values.
        comment: Optional column comment.
        tags: Read-only mapping of Unity Catalog column tag keys to values. Tag
            keys are case-sensitive and are stored verbatim (never lowercased,
            unlike the column name).
        renamed_from: The column's previous name, declaring a rename. Inert
            unless the old name is observed and the new one is not, so it is
            safe to keep on declarations that continue to manage column
            structure, and correct on fresh environments.

    """

    name: str
    data_type: DataType
    nullable: bool = True
    comment: str = ""
    tags: Mapping[str, str] = field(default_factory=dict)
    renamed_from: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", self.name.lower())
        object.__setattr__(self, "tags", MappingProxyType(dict(self.tags)))
        _validate_column_fields(self.name, self.tags)
        if self.renamed_from is not None:
            if not self.renamed_from.strip():
                raise ValueError(f"renamed_from must not be blank: {self.renamed_from!r}")
            object.__setattr__(self, "renamed_from", self.renamed_from.lower())
            if identifier_key(self.renamed_from) == identifier_key(self.name):
                raise ValueError(f"Column {self.name!r} cannot be renamed_from itself")


@dataclass(frozen=True, slots=True)
class ObservedColumn:
    """
    Immutable column as observed in the catalog (current state).

    The observed counterpart of :class:`DesiredColumn`: the same observable fields,
    and none of the declaration-only syntax, so catalog state cannot carry
    declaration history by construction. Only reader adapters build these.
    """

    name: str
    data_type: DataType
    nullable: bool = True
    comment: str = ""
    tags: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", self.name.lower())
        object.__setattr__(self, "tags", MappingProxyType(dict(self.tags)))
        _validate_column_fields(self.name, self.tags)
