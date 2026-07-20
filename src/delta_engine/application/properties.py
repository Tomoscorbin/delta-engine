"""
The Delta table properties this engine manages, and their restrictions.

Reference: https://docs.delta.io/latest/table-properties.html

Properties share their namespace with the platform: Databricks writes keys
like ``delta.minReaderVersion`` and ``delta.enableRowTracking`` into table
metadata autonomously. The engine manages properties by exact declaration
over the registered keys below; everything else is invisible — the reader
adapter filters unregistered keys out of the observed state before the
domain ever sees them.

``Property`` is the single source of the managed key names: the catalogue
below references its members, and the api layer re-exports it as the
user-facing declaration vocabulary. There is no second list to keep in sync.

Deliberately absent: ``delta.enableDeletionVectors``. Databricks manages it
(workspaces auto-enable it on new tables), so the engine leaves the key
entirely to the platform.

Admission policy — adding a key is a breaking change: tables carrying it
undeclared start failing validation on upgrade. Before registering a key,
create a fresh table on a current Databricks Runtime and inspect DESCRIBE
DETAIL's properties: if the platform auto-writes the key, do not register
it. Additions are called out in release notes.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
import re
from types import MappingProxyType
from typing import Final

# A single `interval <n> <unit>` term only — deliberately stricter than the
# catalog, which also accepts compound intervals ("interval 1 hour 30
# minutes"). One canonical spelling keeps declared and observed values
# comparable; see the properties section in docs/how-to-configure-table.md.
_INTERVAL_FORMAT = re.compile(
    r"interval [0-9]+ "
    r"(nanosecond|microsecond|millisecond|second|minute|hour|day|week)s?"
)

_INTEGER_AT_LEAST_MINUS_ONE = re.compile(r"-1|0|[1-9][0-9]*")


def _is_lowercase_boolean(value: str) -> bool:
    # The catalog stores 'true'/'false'; any other casing would re-diff
    # as drift on every sync.
    return value in {"true", "false"}


def _is_interval(value: str) -> bool:
    return _INTERVAL_FORMAT.fullmatch(value) is not None


def _is_integer_at_least_minus_one(value: str) -> bool:
    # Canonical digits only: bare int() also accepts "1_000", "+5", or " 5 ",
    # forms the catalog would not normalize and that can fail Java-side
    # parsing at execution instead of at declaration.
    return _INTEGER_AT_LEAST_MINUS_ONE.fullmatch(value) is not None


def _is_column_mapping_mode(value: str) -> bool:
    return value in {"none", "name"}


class Property(StrEnum):
    """The Delta table properties a user may declare on a table."""

    COLUMN_MAPPING_MODE = "delta.columnMapping.mode"
    CHANGE_DATA_FEED = "delta.enableChangeDataFeed"
    DELETED_FILE_RETENTION_DURATION = "delta.deletedFileRetentionDuration"
    LOG_RETENTION_DURATION = "delta.logRetentionDuration"
    DATA_SKIPPING_NUM_INDEXED_COLS = "delta.dataSkippingNumIndexedCols"
    TYPE_WIDENING = "delta.enableTypeWidening"

    @classmethod
    def keys(cls) -> frozenset[str]:
        return frozenset(p.value for p in cls)


@dataclass(frozen=True, slots=True)
class PropertyDefinition:
    """
    One manageable property key, and the judgments the engine needs about it.

    The fields are ingredients; consumers ask the two questions below rather
    than interpreting the fields themselves, so the semantics — a declared
    ``None`` asserts absence, a first write is always legal, an empty
    transition set is unrestricted — live here and nowhere else.

    ``value_description`` is the human phrase for the expected value format,
    used verbatim in rejection messages; ``permitted_transitions`` holds the
    ``(observed_value, desired_value)`` pairs that are legal in-place
    changes, where a ``desired_value`` of ``None`` means removal.
    """

    key: Property
    value_description: str
    is_valid_value: Callable[[str], bool]
    permitted_transitions: frozenset[tuple[str, str | None]] = field(default_factory=frozenset)

    def reject_declared_value(self, value: str | None) -> str | None:
        """
        Return the error message for an unacceptable declared value, or ``None``.

        A declared ``None`` asserts the key's absence, not a value, and is
        never rejected.
        """
        if value is None or self.is_valid_value(value):
            return None
        return f"Invalid value for {self.key}: {value!r}. Expected {self.value_description}."

    def permits_transition(self, observed: str | None, desired: str | None) -> bool:
        """
        Whether the catalog accepts moving this key from observed to desired.

        A first write (``observed`` is ``None``) is always legal; an empty
        restriction set permits everything; ``desired`` ``None`` means
        removal (the key declared absent).
        """
        if observed == desired:
            return True

        if observed is None or not self.permitted_transitions:
            return True

        return (observed, desired) in self.permitted_transitions


@dataclass(frozen=True, slots=True)
class PropertyPolicy:
    definitions: tuple[PropertyDefinition, ...]
    _definitions_by_name: Mapping[str, PropertyDefinition] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        definitions_by_name = {definition.key.value: definition for definition in self.definitions}

        if len(definitions_by_name) != len(self.definitions):
            raise ValueError("Property policy contains duplicate property definitions")

        managed_keys = definitions_by_name.keys()
        public_keys = Property.keys()

        # Property is the public vocabulary. Every public member must have
        # exactly one policy definition, and no hidden definitions may exist
        if managed_keys != public_keys:
            missing = sorted(public_keys - managed_keys)
            unexpected = sorted(managed_keys - public_keys)
            raise ValueError(
                "Property policy does not match the public Property vocabulary:"
                f" missing={missing}, unexpected={unexpected}"
            )

        object.__setattr__(self, "_definitions_by_name", MappingProxyType(definitions_by_name))

    def validate_declaration(self, properties: Mapping[str, str | None]) -> None:
        """
        Raise valueError when a declaration contains an unmanaged key or bad value.

        A None value asserts absence and is validated by PropertyDefinition
        """
        unmanaged = sorted(name for name in properties if name not in self._definitions_by_name)

        if unmanaged:
            raise ValueError(f"Properties not managed by this engine: {', '.join(unmanaged)}")

        for name, value in properties.items():
            definition = self._definitions_by_name[name]
            rejection = definition.reject_declared_value(value)
            if rejection is not None:
                raise ValueError(rejection)

    def project_observed(self, properties: Mapping[str, str]) -> Mapping[str, str]:
        managed = {
            name: value for name, value in properties.items() if name in self._definitions_by_name
        }
        return MappingProxyType(managed)

    def permits_transition(
        self,
        name: str,
        observed: str | None,
        desired: str | None,
    ) -> bool:
        definition = self._definitions_by_name.get(name)
        if definition is None:  # TODO: raise on unknown properties
            return True
        return definition.permits_transition(observed, desired)

    def permits_removal(
        self,
        name: str,
        observed: str,
    ) -> bool:
        return self.permits_transition(
            name=name,
            observed=observed,
            desired=None,
        )


_DEFINITIONS: Final[tuple[PropertyDefinition, ...]] = (
    PropertyDefinition(
        key=Property.CHANGE_DATA_FEED,
        value_description="'true' or 'false' (lowercase, as the catalog stores it)",
        is_valid_value=_is_lowercase_boolean,
    ),
    PropertyDefinition(
        key=Property.DELETED_FILE_RETENTION_DURATION,
        value_description="an interval string such as 'interval 7 days'",
        is_valid_value=_is_interval,
    ),
    PropertyDefinition(
        key=Property.LOG_RETENTION_DURATION,
        value_description="an interval string such as 'interval 30 days'",
        is_valid_value=_is_interval,
    ),
    PropertyDefinition(
        key=Property.DATA_SKIPPING_NUM_INDEXED_COLS,
        value_description="an integer >= -1 (-1 indexes all columns)",
        is_valid_value=_is_integer_at_least_minus_one,
    ),
    PropertyDefinition(
        key=Property.COLUMN_MAPPING_MODE,
        value_description=("'none' or 'name'; 'id' is unsupported."),
        is_valid_value=_is_column_mapping_mode,
        # Only none -> name is permitted. Databricks can remove column
        # mapping (SET mode='none' / DROP FEATURE), but removal rewrites
        # every data file, so the engine refuses it as an in-place change.
        # The absence of any (value, None) pair blocks removal by the same
        # mechanism — declaring the key absent is a transition to None.
        permitted_transitions=frozenset({("none", "name")}),
    ),
    PropertyDefinition(
        key=Property.TYPE_WIDENING,
        value_description="'true' or 'false' (lowercase, as the catalog stores it)",
        is_valid_value=_is_lowercase_boolean,
        # No transition restrictions: the catalog accepts enabling, disabling,
        # and removal. Disabling only stops future widenings — the typeWidening
        # protocol feature persists until ALTER TABLE ... DROP FEATURE, which
        # is outside this engine's scope (documented, not validated).
    ),
)

DELTA_PROPERTY_POLICY: Final = PropertyPolicy(_DEFINITIONS)
