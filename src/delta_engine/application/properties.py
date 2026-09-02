"""
Restrictions on the Delta table properties this engine manages.

The managed key vocabulary and what declared values mean live in the domain
(``delta_engine.domain.model.property``); this module holds the policy over
that vocabulary — which declared values are admitted, and which in-place
transitions the engine permits.

Admission policy — adding a key is a breaking change: tables carrying it
undeclared start failing validation on upgrade. Before managing a new key,
create a fresh table on a current Databricks Runtime and inspect DESCRIBE
DETAIL's properties: if the platform auto-writes the key, do not add it to
the policy. Additions are called out in release notes.
"""

from collections.abc import Callable, Mapping, Set
from dataclasses import dataclass, field
import re
from types import MappingProxyType
from typing import Final

from delta_engine.domain.collection_types import ListOrTuple
from delta_engine.domain.model.property import TableProperty

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

    key: TableProperty
    value_description: str
    is_valid_value: Callable[[str], bool]
    permitted_transitions: Set[tuple[str, str | None]] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        object.__setattr__(self, "permitted_transitions", frozenset(self.permitted_transitions))

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
        Whether the engine permits moving this key from observed to desired.

        The permitted set is engine policy and can be stricter than the
        catalog: a change the catalog would apply at unacceptable cost (such
        as removing column mapping, which rewrites every data file) is not
        permitted here.

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
    """Validate declarations and transitions for the managed properties."""

    definitions: ListOrTuple[PropertyDefinition]
    _definitions_by_name: Mapping[str, PropertyDefinition] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "definitions", tuple(self.definitions))
        definitions_by_name = {definition.key.value: definition for definition in self.definitions}

        if len(definitions_by_name) != len(self.definitions):
            raise ValueError("Property policy contains duplicate property definitions")

        managed_keys = frozenset(definitions_by_name)
        public_keys = frozenset(member.value for member in TableProperty)

        # TableProperty is the public vocabulary. Every public member must have
        # exactly one policy definition, and no hidden definitions may exist
        if managed_keys != public_keys:
            missing = sorted(public_keys - managed_keys)
            unexpected = sorted(managed_keys - public_keys)
            raise ValueError(
                "Property policy does not match the public TableProperty vocabulary:"
                f" missing={missing}, unexpected={unexpected}"
            )

        object.__setattr__(self, "_definitions_by_name", MappingProxyType(definitions_by_name))

    def validate_declaration(self, properties: Mapping[str, str | None]) -> None:
        """
        Raise ``ValueError`` when a declaration contains an unmanaged key or bad value.

        A ``None`` value asserts absence and is validated by ``PropertyDefinition``.
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
        """Return only managed keys from the catalog's observed properties."""
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
        """
        Return whether a managed property may move to its desired value.

        Only managed keys can reach a transition check — declarations are
        validated and observations projected — so an unmanaged key is a
        programming error, not a permitted transition.

        Raises:
            ValueError: If the key is not managed by this policy.

        """
        definition = self._definitions_by_name.get(name)
        if definition is None:
            raise ValueError(f"Property not managed by this engine: {name}")
        return definition.permits_transition(observed, desired)


_DEFINITIONS: Final[tuple[PropertyDefinition, ...]] = (
    PropertyDefinition(
        key=TableProperty.CHANGE_DATA_FEED,
        value_description="'true' or 'false' (lowercase, as the catalog stores it)",
        is_valid_value=_is_lowercase_boolean,
    ),
    PropertyDefinition(
        key=TableProperty.DELETED_FILE_RETENTION_DURATION,
        value_description="an interval string such as 'interval 7 days'",
        is_valid_value=_is_interval,
    ),
    PropertyDefinition(
        key=TableProperty.LOG_RETENTION_DURATION,
        value_description="an interval string such as 'interval 30 days'",
        is_valid_value=_is_interval,
    ),
    PropertyDefinition(
        key=TableProperty.DATA_SKIPPING_NUM_INDEXED_COLS,
        value_description="an integer >= -1 (-1 indexes all columns)",
        is_valid_value=_is_integer_at_least_minus_one,
    ),
    PropertyDefinition(
        key=TableProperty.COLUMN_MAPPING_MODE,
        value_description=("'none' or 'name'; 'id' is unsupported."),
        is_valid_value=_is_column_mapping_mode,
        # Only none -> name is permitted. Databricks can remove column
        # mapping (SET mode='none' / DROP FEATURE), but removal rewrites
        # every data file, so the engine rejects it as an in-place change.
        # The absence of any (value, None) pair blocks removal by the same
        # mechanism — declaring the key absent is a transition to None.
        permitted_transitions=frozenset({("none", "name")}),
    ),
    PropertyDefinition(
        key=TableProperty.TYPE_WIDENING,
        value_description="'true' or 'false' (lowercase, as the catalog stores it)",
        is_valid_value=_is_lowercase_boolean,
        # No transition restrictions: the catalog accepts enabling, disabling,
        # and removal. Disabling only stops future widenings — the typeWidening
        # protocol feature persists until ALTER TABLE ... DROP FEATURE, which
        # is outside this engine's scope (documented, not validated).
    ),
)

DELTA_PROPERTY_POLICY: Final = PropertyPolicy(_DEFINITIONS)
