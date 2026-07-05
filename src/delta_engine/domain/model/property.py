"""
The shape of table-property knowledge: what a managed key is, backend-free.

Properties share their namespace with the platform, which writes keys the
user never declared. The engine therefore manages properties by exact
declaration: the declaration is the complete list of managed keys, and a
registry — supplied by the application layer — names which keys are
manageable and what per-key restrictions apply. This module defines only
the registry's shape; the concrete keys live outside the domain.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PropertyDefinition:
    """
    One manageable property key and its restrictions.

    ``permitted_transitions``: the ``(observed_value, desired_value)`` pairs
    that are legal in-place value changes. Empty means unrestricted; a
    non-empty set blocks any pair not in it. A first write (key absent from
    the catalog) is always legal and is never looked up here.

    ``unset_permitted``: when False, a declaration asserting the key absent
    fails validation for an existing table that carries it — the key cannot
    be meaningfully removed.
    """

    key: str
    permitted_transitions: frozenset[tuple[str, str]] = field(default_factory=frozenset)
    unset_permitted: bool = True


type PropertyRegistry = Mapping[str, PropertyDefinition]
