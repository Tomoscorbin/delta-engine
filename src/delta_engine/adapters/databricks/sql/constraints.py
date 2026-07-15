"""
Parse the ``table_constraints`` field of ``DESCRIBE … AS JSON``.

Unlike the rest of the JSON, constraints arrive as one embedded formatted
string — a bracketed list of ``(constraint_name, BODY)`` pairs where BODY is
DDL-like text (``PRIMARY KEY (`c`)`` / ``FOREIGN KEY (`c`) REFERENCES
`cat`.`sch`.`tbl` (`r`)``). This field is officially undocumented and less
structurally stable than the structured keys, so it is parsed here in
isolation. Identifiers are backtick-quoted with a doubled backtick escaping a
literal backtick; the referenced table is always a 3-part backticked name.

Assumption: a constraint name does not contain an unbackticked top-level comma
or the literal keyword boundary — true for the catalog-generated names this
reads. Names are returned casefolded, matching the domain's lowercase identity.
"""

from dataclasses import dataclass, field
from typing import Final


class ConstraintParseError(Exception):
    """The table_constraints string is not in the expected format."""


@dataclass(frozen=True, slots=True)
class ParsedPrimaryKey:
    constraint_name: str
    columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ParsedForeignKey:
    constraint_name: str
    local_columns: tuple[str, ...]
    referenced_table: tuple[str, str, str]
    referenced_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ParsedConstraints:
    primary_key: ParsedPrimaryKey | None = None
    foreign_keys: tuple[ParsedForeignKey, ...] = field(default_factory=tuple)


_PRIMARY_KEY: Final = "PRIMARY KEY"
_FOREIGN_KEY: Final = "FOREIGN KEY"
_REFERENCES: Final = "REFERENCES"


def parse_table_constraints(value: str | None) -> ParsedConstraints:
    """Parse the ``table_constraints`` string into structured constraints."""
    if value is None:
        return ParsedConstraints()
    text = value.strip()
    if text in ("", "[]"):
        return ParsedConstraints()
    if not (text.startswith("[") and text.endswith("]")):
        raise ConstraintParseError(f"expected a bracketed list: {value!r}")

    primary_key: ParsedPrimaryKey | None = None
    foreign_keys: list[ParsedForeignKey] = []
    for element in _split_top_level_elements(text[1:-1]):
        name, body = _split_name_and_body(element)
        upper = body.upper()
        if upper.startswith(_PRIMARY_KEY):
            columns = _read_identifier_list(body[len(_PRIMARY_KEY) :])
            primary_key = ParsedPrimaryKey(name.casefold(), columns)
        elif upper.startswith(_FOREIGN_KEY):
            foreign_keys.append(_parse_foreign_key(name.casefold(), body))
        else:
            raise ConstraintParseError(f"unknown constraint body: {body!r}")
    return ParsedConstraints(primary_key=primary_key, foreign_keys=tuple(foreign_keys))


def _split_top_level_elements(text: str) -> list[str]:
    """Split ``(…), (…)`` into balanced ``(…)`` spans, ignoring backticked content."""
    elements: list[str] = []
    depth = 0
    start: int | None = None
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char == "`":
            index = _skip_backtick(text, index)
            continue
        if char == "(":
            if depth == 0:
                start = index
            depth += 1
        elif char == ")":
            if depth == 0:
                raise ConstraintParseError("unbalanced parentheses in constraints")
            depth -= 1
            if depth == 0 and start is not None:
                elements.append(text[start : index + 1])
                start = None
        index += 1
    if depth != 0:
        raise ConstraintParseError("unbalanced parentheses in constraints")
    return elements


def _split_name_and_body(element: str) -> tuple[str, str]:
    inner = element.strip()
    if not (inner.startswith("(") and inner.endswith(")")):
        raise ConstraintParseError(f"expected a parenthesised element: {element!r}")
    content = inner[1:-1]
    comma = _find_top_level_comma(content)
    if comma is None:
        raise ConstraintParseError(f"element missing name/body separator: {element!r}")
    return content[:comma].strip(), content[comma + 1 :].strip()


def _parse_foreign_key(name: str, body: str) -> ParsedForeignKey:
    local_open = body.index("(")
    local_close = _matching_paren(body, local_open)
    local_columns = _read_identifiers(body[local_open + 1 : local_close])

    rest = body[local_close + 1 :]
    references_at = rest.upper().find(_REFERENCES)
    if references_at < 0:
        raise ConstraintParseError(f"foreign key missing REFERENCES: {body!r}")
    after = rest[references_at + len(_REFERENCES) :]

    ref_open = after.index("(")
    ref_close = _matching_paren(after, ref_open)
    referenced_table = _parse_referenced_table(after[:ref_open])
    referenced_columns = _read_identifiers(after[ref_open + 1 : ref_close])
    return ParsedForeignKey(name, local_columns, referenced_table, referenced_columns)


def _parse_referenced_table(text: str) -> tuple[str, str, str]:
    parts = _read_identifiers(text)
    if len(parts) != 3:
        raise ConstraintParseError(f"expected a 3-part referenced table: {text!r}")
    return (parts[0], parts[1], parts[2])


def _read_identifier_list(text: str) -> tuple[str, ...]:
    open_paren = text.index("(")
    close_paren = _matching_paren(text, open_paren)
    return _read_identifiers(text[open_paren + 1 : close_paren])


def _read_identifiers(text: str) -> tuple[str, ...]:
    """Read all backtick-quoted identifiers in ``text``, casefolded, in order."""
    identifiers: list[str] = []
    index = 0
    length = len(text)
    while index < length:
        if text[index] == "`":
            identifier, index = _read_backtick_identifier(text, index)
            identifiers.append(identifier.casefold())
        else:
            index += 1  # separators (commas, dots, spaces) between identifiers
    return tuple(identifiers)


def _read_backtick_identifier(text: str, index: int) -> tuple[str, int]:
    index += 1  # opening backtick
    parts: list[str] = []
    length = len(text)
    while index < length:
        char = text[index]
        if char == "`":
            if index + 1 < length and text[index + 1] == "`":
                parts.append("`")
                index += 2
                continue
            return "".join(parts), index + 1
        parts.append(char)
        index += 1
    raise ConstraintParseError("unterminated backtick identifier")


def _find_top_level_comma(text: str) -> int | None:
    depth = 0
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char == "`":
            index = _skip_backtick(text, index)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            return index
        index += 1
    return None


def _matching_paren(text: str, open_index: int) -> int:
    depth = 0
    index = open_index
    length = len(text)
    while index < length:
        char = text[index]
        if char == "`":
            index = _skip_backtick(text, index)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise ConstraintParseError("unbalanced parentheses in constraint body")


def _skip_backtick(text: str, index: int) -> int:
    """Return the index just past a backtick-quoted span starting at ``index``."""
    index += 1
    length = len(text)
    while index < length:
        if text[index] == "`":
            if index + 1 < length and text[index + 1] == "`":
                index += 2
                continue
            return index + 1
        index += 1
    raise ConstraintParseError("unterminated backtick identifier")
