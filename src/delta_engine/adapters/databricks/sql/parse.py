"""
Parse Databricks DDL type strings into domain data types.

The read half of the shared SQL type mapping — the twin of :mod:`types`,
which renders domain types as DDL text. Input is the type string catalogs
report (``information_schema.columns.full_data_type``), e.g.
``"decimal(10,2)"`` or ``"struct<id: bigint, tags: array<string>>"``.

Returns ``None`` for any type the domain does not model (``interval``,
``void``, geospatial, collated strings, future types) and for malformed
input. An unmappable type is a routine condition — catalogs gain types
before engines that pin a type model — so parsing never raises; callers
decide what to do (both readers skip the column and warn, via the shared
``column_from_catalog`` policy).

Struct fields may carry ``NOT NULL`` and ``COMMENT '...'`` clauses in
catalog output; both are tolerated and discarded — field nullability and
comments are deliberately not modeled (see ``StructField``). Field names
are casefolded; names that collide after casefolding make the struct
unmappable.
"""

import re
from typing import Final

from delta_engine.domain.model import (
    Array,
    Binary,
    Boolean,
    Byte,
    DataType,
    Date,
    Decimal,
    Double,
    Float,
    Integer,
    Long,
    Map,
    Short,
    String,
    Struct,
    StructField,
    Timestamp,
    TimestampNtz,
    Variant,
)

_SIMPLE_TYPES: Final[dict[str, DataType]] = {
    "int": Integer(),
    "integer": Integer(),
    "bigint": Long(),
    "long": Long(),
    "smallint": Short(),
    "short": Short(),
    "tinyint": Byte(),
    "byte": Byte(),
    "float": Float(),
    "real": Float(),
    "double": Double(),
    "boolean": Boolean(),
    "string": String(),
    "date": Date(),
    "timestamp": Timestamp(),
    "timestamp_ntz": TimestampNtz(),
    "binary": Binary(),
    "variant": Variant(),
}

# Spark's defaults when DECIMAL appears without arguments.
_DEFAULT_DECIMAL_PRECISION: Final = 10
_DEFAULT_DECIMAL_SCALE: Final = 0

_WORD: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_NUMBER: Final = re.compile(r"[0-9]+")


class _ParseError(Exception):
    """Internal signal that the input is not a mappable type string."""


def parse_data_type(ddl: str) -> DataType | None:
    """
    Parse a Databricks DDL type string into a domain type, or ``None``.

    ``None`` covers both "the domain does not model this type" and
    "malformed input"; the two are indistinguishable to callers and get the
    same treatment (skip with a warning). Domain constructor rejections —
    e.g. a decimal precision over the Delta limit — also yield ``None``:
    a type the domain refuses to represent is unmappable, not a crash.
    Pathological nesting depth is also treated as unmappable.
    """
    parser = _Parser(ddl)
    try:
        data_type = parser.parse_type()
        parser.expect_end()
    except (_ParseError, ValueError, RecursionError):
        return None
    return data_type


class _Parser:
    """Recursive-descent parser over one type string; position-based, no tokens list."""

    def __init__(self, text: str) -> None:
        self._text = text
        self._position = 0

    # -- grammar ----------------------------------------------------------

    def parse_type(self) -> DataType:
        name = self._read_word().casefold()
        if name in _SIMPLE_TYPES:
            return _SIMPLE_TYPES[name]
        if name in ("decimal", "dec", "numeric"):
            return self._parse_decimal()
        if name in ("char", "varchar", "character"):
            # Lossy normalization shared with the Spark read path: the length
            # bound is not modeled, so these read as plain strings.
            self._parse_length_argument()
            return String()
        if name == "array":
            self._expect("<")
            element = self.parse_type()
            self._expect(">")
            return Array(element)
        if name == "map":
            self._expect("<")
            key = self.parse_type()
            self._expect(",")
            value = self.parse_type()
            self._expect(">")
            return Map(key, value)
        if name == "struct":
            return self._parse_struct()
        raise _ParseError(f"unknown type name: {name!r}")

    def _parse_decimal(self) -> Decimal:
        if not self._try_consume("("):
            return Decimal(_DEFAULT_DECIMAL_PRECISION, _DEFAULT_DECIMAL_SCALE)
        precision = self._read_number()
        scale = self._read_number() if self._try_consume(",") else _DEFAULT_DECIMAL_SCALE
        self._expect(")")
        return Decimal(precision, scale)

    def _parse_length_argument(self) -> None:
        """Consume the ``(n)`` of char/varchar; the bound is discarded."""
        self._expect("(")
        self._read_number()
        self._expect(")")

    def _parse_struct(self) -> Struct:
        self._expect("<")
        fields: list[StructField] = []
        while True:
            fields.append(self._parse_struct_field())
            if not self._try_consume(","):
                break
        self._expect(">")
        return Struct(tuple(fields))

    def _parse_struct_field(self) -> StructField:
        name = self._read_field_name().casefold()
        self._try_consume(":")  # both "name: type" and "name type" occur
        data_type = self.parse_type()
        self._discard_field_decorations()
        return StructField(name=name, data_type=data_type)

    def _discard_field_decorations(self) -> None:
        """Consume trailing ``NOT NULL`` and ``COMMENT '...'`` clauses, unmodeled."""
        while True:
            word = self._peek_word()
            if word is None:
                return
            keyword = word.casefold()
            if keyword == "not":
                self._read_word()
                if self._read_word().casefold() != "null":
                    raise _ParseError("expected NULL after NOT")
            elif keyword == "comment":
                self._read_word()
                self._read_string_literal()
            else:
                return

    # -- lexing -----------------------------------------------------------

    def _skip_whitespace(self) -> None:
        while self._position < len(self._text) and self._text[self._position].isspace():
            self._position += 1

    def _read_word(self) -> str:
        self._skip_whitespace()
        match = _WORD.match(self._text, self._position)
        if match is None:
            raise _ParseError(f"expected a name at position {self._position}")
        self._position = match.end()
        return match.group()

    def _peek_word(self) -> str | None:
        self._skip_whitespace()
        match = _WORD.match(self._text, self._position)
        return match.group() if match else None

    def _read_field_name(self) -> str:
        self._skip_whitespace()
        if self._peek_char() == "`":
            return self._read_quoted("`", "backtick-quoted name")
        return self._read_word()

    def _read_string_literal(self) -> str:
        self._skip_whitespace()
        if self._peek_char() != "'":
            raise _ParseError("expected a string literal")
        return self._read_quoted("'", "string literal")

    def _read_quoted(self, quote: str, description: str) -> str:
        # Inside the quotes a literal quote character is doubled — the
        # inverse of dialect.backtick() / dialect.quote_literal().
        self._position += 1  # opening quote
        parts: list[str] = []
        while self._position < len(self._text):
            char = self._text[self._position]
            self._position += 1
            if char != quote:
                parts.append(char)
            elif self._peek_char() == quote:  # doubled quote: escaped literal
                parts.append(quote)
                self._position += 1
            else:
                return "".join(parts)
        raise _ParseError(f"unterminated {description}")

    def _peek_char(self) -> str | None:
        if self._position < len(self._text):
            return self._text[self._position]
        return None

    def _read_number(self) -> int:
        self._skip_whitespace()
        match = _NUMBER.match(self._text, self._position)
        if match is None:
            raise _ParseError(f"expected a number at position {self._position}")
        self._position = match.end()
        return int(match.group())

    def _expect(self, symbol: str) -> None:
        if not self._try_consume(symbol):
            raise _ParseError(f"expected {symbol!r} at position {self._position}")

    def _try_consume(self, symbol: str) -> bool:
        self._skip_whitespace()
        if self._text.startswith(symbol, self._position):
            self._position += len(symbol)
            return True
        return False

    def expect_end(self) -> None:
        """Fail unless all input was consumed — trailing junk is not a type."""
        self._skip_whitespace()
        if self._position != len(self._text):
            raise _ParseError(f"unexpected trailing input at position {self._position}")
