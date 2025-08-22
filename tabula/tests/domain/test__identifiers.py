import pytest
from tabula.domain._identifiers import normalize_identifier

def test_type_and_empty_checks():
    with pytest.raises(TypeError):
        normalize_identifier(None)           # type: ignore[arg-type]
    with pytest.raises(TypeError):
        normalize_identifier(123)            # type: ignore[arg-type]
    with pytest.raises(ValueError):
        normalize_identifier("")

def test_whitespace_and_dot_rejections():
    for bad in (" a", "a ", "a b", "a\tb", "a\nb", "a\u00A0b"):
        with pytest.raises(ValueError):
            normalize_identifier(bad)
    with pytest.raises(ValueError):
        normalize_identifier("a.b")

def test_invisible_format_chars_rejected():
    with pytest.raises(ValueError):
        normalize_identifier("ord\u200Ber")  # ZERO WIDTH SPACE

def test_casefold_and_permissive_symbols():
    assert normalize_identifier("Sales-Data/2025%") == "sales-data/2025%"
