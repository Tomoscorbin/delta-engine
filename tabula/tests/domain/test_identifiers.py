import pytest
from tabula.domain._identifiers import normalize_identifier


def test_none_raises_value_error():
    with pytest.raises(ValueError) as e:
        normalize_identifier("name", None)
    assert "name cannot be None" in str(e.value)


def test_non_string_raises_type_error():
    with pytest.raises(TypeError) as e:
        normalize_identifier("catalog", 123)  # type: ignore[arg-type]
    assert "catalog must be str, got int" in str(e.value)


@pytest.mark.parametrize("raw", [" id", "id ", "\tid", "id\n"])
def test_leading_or_trailing_whitespace_rejected(raw):
    with pytest.raises(ValueError) as e:
        normalize_identifier("schema", raw)
    assert "leading/trailing whitespace" in str(e.value)


def test_empty_string_rejected():
    with pytest.raises(ValueError) as e:
        normalize_identifier("name", "")
    assert "cannot be empty" in str(e.value)


@pytest.mark.parametrize("raw", ["foo bar", "foo\tbar", "foo\nbar"])
def test_internal_whitespace_rejected(raw):
    with pytest.raises(ValueError) as e:
        normalize_identifier("name", raw)
    assert "must not contain whitespace characters" in str(e.value)


def test_dot_in_identifier_rejected():
    with pytest.raises(ValueError) as e:
        normalize_identifier("schema", "sales.data")
    assert "must not contain '.'" in str(e.value)
