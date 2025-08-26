import pytest

from delta_engine.domain.model.identifier import Identifier


def test_identifier_normalizes_lowercases_and_strips_ends():
    ident = Identifier("  Customer_ID  ")
    assert isinstance(ident, str)          # subclass of str
    assert ident == "customer_id"          # stripped + lowercased
    assert str(ident) == "customer_id"


@pytest.mark.parametrize("bad", [
    "cust omer",           # internal whitespace
    "cat.schema",          # dot not allowed in a single-part identifier
    "",                    # empty
    "   ",                 # empty after strip
    "naïve",               # non-ascii
])
def test_identifier_rejects_bad_inputs(bad):
    with pytest.raises(ValueError):
        Identifier(bad)


def test_identifier_compares_equal_to_lowercase_str():
    assert Identifier("FOO") == "foo"
    assert Identifier("FOO") != "bar"
