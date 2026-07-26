import pytest

from delta_engine.domain.model import identifier_key, index_by_identifier


def test_identifier_key_lowercases_ascii():
    assert identifier_key("RequestId") == "requestid"


def test_identifier_key_preserves_already_lowercase_unicode():
    # 'straße' is already lowercase; casefold would rewrite it to 'strasse',
    # a different identifier from the one Unity Catalog stores.
    assert identifier_key("straße") == "straße"


def test_identifier_key_uses_lower_not_casefold():
    # lower() keeps 'ß'; casefold() would expand it to 'ss' and silently
    # change identity semantics.
    assert identifier_key("GRÖßE") == "größe"


def test_index_by_identifier_keys_items_by_identity_and_keeps_them():
    index = index_by_identifier(["RequestId", "amount"], name_of=lambda item: item)

    assert index == {"requestid": "RequestId", "amount": "amount"}


def test_index_by_identifier_rejects_case_insensitive_duplicates():
    with pytest.raises(ValueError, match="Duplicate identifier"):
        index_by_identifier(["requestId", "REQUESTID"], name_of=lambda item: item)


import pytest

from delta_engine.domain.model import Identifier


class TestIdentifierIdentity:
    def test_case_variant_spellings_are_equal(self) -> None:
        assert Identifier("requestId") == Identifier("REQUESTID")

    def test_equality_is_case_insensitive_against_plain_strings_both_ways(self) -> None:
        assert Identifier("requestId") == "requestid"
        assert "requestid" == Identifier("requestId")
        assert not (Identifier("requestId") != "REQUESTID")
        assert not ("REQUESTID" != Identifier("requestId"))

    def test_different_identifiers_are_unequal(self) -> None:
        assert Identifier("request_id") != Identifier("requestId")
        assert Identifier("requestId") != 5

    def test_hash_follows_identity_so_sets_and_dicts_deduplicate(self) -> None:
        assert hash(Identifier("ID")) == hash(Identifier("id"))
        assert len({Identifier("ID"), Identifier("id")}) == 1
        assert {Identifier("ID"): 1}[Identifier("id")] == 1

    def test_lowercase_keyed_dict_is_probed_by_identifier(self) -> None:
        # Adapter dicts keyed by plain lowercase strings stay probe-able.
        assert {"requestid": 1}[Identifier("RequestId")] == 1


class TestIdentifierSpelling:
    def test_spelling_is_preserved_verbatim(self) -> None:
        assert str(Identifier("requestId")) == "requestId"
        assert f"{Identifier('requestId')}" == "requestId"
        assert repr(Identifier("requestId")) == "'requestId'"

    def test_spelling_property_is_a_plain_case_sensitive_str(self) -> None:
        spelling = Identifier("requestId").spelling
        assert type(spelling) is str
        assert spelling == "requestId"
        assert spelling != "REQUESTID"

    def test_key_is_the_lowercase_identity(self) -> None:
        assert Identifier("RequestId").key == "requestid"
        assert type(Identifier("RequestId").key) is str


class TestIdentifierConstruction:
    def test_blank_spelling_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="must not be blank"):
            Identifier("   ")

    def test_wrapping_an_identifier_is_idempotent(self) -> None:
        original = Identifier("requestId")
        rewrapped = Identifier(original)
        assert isinstance(rewrapped, Identifier)
        assert rewrapped.spelling == "requestId"
