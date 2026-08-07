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

    def test_identity_preserves_already_lowercase_unicode(self) -> None:
        # 'straße' is already lowercase; casefold would rewrite it to
        # 'strasse', a different identifier from the one Unity Catalog stores.
        # The != line is the load-bearing discriminator: under casefold both
        # sides fold to 'strasse' and it would fail.
        assert Identifier("straße") == "straße"
        assert Identifier("straße") != "strasse"

    def test_identity_uses_lower_not_casefold(self) -> None:
        # lower() keeps 'ß'; casefold() would expand it to 'ss' and silently
        # merge distinct identifiers. The != line is the load-bearing
        # discriminator: under casefold both sides fold to 'grösse' and it
        # would fail.
        assert Identifier("GRÖßE") == "größe"
        assert Identifier("GRÖßE") != "GRÖSSE"


class TestIdentifierSpelling:
    def test_spelling_is_preserved_verbatim(self) -> None:
        assert str(Identifier("requestId")) == "requestId"
        assert f"{Identifier('requestId')}" == "requestId"
        assert repr(Identifier("requestId")) == "'requestId'"

    def test_str_returns_a_plain_case_sensitive_str(self) -> None:
        spelling = str(Identifier("requestId"))
        assert type(spelling) is str
        assert spelling == "requestId"
        assert spelling != "REQUESTID"


class TestIdentifierConstruction:
    def test_non_string_spelling_is_rejected_deliberately(self) -> None:
        # Given / When / Then a non-string cannot reach string operations.
        with pytest.raises(TypeError):
            Identifier(42)  # type: ignore[arg-type]

    def test_blank_spelling_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            Identifier("   ")

    def test_wrapping_an_identifier_is_idempotent(self) -> None:
        original = Identifier("requestId")
        rewrapped = Identifier(original)
        assert isinstance(rewrapped, Identifier)
        assert str(rewrapped) == "requestId"
