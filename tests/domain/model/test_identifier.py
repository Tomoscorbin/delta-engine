import pytest

from delta_engine.domain.model import Identifier


class TestIdentifierIdentity:
    def test_case_variant_spellings_are_equal(self) -> None:
        # Then two spellings differing only in case are the same identifier
        assert Identifier("requestId") == Identifier("REQUESTID")

    def test_equality_is_case_insensitive_against_plain_strings_both_ways(self) -> None:
        # Then comparison against a plain string is case-insensitive in both directions
        assert Identifier("requestId") == "requestid"
        assert "requestid" == Identifier("requestId")
        assert not (Identifier("requestId") != "REQUESTID")
        assert not ("REQUESTID" != Identifier("requestId"))

    def test_different_identifiers_are_unequal(self) -> None:
        # Then genuinely different names stay different, as do non-string values
        assert Identifier("request_id") != Identifier("requestId")
        assert Identifier("requestId") != 5

    def test_hash_follows_identity_so_sets_and_dicts_deduplicate(self) -> None:
        # Then case-variant spellings collapse to one entry in hashed collections
        assert hash(Identifier("ID")) == hash(Identifier("id"))
        assert len({Identifier("ID"), Identifier("id")}) == 1
        assert {Identifier("ID"): 1}[Identifier("id")] == 1

    def test_lowercase_keyed_dict_is_probed_by_identifier(self) -> None:
        # Given an adapter dict keyed by plain lowercase strings
        # Then an identifier probes it whatever its spelling
        assert {"requestid": 1}[Identifier("RequestId")] == 1

    def test_identity_preserves_already_lowercase_unicode(self) -> None:
        # Given 'straße', which is already lowercase; casefold would rewrite it
        # to 'strasse', a different identifier from the one Unity Catalog stores
        # Then the identifier equals itself and never merges with the expansion
        assert Identifier("straße") == "straße"
        assert Identifier("straße") != "strasse"

    def test_sharp_s_spellings_never_merge_with_their_expansion(self) -> None:
        # Given an uppercase spelling containing 'ß' — lowercasing keeps 'ß',
        # while casefold would expand it to 'ss' and silently merge distinct
        # identifiers
        # Then the identifier equals its lowercase spelling, not the expansion
        assert Identifier("GRÖßE") == "größe"
        assert Identifier("GRÖßE") != "GRÖSSE"


class TestIdentifierSpelling:
    def test_spelling_is_preserved_verbatim(self) -> None:
        # Then the construction spelling renders verbatim
        assert str(Identifier("requestId")) == "requestId"
        assert f"{Identifier('requestId')}" == "requestId"

    def test_str_returns_a_plain_case_sensitive_str(self) -> None:
        # When converting an identifier to str
        spelling = str(Identifier("requestId"))

        # Then the result is a plain string with ordinary case-sensitive equality
        assert type(spelling) is str
        assert spelling == "requestId"
        assert spelling != "REQUESTID"


class TestIdentifierConstruction:
    def test_non_string_spelling_is_rejected(self) -> None:
        # When a non-string spelling is supplied, then construction fails
        with pytest.raises(TypeError):
            Identifier(42)  # type: ignore[arg-type]

    def test_blank_spelling_is_rejected(self) -> None:
        # When the spelling is blank, then construction fails
        with pytest.raises(ValueError):
            Identifier("   ")

    def test_wrapping_an_identifier_is_idempotent(self) -> None:
        # Given an existing identifier
        original = Identifier("requestId")

        # When wrapping it again
        rewrapped = Identifier(original)

        # Then the result is an identifier with the same spelling
        assert isinstance(rewrapped, Identifier)
        assert str(rewrapped) == "requestId"
