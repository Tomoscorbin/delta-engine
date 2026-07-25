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
