import pytest

from delta_engine.domain.normalise_identifier import normalise_identifier


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("  Hello  ", "hello"),
        ("USER_ID", "user_id"),
        ("a_b_2", "a_b_2"),
        ("user1", "user1"),
    ],
)
def test_normalise_identifier_success_strips_and_lowercases_with_whitelist(
    raw: str, expected: str
) -> None:
    assert normalise_identifier(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        "user id",  # space
        "a.b",  # dot
        "user-id",  # hyphen now invalid
        "user@",  # other punctuation
        "user\tid",  # tab
        "user\nid",  # newline
        "naïve",  # non-ASCII
    ],
)
def test_normalise_identifier_rejects_anything_not_alnum_or_underscore(raw: str) -> None:
    with pytest.raises(ValueError) as exc:
        normalise_identifier(raw)
    assert "Invalid identifier:" in str(exc.value)
