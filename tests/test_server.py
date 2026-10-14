"""Tests for garganorn.server.load_lexicons."""
from garganorn.server import load_lexicons


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_load_lexicons():
    """load_lexicons returns a list of dicts, each with an 'id' key."""
    lexicons = load_lexicons()
    assert isinstance(lexicons, list)
    assert len(lexicons) > 0
    for lex in lexicons:
        assert "id" in lex


def test_no_com_atproto_lexicons():
    """Garganorn ships no lexicon under a com.atproto.* NSID."""
    ids = [lex["id"] for lex in load_lexicons()]
    assert [i for i in ids if i.startswith("com.atproto.")] == []
