"""Tests for garganorn.server.Server."""
import logging

from garganorn.server import Server, load_lexicons


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_server(tile_manifests):
    """Create a Server serving the given tile_manifests map."""
    return Server("places.atgeo.org", logging.getLogger("test"), tile_manifests=tile_manifests)


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


# ---------------------------------------------------------------------------
# describeService
# ---------------------------------------------------------------------------

def test_describe_service_lists_ready_collections():
    """describeService's did is did:web:<repo>, and its collections list is
    exactly the keys of tile_manifests -- the same map get_coverage consults
    for CollectionNotFound (R1)."""
    tile_manifests = {
        "org.atgeo.places.overture.place": object(),
        "org.atgeo.places.osm": object(),
    }
    server = _make_server(tile_manifests=tile_manifests)
    result = server.describe_service(None)
    assert result["did"] == "did:web:places.atgeo.org"
    assert {c["collection"] for c in result["collections"]} == set(tile_manifests)


def test_describe_service_validates_against_lexicon_schema():
    """describeService's result validates against org.atgeo.describeService's
    own output schema."""
    tile_manifests = {"org.atgeo.places.overture.place": object()}
    server = _make_server(tile_manifests=tile_manifests)
    result = server.describe_service(None)
    server.server.validate("org.atgeo.describeService", "output", result)
