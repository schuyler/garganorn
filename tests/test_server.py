"""Tests for garganorn.server.Server."""
import logging
import pytest

from lexrpc.base import XrpcError

from garganorn.server import Server, load_lexicons


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TILE_COLLECTION = "org.atgeo.places.tile"
TILE_SOURCE_URL = "https://example.com/tile-source"
TILE_LICENSE_URL = "https://example.com/tile-license"

LEXICON_SCHEMA_COLLECTION = "com.atproto.lexicon.schema"


class MockTileBackedCollection:
    def __init__(self, collection=TILE_COLLECTION, record=None):
        self.collection = collection
        self.source_url = TILE_SOURCE_URL
        self.license_url = TILE_LICENSE_URL
        self._record = record

    def get_record(self, repo, collection, rkey):
        return self._record


def _make_server(tile_collections=None):
    """Create a Server with the given tile_collections map."""
    logger = logging.getLogger("test")
    return Server("places.atgeo.org", logger, tile_collections=tile_collections)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_record_uri():
    """record_uri returns correct AT-Protocol URI string."""
    server = _make_server()
    uri = server.record_uri(TILE_COLLECTION, "tile001")
    assert uri == f"https://places.atgeo.org/{TILE_COLLECTION}/tile001"


def test_get_record_collection_not_found():
    """get_record raises XrpcError when collection is unknown."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.get_record({}, repo="places.atgeo.org", collection="unknown.collection", rkey="x")
    assert exc_info.value.name == "CollectionNotFound"


def test_get_record_tile_backed_missing_rkey():
    """get_record raises RecordNotFound when the tile-backed collection returns None."""
    mock_col = MockTileBackedCollection(record=None)
    server = _make_server(tile_collections={TILE_COLLECTION: mock_col})
    with pytest.raises(XrpcError) as exc_info:
        server.get_record(
            {}, repo="places.atgeo.org", collection=TILE_COLLECTION, rkey="nonexistent"
        )
    assert exc_info.value.name == "RecordNotFound"


def test_get_record_tile_backed_response_shape():
    """get_record on a tile-backed collection returns correct envelope shape."""
    record = {
        "rkey": "tile001",
        "name": "Tile Place",
        "importance": 5,
    }
    mock_col = MockTileBackedCollection(record=record)
    server = _make_server(tile_collections={TILE_COLLECTION: mock_col})

    result = server.get_record(
        {}, repo="places.atgeo.org", collection=TILE_COLLECTION, rkey="tile001"
    )

    assert result["uri"] == f"https://places.atgeo.org/{TILE_COLLECTION}/tile001"
    assert result["source"] == TILE_SOURCE_URL
    assert result["license"] == TILE_LICENSE_URL
    assert result["value"]["rkey"] == "tile001"
    assert "_query" in result
    assert result["importance"] == 5
    assert "importance" not in result["value"]
    assert "source" not in result["value"]
    assert "license" not in result["value"]


def test_get_record_returns_tile_relations_unmodified():
    """get_record serves the tile's own precomputed relations, unmodified."""
    record = {
        "rkey": "tile001",
        "relations": {"within": [{"rkey": "org.atgeo.places.overture.division:85922583"}]},
    }
    mock_col = MockTileBackedCollection(record=dict(record))
    server = _make_server(tile_collections={TILE_COLLECTION: mock_col})

    result = server.get_record(
        {}, repo="places.atgeo.org", collection=TILE_COLLECTION, rkey="tile001"
    )

    assert result["value"]["relations"] == record["relations"]
    # relations lives inside value, not at the envelope level
    assert "relations" not in result


def test_get_record_without_relations_key_has_no_relations():
    """get_record does not invent a relations key when the tile record has none."""
    record = {"rkey": "tile001"}
    mock_col = MockTileBackedCollection(record=record)
    server = _make_server(tile_collections={TILE_COLLECTION: mock_col})

    result = server.get_record(
        {}, repo="places.atgeo.org", collection=TILE_COLLECTION, rkey="tile001"
    )
    assert "relations" not in result["value"]


def test_load_lexicons():
    """load_lexicons returns a list of dicts, each with an 'id' key."""
    lexicons = load_lexicons()
    assert isinstance(lexicons, list)
    assert len(lexicons) > 0
    for lex in lexicons:
        assert "id" in lex


def test_get_record_lexicon_schema():
    """get_record returns lexicon JSON when collection is com.atproto.lexicon.schema."""
    server = _make_server()
    # rkey is an NSID like "org.atgeo.place"
    result = server.get_record(
        {}, repo="places.atgeo.org",
        collection=LEXICON_SCHEMA_COLLECTION, rkey="org.atgeo.place"
    )
    assert "uri" in result
    assert "value" in result
    assert result["uri"] == "at://did:web:places.atgeo.org/com.atproto.lexicon.schema/org.atgeo.place"
    assert result["value"]["id"] == "org.atgeo.place"
    assert result["value"]["lexicon"] == 1
    # No source for lexicon schemas
    assert "source" not in result
    # No importance for lexicon schemas
    assert "importance" not in result


def test_get_record_lexicon_schema_not_found():
    """get_record raises RecordNotFound for unknown NSID in lexicon schema collection."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.get_record(
            {}, repo="places.atgeo.org",
            collection=LEXICON_SCHEMA_COLLECTION, rkey="nonexistent.lexicon"
        )
    assert exc_info.value.name == "RecordNotFound"
