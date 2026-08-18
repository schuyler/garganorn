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
COLLECTION_METADATA_COLLECTION = "org.atgeo.collection"


class MockTileBackedCollection:
    def __init__(self, collection=TILE_COLLECTION, record=None):
        self.collection = collection
        self.source_url = TILE_SOURCE_URL
        self.license_url = TILE_LICENSE_URL
        self._record = record

    def get_record(self, repo, collection, rkey):
        return self._record


def _make_server(tile_collections=None, tile_manifests=None, collection_metadata=None):
    """Create a Server with the given tile_collections/tile_manifests/
    collection_metadata maps. tile_manifests and collection_metadata are only
    forwarded when given, so callers that don't need them keep exercising
    Server's current constructor signature unchanged."""
    logger = logging.getLogger("test")
    kwargs = {}
    if tile_manifests is not None:
        kwargs["tile_manifests"] = tile_manifests
    if collection_metadata is not None:
        kwargs["collection_metadata"] = collection_metadata
    return Server("places.atgeo.org", logger, tile_collections=tile_collections, **kwargs)


def _collection_record(collection, source="https://overturemaps.org/",
                        license="https://docs.overturemaps.org/attribution/"):
    """A realistic org.atgeo.collection record value, matching
    garganorn/lexicon/collection.json's schema -- source/license
    deliberately distinct from TILE_SOURCE_URL/TILE_LICENSE_URL above, so a
    get_record branch that accidentally reuses those (rather than reading
    the record's own fields) is caught."""
    return {
        "collection": collection,
        "source": source,
        "license": license,
        "generatedAt": "2026-01-01T00:00:00Z",
        "recordCount": 42,
        "extent": {"north": "37.80", "west": "-122.42", "south": "37.77", "east": "-122.41"},
        "locationTypes": ["community.lexicon.location.geo"],
        "containmentLevels": ["locality"],
        "categories": [{"value": "coffee_shop", "count": 42}],
        "attributes": ["name", "category"],
    }


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


def test_get_record_output_validates_against_lexicon_schema():
    """get_record's output on a real record validates against com.atproto.repo.getRecord's schema."""
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

    server.server.validate("com.atproto.repo.getRecord", "output", result)


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


# ---------------------------------------------------------------------------
# describeGazetteer
# ---------------------------------------------------------------------------

def test_describe_gazetteer_lists_ready_collections():
    """describeGazetteer's did is did:web:<repo>, and its collections list is
    exactly the keys of tile_manifests -- the same map get_coverage consults
    for CollectionNotFound (R1)."""
    tile_manifests = {
        "org.atgeo.places.overture.place": object(),
        "org.atgeo.places.osm": object(),
    }
    server = _make_server(tile_manifests=tile_manifests)
    result = server.describe_gazetteer(None)
    assert result["did"] == "did:web:places.atgeo.org"
    assert {c["collection"] for c in result["collections"]} == set(tile_manifests)


def test_describe_gazetteer_excludes_collections_not_coverage_ready():
    """A collection present in tile_collections but absent from tile_manifests
    is not listed -- proves the list comes from tile_manifests, not
    tile_collections. The two maps deliberately differ here (osm is
    record-served but not coverage-ready)."""
    tile_manifests = {"org.atgeo.places.overture.place": object()}
    tile_collections = {
        "org.atgeo.places.overture.place": MockTileBackedCollection(),
        "org.atgeo.places.osm": MockTileBackedCollection(collection="org.atgeo.places.osm"),
    }
    server = _make_server(tile_manifests=tile_manifests, tile_collections=tile_collections)
    result = server.describe_gazetteer(None)
    listed = {c["collection"] for c in result["collections"]}
    assert listed == {"org.atgeo.places.overture.place"}
    assert "org.atgeo.places.osm" not in listed


def test_describe_gazetteer_metadata_link_reflects_collection_metadata_map():
    """A collection with a collection_metadata entry is listed with a
    'metadata' URL of the record_uri form; one without an entry is listed
    with no 'metadata' key at all (not None -- the key is absent)."""
    tile_manifests = {
        "org.atgeo.places.overture.place": object(),
        "org.atgeo.places.osm": object(),
    }
    collection_metadata = {
        "org.atgeo.places.overture.place": _collection_record("org.atgeo.places.overture.place"),
    }
    server = _make_server(tile_manifests=tile_manifests, collection_metadata=collection_metadata)
    result = server.describe_gazetteer(None)
    by_collection = {c["collection"]: c for c in result["collections"]}

    assert by_collection["org.atgeo.places.overture.place"]["metadata"] == (
        "https://places.atgeo.org/org.atgeo.collection/org.atgeo.places.overture.place"
    )
    assert "metadata" not in by_collection["org.atgeo.places.osm"]


def test_describe_gazetteer_validates_against_lexicon_schema():
    """describeGazetteer's result validates against org.atgeo.describeGazetteer's
    own output schema."""
    tile_manifests = {"org.atgeo.places.overture.place": object()}
    collection_metadata = {
        "org.atgeo.places.overture.place": _collection_record("org.atgeo.places.overture.place"),
    }
    server = _make_server(tile_manifests=tile_manifests, collection_metadata=collection_metadata)
    result = server.describe_gazetteer(None)
    server.server.validate("org.atgeo.describeGazetteer", "output", result)


# ---------------------------------------------------------------------------
# get_record on org.atgeo.collection
# ---------------------------------------------------------------------------

def test_get_record_collection_metadata_envelope_uses_described_collections_own_urls():
    """get_record on org.atgeo.collection returns {uri, source, license, value}
    where uri is the record_uri form and source/license come from the
    DESCRIBED collection's own record fields -- not any hardcoded value."""
    described = _collection_record(
        "org.atgeo.places.overture.place",
        source="https://overturemaps.org/",
        license="https://docs.overturemaps.org/attribution/",
    )
    server = _make_server(
        collection_metadata={"org.atgeo.places.overture.place": described}
    )

    result = server.get_record(
        {}, repo="places.atgeo.org", collection=COLLECTION_METADATA_COLLECTION,
        rkey="org.atgeo.places.overture.place",
    )

    assert result["uri"] == (
        "https://places.atgeo.org/org.atgeo.collection/org.atgeo.places.overture.place"
    )
    assert result["source"] == "https://overturemaps.org/"
    assert result["license"] == "https://docs.overturemaps.org/attribution/"
    # Distinct from the tile-backed mock's values -- rules out an
    # implementation that reuses those instead of the record's own fields.
    assert result["source"] != TILE_SOURCE_URL
    assert result["license"] != TILE_LICENSE_URL
    assert result["value"] == described


def test_get_record_collection_metadata_unknown_rkey_and_unknown_collection():
    """A known-nsid-but-unmetadata'd rkey under org.atgeo.collection raises
    RecordNotFound; an entirely unknown top-level collection still raises
    CollectionNotFound (the existing fallthrough, unaffected)."""
    described = _collection_record("org.atgeo.places.overture.place")
    server = _make_server(
        collection_metadata={"org.atgeo.places.overture.place": described}
    )

    with pytest.raises(XrpcError) as exc_info:
        server.get_record(
            {}, repo="places.atgeo.org", collection=COLLECTION_METADATA_COLLECTION,
            rkey="org.atgeo.places.osm",
        )
    assert exc_info.value.name == "RecordNotFound"

    with pytest.raises(XrpcError) as exc_info2:
        server.get_record(
            {}, repo="places.atgeo.org", collection="org.atgeo.nonexistent",
            rkey="x",
        )
    assert exc_info2.value.name == "CollectionNotFound"


def test_get_record_collection_metadata_output_validates_against_lexicon_schema():
    """get_record's org.atgeo.collection envelope validates against
    com.atproto.repo.getRecord's output schema (required: uri, source,
    license, value -- all present in the new branch's envelope)."""
    described = _collection_record("org.atgeo.places.overture.place")
    server = _make_server(
        collection_metadata={"org.atgeo.places.overture.place": described}
    )

    result = server.get_record(
        {}, repo="places.atgeo.org", collection=COLLECTION_METADATA_COLLECTION,
        rkey="org.atgeo.places.overture.place",
    )

    server.server.validate("com.atproto.repo.getRecord", "output", result)
