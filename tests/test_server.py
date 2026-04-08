"""Tests for garganorn.server.Server."""
import logging
import pytest
from unittest.mock import MagicMock

from lexrpc.base import XrpcError

from garganorn.server import Server, load_lexicons


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FSQ_COLLECTION = "org.atgeo.places.foursquare"
OVR_COLLECTION = "org.atgeo.places.overture"

SAMPLE_RECORD = {
    "$type": "org.atgeo.place",
    "collection": FSQ_COLLECTION,
    "rkey": "fsq001",
    "distance_m": 42,
    "locations": [
        {"$type": "community.lexicon.location.geo", "latitude": "37.774900", "longitude": "-122.419400"}
    ],
    "name": "Blue Bottle Coffee",
    "variants": [],
    "attributes": {},
}


TEST_ATTRIBUTION_URL = "https://example.com/attribution"


def _make_mock_db(collection=FSQ_COLLECTION, records=None, nearest_results=None):
    """Create a mock Database object."""
    mock = MagicMock()
    mock.collection = collection
    mock.attribution = TEST_ATTRIBUTION_URL
    mock.get_record.return_value = records[0] if records else None
    mock.nearest.return_value = nearest_results or []
    return mock


def _make_server(collections=None, records=None, nearest_results=None):
    """Create a Server with a mock DB."""
    if collections is None:
        collections = [FSQ_COLLECTION]
    dbs = [
        _make_mock_db(col, records=records, nearest_results=nearest_results)
        for col in collections
    ]
    logger = logging.getLogger("test")
    return Server("places.atgeo.org", dbs, logger)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_record_uri():
    """record_uri returns correct AT-Protocol URI string."""
    server = _make_server()
    uri = server.record_uri(FSQ_COLLECTION, "fsq001")
    assert uri == f"https://places.atgeo.org/{FSQ_COLLECTION}/fsq001"


def test_get_record_collection_not_found():
    """get_record raises XrpcError when collection is unknown."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.get_record({}, repo="places.atgeo.org", collection="unknown.collection", rkey="x")
    assert "CollectionNotFound" in str(exc_info.value) or exc_info.value.name == "CollectionNotFound"


def test_get_record_record_not_found():
    """get_record raises XrpcError when db.get_record returns None."""
    server = _make_server(records=None)
    with pytest.raises(XrpcError) as exc_info:
        server.get_record({}, repo="places.atgeo.org", collection=FSQ_COLLECTION, rkey="nonexistent")
    assert "RecordNotFound" in str(exc_info.value) or exc_info.value.name == "RecordNotFound"


def test_get_record_success():
    """get_record returns dict with uri, value, and _query."""
    record = dict(SAMPLE_RECORD)  # copy since process pops keys
    server = _make_server(records=[record])
    result = server.get_record({}, repo="places.atgeo.org", collection=FSQ_COLLECTION, rkey="fsq001")
    assert "uri" in result
    assert "value" in result
    assert "_query" in result
    assert result["value"]["rkey"] == "fsq001"


def test_search_records_missing_params():
    """No lat/lon and no q raises XrpcError with InvalidQuery."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.search_records({}, collection=FSQ_COLLECTION)
    assert "InvalidQuery" in str(exc_info.value) or exc_info.value.name == "InvalidQuery"


def test_search_records_bad_coordinates():
    """Non-numeric lat/lon raises XrpcError with InvalidCoordinates."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.search_records({}, collection=FSQ_COLLECTION, latitude="not_a_number", longitude="also_bad")
    assert "InvalidCoordinates" in str(exc_info.value) or exc_info.value.name == "InvalidCoordinates"


def test_search_records_collection_not_found():
    """Unknown collection raises XrpcError with CollectionNotFound."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.search_records({}, collection="unknown.collection", latitude="37.7", longitude="-122.4")
    assert "CollectionNotFound" in str(exc_info.value) or exc_info.value.name == "CollectionNotFound"


def test_search_records_requires_positional_body_arg():
    """Calling search_records without `_` positional arg raises TypeError."""
    server = _make_server()
    with pytest.raises(TypeError):
        # Omit the required `_` positional argument
        server.search_records(collection=FSQ_COLLECTION, latitude="37.7", longitude="-122.4")


def test_load_lexicons():
    """load_lexicons returns a list of dicts, each with an 'id' key."""
    lexicons = load_lexicons()
    assert isinstance(lexicons, list)
    assert len(lexicons) > 0
    for lex in lexicons:
        assert "id" in lex


def test_search_records_returns_records():
    """search_records returns dict with 'records' key when results found."""
    nearest_results = [dict(SAMPLE_RECORD)]
    server = _make_server(nearest_results=nearest_results)
    result = server.search_records(
        {}, collection=FSQ_COLLECTION, latitude="37.7749", longitude="-122.4194"
    )
    assert "records" in result
    assert "_query" in result


def test_search_records_with_bbox():
    """search_records accepts bbox parameter and passes bbox tuple to nearest()."""
    nearest_results = [dict(SAMPLE_RECORD)]
    server = _make_server(nearest_results=nearest_results)
    result = server.search_records(
        {}, collection=FSQ_COLLECTION, bbox="-122.5,37.7,-122.3,37.8"
    )
    assert "records" in result
    assert "_query" in result
    # Verify nearest was called with bbox tuple, not lat/lon
    mock_db = server.db[FSQ_COLLECTION]
    call_kwargs = mock_db.nearest.call_args
    assert "bbox" in call_kwargs.kwargs or (call_kwargs.args and isinstance(call_kwargs.args[0], tuple))


def test_search_records_invalid_bbox_format():
    """Malformed bbox raises InvalidBbox."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.search_records({}, collection=FSQ_COLLECTION, bbox="not,valid,bbox")
    assert exc_info.value.name == "InvalidBbox"


def test_search_records_invalid_bbox_order():
    """bbox with xmin >= xmax raises InvalidBbox."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.search_records({}, collection=FSQ_COLLECTION, bbox="-122.3,37.7,-122.5,37.8")
    assert exc_info.value.name == "InvalidBbox"


def test_search_records_bbox_overrides_latlon():
    """When both bbox and lat/lon are provided, bbox is used."""
    nearest_results = [dict(SAMPLE_RECORD)]
    server = _make_server(nearest_results=nearest_results)
    result = server.search_records(
        {}, collection=FSQ_COLLECTION,
        bbox="-122.5,37.7,-122.3,37.8",
        latitude="0.0", longitude="0.0"
    )
    assert "records" in result
    # lat/lon should be ignored; bbox should be passed through
    mock_db = server.db[FSQ_COLLECTION]
    call_kwargs = mock_db.nearest.call_args.kwargs
    bbox = call_kwargs.get("bbox")
    assert bbox is not None
    assert bbox[0] == pytest.approx(-122.5)


def test_search_records_bbox_nan():
    """NaN bbox values raise InvalidBbox."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.search_records({}, collection=FSQ_COLLECTION, bbox="nan,nan,nan,nan")
    assert exc_info.value.name == "InvalidBbox"


def test_search_records_bbox_inf():
    """Inf bbox values raise InvalidBbox."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.search_records({}, collection=FSQ_COLLECTION, bbox="-inf,0,inf,1")
    assert exc_info.value.name == "InvalidBbox"


def test_search_records_bbox_in_query_response():
    """_query.parameters includes bbox and q."""
    nearest_results = [dict(SAMPLE_RECORD)]
    server = _make_server(nearest_results=nearest_results)
    result = server.search_records(
        {}, collection=FSQ_COLLECTION, bbox="-122.5,37.7,-122.3,37.8", q="coffee"
    )
    params = result["_query"]["parameters"]
    assert "bbox" in params
    assert "q" in params


def test_search_records_text_query_includes_score_and_importance():
    """search_records with q= returns wrapper with score and importance; neither leaks into value."""
    record = dict(SAMPLE_RECORD)
    record["score"] = 0.75
    record["importance"] = 3
    server = _make_server(nearest_results=[record])
    result = server.search_records(
        {}, collection=FSQ_COLLECTION, latitude="37.7749", longitude="-122.4194", q="coffee"
    )
    assert "records" in result
    wrapper = result["records"][0]
    assert "score" in wrapper
    assert isinstance(wrapper["score"], float)
    assert "importance" in wrapper
    assert isinstance(wrapper["importance"], int)
    assert "score" not in wrapper["value"]
    assert "importance" not in wrapper["value"]


def test_search_records_spatial_only_includes_importance_not_score():
    """search_records without q= returns wrapper with importance but no score."""
    record = dict(SAMPLE_RECORD)
    record["importance"] = 5
    server = _make_server(nearest_results=[record])
    result = server.search_records(
        {}, collection=FSQ_COLLECTION, latitude="37.7749", longitude="-122.4194"
    )
    assert "records" in result
    wrapper = result["records"][0]
    assert "importance" in wrapper
    assert isinstance(wrapper["importance"], int)
    assert "score" not in wrapper
    assert "importance" not in wrapper["value"]


def test_search_records_score_is_rounded():
    """search_records rounds score to 3 decimal places."""
    record = dict(SAMPLE_RECORD)
    record["score"] = 0.8571428571
    record["importance"] = 1
    server = _make_server(nearest_results=[record])
    result = server.search_records(
        {}, collection=FSQ_COLLECTION, latitude="37.7749", longitude="-122.4194", q="coffee"
    )
    wrapper = result["records"][0]
    assert wrapper["score"] == 0.857


def test_get_record_includes_importance():
    """get_record wrapper includes importance; it does not leak into value."""
    record = dict(SAMPLE_RECORD)
    record["importance"] = 7
    server = _make_server(records=[record])
    result = server.get_record(
        {}, repo="places.atgeo.org", collection=FSQ_COLLECTION, rkey="fsq001"
    )
    assert "importance" in result
    assert isinstance(result["importance"], int)
    assert "importance" not in result["value"]
    assert "score" not in result


def test_get_record_with_boundaries_includes_relations():
    """get_record includes relations.within inside the record value."""
    record = dict(SAMPLE_RECORD)
    mock_db = _make_mock_db(records=[record])

    # Mock BoundaryLookup
    mock_boundaries = MagicMock()
    mock_boundaries.containment.return_value = [
        {"rkey": "org.atgeo.places.wof:85922583", "name": "San Francisco", "level": 50},
    ]

    logger = logging.getLogger("test")
    server = Server("places.atgeo.org", [mock_db], logger, boundaries=mock_boundaries)
    result = server.get_record({}, repo="places.atgeo.org", collection=FSQ_COLLECTION, rkey="fsq001")

    value = result["value"]
    assert "relations" in value
    assert "within" in value["relations"]
    assert value["relations"]["within"][0]["name"] == "San Francisco"
    # relations should NOT be at the envelope level
    assert "relations" not in result


def test_get_record_without_boundaries_has_no_relations():
    """get_record omits relations when no boundaries configured."""
    record = dict(SAMPLE_RECORD)
    server = _make_server(records=[record])
    result = server.get_record({}, repo="places.atgeo.org", collection=FSQ_COLLECTION, rkey="fsq001")
    assert "relations" not in result["value"]


def test_get_record_boundaries_empty_containment():
    """get_record omits relations when containment returns empty list."""
    record = dict(SAMPLE_RECORD)
    mock_db = _make_mock_db(records=[record])
    mock_boundaries = MagicMock()
    mock_boundaries.containment.return_value = []

    logger = logging.getLogger("test")
    server = Server("places.atgeo.org", [mock_db], logger, boundaries=mock_boundaries)
    result = server.get_record({}, repo="places.atgeo.org", collection=FSQ_COLLECTION, rkey="fsq001")
    assert "relations" not in result["value"]


def test_get_record_includes_attribution():
    """get_record response includes attribution as a string at envelope level, not inside value."""
    record = dict(SAMPLE_RECORD)
    server = _make_server(records=[record])
    result = server.get_record({}, repo="places.atgeo.org", collection=FSQ_COLLECTION, rkey="fsq001")
    assert "attribution" in result
    assert isinstance(result["attribution"], str)
    assert result["attribution"] == TEST_ATTRIBUTION_URL
    assert "attribution" not in result["value"]


def test_search_records_includes_attribution():
    """Each record wrapper in search_records includes attribution as a string at envelope level, not inside value."""
    nearest_results = [dict(SAMPLE_RECORD)]
    server = _make_server(nearest_results=nearest_results)
    result = server.search_records(
        {}, collection=FSQ_COLLECTION, latitude="37.7749", longitude="-122.4194"
    )
    assert "records" in result
    wrapper = result["records"][0]
    assert "attribution" in wrapper
    assert isinstance(wrapper["attribution"], str)
    assert wrapper["attribution"] == TEST_ATTRIBUTION_URL
    assert "attribution" not in wrapper["value"]


LEXICON_SCHEMA_COLLECTION = "com.atproto.lexicon.schema"


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
    # No attribution for lexicon schemas
    assert "attribution" not in result
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


def test_list_records_lexicon_schema():
    """list_records returns all lexicon schemas with AT URIs."""
    server = _make_server()
    result = server.list_records(
        {}, repo="places.atgeo.org", collection=LEXICON_SCHEMA_COLLECTION
    )
    assert "records" in result
    records = result["records"]
    assert len(records) > 0
    # Each record has uri and value
    for rec in records:
        assert "uri" in rec
        assert "value" in rec
        nsid = rec["value"]["id"]
        assert rec["uri"] == f"at://did:web:places.atgeo.org/com.atproto.lexicon.schema/{nsid}"
    # Records are sorted by NSID
    nsids = [r["value"]["id"] for r in records]
    assert nsids == sorted(nsids)


def test_list_records_lexicon_schema_limit():
    """list_records respects the limit parameter."""
    server = _make_server()
    result = server.list_records(
        {}, repo="places.atgeo.org", collection=LEXICON_SCHEMA_COLLECTION,
        limit=2
    )
    assert len(result["records"]) == 2
    assert "cursor" in result  # More records available


def test_list_records_lexicon_schema_cursor():
    """list_records paginates using cursor."""
    server = _make_server()
    page1 = server.list_records(
        {}, repo="places.atgeo.org", collection=LEXICON_SCHEMA_COLLECTION,
        limit=2
    )
    assert len(page1["records"]) == 2
    cursor = page1["cursor"]

    page2 = server.list_records(
        {}, repo="places.atgeo.org", collection=LEXICON_SCHEMA_COLLECTION,
        limit=2, cursor=cursor
    )
    assert len(page2["records"]) > 0
    # No overlap between pages
    page1_nsids = {r["value"]["id"] for r in page1["records"]}
    page2_nsids = {r["value"]["id"] for r in page2["records"]}
    assert page1_nsids.isdisjoint(page2_nsids)


def test_list_records_lexicon_schema_reverse():
    """list_records with reverse=True returns records in reverse NSID order."""
    server = _make_server()
    result = server.list_records(
        {}, repo="places.atgeo.org", collection=LEXICON_SCHEMA_COLLECTION,
        reverse=True
    )
    nsids = [r["value"]["id"] for r in result["records"]]
    assert nsids == sorted(nsids, reverse=True)


def test_list_records_collection_not_found():
    """list_records raises CollectionNotFound for unsupported collections."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.list_records(
            {}, repo="places.atgeo.org", collection="unknown.collection"
        )
    assert exc_info.value.name == "CollectionNotFound"


def test_list_records_no_cursor_on_last_page():
    """list_records omits cursor when all records fit in one page."""
    server = _make_server()
    result = server.list_records(
        {}, repo="places.atgeo.org", collection=LEXICON_SCHEMA_COLLECTION,
        limit=100
    )
    assert "cursor" not in result


TILE_COLLECTION = "org.atgeo.places.tile"
TILE_ATTRIBUTION = "https://example.com/tile-attribution"


class MockTileBackedCollection:
    def __init__(self, collection=TILE_COLLECTION, record=None):
        self.collection = collection
        self.attribution = TILE_ATTRIBUTION
        self._record = record

    def get_record(self, repo, collection, rkey):
        return self._record


def test_get_record_tile_backed_response_shape():
    """get_record on a tile-backed collection returns correct envelope shape."""
    record = {
        "rkey": "tile001",
        "name": "Tile Place",
        "importance": 5,
    }
    mock_col = MockTileBackedCollection(record=record)
    logger = logging.getLogger("test")
    server = Server("places.atgeo.org", [], logger,
                    tile_collections={TILE_COLLECTION: mock_col})

    result = server.get_record(
        {}, repo="places.atgeo.org", collection=TILE_COLLECTION, rkey="tile001"
    )

    assert result["uri"] == f"https://places.atgeo.org/{TILE_COLLECTION}/tile001"
    assert result["attribution"] == TILE_ATTRIBUTION
    assert result["value"]["rkey"] == "tile001"
    assert "_query" in result
    assert result["importance"] == 5
    assert "importance" not in result["value"]


def test_get_record_tile_backed_missing_rkey():
    """get_record raises RecordNotFound when tile-backed collection returns None."""
    mock_col = MockTileBackedCollection(record=None)
    logger = logging.getLogger("test")
    server = Server("places.atgeo.org", [], logger,
                    tile_collections={TILE_COLLECTION: mock_col})

    with pytest.raises(XrpcError) as exc_info:
        server.get_record(
            {}, repo="places.atgeo.org", collection=TILE_COLLECTION, rkey="nonexistent"
        )
    assert exc_info.value.name == "RecordNotFound"


def test_search_records_tile_only_collection_returns_error():
    """search_records raises CollectionNotFound for tile-only collections (no search support)."""
    mock_col = MockTileBackedCollection()
    logger = logging.getLogger("test")
    server = Server("places.atgeo.org", [], logger,
                    tile_collections={TILE_COLLECTION: mock_col})

    with pytest.raises(XrpcError) as exc_info:
        server.search_records(
            {}, collection=TILE_COLLECTION, latitude="37.7", longitude="-122.4"
        )
    assert exc_info.value.name == "CollectionNotFound"


def test_get_record_boundaries_error_degrades_gracefully():
    """get_record returns record without relations when boundaries lookup fails."""
    record = dict(SAMPLE_RECORD)
    mock_db = _make_mock_db(records=[record])

    # Mock BoundaryLookup that raises on containment
    mock_boundaries = MagicMock()
    mock_boundaries.containment.side_effect = Exception("database not found")

    logger = logging.getLogger("test")
    server = Server("places.atgeo.org", [mock_db], logger, boundaries=mock_boundaries)
    result = server.get_record({}, repo="places.atgeo.org", collection=FSQ_COLLECTION, rkey="fsq001")

    # Should succeed with record but no relations
    assert "value" in result
    assert result["value"]["rkey"] == "fsq001"
    assert "relations" not in result["value"]
