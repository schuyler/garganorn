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
    "names": [{"text": "Blue Bottle Coffee", "priority": 0}],
    "attributes": {},
}


def _make_mock_db(collection=FSQ_COLLECTION, records=None, nearest_results=None):
    """Create a mock Database object."""
    mock = MagicMock()
    mock.collection = collection
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
    return Server("gazetteer.social", dbs, logger)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_record_uri():
    """record_uri returns correct AT-Protocol URI string."""
    server = _make_server()
    uri = server.record_uri(FSQ_COLLECTION, "fsq001")
    assert uri == f"https://gazetteer.social/{FSQ_COLLECTION}/fsq001"


def test_get_record_collection_not_found():
    """get_record raises XrpcError when collection is unknown."""
    server = _make_server()
    with pytest.raises(XrpcError) as exc_info:
        server.get_record({}, repo="gazetteer.social", collection="unknown.collection", rkey="x")
    assert "CollectionNotFound" in str(exc_info.value) or exc_info.value.name == "CollectionNotFound"


def test_get_record_record_not_found():
    """get_record raises XrpcError when db.get_record returns None."""
    server = _make_server(records=None)
    with pytest.raises(XrpcError) as exc_info:
        server.get_record({}, repo="gazetteer.social", collection=FSQ_COLLECTION, rkey="nonexistent")
    assert "RecordNotFound" in str(exc_info.value) or exc_info.value.name == "RecordNotFound"


def test_get_record_success():
    """get_record returns dict with uri, value, and _query."""
    record = dict(SAMPLE_RECORD)  # copy since process pops keys
    server = _make_server(records=[record])
    result = server.get_record({}, repo="gazetteer.social", collection=FSQ_COLLECTION, rkey="fsq001")
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
