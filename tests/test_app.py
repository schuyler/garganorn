"""Tests for Flask app routes in garganorn.__main__."""
import pytest
from unittest.mock import MagicMock, patch

from garganorn.__main__ import create_app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FSQ_COLLECTION = "org.atgeo.places.foursquare"

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


def _make_mock_db(collection=FSQ_COLLECTION, record=None):
    """Create a mock Database object."""
    mock = MagicMock()
    mock.collection = collection
    mock.get_record.return_value = record
    return mock


@pytest.fixture
def client():
    """Flask test client with mock DBs."""
    mock_db = _make_mock_db(FSQ_COLLECTION, record=dict(SAMPLE_RECORD))
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None)
        app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


@pytest.fixture
def client_no_record():
    """Flask test client where get_record returns None (record not found)."""
    mock_db = _make_mock_db(FSQ_COLLECTION, record=None)
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None)
        app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_resource_success(client):
    """GET /<collection>/<rkey> returns 200 with record fields, no envelope."""
    resp = client.get(f"/{FSQ_COLLECTION}/fsq001")
    assert resp.status_code == 200
    assert resp.mimetype == "application/json"
    data = resp.get_json()
    # Record fields present
    assert data["name"] == "Blue Bottle Coffee"
    assert data["rkey"] == "fsq001"
    assert "locations" in data
    assert "variants" in data
    assert "attributes" in data
    # No XRPC envelope keys
    assert "uri" not in data
    assert "_query" not in data


def test_resource_collection_not_found(client):
    """GET with unknown collection returns 404 with CollectionNotFound error."""
    resp = client.get("/unknown.collection/somekey")
    assert resp.status_code == 404
    data = resp.get_json()
    assert data is not None
    assert data.get("error") == "CollectionNotFound"


def test_resource_record_not_found(client_no_record):
    """GET with valid collection but unknown rkey returns 404 with RecordNotFound error."""
    resp = client_no_record.get(f"/{FSQ_COLLECTION}/nonexistent")
    assert resp.status_code == 404
    data = resp.get_json()
    assert data is not None
    assert data.get("error") == "RecordNotFound"


def test_health_still_works(client):
    """GET /health still returns 200 (regression check)."""
    resp = client.get("/health")
    assert resp.status_code == 200


def test_lexicon_known_nsid(client):
    """GET /<nsid> returns 200 with lexicon JSON for a known NSID."""
    resp = client.get("/org.atgeo.place")
    assert resp.status_code == 200
    assert resp.mimetype == "application/json"
    data = resp.get_json()
    assert data["id"] == "org.atgeo.place"
    assert data["lexicon"] == 1


def test_lexicon_unknown_nsid(client):
    """GET /<nsid> returns 404 for an unknown NSID."""
    resp = client.get("/nonexistent.lexicon")
    assert resp.status_code == 404
