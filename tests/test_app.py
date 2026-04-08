"""Tests for Flask app routes in garganorn.__main__."""
import gzip
import json
import os
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
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, None)
        app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


@pytest.fixture
def client_no_record():
    """Flask test client where get_record returns None (record not found)."""
    mock_db = _make_mock_db(FSQ_COLLECTION, record=None)
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, None)
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


def test_did_document(client):
    """GET /.well-known/did.json returns a valid DID document."""
    resp = client.get("/.well-known/did.json")
    assert resp.status_code == 200
    assert resp.mimetype == "application/json"
    data = resp.get_json()
    assert data["id"] == "did:web:places.atgeo.org"
    assert data["alsoKnownAs"] == ["at://places.atgeo.org"]
    # PDS service endpoint
    services = {s["id"]: s for s in data["service"]}
    assert "#atproto_pds" in services
    assert services["#atproto_pds"]["type"] == "AtprotoPersonalDataServer"
    assert services["#atproto_pds"]["serviceEndpoint"] == "https://places.atgeo.org"


# ---------------------------------------------------------------------------
# Tile serving tests
# ---------------------------------------------------------------------------

@pytest.fixture
def tile_client(tmp_path):
    """Flask test client configured with a real gzipped tile file on disk."""
    tile_subdir = tmp_path / "fsq" / "012301"
    tile_subdir.mkdir(parents=True)
    tile_file = tile_subdir / "012301.json.gz"
    content = b'{"attribution": "https://example.com", "records": []}'
    with gzip.open(tile_file, "wb") as f:
        f.write(content)
    mock_db = _make_mock_db(FSQ_COLLECTION)
    tiles_config = {"serve_dir": str(tmp_path)}
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, tiles_config)
        app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


@pytest.fixture
def tile_client_empty(tmp_path):
    """Flask test client with an empty tile directory (no files)."""
    mock_db = _make_mock_db(FSQ_COLLECTION)
    tiles_config = {"serve_dir": str(tmp_path)}
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, tiles_config)
        app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_tile_served_successfully(tile_client):
    """GET /tiles/<path> returns 200 with gzip content-encoding and JSON content-type."""
    resp = tile_client.get("/tiles/fsq/012301/012301.json.gz")
    assert resp.status_code == 200
    assert resp.headers.get("Content-Encoding") == "gzip"
    assert "application/json" in resp.content_type
    body = gzip.decompress(resp.data)
    data = json.loads(body)
    assert "records" in data


def test_tile_missing_returns_404(tile_client_empty):
    """GET /tiles/<path> for a nonexistent tile returns 404."""
    resp = tile_client_empty.get("/tiles/fsq/000000/nonexistent.json.gz")
    assert resp.status_code == 404


def test_tile_path_traversal_rejected(tmp_path):
    """safe_join blocks paths that escape serve_dir."""
    # Set up a fresh serve_dir with a tile
    serve_dir = tmp_path / "serve"
    serve_dir.mkdir()
    tile_subdir = serve_dir / "fsq" / "012301"
    tile_subdir.mkdir(parents=True)
    tile_file = tile_subdir / "012301.json.gz"
    with gzip.open(tile_file, "wb") as f:
        f.write(b'{"attribution": "https://example.com", "records": []}')

    # Create a "secret" file one level above serve_dir — reachable via ../
    secret = tmp_path / "secret.json.gz"
    with gzip.open(secret, "wb") as f:
        f.write(b'{"secret": true}')

    mock_db = _make_mock_db(FSQ_COLLECTION)
    tiles_config = {"serve_dir": str(serve_dir)}
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, tiles_config)
        app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        # serve_dir is tmp_path/serve/; secret is tmp_path/secret.json.gz
        # Traversal: serve_dir/../secret.json.gz = tmp_path/secret.json.gz
        for path in ["/tiles/../secret.json.gz", "/tiles/%2e%2e/secret.json.gz"]:
            resp = client.get(path)
            assert resp.status_code == 404, (
                f"Expected 404 for traversal path {path!r}, got {resp.status_code}"
            )
