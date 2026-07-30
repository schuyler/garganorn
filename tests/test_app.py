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

import duckdb as _duckdb


def _make_manifest_db(path):
    """Create a minimal manifest.duckdb with a record_tiles table at *path*."""
    con = _duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE record_tiles (rkey VARCHAR, tile_qk VARCHAR)"
        )
        con.execute(
            "INSERT INTO record_tiles VALUES ('r1', '012301')"
        )
    finally:
        con.close()


def _make_manifest_json(manifest_db_path):
    """Write a sibling manifest.json completeness marker next to manifest.duckdb.

    A2 (A1 in oq-p2-5-deploy-execution.md): the build writes manifest.duckdb
    first, then manifest.json LAST as the completeness marker. Content is
    irrelevant to the guard -- only presence matters -- but keep it minimal
    and valid JSON.
    """
    manifest_json_path = os.path.join(
        os.path.dirname(str(manifest_db_path)), "manifest.json"
    )
    with open(manifest_json_path, "w") as f:
        json.dump({"complete": True}, f)
    return manifest_json_path


def _make_tile_config(tiles_dir, manifest_path, slug="foursquare",
                      base_url="https://places.atgeo.org/tiles/foursquare",
                      cache_ttl=None):
    """Build a tiles_config dict for one collection using the slug schema."""
    coll_cfg = {
        "slug": slug,
        "manifest": str(manifest_path),
        "tiles_dir": str(tiles_dir),
        "base_url": base_url,
        "attribution": "https://example.com",
    }
    if cache_ttl is not None:
        coll_cfg["cache_ttl"] = cache_ttl
    return {
        "collections": {
            FSQ_COLLECTION: coll_cfg,
        }
    }


@pytest.fixture
def tile_client(tmp_path):
    """Flask test client with a slug-based collection and a real tile on disk."""
    # Layout: tmp_path/foursquare/tiles/current/<qk6>/<qk>.json.gz
    tiles_current = tmp_path / "foursquare" / "tiles" / "current"
    tile_subdir = tiles_current / "012301"
    tile_subdir.mkdir(parents=True)
    tile_file = tile_subdir / "012301.json.gz"
    content = b'{"attribution": "https://example.com", "records": []}'
    with gzip.open(tile_file, "wb") as f:
        f.write(content)

    manifest_path = tiles_current / "manifest.duckdb"
    _make_manifest_db(manifest_path)
    _make_manifest_json(manifest_path)

    mock_db = _make_mock_db(FSQ_COLLECTION)
    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=manifest_path,
        slug="foursquare",
        base_url="https://places.atgeo.org/tiles/foursquare",
        cache_ttl=86400,
    )
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, tiles_config)
        app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


@pytest.fixture
def tile_client_empty(tmp_path):
    """Flask test client with a registered slug but no tile files on disk."""
    tiles_current = tmp_path / "foursquare" / "tiles" / "current"
    tiles_current.mkdir(parents=True)

    manifest_path = tiles_current / "manifest.duckdb"
    _make_manifest_db(manifest_path)
    _make_manifest_json(manifest_path)

    mock_db = _make_mock_db(FSQ_COLLECTION)
    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=manifest_path,
        slug="foursquare",
        base_url="https://places.atgeo.org/tiles/foursquare",
    )
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, tiles_config)
        app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_tile_served_successfully(tile_client):
    """GET /tiles/<slug>/<qk6>/<qk>.json.gz returns 200 with correct headers."""
    resp = tile_client.get("/tiles/foursquare/012301/012301.json.gz")
    assert resp.status_code == 200
    assert resp.headers.get("Content-Encoding") == "gzip"
    assert "application/json" in resp.content_type
    body = gzip.decompress(resp.data)
    data = json.loads(body)
    assert "records" in data


def test_tile_missing_returns_404(tile_client_empty):
    """GET /tiles/<slug>/... for a nonexistent tile returns 404."""
    resp = tile_client_empty.get("/tiles/foursquare/000000/nonexistent.json.gz")
    assert resp.status_code == 404


def test_tile_unknown_slug_returns_404(tile_client_empty):
    """GET /tiles/<unknown-slug>/... returns 404 when slug is not registered."""
    resp = tile_client_empty.get("/tiles/unknown-source/012301/012301.json.gz")
    assert resp.status_code == 404


def test_tile_cache_control_present_when_ttl_set(tile_client):
    """Cache-Control is 'public, max-age=<ttl>' (not immutable) when cache_ttl is set."""
    resp = tile_client.get("/tiles/foursquare/012301/012301.json.gz")
    assert resp.status_code == 200
    cc = resp.headers.get("Cache-Control", "")
    assert cc == "public, max-age=86400", (
        f"Expected 'public, max-age=86400', got {cc!r}"
    )
    assert "immutable" not in cc


def test_tile_cache_control_absent_when_no_ttl(tmp_path):
    """Cache-Control public/max-age directives absent when cache_ttl is omitted from config."""
    tiles_current = tmp_path / "foursquare" / "tiles" / "current"
    tile_subdir = tiles_current / "012301"
    tile_subdir.mkdir(parents=True)
    tile_file = tile_subdir / "012301.json.gz"
    with gzip.open(tile_file, "wb") as f:
        f.write(b'{"attribution": "https://example.com", "records": []}')
    manifest_path = tiles_current / "manifest.duckdb"
    _make_manifest_db(manifest_path)
    _make_manifest_json(manifest_path)

    mock_db = _make_mock_db(FSQ_COLLECTION)
    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=manifest_path,
        slug="foursquare",
        base_url="https://places.atgeo.org/tiles/foursquare",
        # No cache_ttl
    )
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, tiles_config)
        app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        resp = c.get("/tiles/foursquare/012301/012301.json.gz")
    assert resp.status_code == 200
    # When cache_ttl is omitted, the route must NOT emit a public/max-age directive.
    # Flask's send_file may set Cache-Control: no-cache in testing mode, which is acceptable.
    cc = resp.headers.get("Cache-Control", "")
    assert "public" not in cc
    assert "max-age" not in cc


def test_tile_path_traversal_rejected(tmp_path):
    """safe_join blocks paths that escape tiles_dir (sibling and cross-source)."""
    # Layout: tmp_path/<source>/tiles/current is the serving root.
    # places.parquet lives at tmp_path/<source>/places.parquet — two .. up.
    source_dir = tmp_path / "overture_place"
    tiles_current = source_dir / "tiles" / "current"
    tile_subdir = tiles_current / "012301"
    tile_subdir.mkdir(parents=True)
    tile_file = tile_subdir / "012301.json.gz"
    with gzip.open(tile_file, "wb") as f:
        f.write(b'{"attribution": "https://example.com", "records": []}')

    manifest_path = tiles_current / "manifest.duckdb"
    _make_manifest_db(manifest_path)
    _make_manifest_json(manifest_path)

    # Physically create the escape target so 404 proves blocking, not absence.
    places_parquet = source_dir / "places.parquet"
    places_parquet.write_bytes(b"PAR1")  # minimal parquet magic bytes

    # Cross-source escape target: another source's tiles dir
    fsq_dir = tmp_path / "foursquare" / "tiles" / "current"
    fsq_dir.mkdir(parents=True)
    cross_source_file = fsq_dir / "012301.json.gz"
    with gzip.open(cross_source_file, "wb") as f:
        f.write(b'{"attribution": "https://example.com", "records": []}')

    mock_db = _make_mock_db(FSQ_COLLECTION)
    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=manifest_path,
        slug="overture-place",
        base_url="https://places.atgeo.org/tiles/overture-place",
        cache_ttl=86400,
    )
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, tiles_config)
        app = create_app()
    app.config["TESTING"] = True

    with app.test_client() as client:
        # Sibling escape: tiles/current/../../places.parquet = <source>/places.parquet
        sibling_paths = [
            "/tiles/overture-place/../../places.parquet",
            "/tiles/overture-place/%2e%2e/%2e%2e/places.parquet",
        ]
        for path in sibling_paths:
            resp = client.get(path)
            assert resp.status_code == 404, (
                f"Expected 404 for sibling traversal {path!r}, got {resp.status_code}"
            )

        # Cross-source escape: tiles/current/../../../foursquare/tiles/current/012301.json.gz
        cross_paths = [
            "/tiles/overture-place/../../../foursquare/tiles/current/012301.json.gz",
            "/tiles/overture-place/%2e%2e/%2e%2e/%2e%2e/foursquare/tiles/current/012301.json.gz",
        ]
        for path in cross_paths:
            resp = client.get(path)
            assert resp.status_code == 404, (
                f"Expected 404 for cross-source traversal {path!r}, got {resp.status_code}"
            )


# ---------------------------------------------------------------------------
# A1: completeness guard -- manifest.duckdb alone must not be enough to serve
# ---------------------------------------------------------------------------

def test_incomplete_run_not_served(tmp_path):
    """A run that crashed after writing manifest.duckdb but before writing the
    manifest.json completeness marker must NOT be served: the /tiles/<slug>/...
    route returns 404, AND the collection must not be exposed through
    org.atgeo.getCoverage.

    getCoverage keys off Server.tile_manifests independently of the
    /tiles/<slug>/... route (garganorn/server.py:213,221-223), so a fix that
    only special-cases serve_tile() would pass a route-only check yet still
    leak coverage URLs for an incomplete collection. The design requires a
    single `ready` gate that withholds tile_manifests, tile_collections, AND
    tile_dirs[slug] together.

    Today's code (garganorn/__main__.py) gates tile_manifests registration
    (and thus getCoverage) as well as the /tiles/ route on manifest.duckdb
    existence alone, so this currently serves the tile AND leaks it via
    getCoverage -- this test must FAIL until the fix requires the sibling
    manifest.json for all three (tile_manifests, tile_collections, tile_dirs).
    """
    tiles_current = tmp_path / "foursquare" / "tiles" / "current"
    tile_subdir = tiles_current / "012301"
    tile_subdir.mkdir(parents=True)
    tile_file = tile_subdir / "012301.json.gz"
    with gzip.open(tile_file, "wb") as f:
        f.write(b'{"attribution": "https://example.com", "records": []}')

    manifest_path = tiles_current / "manifest.duckdb"
    _make_manifest_db(manifest_path)
    # Deliberately NOT calling _make_manifest_json -- simulates a crash
    # between writing manifest.duckdb and the manifest.json marker.

    mock_db = _make_mock_db(FSQ_COLLECTION)
    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=manifest_path,
        slug="foursquare",
        base_url="https://places.atgeo.org/tiles/foursquare",
    )
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, tiles_config)
        app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as client:
        resp = client.get("/tiles/foursquare/012301/012301.json.gz")
        assert resp.status_code == 404, (
            "incomplete run (manifest.duckdb without manifest.json) must not "
            f"be served; got {resp.status_code}"
        )

        # Same incompleteness must withhold the collection from getCoverage
        # too, not just the tile-serving route -- see docstring above.
        cov_resp = client.get(
            "/xrpc/org.atgeo.getCoverage",
            query_string={
                "collection": FSQ_COLLECTION,
                "bbox": "-180,-85,180,85",
            },
        )
        assert cov_resp.status_code != 200, (
            "incomplete run must not be exposed via getCoverage; got "
            f"{cov_resp.status_code} body={cov_resp.get_json()!r}"
        )
        cov_data = cov_resp.get_json()
        assert cov_data is not None
        assert cov_data.get("error") == "CollectionNotFound", (
            "incomplete collection must appear as CollectionNotFound to "
            f"getCoverage (i.e. not registered), got {cov_data!r}"
        )


def test_complete_run_served(tmp_path):
    """Positive control: a run with BOTH manifest.duckdb and manifest.json
    present is served normally. May already pass today -- it locks in the
    behavior the A1 fix must preserve.
    """
    tiles_current = tmp_path / "foursquare" / "tiles" / "current"
    tile_subdir = tiles_current / "012301"
    tile_subdir.mkdir(parents=True)
    tile_file = tile_subdir / "012301.json.gz"
    with gzip.open(tile_file, "wb") as f:
        f.write(b'{"attribution": "https://example.com", "records": []}')

    manifest_path = tiles_current / "manifest.duckdb"
    _make_manifest_db(manifest_path)
    _make_manifest_json(manifest_path)

    mock_db = _make_mock_db(FSQ_COLLECTION)
    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=manifest_path,
        slug="foursquare",
        base_url="https://places.atgeo.org/tiles/foursquare",
    )
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, tiles_config)
        app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as client:
        resp = client.get("/tiles/foursquare/012301/012301.json.gz")
        assert resp.status_code == 200, (
            "complete run (manifest.duckdb + manifest.json) must be served; "
            f"got {resp.status_code}"
        )


def test_no_manifest_key_still_boots(tmp_path):
    """F4 regression guard: a collection whose config has no 'manifest' key
    at all must not crash create_app() (today's graceful "no manifest
    configured" path, which A1's short-circuited `and` must preserve).
    """
    mock_db = _make_mock_db(FSQ_COLLECTION)
    tiles_config = {
        "collections": {
            FSQ_COLLECTION: {
                "slug": "foursquare",
                "base_url": "https://places.atgeo.org/tiles/foursquare",
                # No "manifest" key, no "tiles_dir" key.
                "attribution": "https://example.com",
            },
        }
    }
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, tiles_config)
        app = create_app()  # must not raise
    app.config["TESTING"] = True
    with app.test_client() as client:
        resp = client.get("/health")
        assert resp.status_code == 200


def test_create_app_raises_on_base_url_slug_mismatch(tmp_path):
    """create_app raises ValueError when base_url does not end with /<slug>."""
    tiles_current = tmp_path / "foursquare" / "tiles" / "current"
    tiles_current.mkdir(parents=True)
    manifest_path = tiles_current / "manifest.duckdb"
    _make_manifest_db(manifest_path)

    mock_db = _make_mock_db(FSQ_COLLECTION)
    # slug is "foursquare" but base_url ends with "wrong-slug"
    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=manifest_path,
        slug="foursquare",
        base_url="https://places.atgeo.org/tiles/wrong-slug",
    )
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", [mock_db], None, tiles_config)
        with pytest.raises(ValueError, match="base_url must end with '/foursquare'"):
            create_app()
