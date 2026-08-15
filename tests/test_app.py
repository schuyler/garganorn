"""Tests for Flask app routes in garganorn.__main__."""
import gzip
import json
import os
import pytest
from unittest.mock import patch

from garganorn.__main__ import create_app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

OVERTURE_COLLECTION = "org.atgeo.places.overture.place"

SAMPLE_RECORD = {
    "$type": "org.atgeo.place",
    "collection": OVERTURE_COLLECTION,
    "rkey": "ov001",
    "distance_m": 42,
    "locations": [
        {"$type": "community.lexicon.location.geo", "latitude": "37.774900", "longitude": "-122.419400"}
    ],
    "name": "Blue Bottle Coffee",
    "variants": [],
    "attributes": {},
}


@pytest.fixture
def client(tmp_path):
    """Flask test client with a real tile-backed OVERTURE_COLLECTION serving SAMPLE_RECORD."""
    tiles_root = tmp_path / "overture_place" / "tiles"
    tile_content = json.dumps({"records": [{"value": dict(SAMPLE_RECORD)}]}).encode()
    run_dir = _make_run(tiles_root, "20260101T000000", tile_content=tile_content, rkey="ov001")
    tiles_current = _point_current(tiles_root, run_dir)
    tiles_config = _make_tile_config(
        tiles_dir=tiles_current, manifest_path=tiles_current / "manifest.duckdb",
        slug="overture-place", base_url="https://places.atgeo.org/tiles/overture-place",
    )
    app = _build_tile_app(tiles_config)
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


@pytest.fixture
def client_no_record(tmp_path):
    """Flask test client where the OVERTURE_COLLECTION has no record for the requested rkey."""
    tiles_root = tmp_path / "overture_place" / "tiles"
    run_dir = _make_run(tiles_root, "20260101T000000")
    tiles_current = _point_current(tiles_root, run_dir)
    tiles_config = _make_tile_config(
        tiles_dir=tiles_current, manifest_path=tiles_current / "manifest.duckdb",
        slug="overture-place", base_url="https://places.atgeo.org/tiles/overture-place",
    )
    app = _build_tile_app(tiles_config)
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_resource_success(client):
    """GET /<collection>/<rkey> returns 200 with record fields, no envelope."""
    resp = client.get(f"/{OVERTURE_COLLECTION}/ov001")
    assert resp.status_code == 200
    assert resp.mimetype == "application/json"
    data = resp.get_json()
    # Record fields present
    assert data["name"] == "Blue Bottle Coffee"
    assert data["rkey"] == "ov001"
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
    resp = client_no_record.get(f"/{OVERTURE_COLLECTION}/nonexistent")
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


def _make_manifest_db(path, entries=(("r1", "012301"),)):
    """Create a minimal manifest.duckdb with a record_tiles table at *path*,
    seeded with (rkey, tile_qk) *entries*."""
    con = _duckdb.connect(str(path))
    try:
        con.execute(
            "CREATE TABLE record_tiles (rkey VARCHAR, tile_qk VARCHAR)"
        )
        for rkey, qk in entries:
            con.execute("INSERT INTO record_tiles VALUES (?, ?)", [rkey, qk])
    finally:
        con.close()


def _make_manifest_json(manifest_db_path):
    """Write a sibling manifest.json completeness marker next to manifest.duckdb.

    The build writes manifest.duckdb first, then manifest.json LAST as the
    completeness marker. Content is irrelevant to the guard -- only presence
    matters -- but keep it minimal and valid JSON.
    """
    manifest_json_path = os.path.join(
        os.path.dirname(str(manifest_db_path)), "manifest.json"
    )
    with open(manifest_json_path, "w") as f:
        json.dump({"complete": True}, f)
    return manifest_json_path


def _make_tile_config(tiles_dir, manifest_path, slug="overture-place",
                      base_url="https://places.atgeo.org/tiles/overture-place"):
    """Build a tiles_config dict for one collection using the slug schema."""
    coll_cfg = {
        "slug": slug,
        "manifest": str(manifest_path),
        "tiles_dir": str(tiles_dir),
        "base_url": base_url,
        "source": "https://example.com",
        "license": "https://example.com",
    }
    return {
        "collections": {
            OVERTURE_COLLECTION: coll_cfg,
        }
    }


def _make_run(tiles_root, stamp, qk="012301", tile_content=None, complete=True, rkey="r1"):
    """Create tiles_root/<stamp>/ with a manifest (record_tiles: rkey -> qk),
    optionally a tile file at tiles_root/<stamp>/<qk6>/<qk>.json.gz, and --
    unless complete=False -- the manifest.json completeness marker (pass
    complete=False to simulate a run that crashed before writing it).
    Returns the run directory."""
    run_dir = tiles_root / stamp
    run_dir.mkdir(parents=True)
    manifest_path = run_dir / "manifest.duckdb"
    _make_manifest_db(manifest_path, entries=[(rkey, qk)])
    if complete:
        _make_manifest_json(manifest_path)
    if tile_content is not None:
        tile_subdir = run_dir / qk[:6]
        tile_subdir.mkdir(parents=True)
        with gzip.open(tile_subdir / f"{qk}.json.gz", "wb") as f:
            f.write(tile_content)
    return run_dir


def _point_current(tiles_root, run_dir):
    """(Re)point tiles_root/current at run_dir as a symlink -- never a real
    directory, so the stamp a build resolves to is never the literal string
    'current' (see R1's fixture trap)."""
    current = tiles_root / "current"
    if current.exists() or current.is_symlink():
        current.unlink()
    current.symlink_to(run_dir, target_is_directory=True)
    return current


def _tile_json(name):
    """A tile file's decoded content: one record with a distinguishing name,
    shaped for TileBackedCollection.get_record (record.get('value', record))."""
    return json.dumps({"records": [{"value": {"rkey": "r1", "name": name}}]}).encode()


def _build_tile_app(tiles_config):
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", tiles_config)
        app = create_app()
    app.config["TESTING"] = True
    return app


@pytest.fixture
def tile_client(tmp_path):
    """Flask test client with a slug-based collection and a real tile on disk.

    Layout: tmp_path/overture_place/tiles/<stamp>/<qk6>/<qk>.json.gz, with
    'current' a symlink to <stamp> -- not a literal directory named 'current'
    (a literal directory would make the run stamp the string "current" and
    hide any bug in stamp derivation).
    """
    tiles_root = tmp_path / "overture_place" / "tiles"
    run_dir = _make_run(
        tiles_root, "20260101T000000",
        tile_content=b'{"attribution": "https://example.com", "records": []}',
    )
    tiles_current = _point_current(tiles_root, run_dir)

    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=tiles_current / "manifest.duckdb",
        slug="overture-place",
        base_url="https://places.atgeo.org/tiles/overture-place",
    )
    app = _build_tile_app(tiles_config)
    with app.test_client() as c:
        yield c


@pytest.fixture
def tile_client_empty(tmp_path):
    """Flask test client with a registered slug but no tile files on disk."""
    tiles_root = tmp_path / "overture_place" / "tiles"
    run_dir = _make_run(tiles_root, "20260101T000000")
    tiles_current = _point_current(tiles_root, run_dir)

    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=tiles_current / "manifest.duckdb",
        slug="overture-place",
        base_url="https://places.atgeo.org/tiles/overture-place",
    )
    app = _build_tile_app(tiles_config)
    with app.test_client() as c:
        yield c


def test_tile_served_successfully(tile_client):
    """GET /tiles/<slug>/<stamp>/<qk6>/<qk>.json.gz returns 200 with correct
    headers. tile_client's run is stamped 20260101T000000 (R2: tiles_dir
    roots at tiles/, one level above the run, so the stamp is part of the
    path)."""
    resp = tile_client.get("/tiles/overture-place/20260101T000000/012301/012301.json.gz")
    assert resp.status_code == 200
    assert resp.headers.get("Content-Encoding") == "gzip"
    assert "application/json" in resp.content_type
    body = gzip.decompress(resp.data)
    data = json.loads(body)
    assert "records" in data


def test_tile_missing_returns_404(tile_client_empty):
    """GET /tiles/<slug>/... for a nonexistent tile returns 404."""
    resp = tile_client_empty.get("/tiles/overture-place/000000/nonexistent.json.gz")
    assert resp.status_code == 404


def test_tile_unknown_slug_returns_404(tile_client_empty):
    """GET /tiles/<unknown-slug>/... returns 404 when slug is not registered."""
    resp = tile_client_empty.get("/tiles/unknown-source/012301/012301.json.gz")
    assert resp.status_code == 404


def test_tile_cache_control_is_constant_immutable(tile_client):
    """R5: tiles serve exactly this Cache-Control header, unconditionally,
    regardless of collection config."""
    resp = tile_client.get("/tiles/overture-place/20260101T000000/012301/012301.json.gz")
    assert resp.status_code == 200
    assert resp.headers.get("Cache-Control") == "public, max-age=604800, immutable"


def test_tile_path_traversal_rejected(tmp_path):
    """safe_join blocks paths that escape tiles_dir.

    Root is {source}/tiles/, one level above the run directory -- so escaping
    to a sibling of tiles/ (places.parquet) takes one '..', and escaping
    cross-source takes two.

    Two escapes, deliberately targeting different file types, because R17's
    suffix guard (tile_path must end in .json.gz) runs in the same handler
    and would 404 a places.parquet request on its own, before safe_join is
    ever consulted:
      - places.parquet doesn't end in .json.gz, so this case proves the
        suffix guard, not containment.
      - the cross-source target DOES end in .json.gz, so it's the one that
        actually proves safe_join blocks the escape.
    """
    source_dir = tmp_path / "overture_place"
    tiles_root = source_dir / "tiles"
    run_dir = _make_run(
        tiles_root, "20260101T000000",
        tile_content=b'{"attribution": "https://example.com", "records": []}',
    )
    tiles_current = _point_current(tiles_root, run_dir)

    # Physically create the escape targets so 404 proves blocking, not absence.
    places_parquet = source_dir / "places.parquet"
    places_parquet.write_bytes(b"PAR1")  # minimal parquet magic bytes

    # Cross-source escape target: another source's tile file.
    osm_dir = tmp_path / "osm" / "tiles" / "20260101T000000"
    osm_dir.mkdir(parents=True)
    cross_source_file = osm_dir / "012301.json.gz"
    with gzip.open(cross_source_file, "wb") as f:
        f.write(b'{"attribution": "https://example.com", "records": []}')

    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=tiles_current / "manifest.duckdb",
        slug="overture-place",
        base_url="https://places.atgeo.org/tiles/overture-place",
    )
    app = _build_tile_app(tiles_config)

    with app.test_client() as client:
        # Sibling escape to a non-tile file -- exercises the R17 suffix guard,
        # not safe_join containment (see docstring).
        sibling_paths = [
            "/tiles/overture-place/../places.parquet",
            "/tiles/overture-place/%2e%2e/places.parquet",
        ]
        for path in sibling_paths:
            resp = client.get(path)
            assert resp.status_code == 404, (
                f"Expected 404 for sibling traversal {path!r}, got {resp.status_code}"
            )

        # Cross-source escape to a .json.gz file -- this is the case that
        # actually proves safe_join blocks containment escapes.
        cross_paths = [
            "/tiles/overture-place/../../osm/tiles/20260101T000000/012301.json.gz",
            "/tiles/overture-place/%2e%2e/%2e%2e/osm/tiles/20260101T000000/012301.json.gz",
        ]
        for path in cross_paths:
            resp = client.get(path)
            assert resp.status_code == 404, (
                f"Expected 404 for cross-source traversal {path!r}, got {resp.status_code}"
            )


# ---------------------------------------------------------------------------
# R1 -- getCoverage returns run-stamped tile URLs
# ---------------------------------------------------------------------------

def test_consecutive_runs_produce_disjoint_coverage_urls(tmp_path):
    """R1: getCoverage tile URLs carry the serving run's stamp, so two server
    builds -- each pinned to a different completed run via 'current' -- emit
    disjoint URL sets."""
    tiles_root = tmp_path / "overture_place" / "tiles"
    run1 = _make_run(tiles_root, "20260101T000000",
                      tile_content=b'{"attribution": "https://example.com", "records": []}')
    run2 = _make_run(tiles_root, "20260102T000000",
                      tile_content=b'{"attribution": "https://example.com", "records": []}')
    current = _point_current(tiles_root, run1)

    tiles_config = _make_tile_config(
        tiles_dir=current, manifest_path=current / "manifest.duckdb",
        slug="overture-place", base_url="https://places.atgeo.org/tiles/overture-place",
    )

    def coverage_urls(app):
        with app.test_client() as client:
            resp = client.get("/xrpc/org.atgeo.getCoverage", query_string={
                "collection": OVERTURE_COLLECTION, "bbox": "-180,-85,180,85",
            })
        return set(resp.get_json()["tiles"])

    urls1 = coverage_urls(_build_tile_app(tiles_config))

    # Pipeline completes a new run; symlink flips, server not yet restarted.
    _point_current(tiles_root, run2)
    urls2 = coverage_urls(_build_tile_app(tiles_config))  # restart

    assert urls1 and urls2, "expected non-empty coverage from both runs"
    assert urls1.isdisjoint(urls2), (
        f"consecutive runs must produce disjoint URL sets; overlap: {urls1 & urls2}"
    )


def test_prior_run_tile_urls_still_resolve_after_new_build(tmp_path):
    """R1 AC (second half): a URL getCoverage advertised for a run that has
    since been superseded must still resolve to that run's own bytes, as
    long as the run is still on disk (keep-two retention)."""
    tiles_root = tmp_path / "overture_place" / "tiles"
    run1 = _make_run(tiles_root, "20260101T000000", tile_content=b'{"marker": "RunOne"}')
    run2 = _make_run(tiles_root, "20260102T000000", tile_content=b'{"marker": "RunTwo"}')
    current = _point_current(tiles_root, run1)

    tiles_config = _make_tile_config(
        tiles_dir=current, manifest_path=current / "manifest.duckdb",
        slug="overture-place", base_url="https://places.atgeo.org/tiles/overture-place",
    )

    app1 = _build_tile_app(tiles_config)
    with app1.test_client() as client1:
        resp1 = client1.get("/xrpc/org.atgeo.getCoverage", query_string={
            "collection": OVERTURE_COLLECTION, "bbox": "-180,-85,180,85",
        })
    prior_urls = resp1.get_json()["tiles"]
    assert prior_urls, "expected at least one coverage URL from the prior run"

    # New build completes and repoints 'current'; server restarts pinned to run2.
    _point_current(tiles_root, run2)
    app2 = _build_tile_app(tiles_config)

    with app2.test_client() as client2:
        for url in prior_urls:
            path = url[len("https://places.atgeo.org"):]
            resp = client2.get(path)
            assert resp.status_code == 200, (
                f"prior-run URL {path!r} must still resolve after a new build; "
                f"got {resp.status_code}"
            )
            body = json.loads(gzip.decompress(resp.data))
            assert body == {"marker": "RunOne"}, (
                f"prior-run URL {path!r} must serve the PRIOR run's bytes, "
                f"got {body!r}"
            )


# ---------------------------------------------------------------------------
# R2 -- tile_dirs[slug] roots at {source}/tiles/
# ---------------------------------------------------------------------------

def test_tile_route_roots_at_tiles_not_run_dir(tmp_path):
    """tile_dirs[slug] roots at {source}/tiles/, one level above the run
    directory, so a tile request must include the run's stamp segment to
    resolve; a path without that segment does not resolve."""
    tiles_root = tmp_path / "overture_place" / "tiles"
    stamp = "20260101T000000"
    run_dir = _make_run(tiles_root, stamp,
                         tile_content=b'{"attribution": "https://example.com", "records": []}')
    current = _point_current(tiles_root, run_dir)

    tiles_config = _make_tile_config(
        tiles_dir=current, manifest_path=current / "manifest.duckdb",
        slug="overture-place", base_url="https://places.atgeo.org/tiles/overture-place",
    )
    app = _build_tile_app(tiles_config)
    with app.test_client() as client:
        resp = client.get(f"/tiles/overture-place/{stamp}/012301/012301.json.gz")
        assert resp.status_code == 200, (
            f"expected tile_dirs[slug] to root at tiles/, making {stamp}/... "
            f"resolve; got {resp.status_code}"
        )

        resp = client.get("/tiles/overture-place/012301/012301.json.gz")
        assert resp.status_code == 404, (
            "the un-stamped path must not resolve, since tiles_dir roots at "
            f"tiles/, one level above the run dir; got {resp.status_code}"
        )


# ---------------------------------------------------------------------------
# R6 -- getCoverage responses are cached; the lexicon route is not
# ---------------------------------------------------------------------------

def test_getcoverage_response_has_cache_control(tile_client):
    """R6: getCoverage XRPC responses are cached for 1 hour."""
    resp = tile_client.get("/xrpc/org.atgeo.getCoverage", query_string={
        "collection": OVERTURE_COLLECTION, "bbox": "-180,-85,180,85",
    })
    assert resp.status_code == 200
    assert resp.headers.get("Cache-Control") == "public, max-age=3600"


def test_getcoverage_lexicon_route_lacks_cache_control(tile_client):
    """R6 regression guard: GET /<nsid> (the lexicon route) matches
    view_args == {'nsid': 'org.atgeo.getCoverage'}, identical to the XRPC
    route's view_args for the same method -- a hook keyed on view_args alone
    would wrongly cache this lexicon response too. The correct key is
    request.endpoint == 'xrpc-endpoint'."""
    resp = tile_client.get("/org.atgeo.getCoverage")
    assert resp.status_code == 200
    assert resp.headers.get("Cache-Control") != "public, max-age=3600"


# ---------------------------------------------------------------------------
# R17 -- the manifest is not reachable over HTTP
# ---------------------------------------------------------------------------

def test_manifest_files_not_reachable_over_http(tmp_path):
    """R17: neither manifest.duckdb nor manifest.json may be servable via
    /tiles/<slug>/.... tiles_dir is set directly to the run's parent here
    (rather than via a 'current' symlink) to isolate this guard from R2's
    root-relocation, which is covered separately."""
    tiles_root = tmp_path / "overture_place" / "tiles"
    stamp = "20260101T000000"
    run_dir = _make_run(tiles_root, stamp)

    tiles_config = _make_tile_config(
        tiles_dir=tiles_root, manifest_path=run_dir / "manifest.duckdb",
        slug="overture-place", base_url="https://places.atgeo.org/tiles/overture-place",
    )
    app = _build_tile_app(tiles_config)
    with app.test_client() as client:
        resp = client.get(f"/tiles/overture-place/{stamp}/manifest.duckdb")
        assert resp.status_code == 404, (
            f"manifest.duckdb must not be reachable over HTTP; got {resp.status_code}"
        )
        resp = client.get(f"/tiles/overture-place/{stamp}/manifest.json")
        assert resp.status_code == 404, (
            f"manifest.json must not be reachable over HTTP; got {resp.status_code}"
        )


# ---------------------------------------------------------------------------
# R18 -- getRecord reads the run resolved at startup, not 'current' live
# ---------------------------------------------------------------------------

def test_get_record_reads_run_resolved_at_startup(tmp_path):
    """R18: getRecord reads tiles from the run getCoverage advertises
    (frozen at startup), not from wherever 'current' points to at request
    time."""
    tiles_root = tmp_path / "overture_place" / "tiles"
    run1 = _make_run(tiles_root, "20260101T000000", tile_content=_tile_json("RunOne"))
    run2 = _make_run(tiles_root, "20260102T000000", tile_content=_tile_json("RunTwo"))
    current = _point_current(tiles_root, run1)

    tiles_config = _make_tile_config(
        tiles_dir=current, manifest_path=current / "manifest.duckdb",
        slug="overture-place", base_url="https://places.atgeo.org/tiles/overture-place",
    )
    app = _build_tile_app(tiles_config)  # frozen while current -> run1

    # A new build completes and repoints 'current' before the server restarts.
    _point_current(tiles_root, run2)

    with app.test_client() as client:
        resp = client.get(f"/{OVERTURE_COLLECTION}/r1")
    assert resp.status_code == 200
    assert resp.get_json()["name"] == "RunOne", (
        "getRecord followed the 'current' symlink to the new run instead of "
        "the run resolved at startup"
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
    leak coverage URLs for an incomplete collection. A single `ready` gate
    withholds tile_manifests, tile_collections, AND tile_dirs[slug] together.
    """
    # Deliberately complete=False -- simulates a crash between writing
    # manifest.duckdb and the manifest.json marker.
    tiles_root = tmp_path / "overture_place" / "tiles"
    stamp = "20260101T000000"
    run_dir = _make_run(
        tiles_root, stamp,
        tile_content=b'{"attribution": "https://example.com", "records": []}',
        complete=False,
    )
    tiles_current = _point_current(tiles_root, run_dir)

    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=tiles_current / "manifest.duckdb",
        slug="overture-place",
        base_url="https://places.atgeo.org/tiles/overture-place",
    )
    app = _build_tile_app(tiles_config)
    with app.test_client() as client:
        # Stamped path -- an unstamped path would 404 on path resolution
        # alone (tiles_dir roots at tiles/), masking whether the
        # completeness gate itself withholds tile_dirs[slug].
        resp = client.get(f"/tiles/overture-place/{stamp}/012301/012301.json.gz")
        assert resp.status_code == 404, (
            "incomplete run (manifest.duckdb without manifest.json) must not "
            f"be served; got {resp.status_code}"
        )

        # Same incompleteness must withhold the collection from getCoverage
        # too, not just the tile-serving route -- see docstring above.
        cov_resp = client.get(
            "/xrpc/org.atgeo.getCoverage",
            query_string={
                "collection": OVERTURE_COLLECTION,
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
    present is served normally -- it locks in the behavior the A1 guard must
    preserve. Requests the stamped path (R2): tiles_dir roots at tiles/, so
    the un-stamped path does not resolve.
    """
    tiles_root = tmp_path / "overture_place" / "tiles"
    stamp = "20260101T000000"
    run_dir = _make_run(
        tiles_root, stamp,
        tile_content=b'{"attribution": "https://example.com", "records": []}',
    )
    tiles_current = _point_current(tiles_root, run_dir)

    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=tiles_current / "manifest.duckdb",
        slug="overture-place",
        base_url="https://places.atgeo.org/tiles/overture-place",
    )
    app = _build_tile_app(tiles_config)
    with app.test_client() as client:
        resp = client.get(f"/tiles/overture-place/{stamp}/012301/012301.json.gz")
        assert resp.status_code == 200, (
            "complete run (manifest.duckdb + manifest.json) must be served; "
            f"got {resp.status_code}"
        )


def test_no_manifest_key_still_boots(tmp_path):
    """F4 regression guard: a collection whose config has no 'manifest' key
    at all must not crash create_app(); the readiness check's short-circuited
    `and` must gracefully treat the collection as tile-less.
    """
    tiles_config = {
        "collections": {
            OVERTURE_COLLECTION: {
                "slug": "overture-place",
                "base_url": "https://places.atgeo.org/tiles/overture-place",
                # No "manifest" key, no "tiles_dir" key.
                "source": "https://example.com",
                "license": "https://example.com",
            },
        }
    }
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", tiles_config)
        app = create_app()  # must not raise
    app.config["TESTING"] = True
    with app.test_client() as client:
        resp = client.get("/health")
        assert resp.status_code == 200


def test_create_app_raises_on_base_url_slug_mismatch(tmp_path):
    """create_app raises ValueError when base_url does not end with /<slug>."""
    tiles_root = tmp_path / "overture_place" / "tiles"
    run_dir = _make_run(tiles_root, "20260101T000000")
    tiles_current = _point_current(tiles_root, run_dir)

    # slug is "overture-place" but base_url ends with "wrong-slug"
    tiles_config = _make_tile_config(
        tiles_dir=tiles_current,
        manifest_path=tiles_current / "manifest.duckdb",
        slug="overture-place",
        base_url="https://places.atgeo.org/tiles/wrong-slug",
    )
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", tiles_config)
        with pytest.raises(ValueError, match="base_url must end with '/overture-place'"):
            create_app()
