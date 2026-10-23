"""Tests for Flask app routes in garganorn.__main__."""
import gzip
import json
import pytest

from tests.served_runs import (
    OVERTURE, PUBLIC_BASE, STAMP, TILE_BASE, build_app, make_run, place,
    point_current, serve_run, tileset_config,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

QK = "012301"
WORLD = "-180,-85,180,85"


def _tiles_url_paths(client):
    """The /tiles/<slug>/... routes of every tile getTiles advertises."""
    resp = client.get("/xrpc/org.atgeo.getTiles", query_string={
        "dataset": OVERTURE, "bbox": WORLD,
    })
    assert resp.status_code == 200
    return ["/tiles" + url[len(TILE_BASE):] for url in resp.get_json()["tiles"]]


@pytest.fixture
def client(tmp_path):
    """Flask test client serving one Overture place tileset."""
    tiles = {QK: [place(OVERTURE, "place", "ov001")]}
    app = build_app(serve_run(tmp_path, tiles=tiles))
    with app.test_client() as c:
        yield c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

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
    resp = client.get("/org.atgeo.nonexistent")
    assert resp.status_code == 404
    assert resp.mimetype == "application/json"
    assert resp.get_json() == {
        "error": "LexiconNotFound", "message": "Unknown lexicon: org.atgeo.nonexistent",
    }
    assert resp.headers["Access-Control-Allow-Origin"] == "*"


def test_xrpc_get_record_not_implemented(client):
    """com.atproto.repo.getRecord is not served, even for a record the tile holds."""
    resp = client.get("/xrpc/com.atproto.repo.getRecord", query_string={
        "repo": "places.atgeo.org", "collection": "org.atgeo.place", "rkey": "ov001",
    })
    assert resp.status_code == 501
    assert resp.get_json()["error"] == "MethodNotImplemented"


def test_repo_style_paths_404(client):
    """No two-segment path resolves: not the lexicon schema path, not a
    dataset/collection path. The tile holds the place and tileset.json is on
    disk, so each 404 can only come from the missing route."""
    assert client.get("/com.atproto.lexicon.schema/org.atgeo.getTiles").status_code == 404
    assert client.get(f"/{OVERTURE}/place").status_code == 404


def test_did_document(client):
    """GET /.well-known/did.json advertises the tile service and nothing else."""
    resp = client.get("/.well-known/did.json")
    assert resp.status_code == 200
    assert resp.mimetype == "application/json"
    assert resp.get_json() == {
        "id": "did:web:places.atgeo.org",
        "service": [{
            "id": "#atgeo_tiles",
            "type": "AtgeoTileService",
            "serviceEndpoint": PUBLIC_BASE,
        }],
    }


# ---------------------------------------------------------------------------
# Tile serving tests
# ---------------------------------------------------------------------------

@pytest.fixture
def tile_client(tmp_path):
    """Flask test client with a real tile on disk.

    Layout: tmp_path/overture-place/tiles/<stamp>/<qk6>/<qk>.json.gz, with
    'current' a symlink to <stamp> -- not a literal directory named 'current'
    (a literal directory would make the run stamp the string "current" and
    hide any bug in stamp derivation).
    """
    app = build_app(serve_run(tmp_path, tiles={QK: []}))
    with app.test_client() as c:
        yield c


@pytest.fixture
def tile_client_empty(tmp_path):
    """Flask test client with a registered slug but no tile files on disk."""
    app = build_app(serve_run(tmp_path))
    with app.test_client() as c:
        yield c


def test_tile_served_successfully(tile_client):
    """GET /tiles/<slug>/<stamp>/<qk6>/<qk>.json.gz returns 200 with plain
    JSON and no Content-Encoding -- the on-disk file is gzip, but the
    response body is decompressed before serving, so the client's
    Accept-Encoding governs the wire format instead of a forced header.
    The run is stamped 20260101T000000 (the served root is the run's parent,
    so the stamp is part of the path)."""
    resp = tile_client.get(f"/tiles/overture-place/{STAMP}/{QK}/{QK}.json.gz")
    assert resp.status_code == 200
    assert resp.headers.get("Content-Encoding") is None
    assert "application/json" in resp.content_type
    data = json.loads(resp.data)
    assert "records" in data


def test_tile_missing_returns_404(tile_client_empty):
    """GET /tiles/<slug>/... for a nonexistent tile returns 404."""
    resp = tile_client_empty.get("/tiles/overture-place/000000/nonexistent.json.gz")
    assert resp.status_code == 404
    assert resp.mimetype == "application/json"
    assert resp.get_json() == {
        "error": "TileNotFound",
        "message": "No such tile: overture-place/000000/nonexistent.json.gz",
    }
    assert resp.headers["Access-Control-Allow-Origin"] == "*"


def test_tile_unknown_slug_returns_404(tile_client_empty):
    """GET /tiles/<unknown-slug>/... returns 404 when slug is not registered."""
    resp = tile_client_empty.get("/tiles/unknown-source/012301/012301.json.gz")
    assert resp.status_code == 404
    assert resp.mimetype == "application/json"
    assert resp.get_json() == {
        "error": "TilesetNotFound", "message": "Unknown tileset: unknown-source",
    }
    assert resp.headers["Access-Control-Allow-Origin"] == "*"


def test_tile_cache_control_is_constant_immutable(tile_client):
    """Tiles serve exactly this Cache-Control header, unconditionally."""
    resp = tile_client.get(f"/tiles/overture-place/{STAMP}/{QK}/{QK}.json.gz")
    assert resp.status_code == 200
    assert resp.headers.get("Cache-Control") == "public, max-age=604800, immutable"


def test_tile_allows_cross_origin_reads(tile_client):
    """Tiles are fetched by browser pages on other origins."""
    resp = tile_client.get(f"/tiles/overture-place/{STAMP}/{QK}/{QK}.json.gz")
    assert resp.status_code == 200
    assert resp.headers.get("Access-Control-Allow-Origin") == "*"


def test_tile_path_traversal_rejected(tmp_path):
    """safe_join blocks paths that escape the served tiles root.

    Root is {source}/tiles/, one level above the run directory -- so escaping
    to a sibling of tiles/ (places.parquet) takes one '..', and escaping
    cross-source takes two.

    Two escapes, deliberately targeting different file types, because the
    suffix guard (tile_path must end in .json.gz) runs in the same handler
    and would 404 a places.parquet request on its own, before safe_join is
    ever consulted:
      - places.parquet doesn't end in .json.gz, so this case proves the
        suffix guard, not containment.
      - the cross-source target DOES end in .json.gz, so it's the one that
        actually proves safe_join blocks the escape.
    """
    source_dir = tmp_path / "overture-place"
    tileset = serve_run(tmp_path, tiles={QK: []})

    # Physically create the escape targets so 404 proves blocking, not absence.
    (source_dir / "places.parquet").write_bytes(b"PAR1")  # minimal parquet magic bytes

    # Cross-source escape target: another source's tile file.
    osm_dir = tmp_path / "osm" / "tiles" / STAMP
    osm_dir.mkdir(parents=True)
    with gzip.open(osm_dir / "012301.json.gz", "wb") as f:
        f.write(b'{"records": []}')

    app = build_app(tileset)

    with app.test_client() as client:
        # Sibling escape to a non-tile file -- exercises the suffix guard,
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
            assert resp.mimetype == "application/json"
            assert resp.get_json()["error"] == "TileNotFound"

        # Cross-source escape to a .json.gz file -- this is the case that
        # actually proves safe_join blocks containment escapes.
        cross_paths = [
            f"/tiles/overture-place/../../osm/tiles/{STAMP}/012301.json.gz",
            f"/tiles/overture-place/%2e%2e/%2e%2e/osm/tiles/{STAMP}/012301.json.gz",
        ]
        for path in cross_paths:
            resp = client.get(path)
            assert resp.status_code == 404, (
                f"Expected 404 for cross-source traversal {path!r}, got {resp.status_code}"
            )
            assert resp.mimetype == "application/json"
            assert resp.get_json()["error"] == "TileNotFound"


# ---------------------------------------------------------------------------
# getTiles returns run-stamped tile URLs
# ---------------------------------------------------------------------------

def test_consecutive_runs_produce_disjoint_tile_urls(tmp_path):
    """getTiles URLs carry the serving run's stamp, so two server builds --
    each pinned to a different completed run via 'current' -- emit disjoint
    URL sets."""
    tiles_root = tmp_path / "overture-place" / "tiles"
    collections = [(OVERTURE, "place")]
    run1 = make_run(tiles_root, "20260101T000000", "overture-place", collections, {QK: []})
    run2 = make_run(tiles_root, "20260102T000000", "overture-place", collections, {QK: []})
    current = point_current(tiles_root, run1)
    tileset = tileset_config(current, "overture-place")

    def tile_urls(app):
        with app.test_client() as client:
            resp = client.get("/xrpc/org.atgeo.getTiles", query_string={
                "dataset": OVERTURE, "bbox": WORLD,
            })
        return set(resp.get_json()["tiles"])

    urls1 = tile_urls(build_app(tileset))

    # Pipeline completes a new run; symlink flips, server not yet restarted.
    point_current(tiles_root, run2)
    urls2 = tile_urls(build_app(tileset))  # restart

    assert urls1 and urls2, "expected non-empty tile lists from both runs"
    assert urls1.isdisjoint(urls2), (
        f"consecutive runs must produce disjoint URL sets; overlap: {urls1 & urls2}"
    )


def test_prior_run_tile_urls_still_resolve_after_new_build(tmp_path):
    """A URL getTiles advertised for a run that has since been superseded
    must still resolve to that run's own bytes, as long as the run is still
    on disk (keep-two retention)."""
    tiles_root = tmp_path / "overture-place" / "tiles"
    collections = [(OVERTURE, "place")]
    run1 = make_run(tiles_root, "20260101T000000", "overture-place", collections,
                    {QK: [place(OVERTURE, "place", "ov001", name="RunOne")]})
    run2 = make_run(tiles_root, "20260102T000000", "overture-place", collections,
                    {QK: [place(OVERTURE, "place", "ov001", name="RunTwo")]})
    tileset = tileset_config(point_current(tiles_root, run1), "overture-place")

    with build_app(tileset).test_client() as client1:
        prior_paths = _tiles_url_paths(client1)
    assert prior_paths, "expected at least one tile URL from the prior run"

    # New build completes and repoints 'current'; server restarts pinned to run2.
    point_current(tiles_root, run2)

    with build_app(tileset).test_client() as client2:
        for path in prior_paths:
            resp = client2.get(path)
            assert resp.status_code == 200, (
                f"prior-run URL {path!r} must still resolve after a new build; "
                f"got {resp.status_code}"
            )
            names = [r["value"]["name"] for r in json.loads(resp.data)["records"]]
            assert names == ["RunOne"], (
                f"prior-run URL {path!r} must serve the PRIOR run's bytes, "
                f"got {names!r}"
            )


# ---------------------------------------------------------------------------
# The served tile root is the run's parent
# ---------------------------------------------------------------------------

def test_tile_route_roots_at_tiles_not_run_dir(tile_client):
    """The served root is {source}/tiles/, one level above the run directory,
    so a tile request must include the run's stamp segment to resolve; a path
    without that segment does not resolve."""
    resp = tile_client.get(f"/tiles/overture-place/{STAMP}/{QK}/{QK}.json.gz")
    assert resp.status_code == 200, (
        f"expected the served root to be tiles/, making {STAMP}/... "
        f"resolve; got {resp.status_code}"
    )

    resp = tile_client.get(f"/tiles/overture-place/{QK}/{QK}.json.gz")
    assert resp.status_code == 404, (
        "the un-stamped path must not resolve, since the served root is "
        f"tiles/, one level above the run dir; got {resp.status_code}"
    )


# ---------------------------------------------------------------------------
# getTiles responses are cached; the lexicon route is not
# ---------------------------------------------------------------------------

def test_gettiles_response_has_cache_control(tile_client):
    """getTiles XRPC responses are cached for 1 hour."""
    resp = tile_client.get("/xrpc/org.atgeo.getTiles", query_string={
        "dataset": OVERTURE, "bbox": WORLD,
    })
    assert resp.status_code == 200
    assert resp.headers.get("Cache-Control") == "public, max-age=3600"


def test_gettiles_lexicon_route_lacks_cache_control(tile_client):
    """Regression guard: GET /<nsid> (the lexicon route) matches
    view_args == {'nsid': 'org.atgeo.getTiles'}, identical to the XRPC
    route's view_args for the same method -- a hook keyed on view_args alone
    would wrongly cache this lexicon response too. The correct key is
    request.endpoint == 'xrpc-endpoint'."""
    resp = tile_client.get("/org.atgeo.getTiles")
    assert resp.status_code == 200
    assert resp.headers.get("Cache-Control") != "public, max-age=3600"


# ---------------------------------------------------------------------------
# The manifest and descriptor files are not reachable over HTTP
# ---------------------------------------------------------------------------

def test_run_files_not_reachable_over_http(tile_client):
    """None of manifest.duckdb, manifest.json or tileset.json may be servable
    via /tiles/<slug>/...; the descriptor is served only at /tilesets/<slug>."""
    for name in ("manifest.duckdb", "manifest.json", "tileset.json"):
        resp = tile_client.get(f"/tiles/overture-place/{STAMP}/{name}")
        assert resp.status_code == 404, (
            f"{name} must not be reachable over HTTP; got {resp.status_code}"
        )


# ---------------------------------------------------------------------------
# Completeness guard -- manifest.duckdb alone must not be enough to serve
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("missing", ["manifest.json", "tileset.json"])
def test_incomplete_run_not_served(tmp_path, missing):
    """A run lacking its manifest.json completeness marker (a crash after
    manifest.duckdb) or its tileset.json must NOT be served: the
    /tiles/<slug>/... route returns 404, AND the dataset must not be exposed
    through org.atgeo.getTiles or org.atgeo.describeService.

    getTiles and describeService key off the served-tileset map
    independently of the /tiles/<slug>/... route, so a fix that only
    special-cases serve_tile() would pass a route-only check yet still leak
    tile URLs for an incomplete run. A single `ready` gate withholds the
    tileset from all of them.
    """
    tileset = serve_run(
        tmp_path, tiles={QK: []},
        complete=(missing != "manifest.json"), descriptor=(missing != "tileset.json"),
    )
    app = build_app(tileset)
    with app.test_client() as client:
        # Stamped path -- an unstamped path would 404 on path resolution
        # alone (the served root is tiles/), masking whether the
        # completeness gate itself withholds the tileset.
        resp = client.get(f"/tiles/overture-place/{STAMP}/{QK}/{QK}.json.gz")
        assert resp.status_code == 404, (
            f"run without {missing} must not be served; got {resp.status_code}"
        )

        tiles_resp = client.get("/xrpc/org.atgeo.getTiles", query_string={
            "dataset": OVERTURE, "bbox": WORLD,
        })
        assert tiles_resp.status_code != 200, (
            f"run without {missing} must not be exposed via getTiles; got "
            f"{tiles_resp.status_code} body={tiles_resp.get_json()!r}"
        )
        assert tiles_resp.get_json()["error"] == "DatasetNotFound"

        service = client.get("/xrpc/org.atgeo.describeService").get_json()
        assert service["tilesets"] == []


def test_complete_run_served(tile_client):
    """Positive control: a run with manifest.duckdb, manifest.json and
    tileset.json present is served normally -- it locks in the behavior
    test_incomplete_run_not_served guards. Requests the stamped path: the
    served root is tiles/, so the un-stamped path does not resolve.
    """
    resp = tile_client.get(f"/tiles/overture-place/{STAMP}/{QK}/{QK}.json.gz")
    assert resp.status_code == 200, (
        "complete run (manifest.duckdb + manifest.json + tileset.json) must be "
        f"served; got {resp.status_code}"
    )


def test_missing_run_dir_still_boots(tmp_path):
    """Regression guard: a configured tileset whose run directory does not
    exist must not crash create_app(); it is simply not served."""
    missing = {"run_dir": str(tmp_path / "nothing" / "current"),
               "base_url": f"{TILE_BASE}/overture-place"}
    app = build_app(missing)
    with app.test_client() as client:
        assert client.get("/health").status_code == 200
        service = client.get("/xrpc/org.atgeo.describeService").get_json()
        assert service["tilesets"] == []


def test_create_app_raises_on_base_url_slug_mismatch(tmp_path):
    """create_app raises ValueError when base_url does not end with the
    descriptor's /<slug>."""
    tileset = serve_run(tmp_path)
    tileset["base_url"] = f"{TILE_BASE}/wrong-slug"
    with pytest.raises(ValueError, match="base_url must end with '/overture-place'"):
        build_app(tileset)


# ---------------------------------------------------------------------------
# org.atgeo.describeService
# ---------------------------------------------------------------------------

def test_describe_service_endpoint_returns_did_and_tilesets(tmp_path):
    """GET /xrpc/org.atgeo.describeService returns 200, cached for 1 hour,
    with the did and the served tileset."""
    app = build_app(serve_run(tmp_path))
    with app.test_client() as client:
        resp = client.get("/xrpc/org.atgeo.describeService")

    assert resp.status_code == 200
    assert resp.headers.get("Cache-Control") == "public, max-age=3600"
    assert resp.get_json() == {
        "did": "did:web:places.atgeo.org",
        "tilesets": [{
            "slug": "overture-place",
            "collections": [{"dataset": OVERTURE, "collection": "place"}],
        }],
    }


def test_describe_service_lexicon_route_lacks_cache_control(client):
    """GET /<nsid> (the lexicon route) for org.atgeo.describeService must
    not carry the XRPC cache header -- same guard as
    test_gettiles_lexicon_route_lacks_cache_control, for the new NSID:
    request.endpoint == 'xrpc-endpoint' is what keeps the header off this
    route, not view_args['nsid'] alone."""
    resp = client.get("/org.atgeo.describeService")
    assert resp.status_code == 200
    assert resp.headers.get("Cache-Control") != "public, max-age=3600"


@pytest.mark.parametrize("nsid", [
    "org.atgeo.describeService", "org.atgeo.describeTileset",
    "org.atgeo.getPlace", "org.atgeo.getTiles", "org.atgeo.defs",
])
def test_lexicons_served_at_nsid_route(client, nsid):
    """Each lexicon document is served at GET /<nsid>, like every other
    lexicon (see test_lexicon_known_nsid)."""
    resp = client.get(f"/{nsid}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["id"] == nsid
    assert data["lexicon"] == 1


@pytest.mark.parametrize("nsid", ["org.atgeo.collection", "org.atgeo.getCoverage"])
def test_removed_lexicons_not_served(client, nsid):
    assert client.get(f"/{nsid}").status_code == 404


# ---------------------------------------------------------------------------
# CORS: every route is readable from pages on other origins
# ---------------------------------------------------------------------------

def test_gettiles_allows_cross_origin_reads(tile_client):
    resp = tile_client.get("/xrpc/org.atgeo.getTiles", query_string={
        "dataset": OVERTURE, "bbox": WORLD,
    })
    assert resp.status_code == 200
    assert resp.headers.get("Access-Control-Allow-Origin") == "*"


def test_describe_service_allows_cross_origin_reads(client):
    resp = client.get("/xrpc/org.atgeo.describeService")
    assert resp.status_code == 200
    assert resp.headers.get("Access-Control-Allow-Origin") == "*"


def test_tile_response_has_exactly_one_cors_header(tile_client):
    resp = tile_client.get(f"/tiles/overture-place/{STAMP}/{QK}/{QK}.json.gz")
    assert resp.headers.getlist("Access-Control-Allow-Origin") == ["*"]


# ---------------------------------------------------------------------------
# JSON errors for everything outside the XRPC views
# ---------------------------------------------------------------------------

def _assert_json_error(resp, status, error):
    assert resp.status_code == status
    assert resp.mimetype == "application/json"
    body = resp.get_json()
    assert body["error"] == error
    assert isinstance(body["message"], str) and body["message"]
    assert set(body) == {"error", "message"}
    assert resp.headers["Access-Control-Allow-Origin"] == "*"
    assert resp.headers["X-Content-Type-Options"] == "nosniff"


@pytest.mark.parametrize("path", [
    "/", "/a/b", "/a/b/c/d", "/tiles/overture-place/", "/tiles/overture-place",
    "/tiles/", "/xrpc/", "/.well-known/nope",
])
def test_unmatched_routes_are_json_not_found(client, path):
    _assert_json_error(client.get(path), 404, "NotFound")


@pytest.mark.parametrize("path,allowed", [
    ("/health", {"GET", "HEAD", "OPTIONS"}),
    ("/tilesets/x", {"GET", "HEAD", "OPTIONS"}),
    ("/tiles/overture-place/x.json.gz", {"GET", "HEAD", "OPTIONS"}),
])
def test_wrong_method_is_json_405_with_allow(client, path, allowed):
    resp = client.post(path)
    _assert_json_error(resp, 405, "MethodNotAllowed")
    assert {m.strip() for m in resp.headers["Allow"].split(",")} == allowed


def test_tile_with_wrong_suffix_is_tile_not_found(client):
    resp = client.get("/tiles/overture-place/foo.txt")
    _assert_json_error(resp, 404, "TileNotFound")
    assert resp.get_json()["message"] == "No such tile: overture-place/foo.txt"


def test_tile_traversal_miss_is_json_tile_not_found(client):
    resp = client.get(f"/tiles/overture-place/{STAMP}/..%2f..%2f..%2fetc%2fpasswd.json.gz")
    _assert_json_error(resp, 404, "TileNotFound")


def test_nosniff_on_success_responses(client):
    assert client.get("/health").headers["X-Content-Type-Options"] == "nosniff"


def test_xrpc_unknown_method_keeps_lexrpc_501_body(client):
    resp = client.get("/xrpc/org.atgeo.nope")
    assert resp.status_code == 501
    body = resp.get_json()
    assert body["error"] == "MethodNotImplemented"
    assert "org.atgeo.nope" in body["message"]
    assert resp.headers["Access-Control-Allow-Origin"] == "*"


def test_xrpc_missing_param_keeps_lexrpc_400_body(client):
    resp = client.get("/xrpc/org.atgeo.getPlace")
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "InvalidRequest"
    resp = client.get("/xrpc/not_an_nsid")
    assert resp.status_code == 400
    assert resp.get_json() == {"error": "InvalidRequest", "message": "not_an_nsid is not a valid NSID"}


def test_unhandled_exception_is_json_500_without_details(client, caplog):
    app = client.application
    app.config["PROPAGATE_EXCEPTIONS"] = False
    app.testing = False

    @app.route("/boom")
    def boom():
        raise RuntimeError("secret internal detail")

    with caplog.at_level("ERROR"):
        resp = client.get("/boom")
    _assert_json_error(resp, 500, "InternalServerError")
    assert "secret internal detail" not in resp.get_data(as_text=True)
    assert "secret internal detail" in caplog.text  # traceback still logged


# ---------------------------------------------------------------------------
# Errors lexrpc lets through on /xrpc/, NSID gating, redirects, 5xx, CORS
# ---------------------------------------------------------------------------

def test_xrpc_non_json_body_is_415(client):
    resp = client.post("/xrpc/org.atgeo.getPlace", data="x", content_type="text/plain")
    _assert_json_error(resp, 415, "UnsupportedMediaType")


def test_xrpc_malformed_json_body_is_invalid_request_400(client):
    resp = client.post("/xrpc/org.atgeo.getPlace", data="{nope",
                       content_type="application/json")
    _assert_json_error(resp, 400, "InvalidRequest")


def test_xrpc_wrong_method_is_405_with_allow(client):
    resp = client.put("/xrpc/org.atgeo.getPlace")
    _assert_json_error(resp, 405, "MethodNotAllowed")
    assert "GET" in resp.headers["Allow"]


def test_non_xrpc_400_keeps_its_own_name(client):
    app = client.application
    from werkzeug.exceptions import BadRequest

    @app.route("/bad")
    def bad():
        raise BadRequest()

    _assert_json_error(client.get("/bad"), 400, "BadRequest")


@pytest.mark.parametrize("path", [
    "/favicon.ico", "/robots.txt", "/xrpc", "/tiles", "/tilesets", "/health/",
])
def test_non_nsid_single_segments_are_plain_not_found(client, path):
    _assert_json_error(client.get(path), 404, "NotFound")


def test_nsid_shaped_unknown_is_lexicon_not_found(client):
    _assert_json_error(client.get("/org.atgeo.nope"), 404, "LexiconNotFound")


def test_redirects_still_redirect(client):
    resp = client.get("/tilesets//x")
    assert resp.status_code == 308
    assert resp.headers["Location"].endswith("/tilesets/x")


def test_explicit_5xx_abort_is_generic_json_and_logged(client, caplog):
    from flask import abort

    @client.application.route("/unavailable")
    def unavailable():
        abort(503, description="database exploded at /var/secret")

    with caplog.at_level("ERROR"):
        resp = client.get("/unavailable")
    _assert_json_error(resp, 503, "ServiceUnavailable")
    assert "secret" not in resp.get_data(as_text=True)
    assert "database exploded at /var/secret" in caplog.text


@pytest.mark.parametrize("path", [
    "/xrpc/org.atgeo.getPlace", f"/{OVERTURE}/place/ov001", "/tilesets/overture-place",
])
def test_options_preflight_has_cors(client, path):
    resp = client.options(path, headers={
        "Origin": "https://example.com", "Access-Control-Request-Method": "GET",
    })
    assert resp.status_code < 400
    assert resp.headers["Access-Control-Allow-Origin"] == "*"


def test_head_on_404_is_json_with_empty_body(client):
    resp = client.head("/tilesets/nope")
    assert resp.status_code == 404
    assert resp.mimetype == "application/json"
    assert resp.get_data() == b""
    assert resp.headers["Access-Control-Allow-Origin"] == "*"
