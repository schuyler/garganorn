"""Server API through the Flask client: getPlace, describeTileset,
describeService and the plain routes. lexrpc validates every output."""
import ast
import json
import pathlib
from urllib.parse import urlsplit

import pytest

from tests.lexicon_helpers import goat_validate
from tests.served_runs import (
    OSM, OVERTURE, PUBLIC_BASE, STAMP, TILE_BASE, build_app, make_run, place,
    point_current, serve_run, tile_url, tileset_config,
)

OSM_COLLECTIONS = [(OSM, "node"), (OSM, "way"), (OSM, "relation")]
PLACES = [
    (OSM, "node", "5", 61),
    (OSM, "way", "5", 62),
    (OVERTURE, "place", "ov-1", 63),
]
WORLD = "-180,-85,180,85"


def _records(*places):
    return [place(d, c, i, name=f"{d} {c} {i}", importance=imp) for d, c, i, imp in places]


@pytest.fixture
def tmp_osm(tmp_path):
    """tiles config entry for an OSM tileset holding node 5 and way 5."""
    return serve_run(tmp_path, "osm", OSM_COLLECTIONS,
                     {"023010": _records(*PLACES[:2])})


@pytest.fixture
def tmp_overture(tmp_path):
    return serve_run(tmp_path, "overture-place", [(OVERTURE, "place")],
                     {"023011": _records(PLACES[2])})


@pytest.fixture
def client(tmp_osm, tmp_overture):
    with build_app(tmp_osm, tmp_overture).test_client() as c:
        yield c


def _get_place(client, uri):
    return client.get("/xrpc/org.atgeo.getPlace", query_string={"uri": uri})


# ---------------------------------------------------------------------------
# T1-R4: getPlace
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dataset,collection,id,importance", PLACES)
def test_get_place_round_trip(client, dataset, collection, id, importance):
    resp = _get_place(client, f"urn:atgeo:{dataset}:{collection}:{id}")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["uri"] == f"{PUBLIC_BASE}/{dataset}/{collection}/{id}"
    assert body["importance"] == importance
    assert body["record"]["origin"] == f"urn:atgeo:{dataset}:{collection}:{id}"
    assert body["record"]["name"] == f"{dataset} {collection} {id}"
    assert "importance" not in body["record"]
    goat_validate(body["record"])


def test_get_place_distinguishes_node_and_way_with_one_id(client):
    node = _get_place(client, f"urn:atgeo:{OSM}:node:5").get_json()
    way = _get_place(client, f"urn:atgeo:{OSM}:way:5").get_json()
    assert node["record"]["origin"] != way["record"]["origin"]
    assert node["importance"] != way["importance"]


@pytest.mark.parametrize("uri", [
    f"urn:atgeo:{OSM}:node:999",           # unknown id
    f"urn:atgeo:{OSM}:relation:5",         # collection carried, id absent
    f"urn:atgeo:{OVERTURE}:division:5",    # collection not served
    "https://example.org/x",               # not an atgeo URN
    "urn:atgeo:x",                         # malformed atgeo URN
])
def test_get_place_unserved_uri_is_place_not_found(client, uri):
    resp = _get_place(client, uri)
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "PlaceNotFound"


def test_get_place_rejects_a_string_that_is_not_a_uri(client):
    resp = _get_place(client, "foo")
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "InvalidRequest"


@pytest.mark.parametrize("dataset,collection,id,importance", PLACES)
def test_place_path_returns_the_getplace_body(client, dataset, collection, id, importance):
    xrpc = _get_place(client, f"urn:atgeo:{dataset}:{collection}:{id}")
    alias = client.get(f"/{dataset}/{collection}/{id}")
    assert alias.status_code == 200
    assert alias.get_json() == xrpc.get_json()


def test_place_path_with_dotless_first_segment_is_404(client):
    assert client.get("/osm/node/5").status_code == 404


def test_place_path_for_unserved_place_is_404(client):
    assert client.get(f"/{OSM}/node/999").status_code == 404


def test_get_place_reads_the_run_resolved_at_startup(tmp_path):
    """Repointing 'current' after startup must not change what getPlace
    answers: the server reads the run it resolved when it started."""
    tiles_root = tmp_path / "osm" / "tiles"
    run1 = make_run(tiles_root, "20260101T000000", "osm", OSM_COLLECTIONS,
                    {"023010": [place(OSM, "node", "5", name="Old")]})
    run2 = make_run(tiles_root, "20260102T000000", "osm", OSM_COLLECTIONS,
                    {"023010": [place(OSM, "node", "5", name="New")]})
    tileset = tileset_config(point_current(tiles_root, run1), "osm")
    app = build_app(tileset)
    point_current(tiles_root, run2)

    with app.test_client() as client:
        body = _get_place(client, f"urn:atgeo:{OSM}:node:5").get_json()
    assert body["record"]["name"] == "Old"


# ---------------------------------------------------------------------------
# T1-R5: describeTileset, /tilesets/<slug>, slug
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("slug", ["osm", "overture-place"])
def test_describe_tileset_equals_alias_equals_stored_descriptor(tmp_path, client, slug):
    stored = json.loads(
        (tmp_path / slug / "tiles" / "current" / "tileset.json").read_text()
    )
    by_xrpc = client.get("/xrpc/org.atgeo.describeTileset", query_string={"slug": slug})
    by_alias = client.get(f"/tilesets/{slug}")
    assert by_xrpc.status_code == 200
    assert by_xrpc.get_json() == stored
    assert by_alias.status_code == 200
    assert by_alias.get_json() == stored


def test_unknown_slug_is_tileset_not_found(client):
    resp = client.get("/xrpc/org.atgeo.describeTileset", query_string={"slug": "nope"})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "TilesetNotFound"
    assert client.get("/tilesets/nope").status_code == 404


def test_tileset_is_served_at_its_descriptor_slug(tmp_path):
    """The slug is read from tileset.json, not from the config or the run path."""
    tileset = serve_run(tmp_path, "my-tiles", [(OVERTURE, "place")],
                        {"023010": []}, subdir="somewhere-else")
    with build_app(tileset).test_client() as client:
        assert client.get(f"/tiles/my-tiles/{STAMP}/023010/023010.json.gz").status_code == 200
        assert client.get("/tilesets/my-tiles").get_json()["slug"] == "my-tiles"
        assert client.get("/tilesets/somewhere-else").status_code == 404


def test_two_tilesets_with_one_slug_fail_startup(tmp_path):
    first = serve_run(tmp_path, "osm", OSM_COLLECTIONS, {"023010": []})
    second = serve_run(tmp_path, "osm", OSM_COLLECTIONS, {"023011": []}, subdir="osm-copy")
    with pytest.raises(ValueError, match="osm"):
        build_app(first, second)


def test_zero_tile_run_is_served(tmp_path):
    """A run with no tiles has a descriptor without extent and is still a
    served tileset."""
    tileset = serve_run(tmp_path, "osm", OSM_COLLECTIONS)
    stored = json.loads((tmp_path / "osm" / "tiles" / "current" / "tileset.json").read_text())
    assert "extent" not in stored
    with build_app(tileset).test_client() as client:
        described = client.get("/xrpc/org.atgeo.describeTileset", query_string={"slug": "osm"})
        service = client.get("/xrpc/org.atgeo.describeService")
        tiles = client.get("/xrpc/org.atgeo.getTiles",
                           query_string={"dataset": OSM, "bbox": WORLD})
    assert described.status_code == 200
    assert described.get_json() == stored
    assert [t["slug"] for t in service.get_json()["tilesets"]] == ["osm"]
    assert tiles.get_json() == {"tiles": []}


# ---------------------------------------------------------------------------
# T1-R6: describeService
# ---------------------------------------------------------------------------

def _strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from _strings(v)
    elif isinstance(value, list):
        for v in value:
            yield from _strings(v)


def test_describe_service_lists_every_tileset_in_config_order(tmp_osm, tmp_overture):
    for configured in ((tmp_osm, tmp_overture), (tmp_overture, tmp_osm)):
        with build_app(*configured).test_client() as client:
            body = client.get("/xrpc/org.atgeo.describeService").get_json()
        by_slug = {
            "osm": {"slug": "osm", "collections": [
                {"dataset": OSM, "collection": c} for c in ("node", "way", "relation")]},
            "overture-place": {"slug": "overture-place", "collections": [
                {"dataset": OVERTURE, "collection": "place"}]},
        }
        assert body == {
            "did": "did:web:places.atgeo.org",
            "tilesets": [by_slug[t["base_url"].rsplit("/", 1)[1]] for t in configured],
        }


def test_describe_service_contains_no_url(client):
    body = client.get("/xrpc/org.atgeo.describeService").get_json()
    urls = [s for s in _strings(body) if urlsplit(s).scheme in ("http", "https")]
    assert urls == []


# ---------------------------------------------------------------------------
# T1-R7: nothing is hardcoded to a host
# ---------------------------------------------------------------------------

def test_get_tiles_urls_start_with_the_configured_base_url(tmp_osm):
    with build_app(tmp_osm).test_client() as client:
        tiles = client.get("/xrpc/org.atgeo.getTiles",
                           query_string={"dataset": OSM, "bbox": WORLD}).get_json()["tiles"]
    assert tiles == [tile_url("osm", "023010")]
    assert all(url.startswith(f"{TILE_BASE}/osm/") for url in tiles)


def test_get_place_uri_starts_with_public_base_and_dereferences(client):
    body = _get_place(client, f"urn:atgeo:{OSM}:way:5").get_json()
    assert body["uri"].startswith(PUBLIC_BASE + "/")
    resp = client.get(body["uri"][len(PUBLIC_BASE):])
    assert resp.status_code == 200
    assert resp.get_json() == body


# ---------------------------------------------------------------------------
# T1-R10: no test calls a handler directly
# ---------------------------------------------------------------------------

HANDLERS = {"get_tiles", "get_place", "describe_tileset", "describe_service"}


def test_no_test_calls_a_handler_directly():
    offenders = []
    for path in sorted(pathlib.Path(__file__).parent.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr in HANDLERS):
                offenders.append(f"{path.name}:{node.lineno}")
    assert offenders == []
