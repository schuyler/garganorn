"""Export of the place-identity shape, end to end.

Runs run_pipeline for all three sources over the conftest fixtures and
checks the tiles, tileset.json and manifest.duckdb it writes, then serves
the three runs and fetches one place of each collection by origin.
"""
import gzip
import json
import os
import re
import time
from unittest.mock import patch

import duckdb
import pytest

from garganorn import database
from garganorn.__main__ import create_app
from garganorn.levels import LEVEL_VOCAB
from garganorn.quadtree import run_pipeline
from garganorn.stages import stage_export
from tests.lexicon_helpers import goat_validate, validate_def

BBOX = (-122.55, 37.60, -122.30, 37.85)
RELEASES = {"overturemaps.org": "2026-09-17.0", "openstreetmap.org": "2026-10-01T00:00:00Z"}
SLUGS = {"overture_division": "overture-division", "overture_place": "overture-place", "osm": "osm"}
ATTRIBUTION_KEYS = {"dataset", "collection", "release", "source", "licenses", "attribution"}
PUBLIC_BASE = "http://localhost:8000"
OSM_COLLECTIONS = {"n": "node", "w": "way", "r": "relation"}
_RECORDS_RE = re.compile(rb'"records"\s*:\s*\[')


def _tiles(run_dir):
    """{quadkey: raw decompressed bytes} for every tile file under run_dir."""
    tiles = {}
    for sub in sorted(os.listdir(run_dir)):
        sub_dir = os.path.join(run_dir, sub)
        if not os.path.isdir(sub_dir):
            continue
        for name in sorted(os.listdir(sub_dir)):
            assert name.endswith(".json.gz"), name
            with gzip.open(os.path.join(sub_dir, name)) as f:
                tiles[name[: -len(".json.gz")]] = f.read()
    return tiles


def _split_tile(raw):
    """(header bytes before "records", [raw bytes of each record])."""
    m = _RECORDS_RE.search(raw)
    text = raw.decode("utf-8")
    pos = len(raw[: m.end()].decode("utf-8"))
    decoder = json.JSONDecoder()
    records = []
    while True:
        while text[pos].isspace():
            pos += 1
        if text[pos] == "]":
            break
        _, end = decoder.raw_decode(text, pos)
        records.append(text[pos:end].encode("utf-8"))
        pos = end
        while text[pos].isspace():
            pos += 1
        if text[pos] == ",":
            pos += 1
    return raw[: m.start()], records


def _manifest_keys(run_dir):
    con = duckdb.connect(os.path.join(run_dir, "manifest.duckdb"), read_only=True)
    try:
        return {r[0] for r in con.execute("SELECT key FROM record_tiles").fetchall()}
    finally:
        con.close()


def _walk(node):
    yield node
    if isinstance(node, dict):
        for v in node.values():
            yield from _walk(v)
    elif isinstance(node, list):
        for v in node:
            yield from _walk(v)


def _origin_key(value):
    return value["origin"][len("urn:atgeo:"):]


def _expected_importance(source, places_pq, dataset):
    """{manifest key: importance} for every place in places.parquet."""
    pk = "rkey" if source == "osm" else "id"
    rows = duckdb.sql(f"SELECT {pk}, importance FROM read_parquet('{places_pq}')").fetchall()
    out = {}
    for pid, importance in rows:
        if source == "osm":
            collection, pid = OSM_COLLECTIONS[pid[0]], pid[1:]
        else:
            collection = "place" if source == "overture_place" else "division"
        out[f"{dataset}:{collection}:{pid}"] = importance
    return out


@pytest.fixture(scope="module")
def built(division_parquet, overture_parquet, density_parquet, osm_parquet, tmp_path_factory):
    out = str(tmp_path_factory.mktemp("identity_export"))
    kwargs = dict(memory_limit="4GB", max_per_tile=100, releases=RELEASES)
    run_pipeline("overture_division", division_parquet, BBOX, out, **kwargs)
    boundaries = os.path.join(out, "overture_division", "boundaries.duckdb")
    run_pipeline("overture_place", overture_parquet, BBOX, out,
                 boundaries_db=boundaries, density_parquet=density_parquet, **kwargs)
    run_pipeline("osm", (osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"]), BBOX, out,
                 boundaries_db=boundaries, density_parquet=density_parquet, **kwargs)

    runs = {}
    for source in SLUGS:
        source_dir = os.path.join(out, source)
        run_dir = os.path.join(source_dir, "tiles", "current")
        with open(os.path.join(run_dir, "tileset.json")) as f:
            tileset = json.load(f)
        raw_tiles = _tiles(run_dir)
        runs[source] = {
            "source_dir": source_dir,
            "run_dir": run_dir,
            "places": os.path.join(source_dir, "places.parquet"),
            "combined": os.path.join(source_dir, "tile_assignments_combined.parquet"),
            "tileset": tileset,
            "dataset": tileset["collections"][0]["dataset"],
            "raw": raw_tiles,
            "tiles": {qk: json.loads(raw) for qk, raw in raw_tiles.items()},
            "keys": _manifest_keys(run_dir),
        }
    return runs


def _records(run):
    return [(qk, rec) for qk, tile in run["tiles"].items() for rec in tile["records"]]


def _values_by_origin(run):
    return {rec["value"]["origin"]: rec["value"] for _, rec in _records(run)}


def _hand_built_osm(tmp_path, rkeys, name):
    """places.parquet + tile_assignments.parquet for osm, one tile for all."""
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE places (
            rkey             VARCHAR PRIMARY KEY,
            name             VARCHAR,
            importance       INTEGER,
            latitude         DOUBLE,
            longitude        DOUBLE,
            variants         STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT [],
            primary_category VARCHAR,
            tags             MAP(VARCHAR, VARCHAR)
        )
    """)
    conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
    for rkey in rkeys:
        conn.execute(
            "INSERT INTO places VALUES (?, ?, 50, 37.7749, -122.4194, [], NULL, "
            "map([]::VARCHAR[], []::VARCHAR[]))",
            [rkey, f"Place {rkey}"],
        )
        conn.execute("INSERT INTO tile_assignments VALUES (?, '0123')", [rkey])
    places_pq = str(tmp_path / f"{name}_places.parquet")
    ta_pq = str(tmp_path / f"{name}_ta.parquet")
    conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
    conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
    conn.close()
    return places_pq, ta_pq


def _empty_containment_dir(tmp_path, name):
    cd = tmp_path / name
    cd.mkdir()
    (cd / "_meta.json").write_text(json.dumps({"empty": True, "params": {}, "inputs": []}))
    return str(cd)


def _export_hand_built_osm(tmp_path, rkeys, name):
    places_pq, ta_pq = _hand_built_osm(tmp_path, rkeys, name)
    containment = _empty_containment_dir(tmp_path, f"{name}_containment")
    tiles_root = str(tmp_path / f"{name}_tiles")
    os.makedirs(tiles_root)
    return stage_export(
        "osm", places_pq, ta_pq, containment, tiles_root, time.monotonic(),
        memory_limit="4GB", releases={"openstreetmap.org": RELEASES["openstreetmap.org"]},
    )


@pytest.fixture(scope="module")
def osm_node_and_way(tmp_path_factory):
    return _export_hand_built_osm(tmp_path_factory.mktemp("osm_nw"), ["n5", "w5"], "nw")


# ---------------------------------------------------------------------------
# T1-R1
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("source", SLUGS)
class TestValidValues:
    def test_every_value_passes_goat(self, built, source):
        values = _values_by_origin(built[source])
        assert values, f"{source}: no records in any tile"
        for value in values.values():
            goat_validate(value)

    def test_every_tile_validates_as_tile_payload(self, built, source):
        for tile in built[source]["tiles"].values():
            validate_def("org.atgeo.tilePayload", tile)

    def test_no_float_in_any_value(self, built, source):
        for _, rec in _records(built[source]):
            floats = [n for n in _walk(rec["value"]) if isinstance(n, float)]
            assert not floats, f"{rec['value']['origin']}: floats {floats}"


def test_overture_sources_confidence_is_a_string(built):
    sources = [
        s
        for value in _values_by_origin(built["overture_place"]).values()
        for s in value["attributes"].get("sources") or []
    ]
    assert sources, "fixture must export a place with non-null sources"
    for s in sources:
        assert isinstance(s["confidence"], str), s
        assert float(s["confidence"]) == pytest.approx(0.95)


# ---------------------------------------------------------------------------
# T1-R2, T1-R3
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("source", SLUGS)
class TestIdentityAndImportance:
    def test_origin_is_urn_of_dataset_collection_id(self, built, source):
        run = built[source]
        expected = _expected_importance(source, run["places"], run["dataset"])
        origins = {v["origin"] for v in _values_by_origin(run).values()}
        assert origins == {"urn:atgeo:" + key for key in expected}

    def test_origin_matches_manifest_key(self, built, source):
        run = built[source]
        assert {_origin_key(v) for v in _values_by_origin(run).values()} == run["keys"]

    def test_value_has_no_importance_rkey_or_uri(self, built, source):
        for _, rec in _records(built[source]):
            assert not {"importance", "rkey", "uri"} & rec["value"].keys(), rec["value"]["origin"]

    def test_record_is_value_and_importance(self, built, source):
        run = built[source]
        expected = _expected_importance(source, run["places"], run["dataset"])
        for _, rec in _records(run):
            assert set(rec) == {"value", "importance"}
            assert rec["importance"] == expected[_origin_key(rec["value"])]


def test_osm_node_and_way_with_one_id_have_distinct_origins(osm_node_and_way):
    origins = {
        rec["value"]["origin"]
        for tile in _tiles(osm_node_and_way).values()
        for rec in json.loads(tile)["records"]
    }
    assert origins == {"urn:atgeo:openstreetmap.org:node:5", "urn:atgeo:openstreetmap.org:way:5"}
    assert _manifest_keys(osm_node_and_way) == {
        "openstreetmap.org:node:5", "openstreetmap.org:way:5",
    }


# ---------------------------------------------------------------------------
# T1-R5 (export side)
# ---------------------------------------------------------------------------

def _record_count(run_dir, dataset, collection):
    prefix = f"{dataset}:{collection}:"
    return len({k for k in _manifest_keys(run_dir) if k.startswith(prefix)})


@pytest.mark.parametrize("source", SLUGS)
class TestTilesetDescriptor:
    def test_validates_and_carries_the_slug(self, built, source):
        ts = built[source]["tileset"]
        validate_def("org.atgeo.defs#tileset", ts)
        assert ts["slug"] == SLUGS[source]

    def test_every_collection_states_terms_release_and_statistics(self, built, source):
        run = built[source]
        ts = run["tileset"]
        assert {"publishedAt", "extent", "references"} <= ts.keys()
        for c in ts["collections"]:
            terms = database.COLLECTION_TERMS[(c["dataset"], c["collection"])]
            assert ATTRIBUTION_KEYS | {"recordCount", "locationFormats"} <= c.keys()
            assert c["release"] == RELEASES[c["dataset"]]
            assert {k: c[k] for k in terms} == terms
            assert c["recordCount"] == _record_count(run["run_dir"], c["dataset"], c["collection"])
            assert c["recordCount"] > 0

    def test_every_collection_states_its_vocabularies(self, built, source):
        """attributes: the keys its records' values carry. categories: counts
        of the dataset's primary classification (OSM primary_category,
        division subtype) within the collection. Overture place categories
        come from the IDF artifact, which this build does not pass."""
        run = built[source]
        attribute_keys = {}
        for _, rec in _records(run):
            collection = _origin_key(rec["value"]).split(":")[1]
            attribute_keys.setdefault(collection, set()).update(rec["value"].get("attributes", {}))
        categories = {}
        if source == "osm":
            rows = duckdb.sql(
                f"SELECT left(rkey, 1), primary_category, count(*) FROM read_parquet('{run['places']}') "
                f"WHERE primary_category IS NOT NULL GROUP BY ALL"
            ).fetchall()
            for letter, value, n in rows:
                categories.setdefault(OSM_COLLECTIONS[letter], {})[value] = n
        elif source == "overture_division":
            rows = duckdb.sql(
                f"SELECT subtype, count(*) FROM read_parquet('{run['places']}') GROUP BY ALL"
            ).fetchall()
            categories["division"] = dict(rows)

        for c in run["tileset"]["collections"]:
            expected_attributes = sorted(attribute_keys.get(c["collection"], ()))
            assert c.get("attributes") == (expected_attributes or None), c["collection"]
            if source == "overture_place":
                continue
            counts = [cat["count"] for cat in c["categories"]]
            assert {cat["value"]: cat["count"] for cat in c["categories"]} == categories[c["collection"]]
            assert counts == sorted(counts, reverse=True)

    def test_every_tile_header_matches_the_tileset(self, built, source):
        run = built[source]
        ts = run["tileset"]
        expected = [
            {k: c[k] for k in ATTRIBUTION_KEYS} for c in ts["collections"] + ts["references"]
        ]
        for tile in run["tiles"].values():
            assert set(tile) == {"publishedAt", "attribution", "records"}
            assert tile["publishedAt"] == ts["publishedAt"]
            assert tile["attribution"] == expected

    def test_header_is_byte_identical_across_tiles(self, built, source):
        headers = {_split_tile(raw)[0] for raw in built[source]["raw"].values()}
        assert len(built[source]["raw"]) > 1
        assert len(headers) == 1

    def test_run_dir_has_no_collection_json_or_metadata_table(self, built, source):
        run_dir = built[source]["run_dir"]
        assert not os.path.exists(os.path.join(run_dir, "collection.json"))
        con = duckdb.connect(os.path.join(run_dir, "manifest.duckdb"), read_only=True)
        try:
            tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
            columns = [r[0] for r in con.execute("DESCRIBE record_tiles").fetchall()]
        finally:
            con.close()
        assert tables == {"record_tiles"}
        assert columns == ["key", "tile_qk"]


def test_division_tileset_references_nothing(built):
    ts = built["overture_division"]["tileset"]
    assert [c["collection"] for c in ts["collections"]] == ["division"]
    assert ts["references"] == []


def test_overture_place_tileset_references_divisions(built):
    ts = built["overture_place"]["tileset"]
    assert [c["collection"] for c in ts["collections"]] == ["place"]
    assert [(r["dataset"], r["collection"]) for r in ts["references"]] == [("overturemaps.org", "division")]


def test_osm_tileset_carries_osm_collections_and_references_divisions(built):
    run = built["osm"]
    ts = run["tileset"]
    assert {c["collection"] for c in ts["collections"]} == {"node", "way", "relation"}
    assert {c["dataset"] for c in ts["collections"]} == {"openstreetmap.org"}
    [ref] = ts["references"]
    assert (ref["dataset"], ref["collection"]) == ("overturemaps.org", "division")
    assert ref["release"] == RELEASES["overturemaps.org"]
    assert ref["licenses"] == [database.ODBL, database.CC_BY_4]

    for tile in run["tiles"].values():
        by_pair = {(a["dataset"], a["collection"]): a for a in tile["attribution"]}
        assert len(tile["attribution"]) == 4
        for collection in ("node", "way", "relation"):
            entry = by_pair[("openstreetmap.org", collection)]
            assert entry["attribution"] == "https://www.openstreetmap.org/copyright"
            assert entry["licenses"] == [database.ODBL]
        division = by_pair[("overturemaps.org", "division")]
        assert division["licenses"] == [database.ODBL, database.CC_BY_4]
        assert division["release"] == RELEASES["overturemaps.org"]


def test_osm_without_containment_references_nothing(osm_node_and_way):
    run_dir = osm_node_and_way
    with open(os.path.join(run_dir, "tileset.json")) as f:
        ts = json.load(f)
    validate_def("org.atgeo.defs#tileset", ts)
    assert ts["references"] == []
    for raw in _tiles(run_dir).values():
        tile = json.loads(raw)
        assert {a["collection"] for a in tile["attribution"]} <= {"node", "way", "relation"}


def test_osm_referencing_divisions_needs_the_overture_release(built, tmp_path):
    run = built["osm"]
    containment = os.path.join(run["source_dir"], "containment")
    tiles_root = str(tmp_path / "tiles")
    os.makedirs(tiles_root)
    with pytest.raises(KeyError, match="overturemaps.org"):
        stage_export(
            "osm", run["places"], run["combined"], containment, tiles_root, time.monotonic(),
            memory_limit="4GB", releases={"openstreetmap.org": RELEASES["openstreetmap.org"]},
        )


def test_zero_tile_run_writes_tileset_without_extent(tmp_path):
    run_dir = _export_hand_built_osm(tmp_path, [], "zero")
    assert _tiles(run_dir) == {}
    with open(os.path.join(run_dir, "tileset.json")) as f:
        ts = json.load(f)
    validate_def("org.atgeo.defs#tileset", ts)
    assert "extent" not in ts
    assert ts["collections"]
    assert {c["recordCount"] for c in ts["collections"]} == {0}
    assert not os.path.exists(os.path.join(run_dir, "collection.json"))


# ---------------------------------------------------------------------------
# Byte-identical copies
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("source", SLUGS)
def test_record_copies_are_byte_identical_across_tiles(built, source):
    copies = {}
    for qk, raw in built[source]["raw"].items():
        for rec_bytes in _split_tile(raw)[1]:
            origin = json.loads(rec_bytes)["value"]["origin"]
            copies.setdefault(origin, {})[qk] = rec_bytes
    multi = {o: c for o, c in copies.items() if len(c) > 1}
    assert multi, f"{source}: fixture must put a record in more than one tile"
    for origin, by_tile in multi.items():
        assert len(set(by_tile.values())) == 1, f"{origin} differs across tiles {sorted(by_tile)}"
    if source != "overture_division":
        assert any(
            any(len(qk) < 6 for qk in c) and any(len(qk) >= 6 for qk in c) for c in multi.values()
        ), f"{source}: fixture must put a record in both a summary and a regular tile"


# ---------------------------------------------------------------------------
# T1-R7
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("source", SLUGS)
def test_no_tile_contains_the_public_host(built, source):
    assert built[source]["raw"]
    for qk, raw in built[source]["raw"].items():
        assert b"places.atgeo.org" not in raw, qk


# ---------------------------------------------------------------------------
# T1-R12
# ---------------------------------------------------------------------------

def _within_lists(run):
    return [
        (rec["value"], rec["value"]["relations"]["within"])
        for _, rec in _records(run)
        if rec["value"].get("relations", {}).get("within")
    ]


@pytest.mark.parametrize("source", SLUGS)
class TestContainmentRelations:
    def test_within_items_are_division_urns_present_in_the_division_run(self, built, source):
        division_keys = built["overture_division"]["keys"]
        within = _within_lists(built[source])
        assert within, f"{source}: fixture must yield within relations"
        for value, items in within:
            for item in items:
                assert item["uri"].startswith(database.DIVISION_URN_PREFIX)
                division_id = item["uri"][len(database.DIVISION_URN_PREFIX):]
                assert f"overturemaps.org:division:{division_id}" in division_keys
                assert set(item) <= {"uri", "name", "type"}

    def test_within_is_ordered_broadest_first(self, built, source):
        for value, items in _within_lists(built[source]):
            levels = [LEVEL_VOCAB[item["type"]] for item in items]
            assert levels == sorted(levels), value["origin"]

    def test_containment_types_follow_level_vocab_order(self, built, source):
        run = built[source]
        for c in run["tileset"]["collections"]:
            prefix = f"urn:atgeo:{c['dataset']}:{c['collection']}:"
            seen = {
                item["type"]
                for value, items in _within_lists(run)
                if value["origin"].startswith(prefix)
                for item in items
            }
            assert c.get("containmentTypes", []) == sorted(seen, key=LEVEL_VOCAB.__getitem__)


def test_division_values_have_no_level_attribute(built):
    values = _values_by_origin(built["overture_division"])
    assert values
    for value in values.values():
        assert "level" not in value["attributes"]
        assert "subtype" in value["attributes"]


def test_division_tileset_states_containment_types(built):
    [c] = built["overture_division"]["tileset"]["collections"]
    assert c["containmentTypes"], "fixture divisions are nested, so the descriptor must list types"


def test_containment_meta_records_the_division_urn_prefix(built):
    for source, run in built.items():
        with open(os.path.join(run["source_dir"], "containment", "_meta.json")) as f:
            params = json.load(f)["params"]
        assert params["division_urn_prefix"] == database.DIVISION_URN_PREFIX, source


# ---------------------------------------------------------------------------
# Export -> serve
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client(built):
    tiles_config = {
        "tilesets": [
            {"run_dir": run["run_dir"], "base_url": f"{PUBLIC_BASE}/tiles/{SLUGS[source]}"}
            for source, run in built.items()
        ],
        "max_tiles": 50,
    }
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("localhost", tiles_config, PUBLIC_BASE)
        app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


@pytest.mark.parametrize("source, collection", [
    ("overture_division", "division"),
    ("overture_place", "place"),
    ("osm", "node"),
    ("osm", "way"),
    ("osm", "relation"),
])
def test_get_place_serves_the_exported_record(built, client, source, collection):
    run = built[source]
    prefix = f"urn:atgeo:{run['dataset']}:{collection}:"
    candidates = sorted(
        (rec for _, rec in _records(run) if rec["value"]["origin"].startswith(prefix)),
        key=lambda rec: rec["value"]["origin"],
    )
    assert candidates, f"fixture holds no {collection}"
    rec = candidates[0]

    resp = client.get("/xrpc/org.atgeo.getPlace", query_string={"uri": rec["value"]["origin"]})

    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body["record"] == rec["value"]
    assert body["importance"] == rec["importance"]
    assert body["uri"].startswith(PUBLIC_BASE + "/")
