"""Integration tests for the full pipeline → tile → server chain.

Unlike unit tests in test_quadtree_*.py, these tests call run_pipeline() against
real parquet data and assert on the actual files and database written to disk.

Chains covered:
  run_pipeline() → TileManifest → Server.get_coverage → tile files on disk
  run_pipeline() → TileBackedCollection → Server.get_record
  run_pipeline() → empty bbox → manifest with 0 rows → Server.get_coverage returns []
"""
import gzip
import json
import logging

import duckdb
import pytest
from lexrpc.base import XrpcError

from garganorn.quadtree import SOURCES, TileManifest, run_pipeline
from garganorn.tile_reader import TileBackedCollection
from garganorn.server import Server
from tests.quadtree_helpers import FSQ_ROWS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FSQ_COLLECTION = "org.atgeo.places.foursquare"
SF_BBOX_STR = "-122.55,37.60,-122.30,37.85"  # 2 decimal places, passes precision check
BASE_URL = "https://tiles.test.example.com"
REPO = "places.atgeo.org"
# rkeys expected to survive FSQ import filtering.
# Dynamically derived from quadtree_helpers.py::FSQ_ROWS — rows with expected_in_result=True.
EXPECTED_RKEYS = {row[0] for row in FSQ_ROWS if row[-1]}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _build_server(pipeline_dir, max_coverage_tiles=50):
    """Build a Server with TileManifest and TileBackedCollection from pipeline output."""
    manifest_path = str(pipeline_dir / "manifest.duckdb")
    manifest = TileManifest(manifest_path, BASE_URL)
    collection = TileBackedCollection(
        collection=FSQ_COLLECTION,
        manifest_db_path=manifest_path,
        tiles_dir=str(pipeline_dir),
        attribution=SOURCES["foursquare"].attribution,
    )
    return Server(
        REPO, dbs=[], logger=logging.getLogger("test"),
        tile_manifests={FSQ_COLLECTION: manifest},
        tile_collections={FSQ_COLLECTION: collection},
        max_coverage_tiles=max_coverage_tiles,
    )


def _collect_tile_records(pipeline_dir):
    """Read all wrapped records ({uri, cid, value}) from all tile .json.gz
    files. Returns list of wrapper dicts (docs/pipeline-implementation-decisions.md,
    "OQ-P2-1 — record envelope adoption"). Use _collect_tile_values()
    for the unwrapped value dicts."""
    records = []
    for gz_path in pipeline_dir.rglob("*.json.gz"):
        with gzip.open(gz_path, "rt") as f:
            tile = json.load(f)
        records.extend(tile["records"])
    return records


def _collect_tile_values(pipeline_dir):
    """Read all record `value` dicts (unwrapped) from all tile .json.gz files."""
    return [rec["value"] for rec in _collect_tile_records(pipeline_dir)]


def _make_single_place_parquet(tmp_path):
    """Create a parquet with exactly one FSQ place. Returns glob path.

    Follows the same schema as conftest.py's fsq_parquet fixture (tmp_fsq table).
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    parquet_path = tmp_path / "solo.parquet"
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE tmp_fsq (
            fsq_place_id VARCHAR, name VARCHAR, latitude DOUBLE, longitude DOUBLE,
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            geom VARCHAR, date_refreshed DATE, date_closed DATE, date_created DATE,
            address VARCHAR, locality VARCHAR, region VARCHAR, postcode VARCHAR,
            country VARCHAR, admin_region VARCHAR, post_town VARCHAR, po_box VARCHAR,
            tel VARCHAR, website VARCHAR, email VARCHAR, facebook_id VARCHAR,
            instagram VARCHAR, twitter VARCHAR,
            fsq_category_ids VARCHAR[], fsq_category_labels VARCHAR[],
            placemaker_url VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO tmp_fsq VALUES (
            'solo001', 'Solo Place', 37.7749, -122.4194,
            {'xmin': -122.4204, 'ymin': 37.7739, 'xmax': -122.4184, 'ymax': 37.7759},
            'POINT(-122.4194 37.7749)', '2023-01-01', NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL,
            ['13065143']::VARCHAR[], NULL::VARCHAR[], NULL
        )
    """)
    conn.execute(f"COPY tmp_fsq TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()
    return str(tmp_path / "*.parquet")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pipeline_output(fsq_parquet, density_parquet, tmp_path_factory):
    """Run FSQ pipeline once; return resolved current/ directory path."""
    output_dir = tmp_path_factory.mktemp("integration")
    run_pipeline("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                 str(output_dir), memory_limit="4GB", max_per_tile=100, density_parquet=density_parquet)
    current = output_dir / "foursquare" / "tiles" / "current"
    assert current.exists()
    return current


@pytest.fixture
def empty_pipeline_output(fsq_parquet, density_parquet, tmp_path):
    """Pipeline with bbox in open ocean — no places survive import.

    Function-scoped. The returned path (current symlink) may not exist if
    run_pipeline writes no records; callers must handle the missing-symlink case.
    """
    output_dir = tmp_path / "empty"
    output_dir.mkdir()
    run_pipeline("foursquare", fsq_parquet, (0.0, 0.0, 0.01, 0.01),
                 str(output_dir), memory_limit="4GB", max_per_tile=100, density_parquet=density_parquet)
    # Empty pipeline may not create current symlink if no records are written.
    # Return the source dir so callers can handle either case.
    current = output_dir / "foursquare" / "tiles" / "current"
    return current


@pytest.fixture
def single_place_output(density_parquet, tmp_path):
    """Pipeline with exactly one place. Function-scoped; uses _make_single_place_parquet."""
    parquet_glob = _make_single_place_parquet(tmp_path / "parquet")
    output_dir = tmp_path / "single"
    output_dir.mkdir()
    run_pipeline("foursquare", parquet_glob, (-122.55, 37.60, -122.30, 37.85),
                 str(output_dir), memory_limit="4GB", max_per_tile=100, density_parquet=density_parquet)
    return output_dir / "foursquare" / "tiles" / "current"


@pytest.fixture
def dense_cluster_output(fsq_parquet, density_parquet, tmp_path):
    """Pipeline with max_per_tile=1 to force quadtree subdivision. Function-scoped."""
    output_dir = tmp_path / "dense"
    output_dir.mkdir()
    run_pipeline("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                 str(output_dir), memory_limit="4GB", max_per_tile=1, density_parquet=density_parquet)
    return output_dir / "foursquare" / "tiles" / "current"


# ---------------------------------------------------------------------------
# TestPipelineToCoverage
# ---------------------------------------------------------------------------

class TestPipelineToCoverage:
    """Chain: pipeline → TileManifest → Server.get_coverage → tile files on disk."""

    def setup_method(self):
        """Clear LRU cache between tests to prevent cross-test tile data bleed."""
        TileBackedCollection._cached_read_tile.cache_clear()

    def test_coverage_returns_tile_urls(self, pipeline_output):
        """get_coverage returns a non-empty list of URL strings for the SF bbox."""
        server = _build_server(pipeline_output)
        result = server.get_coverage({}, collection=FSQ_COLLECTION, bbox=SF_BBOX_STR)
        assert "tiles" in result
        assert len(result["tiles"]) >= 1
        for url in result["tiles"]:
            assert isinstance(url, str)

    def test_tile_urls_resolve_to_files(self, pipeline_output):
        """Each URL returned by get_coverage maps to an existing .json.gz file on disk."""
        server = _build_server(pipeline_output)
        result = server.get_coverage({}, collection=FSQ_COLLECTION, bbox=SF_BBOX_STR)
        prefix = BASE_URL + "/"
        for url in result["tiles"]:
            assert url.startswith(prefix), f"URL does not start with base: {url}"
            relative = url[len(prefix):]
            file_path = pipeline_output / relative
            assert file_path.exists(), f"Tile file not found: {file_path}"
            assert str(file_path).endswith(".json.gz"), f"Unexpected extension: {file_path}"

    def test_tile_files_are_valid_gzipped_json(self, pipeline_output):
        """Each tile file is valid gzip-compressed JSON with the full atgeo v1
        envelope: {atgeo, collection, attribution, generated_at, records}
        (pipeline-implementation-decisions.md "OQ-P2-1 — record envelope
        adoption", §6 item 6)."""
        server = _build_server(pipeline_output)
        result = server.get_coverage({}, collection=FSQ_COLLECTION, bbox=SF_BBOX_STR)
        prefix = BASE_URL + "/"
        for url in result["tiles"]:
            relative = url[len(prefix):]
            file_path = pipeline_output / relative
            with gzip.open(file_path, "rt") as f:
                tile = json.load(f)
            assert set(tile.keys()) == {"atgeo", "collection", "attribution", "generated_at", "records"}, (
                f"Tile {relative} top-level must be exactly {{atgeo, collection, attribution, "
                f"generated_at, records}}; got {list(tile)}"
            )
            assert tile["atgeo"] == 1
            assert isinstance(tile["attribution"], str)
            assert isinstance(tile["collection"], str)
            assert isinstance(tile["generated_at"], str) and tile["generated_at"].endswith("Z")
            assert isinstance(tile["records"], list)

    def test_tile_records_match_expected_schema(self, pipeline_output):
        """Every record in all tile files is {uri, cid, value}-wrapped (per the
        envelope decisions above, §6 item 6), and value conforms to the
        org.atgeo.place schema."""
        records = _collect_tile_records(pipeline_output)
        assert len(records) > 0, "No records found in any tile"
        for record in records:
            assert set(record.keys()) == {"uri", "cid", "value"}, (
                f"Record must be exactly {{uri, cid, value}} (atgeo v1 envelope); "
                f"got keys: {list(record)}"
            )
            assert record["cid"] is None, f"cid must be null (never computed, per the envelope decisions above): {record}"
            assert record["uri"].startswith("https://places.atgeo.org/"), \
                f"uri must be https://{{repo}}/... : {record['uri']}"
            assert record["uri"].rsplit("/", 1)[-1] == record["value"]["rkey"], (
                f"uri's trailing rkey segment must equal value.rkey: {record}"
            )

            value = record["value"]
            assert value["$type"] == "org.atgeo.place", f"Unexpected $type: {value['$type']}"
            assert isinstance(value["rkey"], str) and value["rkey"], "rkey must be non-empty string"
            assert isinstance(value["name"], str), "name must be a string"
            assert isinstance(value["importance"], int), "importance must be int"
            assert isinstance(value["locations"], list) and len(value["locations"]) > 0, \
                "locations must be non-empty list"
            first_loc = value["locations"][0]
            assert first_loc["$type"] == "community.lexicon.location.geo", \
                f"First location $type: {first_loc['$type']}"
            assert isinstance(first_loc["latitude"], str), "latitude must be str"
            assert isinstance(first_loc["longitude"], str), "longitude must be str"
            float(first_loc["latitude"])   # must be parseable as float
            float(first_loc["longitude"])  # must be parseable as float

    def test_manifest_rkeys_match_tiles(self, pipeline_output):
        """manifest.duckdb record_tiles rkeys match tile value.rkeys and equal EXPECTED_RKEYS."""
        manifest_path = str(pipeline_output / "manifest.duckdb")
        con = duckdb.connect(manifest_path, read_only=True)
        manifest_rkeys = {row[0] for row in con.execute("SELECT rkey FROM record_tiles").fetchall()}
        con.close()

        tile_rkeys = set()
        for value in _collect_tile_values(pipeline_output):
            tile_rkeys.add(value["rkey"])

        assert manifest_rkeys == tile_rkeys, \
            f"Manifest rkeys {manifest_rkeys} != tile rkeys {tile_rkeys}"
        assert manifest_rkeys == EXPECTED_RKEYS, \
            f"Expected rkeys {EXPECTED_RKEYS}, got {manifest_rkeys}"

    def test_bbox_too_large_real_manifest(self, pipeline_output):
        """BboxTooLarge from real TileManifest (not a mock) with max_coverage_tiles=0."""
        server = _build_server(pipeline_output, max_coverage_tiles=0)
        with pytest.raises(XrpcError) as exc_info:
            server.get_coverage({}, collection=FSQ_COLLECTION, bbox=SF_BBOX_STR)
        assert exc_info.value.name == "BboxTooLarge", \
            f"Expected XrpcError name 'BboxTooLarge', got '{exc_info.value.name}'"

    def test_manifest_metadata(self, pipeline_output):
        """manifest.duckdb metadata table contains atgeo=1, collection,
        source='foursquare', and an RFC 3339 Z generated_at (per the envelope
        decisions above, §6 items 8/9)."""
        manifest_path = str(pipeline_output / "manifest.duckdb")
        con = duckdb.connect(manifest_path, read_only=True)
        cols = {row[1] for row in con.execute("PRAGMA table_info('metadata')").fetchall()}
        row = con.execute("SELECT atgeo, collection, source, generated_at FROM metadata").fetchone()
        con.close()
        assert {"atgeo", "collection"} <= cols, (
            f"manifest.duckdb metadata must have atgeo/collection columns; got {cols}"
        )
        assert row is not None, "metadata table is empty"
        atgeo, collection, source, generated_at = row
        assert atgeo == 1
        assert collection == FSQ_COLLECTION
        assert source == "foursquare", f"Expected source='foursquare', got '{source}'"
        assert isinstance(generated_at, str) and generated_at.endswith("Z"), (
            f"generated_at must be an RFC 3339 Z string; got {generated_at!r}"
        )

    def test_manifest_json_and_tile_generated_at_agree(self, pipeline_output):
        """§6 item 8 — every tile's generated_at == manifest.json's generated_at
        == manifest.duckdb metadata.generated_at == the run-dir name (RFC 3339 Z)."""
        with open(pipeline_output / "manifest.json") as f:
            manifest_json = json.load(f)
        manifest_generated_at = manifest_json["generated_at"]

        manifest_path = str(pipeline_output / "manifest.duckdb")
        con = duckdb.connect(manifest_path, read_only=True)
        db_generated_at = con.execute("SELECT generated_at FROM metadata").fetchone()[0]
        con.close()

        assert db_generated_at == manifest_generated_at, (
            f"manifest.duckdb generated_at {db_generated_at!r} must equal "
            f"manifest.json generated_at {manifest_generated_at!r}"
        )

        # run-dir name: pipeline_output is a resolved 'current' symlink target,
        # named %Y%m%dT%H%M%S; its RFC 3339 Z rendering must match generated_at.
        import re as _re
        from datetime import datetime as _dt, timezone as _tz
        run_dir_name = pipeline_output.resolve().name
        assert _re.match(r"^\d{8}T\d{6}$", run_dir_name), (
            f"expected a %Y%m%dT%H%M%S run-dir name, got {run_dir_name!r}"
        )
        expected = _dt.strptime(run_dir_name, "%Y%m%dT%H%M%S").replace(tzinfo=_tz.utc) \
                      .strftime("%Y-%m-%dT%H:%M:%SZ")
        assert manifest_generated_at == expected, (
            f"generated_at {manifest_generated_at!r} must derive from the run-dir name "
            f"{run_dir_name!r} (expected {expected!r}), per the envelope decisions above"
        )

        gz_files = list(pipeline_output.rglob("*.json.gz"))
        assert gz_files, "expected at least one tile"
        for gz in gz_files:
            with gzip.open(gz, "rt") as f:
                tile = json.load(f)
            assert tile["generated_at"] == manifest_generated_at, (
                f"tile {gz} generated_at {tile['generated_at']!r} must equal "
                f"manifest generated_at {manifest_generated_at!r}"
            )


# ---------------------------------------------------------------------------
# TestPipelineToGetRecord
# ---------------------------------------------------------------------------

class TestPipelineToGetRecord:
    """Chain: pipeline → TileBackedCollection → Server.get_record."""

    def setup_method(self):
        """Clear LRU cache between tests to prevent cross-test tile data bleed."""
        TileBackedCollection._cached_read_tile.cache_clear()

    def test_get_record_returns_known_place(self, pipeline_output):
        """get_record for fsq001 returns the correct URI and place name."""
        server = _build_server(pipeline_output)
        result = server.get_record({}, repo=REPO, collection=FSQ_COLLECTION, rkey="fsq001")
        assert "fsq001" in result["uri"], f"Expected 'fsq001' in uri: {result['uri']}"
        assert result["value"]["name"] == "Blue Bottle Coffee", \
            f"Expected 'Blue Bottle Coffee', got {result['value']['name']}"

    def test_get_record_value_has_geo_location(self, pipeline_output):
        """get_record value includes at least one community.lexicon.location.geo entry."""
        server = _build_server(pipeline_output)
        result = server.get_record({}, repo=REPO, collection=FSQ_COLLECTION, rkey="fsq001")
        locations = result["value"]["locations"]
        geo_locs = [loc for loc in locations if loc.get("$type") == "community.lexicon.location.geo"]
        assert len(geo_locs) >= 1, "Expected at least one geo location"

    def test_get_record_nonexistent_rkey_raises(self, pipeline_output):
        """get_record raises XrpcError with name 'RecordNotFound' for an unknown rkey."""
        server = _build_server(pipeline_output)
        with pytest.raises(XrpcError) as exc_info:
            server.get_record({}, repo=REPO, collection=FSQ_COLLECTION, rkey="nonexistent")
        assert exc_info.value.name == "RecordNotFound", \
            f"Expected XrpcError name 'RecordNotFound', got '{exc_info.value.name}'"

    def test_all_expected_rkeys_retrievable(self, pipeline_output):
        """Every rkey in EXPECTED_RKEYS can be fetched via get_record."""
        server = _build_server(pipeline_output)
        for rkey in EXPECTED_RKEYS:
            result = server.get_record({}, repo=REPO, collection=FSQ_COLLECTION, rkey=rkey)
            assert result["value"]["rkey"] == rkey, \
                f"Expected rkey '{rkey}', got '{result['value']['rkey']}'"


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Boundary conditions: empty bbox, single place, and forced quadtree subdivision."""

    def setup_method(self):
        """Clear LRU cache between tests to prevent cross-test tile data bleed."""
        TileBackedCollection._cached_read_tile.cache_clear()

    def test_empty_bbox_no_tiles(self, empty_pipeline_output):
        """Pipeline over open ocean writes manifest with 0 rows and no .json.gz files."""
        # The empty pipeline may or may not create the symlink depending on whether
        # any records survived. Check manifest.duckdb if directory exists.
        if not empty_pipeline_output.exists():
            # No current symlink — locate the timestamped dir written by the pipeline
            import re
            source_dir = empty_pipeline_output.parent
            ts_re = re.compile(r"^\d{8}T\d{6}$")
            ts_dirs = [d for d in source_dir.iterdir() if ts_re.match(d.name) and d.is_dir()]
            assert ts_dirs, "Pipeline produced no output directory — run_pipeline may have failed"
            pipeline_dir = ts_dirs[0]
        else:
            pipeline_dir = empty_pipeline_output

        manifest_path = pipeline_dir / "manifest.duckdb"
        assert manifest_path.exists(), "manifest.duckdb should always be written"

        con = duckdb.connect(str(manifest_path), read_only=True)
        count = con.execute("SELECT COUNT(*) FROM record_tiles").fetchone()[0]
        con.close()
        assert count == 0, f"Expected 0 record_tiles rows, got {count}"

        manifest = TileManifest(str(manifest_path), BASE_URL)
        tiles = manifest.get_tiles_for_bbox(-122.55, 37.60, -122.30, 37.85)
        assert tiles == [], f"Expected empty tile list, got {tiles}"

        server = Server(
            REPO, dbs=[], logger=logging.getLogger("test"),
            tile_manifests={FSQ_COLLECTION: manifest},
            tile_collections={},
            max_coverage_tiles=50,
        )
        result = server.get_coverage({}, collection=FSQ_COLLECTION, bbox=SF_BBOX_STR)
        assert result == {"tiles": []}, f"Expected {{'tiles': []}}, got {result}"

        gz_files = list(pipeline_dir.rglob("*.json.gz"))
        assert gz_files == [], f"Expected no .json.gz files, found {gz_files}"

    def test_single_place_one_zoom6_tile(self, single_place_output):
        """A single place produces exactly one tile file with a zoom-6 quadkey."""
        assert single_place_output.exists(), "current symlink must exist"
        gz_files = list(single_place_output.rglob("*.json.gz"))
        assert len(gz_files) == 1, f"Expected exactly 1 .json.gz file, got {len(gz_files)}"

        # Verify the quadkey in manifest.duckdb has length 6
        manifest_path = str(single_place_output / "manifest.duckdb")
        con = duckdb.connect(manifest_path, read_only=True)
        rows = con.execute("SELECT DISTINCT tile_qk FROM record_tiles").fetchall()
        con.close()
        assert len(rows) == 1, f"Expected 1 distinct quadkey, got {len(rows)}"
        qk = rows[0][0]
        assert len(qk) == 6, f"Expected zoom-6 quadkey (len 6), got '{qk}' (len {len(qk)})"

        # Tile contains exactly 1 record
        with gzip.open(gz_files[0], "rt") as f:
            tile = json.load(f)
        assert len(tile["records"]) == 1, \
            f"Expected 1 record in tile, got {len(tile['records'])}"

    def test_dense_cluster_subdivides(self, dense_cluster_output):
        """max_per_tile=1 forces subdivision: multiple tiles, at least one quadkey longer than 6."""
        assert dense_cluster_output.exists(), "current symlink must exist"
        gz_files = list(dense_cluster_output.rglob("*.json.gz"))
        assert len(gz_files) > 1, \
            f"Expected more than 1 tile with max_per_tile=1, got {len(gz_files)}"

        # At least one quadkey should be longer than 6 (subdivided past zoom 6)
        manifest_path = str(dense_cluster_output / "manifest.duckdb")
        con = duckdb.connect(manifest_path, read_only=True)
        rows = con.execute("SELECT DISTINCT tile_qk FROM record_tiles").fetchall()
        con.close()
        quadkeys = [row[0] for row in rows]
        assert any(len(qk) > 6 for qk in quadkeys), \
            f"Expected at least one quadkey longer than 6, got: {quadkeys}"

        # Each tile has <= 1 record (honoring max_per_tile)
        for gz_path in gz_files:
            with gzip.open(gz_path, "rt") as f:
                tile = json.load(f)
            assert len(tile["records"]) <= 1, \
                f"Tile {gz_path.name} has {len(tile['records'])} records; expected <= 1"


# ---------------------------------------------------------------------------
# TestExportWorkersParity
# ---------------------------------------------------------------------------

class TestExportWorkersParity:
    """Verify that export_workers=1 and export_workers=4 produce identical output."""

    def setup_method(self):
        """Clear LRU cache between tests to prevent cross-test tile data bleed."""
        TileBackedCollection._cached_read_tile.cache_clear()

    def test_workers_produce_identical_output(self, fsq_parquet, density_parquet, tmp_path):
        """run_pipeline with export_workers=1 and export_workers=4 produce the same tiles."""
        bbox = (-122.55, 37.60, -122.30, 37.85)

        out1 = tmp_path / "workers1"
        out1.mkdir()
        run_pipeline("foursquare", fsq_parquet, bbox, str(out1),
                     memory_limit="4GB", max_per_tile=100, export_workers=1, density_parquet=density_parquet)
        current1 = out1 / "foursquare" / "tiles" / "current"
        assert current1.exists(), "current symlink must exist for export_workers=1 run"

        out4 = tmp_path / "workers4"
        out4.mkdir()
        run_pipeline("foursquare", fsq_parquet, bbox, str(out4),
                     memory_limit="4GB", max_per_tile=100, export_workers=4, density_parquet=density_parquet)
        current4 = out4 / "foursquare" / "tiles" / "current"
        assert current4.exists(), "current symlink must exist for export_workers=4 run"

        # Collect relative path → content for each run
        def _tile_contents(pipeline_dir):
            tiles = {}
            for gz_path in pipeline_dir.rglob("*.json.gz"):
                rel = gz_path.relative_to(pipeline_dir)
                with gzip.open(gz_path, "rt") as f:
                    tiles[str(rel)] = json.load(f)
            return tiles

        tiles1 = _tile_contents(current1)
        tiles4 = _tile_contents(current4)

        assert set(tiles1.keys()) == set(tiles4.keys()), (
            f"Tile path sets differ.\n"
            f"  workers=1: {sorted(tiles1.keys())}\n"
            f"  workers=4: {sorted(tiles4.keys())}"
        )

        for rel_path in sorted(tiles1.keys()):
            content1 = tiles1[rel_path]
            content4 = tiles4[rel_path]
            assert content1["attribution"] == content4["attribution"], (
                f"Attribution differs in tile {rel_path}"
            )
            # Sort wrapped records by value.rkey for deterministic comparison
            # (records are {uri, cid, value}-wrapped; rkey lives at value.rkey,
            # per the envelope decisions above).
            records1 = sorted(content1["records"], key=lambda r: r["value"]["rkey"])
            records4 = sorted(content4["records"], key=lambda r: r["value"]["rkey"])
            assert records1 == records4, (
                f"Records differ in tile {rel_path}"
            )


# ---------------------------------------------------------------------------
# §2.1 Gate #13 Finding #3 — backward-compat symlink must not exist
# ---------------------------------------------------------------------------

class TestPhase2SymlinkLayout:
    """Finding #3 from Gate #13: run_pipeline must create ONLY tiles/current → <timestamp>.

    Spec §2.1 'after' layout: the ONLY symlink is <source_dir>/tiles/current → <timestamp>.
    The backward-compat symlink at <source_dir>/current → tiles/<timestamp>
    (quadtree.py lines 336–344) must be removed.
    """

    def test_no_compat_symlink_at_source_root(self, fsq_parquet, density_parquet, tmp_path):
        """§2.1: after run_pipeline, <output>/foursquare/tiles/current exists but
        <output>/foursquare/current does NOT exist (not even as a dangling symlink).

        Fails RED because run_pipeline currently creates the compat symlink
        <source_dir>/current → tiles/<timestamp> (quadtree.py lines 336–344).
        """
        import os as _os
        out = tmp_path / "p2_symlink_out"
        out.mkdir()

        run_pipeline(
            "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
            str(out), memory_limit="4GB", max_per_tile=100,
            density_parquet=density_parquet,
        )

        tiles_current = out / "foursquare" / "tiles" / "current"
        compat_link = out / "foursquare" / "current"

        # The canonical Phase 2 symlink must exist
        assert _os.path.exists(str(tiles_current)), (
            f"Phase 2 canonical symlink tiles/current must exist at {tiles_current}"
        )

        # The backward-compat symlink must NOT exist (not even dangling)
        assert not _os.path.lexists(str(compat_link)), (
            f"Backward-compat symlink must NOT exist at {compat_link}; "
            "spec §2.1 says the ONLY symlink is <source_dir>/tiles/current → <timestamp>. "
            "Fails RED because run_pipeline still creates it (quadtree.py lines 336–344)."
        )

