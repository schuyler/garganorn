"""Tests for garganorn.stages module functions.

Tests are organized by stage function:
- TestStageTileAssignment: tests for stage_tile_assignment()
- TestStageContainment: tests for compute_containment()
- TestStageExport: tests for stage_export()
- TestStageManifest: tests for write_manifest() and write_manifest_db()
- TestStageDensityExtractMtime: mtime-based caching tests for stage_density_extract()
- TestEnvelopeShape: §6 item 6 -- new atgeo v1 envelope shape on the real
  stage_export production path (pipeline-implementation-decisions.md
  "OQ-P2-1 — record envelope adoption").
- TestGeneratedAtCoherence: §6 items 7/8 -- fixed-timestamp injection seam,
  determinism, and run-dir/tile/manifest/manifest.duckdb generated_at
  agreement (pipeline-implementation-decisions.md
  "OQ-P2-1 — record envelope adoption").
"""
import time
import json
import gzip
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest
import duckdb

from garganorn.stages import (
    stage_import,
    stage_tile_assignment,
    compute_containment,
    stage_export,
    write_manifest,
    write_manifest_db,
    _coord_exprs,
)
from garganorn.database import FoursquareOSP, OverturePlaces, OpenStreetMap, OvertureDivisions
from garganorn.quadtree import run_pipeline, SOURCES


class TestStageTileAssignment:
    """Tests for stage_tile_assignment function."""

    def test_creates_tile_assignments(self, fsq_parquet, density_parquet, tmp_path):
        """stage_tile_assignment creates tile_assignments after stage_import."""
        places_parquet = str(tmp_path / "places.parquet")
        ta_parquet = str(tmp_path / "tile_assignments.parquet")

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     places_parquet, memory_limit="4GB", force=True)

        stage_tile_assignment(places_parquet, ta_parquet, "foursquare",
                              max_per_tile=100, memory_limit="4GB", force=True)

        con = duckdb.connect()
        count = con.execute(f"SELECT count(*) FROM read_parquet('{ta_parquet}')").fetchone()[0]
        con.close()
        assert count > 0, "tile_assignments should have rows"


class TestStageContainment:
    """Tests for compute_containment function."""

    def test_creates_place_containment(self, fsq_parquet, density_parquet, division_db_path, tmp_path):
        """compute_containment creates containment when boundaries_db is provided."""
        places_parquet = str(tmp_path / "places.parquet")
        ta_parquet = str(tmp_path / "tile_assignments.parquet")
        containment_dir = str(tmp_path / "containment")

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     places_parquet, memory_limit="4GB", force=True)
        stage_tile_assignment(places_parquet, ta_parquet, "foursquare",
                              max_per_tile=100, memory_limit="4GB", force=True)

        pk_expr = SOURCES["foursquare"].source_pk
        lon_expr, lat_expr = _coord_exprs("foursquare", alias="p")
        compute_containment(places_parquet, ta_parquet, str(division_db_path),
                            pk_expr, lon_expr, lat_expr, containment_dir,
                            memory_limit="4GB", force=True)

        meta_path = os.path.join(containment_dir, "_meta.json")
        assert os.path.exists(meta_path), "containment _meta.json must exist"
        meta = json.loads(open(meta_path).read())
        # _meta.json must carry the standard artifact structure (params + inputs
        # tracked for freshness, generated_at timestamp).
        assert "params" in meta, f"meta missing 'params'; keys={list(meta)}"
        assert "inputs" in meta, f"meta missing 'inputs'; keys={list(meta)}"
        assert "generated_at" in meta, f"meta missing 'generated_at'; keys={list(meta)}"
        # boundaries_db was provided; places.parquet + tile_assignments.parquet
        # must both be recorded as tracked inputs.
        assert places_parquet in meta["inputs"]
        assert ta_parquet in meta["inputs"]

    def test_no_boundaries_creates_empty_table(self, fsq_parquet, density_parquet, tmp_path):
        """compute_containment with boundaries_db=None creates empty containment dir."""
        places_parquet = str(tmp_path / "places.parquet")
        ta_parquet = str(tmp_path / "tile_assignments.parquet")
        containment_dir = str(tmp_path / "containment")

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     places_parquet, memory_limit="4GB", force=True)
        stage_tile_assignment(places_parquet, ta_parquet, "foursquare",
                              max_per_tile=100, memory_limit="4GB", force=True)

        pk_expr = SOURCES["foursquare"].source_pk
        lon_expr, lat_expr = _coord_exprs("foursquare", alias="p")
        compute_containment(places_parquet, ta_parquet, None,
                            pk_expr, lon_expr, lat_expr, containment_dir,
                            memory_limit="4GB", force=True)

        meta_path = os.path.join(containment_dir, "_meta.json")
        assert os.path.exists(meta_path), "containment _meta.json must exist"
        meta = json.loads(open(meta_path).read())
        assert meta.get("empty") is True, "meta should have empty=True when no boundaries"


class TestStageExport:
    """Tests for stage_export function."""

    def test_writes_gzipped_tiles(self, fsq_parquet, density_parquet, tmp_path):
        """stage_export writes gzipped JSON tile files."""
        run_pipeline("foursquare", fsq_parquet,
                     (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path), memory_limit="4GB", max_per_tile=100,
                     density_parquet=density_parquet)

        tiles_current = tmp_path / "foursquare" / "tiles" / "current"
        gz_files = list(tiles_current.rglob("*.json.gz"))
        assert len(gz_files) > 0, "at least one .json.gz file should be written"


class TestStageManifest:
    """Tests for write_manifest and write_manifest_db functions."""

    def test_writes_manifest_json(self, fsq_parquet, density_parquet, tmp_path):
        """stage_export writes manifest.json."""
        run_pipeline("foursquare", fsq_parquet,
                     (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path), memory_limit="4GB", max_per_tile=100,
                     density_parquet=density_parquet)

        tiles_current = tmp_path / "foursquare" / "tiles" / "current"
        manifest_path = tiles_current / "manifest.json"
        assert manifest_path.exists()

        with open(manifest_path) as f:
            data = json.load(f)
        assert "source" in data
        assert "generated_at" in data
        assert "quadkeys" in data
        assert data["source"] == "foursquare"
        assert isinstance(data["quadkeys"], list)

    def test_writes_manifest_duckdb(self, fsq_parquet, density_parquet, tmp_path):
        """stage_export writes manifest.duckdb."""
        run_pipeline("foursquare", fsq_parquet,
                     (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path), memory_limit="4GB", max_per_tile=100,
                     density_parquet=density_parquet)

        tiles_current = tmp_path / "foursquare" / "tiles" / "current"
        manifest_db = tiles_current / "manifest.duckdb"
        assert manifest_db.exists()

        con_check = duckdb.connect(str(manifest_db))
        count = con_check.execute("SELECT count(*) FROM record_tiles").fetchone()[0]
        assert count > 0
        con_check.close()


# ---------------------------------------------------------------------------
# pipeline-implementation-decisions.md ("OQ-P2-1 — record envelope adoption") — §6 items 6, 7, 8, 9
# ---------------------------------------------------------------------------
#
# These tests exercise the REAL stage_export production path (via
# run_pipeline, and directly for the fixed-timestamp seam) rather than
# hand-built fixtures, so they exercise the full Python wrapping integration
# (per the envelope decisions above), not just envelope.py's pure functions (covered in isolation by
# tests/test_envelope.py).

def _read_one_tile(tiles_current: Path) -> dict:
    """Read and parse the first .json.gz tile found under tiles_current."""
    gz_files = sorted(tiles_current.rglob("*.json.gz"))
    assert gz_files, f"no .json.gz files found under {tiles_current}"
    with gzip.open(gz_files[0], "rt") as f:
        return json.load(f)


class TestEnvelopeShape:
    """§6 item 6 — envelope shape on the real stage_export production path."""

    def test_tile_top_level_keys_exact(self, fsq_parquet, density_parquet, tmp_path):
        """Tile top-level == exactly {atgeo, collection, attribution,
        generated_at, records} with atgeo == 1 (per the envelope decisions above)."""
        run_pipeline("foursquare", fsq_parquet,
                     (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path), memory_limit="4GB", max_per_tile=100,
                     density_parquet=density_parquet)
        tiles_current = tmp_path / "foursquare" / "tiles" / "current"
        tile = _read_one_tile(tiles_current)
        assert set(tile.keys()) == {"atgeo", "collection", "attribution", "generated_at", "records"}, (
            f"tile top-level must be exactly {{atgeo, collection, attribution, generated_at, "
            f"records}}; got {list(tile)}"
        )
        assert tile["atgeo"] == 1

    def test_tile_records_are_uri_cid_value_wrapped(self, fsq_parquet, density_parquet, tmp_path):
        """Each record == exactly {uri, cid, value} with cid is None (per the envelope decisions above)."""
        run_pipeline("foursquare", fsq_parquet,
                     (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path), memory_limit="4GB", max_per_tile=100,
                     density_parquet=density_parquet)
        tiles_current = tmp_path / "foursquare" / "tiles" / "current"
        tile = _read_one_tile(tiles_current)
        assert tile["records"], "tile must have at least one record"
        for rec in tile["records"]:
            assert set(rec.keys()) == {"uri", "cid", "value"}, (
                f"record must be exactly {{uri, cid, value}}; got {list(rec)}"
            )
            assert rec["cid"] is None

    def test_tile_record_uri_form_and_rkey_agreement(self, fsq_parquet, density_parquet, tmp_path):
        """uri == https://{repo}/{collection}/{rkey}; uri's rkey segment ==
        value.rkey == record_tiles.rkey for sampled records (per the envelope decisions above, §6 item 6)."""
        run_pipeline("foursquare", fsq_parquet,
                     (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path), memory_limit="4GB", max_per_tile=100,
                     density_parquet=density_parquet)
        tiles_current = tmp_path / "foursquare" / "tiles" / "current"
        tile = _read_one_tile(tiles_current)

        manifest_db = tiles_current / "manifest.duckdb"
        con = duckdb.connect(str(manifest_db), read_only=True)
        manifest_rkeys = {row[0] for row in con.execute("SELECT rkey FROM record_tiles").fetchall()}
        con.close()

        for rec in tile["records"]:
            uri_rkey = rec["uri"].rsplit("/", 1)[-1]
            assert uri_rkey == rec["value"]["rkey"], (
                f"uri rkey segment {uri_rkey!r} must equal value.rkey {rec['value']['rkey']!r}"
            )
            assert rec["uri"] == f"https://places.atgeo.org/org.atgeo.places.foursquare/{uri_rkey}"
            assert uri_rkey in manifest_rkeys, (
                f"uri rkey {uri_rkey!r} must appear in manifest.duckdb record_tiles.rkey"
            )

    def test_manifest_json_field_set(self, fsq_parquet, density_parquet, tmp_path):
        """manifest.json matches the manifest-shape field set (per the envelope decisions above, §6 item 9)."""
        run_pipeline("foursquare", fsq_parquet,
                     (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path), memory_limit="4GB", max_per_tile=100,
                     density_parquet=density_parquet)
        tiles_current = tmp_path / "foursquare" / "tiles" / "current"
        with open(tiles_current / "manifest.json") as f:
            manifest = json.load(f)
        expected = {
            "atgeo", "source", "collection", "attribution", "generated_at",
            "tile_url_template", "cache", "quadkeys",
        }
        assert set(manifest.keys()) == expected, (
            f"manifest.json must match the manifest-shape field set; got {sorted(manifest.keys())}"
        )
        assert manifest["atgeo"] == 1
        assert manifest["collection"] == "org.atgeo.places.foursquare"
        assert manifest["cache"] == {"max_age": 86400, "immutable": False}

    def test_manifest_duckdb_metadata_has_atgeo_and_collection(self, fsq_parquet, density_parquet, tmp_path):
        """manifest.duckdb metadata gains atgeo/collection columns (per the envelope decisions above, §6 item 9)."""
        run_pipeline("foursquare", fsq_parquet,
                     (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path), memory_limit="4GB", max_per_tile=100,
                     density_parquet=density_parquet)
        tiles_current = tmp_path / "foursquare" / "tiles" / "current"
        con = duckdb.connect(str(tiles_current / "manifest.duckdb"), read_only=True)
        cols = {row[1] for row in con.execute("PRAGMA table_info('metadata')").fetchall()}
        row = con.execute("SELECT atgeo, collection, source, generated_at FROM metadata").fetchone()
        con.close()
        assert {"atgeo", "collection"} <= cols, (
            f"manifest.duckdb metadata must have atgeo/collection columns; got {cols}"
        )
        atgeo, collection, source, generated_at = row
        assert atgeo == 1
        assert collection == "org.atgeo.places.foursquare"


class TestGeneratedAtDeterminismAndCoherence:
    """§6 items 7, 8 — fixed-timestamp injection, determinism, coherence.

    Seam under test: stage_export accepts an optional `now: datetime` keyword
    argument (aware, UTC). When provided, it is used BOTH to name the run
    directory (`now.strftime("%Y%m%dT%H%M%S")`, replacing the internal
    `datetime.now(timezone.utc)` call at stages.py:1278) AND to derive the
    shared `generated_at` RFC 3339 Z string stamped into every tile,
    manifest.json, and manifest.duckdb metadata (per the envelope decisions above). When omitted,
    stage_export defaults to the current wall-clock time as it does today.
    This is the minimal seam consistent with the design's own description:
    "one timestamp per export run, derived from the run-dir name ... already
    produces %Y%m%dT%H%M%S UTC" (per the envelope decisions above) — injecting `now` and deriving both
    the dir name and generated_at from the SAME value is what makes two runs
    with an identical injected `now` byte-identical tile-for-tile.
    """

    def _build_export_inputs(self, fsq_parquet, density_parquet, tmp_path, subdir):
        """Run stage_import/stage_tile_assignment/compute_containment; return
        (places_parquet, ta_parquet, containment_dir) for a direct stage_export call."""
        base = tmp_path / subdir
        base.mkdir()
        places_parquet = str(base / "places.parquet")
        ta_parquet = str(base / "tile_assignments.parquet")
        containment_dir = str(base / "containment")

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     places_parquet, memory_limit="4GB", force=True)
        stage_tile_assignment(places_parquet, ta_parquet, "foursquare",
                              max_per_tile=100, memory_limit="4GB", force=True)
        pk_expr = SOURCES["foursquare"].source_pk
        lon_expr, lat_expr = _coord_exprs("foursquare", alias="p")
        compute_containment(places_parquet, ta_parquet, None,
                            pk_expr, lon_expr, lat_expr, containment_dir,
                            memory_limit="4GB", force=True)
        return places_parquet, ta_parquet, containment_dir

    def test_stage_export_accepts_now_kwarg(self, fsq_parquet, density_parquet, tmp_path):
        """stage_export(..., now=<aware UTC datetime>) is accepted (the injection seam)."""
        import inspect
        sig = inspect.signature(stage_export)
        assert "now" in sig.parameters, (
            f"stage_export must accept a 'now' keyword for fixed-timestamp injection "
            f"(per the envelope determinism seam above); got params: {list(sig.parameters)}"
        )

    def test_run_dir_name_matches_injected_now(self, fsq_parquet, density_parquet, tmp_path):
        """The run dir created under tiles_root is named now.strftime('%Y%m%dT%H%M%S')."""
        places_parquet, ta_parquet, containment_dir = self._build_export_inputs(
            fsq_parquet, density_parquet, tmp_path, "inputs_a"
        )
        tiles_root = str(tmp_path / "tiles_a")
        fixed_now = datetime(2026, 7, 9, 18, 0, 0, tzinfo=timezone.utc)

        run_dir = stage_export("foursquare", places_parquet, ta_parquet, containment_dir,
                               tiles_root, time.monotonic(), memory_limit="4GB",
                               force=True, now=fixed_now)

        assert os.path.basename(run_dir.rstrip("/")) == "20260709T180000", (
            f"run dir must be named from the injected now; got {run_dir}"
        )

    def test_generated_at_derived_from_injected_now(self, fsq_parquet, density_parquet, tmp_path):
        """generated_at (tile + manifest.json + manifest.duckdb metadata) ==
        RFC 3339 Z rendering of the injected now (per the envelope decisions above)."""
        places_parquet, ta_parquet, containment_dir = self._build_export_inputs(
            fsq_parquet, density_parquet, tmp_path, "inputs_b"
        )
        tiles_root = str(tmp_path / "tiles_b")
        fixed_now = datetime(2026, 7, 9, 18, 0, 0, tzinfo=timezone.utc)
        expected_ts = "2026-07-09T18:00:00Z"

        run_dir = stage_export("foursquare", places_parquet, ta_parquet, containment_dir,
                               tiles_root, time.monotonic(), memory_limit="4GB",
                               force=True, now=fixed_now)
        run_dir = Path(run_dir)

        tile = _read_one_tile(run_dir)
        assert tile["generated_at"] == expected_ts

        with open(run_dir / "manifest.json") as f:
            manifest = json.load(f)
        assert manifest["generated_at"] == expected_ts

        con = duckdb.connect(str(run_dir / "manifest.duckdb"), read_only=True)
        db_generated_at = con.execute("SELECT generated_at FROM metadata").fetchone()[0]
        con.close()
        assert db_generated_at == expected_ts

        # Coherence: run-dir name, tile, manifest.json, and manifest.duckdb all agree.
        assert os.path.basename(str(run_dir)) == "20260709T180000"

    def test_every_tile_shares_the_same_generated_at(self, fsq_parquet, density_parquet, tmp_path):
        """Every tile in a multi-tile run carries the identical generated_at (per
        the envelope decisions above: 'identical across all tiles of a run')."""
        places_parquet, ta_parquet, containment_dir = self._build_export_inputs(
            fsq_parquet, density_parquet, tmp_path, "inputs_c"
        )
        tiles_root = str(tmp_path / "tiles_c")
        fixed_now = datetime(2026, 7, 9, 18, 0, 0, tzinfo=timezone.utc)

        run_dir = Path(stage_export("foursquare", places_parquet, ta_parquet, containment_dir,
                                    tiles_root, time.monotonic(), memory_limit="4GB",
                                    force=True, now=fixed_now))

        gz_files = list(run_dir.rglob("*.json.gz"))
        assert gz_files, "expected at least one tile"
        generated_ats = set()
        for gz in gz_files:
            with gzip.open(gz, "rt") as f:
                generated_ats.add(json.load(f)["generated_at"])
        assert generated_ats == {"2026-07-09T18:00:00Z"}, (
            f"all tiles must share one generated_at; got {generated_ats}"
        )

    def test_two_runs_with_identical_injected_now_are_byte_identical(
        self, fsq_parquet, density_parquet, tmp_path
    ):
        """§6 item 7 — two stage_export runs over identical inputs with an
        injected fixed timestamp are byte-identical tile-for-tile."""
        places_a, ta_a, cont_a = self._build_export_inputs(
            fsq_parquet, density_parquet, tmp_path, "inputs_d1"
        )
        places_b, ta_b, cont_b = self._build_export_inputs(
            fsq_parquet, density_parquet, tmp_path, "inputs_d2"
        )
        fixed_now = datetime(2026, 7, 9, 18, 0, 0, tzinfo=timezone.utc)

        run_dir_a = Path(stage_export("foursquare", places_a, ta_a, cont_a,
                                      str(tmp_path / "tiles_d1"), time.monotonic(),
                                      memory_limit="4GB", force=True, now=fixed_now))
        run_dir_b = Path(stage_export("foursquare", places_b, ta_b, cont_b,
                                      str(tmp_path / "tiles_d2"), time.monotonic(),
                                      memory_limit="4GB", force=True, now=fixed_now))

        def _tile_bytes(run_dir):
            out = {}
            for gz in run_dir.rglob("*.json.gz"):
                rel = gz.relative_to(run_dir)
                out[str(rel)] = gz.read_bytes()
            return out

        tiles_a = _tile_bytes(run_dir_a)
        tiles_b = _tile_bytes(run_dir_b)
        assert set(tiles_a.keys()) == set(tiles_b.keys()), (
            f"tile path sets must match: {sorted(tiles_a)} vs {sorted(tiles_b)}"
        )
        for rel in tiles_a:
            assert tiles_a[rel] == tiles_b[rel], (
                f"tile {rel} must be byte-identical across runs with the same injected now"
            )


class TestStageDensityExtractMtime:
    """Tests for mtime-based caching in stage_density_extract()."""

    @pytest.fixture
    def small_overture_parquet(self, tmp_path):
        """Create a minimal Overture place parquet file for mtime testing."""
        parquet_path = tmp_path / "overture_places.parquet"

        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")

        conn.execute("""
            CREATE TABLE tmp_ov (
                id          VARCHAR,
                bbox        STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
                geometry    VARCHAR,
                names       STRUCT("primary" VARCHAR),
                categories  STRUCT("primary" VARCHAR)
            )
        """)

        conn.execute("""
            INSERT INTO tmp_ov VALUES (
                'ovr001',
                {'xmin': -122.5, 'ymin': 37.7, 'xmax': -122.4, 'ymax': 37.8},
                'POINT(-122.45 37.75)',
                {'primary': 'Place 1'},
                {'primary': NULL}
            )
        """)

        conn.execute("""
            INSERT INTO tmp_ov VALUES (
                'ovr002',
                {'xmin': -122.45, 'ymin': 37.75, 'xmax': -122.35, 'ymax': 37.85},
                'POINT(-122.4 37.8)',
                {'primary': 'Place 2'},
                {'primary': NULL}
            )
        """)

        conn.execute(f"COPY tmp_ov TO '{parquet_path}' (FORMAT PARQUET)")
        conn.close()

        return str(parquet_path)

    @pytest.fixture
    def density_output(self, tmp_path):
        """Path for density output file in tmp_path."""
        return str(tmp_path / "density_test.parquet")

    def test_skips_when_output_fresh(self, small_overture_parquet, density_output, caplog):
        """stage_density_extract skips when output mtime is newer than input."""
        import time
        from garganorn.stages import stage_density_extract

        with caplog.at_level(logging.INFO):
            stage_density_extract(small_overture_parquet, density_output, time.monotonic())

        assert os.path.exists(density_output), "First run should create output"

        future_time = time.time() + 3600
        os.utime(density_output, (future_time, future_time))
        os.utime(density_output + ".meta.json", (future_time, future_time))

        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_density_extract(small_overture_parquet, density_output, time.monotonic())

        assert any("skip" in record.message.lower() for record in caplog.records), (
            "Should log skip message when output is fresh"
        )

    def test_runs_when_output_missing(self, small_overture_parquet, density_output, caplog):
        """stage_density_extract runs normally when output doesn't exist."""
        import time
        from garganorn.stages import stage_density_extract

        assert not os.path.exists(density_output), "Output should not exist initially"

        with caplog.at_level(logging.INFO):
            stage_density_extract(small_overture_parquet, density_output, time.monotonic())

        assert os.path.exists(density_output), "Should create output when missing"
        assert any("starting" in record.message.lower() for record in caplog.records), (
            "Should log starting message when output is missing"
        )

    def test_runs_when_input_newer(self, small_overture_parquet, density_output, caplog):
        """stage_density_extract re-runs when input parquet is newer than output."""
        import time
        from garganorn.stages import stage_density_extract

        stage_density_extract(small_overture_parquet, density_output, time.monotonic())
        assert os.path.exists(density_output)

        past_time = time.time() - 3600
        os.utime(density_output, (past_time, past_time))
        os.utime(density_output + ".meta.json", (past_time, past_time))

        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_density_extract(small_overture_parquet, density_output, time.monotonic())

        assert any("starting" in record.message.lower() for record in caplog.records), (
            "Should log starting message when input is newer"
        )

    def test_force_overrides_fresh(self, small_overture_parquet, density_output, caplog):
        """stage_density_extract with force=True re-runs even when output is fresh."""
        import time
        from garganorn.stages import stage_density_extract

        stage_density_extract(small_overture_parquet, density_output, time.monotonic())
        assert os.path.exists(density_output)

        future_time = time.time() + 3600
        os.utime(density_output, (future_time, future_time))

        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_density_extract(small_overture_parquet, density_output, time.monotonic(), force=True)

        assert any("starting" in record.message.lower() for record in caplog.records), (
            "Should log starting message when force=True"
        )
