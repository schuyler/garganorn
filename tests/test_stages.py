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
import argparse
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

    def test_logs_per_batch_progress(self, fsq_parquet, density_parquet, division_db_path,
                                      tmp_path, caplog):
        """compute_containment logs per-batch start/done progress (not DuckDB's progress bar)."""
        from garganorn.covering import stage_covering

        places_parquet = str(tmp_path / "places.parquet")
        ta_parquet = str(tmp_path / "tile_assignments.parquet")
        covering_dir = str(tmp_path / "covering")
        containment_dir = str(tmp_path / "containment")

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     places_parquet, memory_limit="4GB", force=True)
        stage_tile_assignment(places_parquet, ta_parquet, "foursquare",
                              max_per_tile=100, memory_limit="4GB", force=True)
        stage_covering(str(division_db_path), covering_dir,
                       cover_min_zoom=4, cover_max_zoom=12, force=True)

        pk_expr = SOURCES["foursquare"].source_pk
        lon_expr, lat_expr = _coord_exprs("foursquare", alias="p")
        with caplog.at_level(logging.INFO):
            compute_containment(places_parquet, ta_parquet, str(division_db_path),
                                pk_expr, lon_expr, lat_expr, containment_dir,
                                covering_dir=covering_dir,
                                memory_limit="4GB", force=True)

        messages = [r.message for r in caplog.records]
        starts = [m for m in messages if "batch 1/" in m and "prefix=" in m and "n=" in m]
        assert starts, f"expected a per-batch start log line; got: {messages}"
        done = [m for m in messages if "batch 1/" in m and "done" in m]
        assert done, f"expected a per-batch completion log line with elapsed time; got: {messages}"


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
        assert "generated_at" in data

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
        """Tile top-level == exactly {collection, source, license,
        generated_at, records} (per the envelope decisions above)."""
        run_pipeline("foursquare", fsq_parquet,
                     (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path), memory_limit="4GB", max_per_tile=100,
                     density_parquet=density_parquet)
        tiles_current = tmp_path / "foursquare" / "tiles" / "current"
        tile = _read_one_tile(tiles_current)
        assert set(tile.keys()) == {"collection", "source", "license", "generated_at", "records"}, (
            f"tile top-level must be exactly {{collection, source, license, generated_at, "
            f"records}}; got {list(tile)}"
        )

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
        assert set(manifest.keys()) == {"generated_at"}, (
            f"manifest.json must match the manifest-shape field set; got {sorted(manifest.keys())}"
        )

    def test_manifest_duckdb_metadata_has_collection(self, fsq_parquet, density_parquet, tmp_path):
        """manifest.duckdb metadata has a collection column (per the envelope decisions above, §6 item 9)."""
        run_pipeline("foursquare", fsq_parquet,
                     (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path), memory_limit="4GB", max_per_tile=100,
                     density_parquet=density_parquet)
        tiles_current = tmp_path / "foursquare" / "tiles" / "current"
        con = duckdb.connect(str(tiles_current / "manifest.duckdb"), read_only=True)
        cols = {row[1] for row in con.execute("PRAGMA table_info('metadata')").fetchall()}
        row = con.execute("SELECT collection, source, generated_at FROM metadata").fetchone()
        con.close()
        assert "collection" in cols, (
            f"manifest.duckdb metadata must have a collection column; got {cols}"
        )
        collection, source, generated_at = row
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


# ---------------------------------------------------------------------------
# stage_export accepts and honors temp_directory, consistently with every
# sibling stage (stage_import, stage_tile_assignment, compute_containment,
# stage_covering): SET temp_directory is issued against the caller-supplied
# scratch volume instead of always spilling to run_dir + ".spill" -- a
# sibling of the tiles output dir, i.e. the tiles volume rather than the
# configured scratch volume. This matters because export runs a global
# ORDER BY tile_qk, place_id over fully-rendered record_json, roughly
# dataset-JSON-sized spill and the pipeline's next scaling cliff.
# ---------------------------------------------------------------------------

def _write_duplicate_density_parquet(path):
    """Write a density_tiles-shaped parquet with a duplicated tile_qk15 key.

    Schema matches stage_density_extract's output (tile_qk15, density_score,
    tile_xmin, tile_ymin, tile_xmax, tile_ymax). 3 rows total, 2 sharing one
    tile_qk15 value: total=3, distinct=2.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE t (
            tile_qk15 VARCHAR, density_score DOUBLE,
            tile_xmin DOUBLE, tile_ymin DOUBLE, tile_xmax DOUBLE, tile_ymax DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO t VALUES
            ('123030123030123', 5.0, -122.5, 37.7, -122.4, 37.8),
            ('123030123030123', 9.0, -122.5, 37.7, -122.4, 37.8),
            ('300000000000000', 1.0, -180.0, -90.0, 180.0, 90.0)
    """)
    conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    conn.close()


def _write_duplicate_idf_parquet(path):
    """Write an idf_scores-shaped parquet with a duplicated category key.

    Schema matches stage_idf's output (category, n_places, idf_score). 3 rows
    total, 2 sharing one category value: total=3, distinct=2.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE t (category VARCHAR, n_places BIGINT, idf_score DOUBLE)
    """)
    conn.execute("""
        INSERT INTO t VALUES
            ('coffee_shop', 10, 5.0),
            ('coffee_shop', 20, 9.0),
            ('park', 5, 1.0)
    """)
    conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    conn.close()


class TestStageExportTempDirectory:
    """stage_export must accept a temp_directory kwarg and honor it for
    DuckDB spill, the same contract as stage_covering (garganorn/covering.py):
    None preserves existing behavior (spill under run_dir + '.spill');
    supplied, spill happens under the caller's chosen volume instead of the
    tiles volume."""

    def _build_export_inputs(self, fsq_parquet, tmp_path, subdir):
        return _build_export_inputs(fsq_parquet, tmp_path, subdir)

    def test_accepts_temp_directory_kwarg(self, fsq_parquet, tmp_path):
        """stage_export(..., temp_directory=<dir>) must be accepted, mirroring
        every sibling stage's temp_directory parameter."""
        places_parquet, ta_parquet, containment_dir = self._build_export_inputs(
            fsq_parquet, tmp_path, "inputs_td1"
        )
        tiles_root = str(tmp_path / "tiles_td1")
        scratch = tmp_path / "scratch_td1"
        scratch.mkdir()

        # Must not raise TypeError for an unexpected keyword argument.
        stage_export("foursquare", places_parquet, ta_parquet, containment_dir,
                     tiles_root, time.monotonic(), memory_limit="4GB",
                     force=True, temp_directory=str(scratch))

    def test_honors_temp_directory_for_spill_location(
        self, fsq_parquet, tmp_path, monkeypatch
    ):
        """When temp_directory is supplied, stage_export's DuckDB connection
        must SET temp_directory under the caller's chosen volume, not under
        run_dir + '.spill' on the tiles volume."""
        import garganorn.stages as stages_mod

        places_parquet, ta_parquet, containment_dir = self._build_export_inputs(
            fsq_parquet, tmp_path, "inputs_td2"
        )
        tiles_root = str(tmp_path / "tiles_td2")
        scratch = tmp_path / "scratch_td2"
        scratch.mkdir()

        real_connect = stages_mod.duckdb.connect
        recorded = {"statements": []}

        class _RecordingConn:
            def __init__(self, real):
                self._real = real

            def execute(self, sql, *a, **kw):
                recorded["statements"].append(sql)
                return self._real.execute(sql, *a, **kw)

            def __getattr__(self, name):
                return getattr(self._real, name)

        def spy_connect(*a, **kw):
            return _RecordingConn(real_connect(*a, **kw))

        monkeypatch.setattr(stages_mod.duckdb, "connect", spy_connect)

        stage_export("foursquare", places_parquet, ta_parquet, containment_dir,
                     tiles_root, time.monotonic(), memory_limit="4GB",
                     force=True, temp_directory=str(scratch))

        temp_dir_stmts = [s for s in recorded["statements"] if "SET temp_directory" in s]
        assert temp_dir_stmts, f"no SET temp_directory statement recorded: {recorded['statements']}"
        stmt = temp_dir_stmts[0]
        assert str(scratch) in stmt, (
            f"stage_export must SET temp_directory under the caller-supplied "
            f"temp_directory ({scratch}); got: {stmt!r}"
        )
        assert "tiles_td2" not in stmt, (
            f"stage_export must not spill under tiles_root when temp_directory "
            f"is supplied; got: {stmt!r}"
        )
        # stage_export owns and cleans up only its private subdirectory --
        # the caller-supplied temp_directory itself must survive the call.
        assert scratch.exists(), "caller-supplied temp_directory must not be removed"

    def test_default_spill_location_preserved_when_temp_directory_omitted(
        self, fsq_parquet, tmp_path, monkeypatch
    ):
        """When temp_directory is omitted, spill must still land at
        run_dir + '.spill' (existing behavior, preserved for callers that
        don't supply temp_directory)."""
        import garganorn.stages as stages_mod

        places_parquet, ta_parquet, containment_dir = self._build_export_inputs(
            fsq_parquet, tmp_path, "inputs_td3"
        )
        tiles_root = str(tmp_path / "tiles_td3")

        real_connect = stages_mod.duckdb.connect
        recorded = {"statements": []}

        class _RecordingConn:
            def __init__(self, real):
                self._real = real

            def execute(self, sql, *a, **kw):
                recorded["statements"].append(sql)
                return self._real.execute(sql, *a, **kw)

            def __getattr__(self, name):
                return getattr(self._real, name)

        def spy_connect(*a, **kw):
            return _RecordingConn(real_connect(*a, **kw))

        monkeypatch.setattr(stages_mod.duckdb, "connect", spy_connect)

        run_dir = stage_export("foursquare", places_parquet, ta_parquet, containment_dir,
                               tiles_root, time.monotonic(), memory_limit="4GB",
                               force=True)

        temp_dir_stmts = [s for s in recorded["statements"] if "SET temp_directory" in s]
        assert temp_dir_stmts, f"no SET temp_directory statement recorded: {recorded['statements']}"
        assert run_dir + ".spill" in temp_dir_stmts[0], (
            f"default spill location must remain run_dir + '.spill'; got: {temp_dir_stmts[0]!r}"
        )


class TestRunPipelineTempDirectoryThreading:
    """run_pipeline must thread temp_directory into stage_export, matching
    every other stage it already threads it into (stage_import,
    stage_covering, ensure_covering, stage_tile_assignment,
    compute_containment)."""

    def test_run_pipeline_passes_temp_directory_to_stage_export(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        import garganorn.quadtree as quadtree_mod

        calls = []

        def fake_stage_export(*args, **kwargs):
            calls.append(kwargs)
            return str(tmp_path / "fake_run_dir")

        monkeypatch.setattr(quadtree_mod, "stage_export", fake_stage_export)

        scratch = tmp_path / "scratch_rp"
        scratch.mkdir()

        quadtree_mod.run_pipeline(
            "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
            str(tmp_path / "output_rp"), memory_limit="4GB", max_per_tile=100,
            density_parquet=density_parquet, temp_directory=str(scratch),
        )

        assert calls, "stage_export must have been called"
        assert calls[-1].get("temp_directory") == str(scratch), (
            f"run_pipeline must pass temp_directory through to stage_export; "
            f"got kwargs={calls[-1]}"
        )


# ---------------------------------------------------------------------------
# stage_import applies its temp_directory parameter to its own ephemeral
# connection for the foursquare/overture_place/osm branches, and forwards it
# to stage_division_import for the overture_division dispatch, matching
# stage_division_import's own `if temp_directory: con.execute(f"SET
# temp_directory = '{temp_directory}'")` precedent.
#
# Production incident: a global run configured with
# temp_directory=/home/sderle/garganorn-global-tiles/tmp crashed at 506 GiB
# -- free space on the root filesystem, not the configured scratch volume.
# After the crash the configured temp_directory measured 4.0K (empty): the
# spill never went there. max_temp_directory_size defaults to available disk
# on whatever volume temp_directory actually resolves to, so the ceiling hit
# belonged to the wrong filesystem.
# ---------------------------------------------------------------------------

class TestStageImportTempDirectory:
    """stage_import must apply its temp_directory parameter to its own
    DuckDB connection (foursquare/overture_place/osm branches), matching
    stage_division_import's precedent (stages.py:766-767) and every other
    stage."""

    def test_honors_temp_directory_on_its_own_connection(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        import garganorn.stages as stages_mod

        places_parquet = str(tmp_path / "places_import_td.parquet")
        scratch = tmp_path / "scratch_import"
        scratch.mkdir()

        real_connect = stages_mod.duckdb.connect
        recorded = {"statements": []}

        class _RecordingConn:
            def __init__(self, real):
                self._real = real

            def execute(self, sql, *a, **kw):
                recorded["statements"].append(sql)
                return self._real.execute(sql, *a, **kw)

            def __getattr__(self, name):
                return getattr(self._real, name)

        def spy_connect(*a, **kw):
            return _RecordingConn(real_connect(*a, **kw))

        monkeypatch.setattr(stages_mod.duckdb, "connect", spy_connect)

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     places_parquet, memory_limit="4GB", force=True,
                     density_parquet=density_parquet, temp_directory=str(scratch))

        temp_dir_stmts = [s for s in recorded["statements"] if "SET temp_directory" in s]
        assert temp_dir_stmts, (
            f"stage_import must SET temp_directory on its own connection when "
            f"temp_directory is supplied (matching stage_division_import's "
            f"precedent); no such statement was recorded. "
            f"Statements executed: {recorded['statements']}"
        )
        assert str(scratch) in temp_dir_stmts[0], (
            f"stage_import must SET temp_directory to the caller-supplied "
            f"value ({scratch}); got: {temp_dir_stmts[0]!r}"
        )

    def test_omitted_temp_directory_does_not_set_it(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        """Preserve existing behavior when temp_directory is None: no SET
        temp_directory statement at all (DuckDB's own default applies), same
        as stage_division_import's `if temp_directory:` guard."""
        import garganorn.stages as stages_mod

        places_parquet = str(tmp_path / "places_import_td2.parquet")

        real_connect = stages_mod.duckdb.connect
        recorded = {"statements": []}

        class _RecordingConn:
            def __init__(self, real):
                self._real = real

            def execute(self, sql, *a, **kw):
                recorded["statements"].append(sql)
                return self._real.execute(sql, *a, **kw)

            def __getattr__(self, name):
                return getattr(self._real, name)

        def spy_connect(*a, **kw):
            return _RecordingConn(real_connect(*a, **kw))

        monkeypatch.setattr(stages_mod.duckdb, "connect", spy_connect)

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     places_parquet, memory_limit="4GB", force=True,
                     density_parquet=density_parquet)

        temp_dir_stmts = [s for s in recorded["statements"] if "SET temp_directory" in s]
        assert not temp_dir_stmts, (
            f"stage_import must not SET temp_directory when it wasn't "
            f"supplied; got: {temp_dir_stmts}"
        )


# ---------------------------------------------------------------------------
# Fix 2 -- density_tiles and idf_scores lookup tables built by stage_import
# from --density-parquet/--idf-parquet are not checked for key uniqueness
# anywhere. The import SQL LEFT JOINs them on tile_qk15 and category
# respectively; a duplicate key multiplies the matching place's row through
# the join at every join site. Production data is clean today (density_tiles
# is GROUP BY-guaranteed unique by its producer, density_extract.sql), but
# --density-parquet/--idf-parquet accept arbitrary files, so the guarantee is
# convention, not contract. stage_import must fail loudly, before running the
# import SQL, when either lookup table's key isn't unique.
# ---------------------------------------------------------------------------

def _write_null_key_density_parquet(path):
    """Write a density_tiles-shaped parquet with a single NULL tile_qk15 key
    and no duplicates among the non-NULL keys.

    Mirrors production reachability: density_extract.sql groups
    left(ST_QuadKey(...), 15) over the whole Overture places parquet with no
    NULL/geometry filter, so a single NULL-bbox row yields exactly one NULL
    tile_qk15 group. NULL = NULL is never true in SQL, so a NULL key can
    never fan out a LEFT JOIN -- this must not trip the uniqueness guard.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE t (
            tile_qk15 VARCHAR, density_score DOUBLE,
            tile_xmin DOUBLE, tile_ymin DOUBLE, tile_xmax DOUBLE, tile_ymax DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO t VALUES
            (NULL, 3.0, NULL, NULL, NULL, NULL),
            ('123030123030123', 5.0, -122.5, 37.7, -122.4, 37.8),
            ('300000000000000', 1.0, -180.0, -90.0, 180.0, 90.0)
    """)
    conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    conn.close()


def _write_null_key_idf_parquet(path):
    """Write an idf_scores-shaped parquet with a single NULL category key
    and no duplicates among the non-NULL keys.

    Mirrors production reachability: foursquare_idf.sql derives category
    from fsq_category_ids, and a NULL inside that array survives as a NULL
    category group. Must not trip the uniqueness guard (see
    _write_null_key_density_parquet's docstring for why).
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE t (category VARCHAR, n_places BIGINT, idf_score DOUBLE)
    """)
    conn.execute("""
        INSERT INTO t VALUES
            (NULL, 2, 3.0),
            ('coffee_shop', 10, 5.0),
            ('park', 5, 1.0)
    """)
    conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    conn.close()


class TestStageImportLookupKeyUniqueness:
    """stage_import must assert every non-NULL key in density_tiles
    (tile_qk15) and idf_scores (category) is unique after loading them from
    parquet, and raise a clear error naming the offending file and key
    counts if the assertion fails -- before hours of downstream work run on
    a silently-multiplied join. NULL keys are exempt (NULL = NULL is never
    true, so a NULL key cannot fan out the LEFT JOIN)."""

    def test_duplicate_density_tile_qk15_raises(self, fsq_parquet, tmp_path):
        density_path = tmp_path / "dup_density.parquet"
        _write_duplicate_density_parquet(density_path)
        places_parquet = str(tmp_path / "places_dup_density.parquet")

        with pytest.raises(ValueError) as excinfo:
            stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                         places_parquet, memory_limit="4GB", force=True,
                         density_parquet=str(density_path))

        msg = str(excinfo.value)
        assert str(density_path) in msg, f"error must name the offending file; got: {msg!r}"
        assert "tile_qk15" in msg, f"error must name the offending key; got: {msg!r}"
        assert "3 rows" in msg and "2 distinct" in msg, (
            f"error should surface the offending key counts (3 rows, 2 distinct "
            f"tile_qk15 values); got: {msg!r}"
        )
        assert not os.path.exists(places_parquet), (
            "stage_import must fail before writing any output artifact"
        )

    @pytest.mark.parametrize(
        "source", ["foursquare", "overture_place", "osm", "overture_division"]
    )
    def test_duplicate_density_tile_qk15_raises_for_every_density_parquet_consumer(
        self, source, fsq_parquet, overture_parquet, osm_parquet, division_parquet, tmp_path
    ):
        """stage_import dispatches to stage_division_import for
        overture_division and RETURNS before its own _assert_unique_key call
        -- but overture_division is on the global run path and IS passed
        density_parquet (quadtree.py _cmd_all). Without a guard covering it,
        a duplicated tile_qk15 in --density-parquet silently corrupts
        importance for division rows too (division_density's
        avg(density_score) double-counts the duplicate), rather than
        raising. Parametrized over all four sources that can receive
        --density-parquet through stage_import so a fix covering only the
        non-division path can't pass silently."""
        density_path = tmp_path / f"dup_density_{source}.parquet"
        _write_duplicate_density_parquet(density_path)
        places_parquet = str(tmp_path / f"places_dup_density_{source}.parquet")

        parquet_glob = {
            "foursquare": fsq_parquet,
            "overture_place": overture_parquet,
            "osm": (osm_parquet["node"], osm_parquet["way"]),
            "overture_division": division_parquet,
        }[source]

        with pytest.raises(ValueError) as excinfo:
            stage_import(source, parquet_glob, (-122.55, 37.60, -122.30, 37.85),
                         places_parquet, memory_limit="4GB", force=True,
                         density_parquet=str(density_path))

        msg = str(excinfo.value)
        assert str(density_path) in msg, f"error must name the offending file; got: {msg!r}"
        assert "tile_qk15" in msg, f"error must name the offending key; got: {msg!r}"
        assert not os.path.exists(places_parquet), (
            f"stage_import must fail before writing any output artifact "
            f"(source={source})"
        )
        if source == "overture_division":
            boundaries_path = str(Path(places_parquet).parent / "boundaries.duckdb")
            assert not os.path.exists(boundaries_path), (
                "stage_import must fail before writing boundaries.duckdb "
                "for overture_division"
            )

    def test_uniqueness_check_runs_before_division_dispatch(
        self, division_parquet, tmp_path, monkeypatch
    ):
        """The density guard for overture_division must run before
        stage_import dispatches to stage_division_import (and returns) --
        otherwise duplicate density keys silently reach division_density's
        avg() aggregation instead of raising. Spy on stage_division_import
        to prove it is never called when the density lookup fails
        uniqueness -- the same ordering guarantee
        test_uniqueness_check_runs_before_expensive_import_sql below pins
        for the non-division sources."""
        import garganorn.stages as stages_mod

        density_path = tmp_path / "dup_density_division_ordering.parquet"
        _write_duplicate_density_parquet(density_path)
        places_parquet = str(tmp_path / "places_dup_density_division_ordering.parquet")

        calls = []
        real_stage_division_import = stages_mod.stage_division_import

        def spy_stage_division_import(*args, **kwargs):
            calls.append((args, kwargs))
            return real_stage_division_import(*args, **kwargs)

        monkeypatch.setattr(stages_mod, "stage_division_import", spy_stage_division_import)

        with pytest.raises(ValueError):
            stages_mod.stage_import(
                "overture_division", division_parquet, (-122.55, 37.60, -122.30, 37.85),
                places_parquet, force=True, density_parquet=str(density_path),
            )

        assert not calls, (
            "stage_division_import must not be called when the density "
            f"uniqueness check fails; got {len(calls)} call(s)"
        )

    def test_duplicate_idf_category_raises(self, fsq_parquet, tmp_path):
        idf_path = tmp_path / "dup_idf.parquet"
        _write_duplicate_idf_parquet(idf_path)
        places_parquet = str(tmp_path / "places_dup_idf.parquet")

        with pytest.raises(ValueError) as excinfo:
            stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                         places_parquet, memory_limit="4GB", force=True,
                         idf_parquet=str(idf_path))

        msg = str(excinfo.value)
        assert str(idf_path) in msg, f"error must name the offending file; got: {msg!r}"
        assert "category" in msg, f"error must name the offending key; got: {msg!r}"
        assert "3 rows" in msg and "2 distinct" in msg, (
            f"error should surface the offending key counts (3 rows, 2 distinct "
            f"category values); got: {msg!r}"
        )
        assert not os.path.exists(places_parquet), (
            "stage_import must fail before writing any output artifact"
        )

    def test_null_density_tile_qk15_does_not_raise(self, fsq_parquet, tmp_path):
        """A single NULL tile_qk15 key (and zero duplicates among the
        non-NULL keys) must not trip the uniqueness guard -- this is exactly
        the production shape density_extract.sql produces for a NULL-bbox
        row (see IMPORTANT 1 in the review that motivated this test)."""
        density_path = tmp_path / "null_density.parquet"
        _write_null_key_density_parquet(density_path)
        places_parquet = str(tmp_path / "places_null_density.parquet")

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     places_parquet, memory_limit="4GB", force=True,
                     density_parquet=str(density_path))
        assert os.path.exists(places_parquet)

    def test_null_idf_category_does_not_raise(self, fsq_parquet, tmp_path):
        """A single NULL category key (and zero duplicates among the
        non-NULL keys) must not trip the uniqueness guard."""
        idf_path = tmp_path / "null_idf.parquet"
        _write_null_key_idf_parquet(idf_path)
        places_parquet = str(tmp_path / "places_null_idf.parquet")

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     places_parquet, memory_limit="4GB", force=True,
                     idf_parquet=str(idf_path))
        assert os.path.exists(places_parquet)

    def test_uniqueness_check_runs_before_expensive_import_sql(
        self, fsq_parquet, tmp_path, monkeypatch
    ):
        """_assert_unique_key must raise BEFORE osm_import.sql/
        overture_place_import.sql/foursquare_import.sql runs -- that
        ordering is the entire point of the guard (fail fast, before hours
        of downstream work). Spy on stages._run_sql (the function that runs
        the import SQL) to prove it is never called when the density lookup
        table fails the uniqueness check."""
        import garganorn.stages as stages_mod

        density_path = tmp_path / "dup_density_ordering.parquet"
        _write_duplicate_density_parquet(density_path)
        places_parquet = str(tmp_path / "places_dup_density_ordering.parquet")

        calls = []
        real_run_sql = stages_mod._run_sql

        def spy_run_sql(*args, **kwargs):
            calls.append((args, kwargs))
            return real_run_sql(*args, **kwargs)

        monkeypatch.setattr(stages_mod, "_run_sql", spy_run_sql)

        with pytest.raises(ValueError):
            stages_mod.stage_import(
                "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                places_parquet, memory_limit="4GB", force=True,
                density_parquet=str(density_path),
            )

        assert not calls, (
            "_run_sql (which runs the import SQL) must not be called when "
            f"the uniqueness check fails; got {len(calls)} call(s)"
        )

    def test_unique_keys_do_not_raise(self, fsq_parquet, density_parquet, tmp_path):
        """Sanity check: clean lookup tables (the production contract,
        guaranteed unique by density_extract.sql's GROUP BY) must not trip
        the uniqueness guard, and the density lookup must actually have been
        used -- an implementation that silently loaded density_tiles empty
        would also pass a bare os.path.exists() check."""
        places_parquet = str(tmp_path / "places_clean.parquet")
        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     places_parquet, memory_limit="4GB", force=True,
                     density_parquet=density_parquet)
        assert os.path.exists(places_parquet)

        con = duckdb.connect()
        max_importance = con.execute(
            f"SELECT max(importance) FROM read_parquet('{places_parquet}')"
        ).fetchone()[0]
        con.close()
        assert max_importance is not None and max_importance > 0, (
            f"expected at least one place with importance > 0 (proving the "
            f"density lookup was actually joined in, not loaded empty); got "
            f"max(importance) = {max_importance!r}"
        )

    def test_does_not_materialize_the_parquet(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        """The uniqueness count must query read_parquet(density_parquet)
        directly so DuckDB streams the single column it needs via
        projection pushdown, rather than materializing the whole file into
        a TEMP TABLE first."""
        import garganorn.stages as stages_mod

        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)
        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path / "places_stream.parquet"), memory_limit="4GB",
                     force=True, density_parquet=density_parquet)

        assert not any("_density_uniqueness_check" in s for s in statements), (
            f"density_parquet must not be materialized into a TEMP TABLE "
            f"before the uniqueness count runs; statements: {statements}"
        )
        count_stmt = next(s for s in statements if "count(DISTINCT tile_qk15)" in s)
        assert "read_parquet(" in count_stmt, (
            f"the uniqueness count must query read_parquet(...) directly; "
            f"got: {count_stmt!r}"
        )

    def test_memory_limit_set_before_any_query(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        """memory_limit must be set on the uniqueness-check's own connection
        before any query runs on it, so the count itself is bounded rather
        than running at DuckDB's ~80%-of-RAM default."""
        import garganorn.stages as stages_mod

        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)
        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path / "places_memlimit.parquet"), memory_limit="4GB",
                     force=True, density_parquet=density_parquet)

        assert statements, "expected at least one statement recorded"
        assert "SET memory_limit" in statements[0] and "4GB" in statements[0], (
            f"the uniqueness-check connection must SET memory_limit before "
            f"any query runs on it; first statement was: {statements[0]!r}"
        )


# ---------------------------------------------------------------------------
# max_temp_directory_size bounds DuckDB's spill so a runaway query fails fast
# with a clear "temp directory full" error instead of consuming every byte on
# the volume. Configuring temp_directory alone only chooses WHERE spill goes;
# without a size bound the ceiling is whatever free space that filesystem
# happens to have.
#
# max_temp_directory_size is an independent DuckDB setting: every stage
# guards it on its own truthiness only, applying it whether or not
# temp_directory is also configured, so an unconfigured temp_directory never
# leaves spill unbounded at DuckDB's default location.
# ---------------------------------------------------------------------------

def _spy_on_duckdb_connect(monkeypatch, module):
    """Record every SQL statement executed on connections opened by `module`.

    Returns the list that accumulates statements. The connection is a
    transparent proxy: __getattr__ forwards everything untouched, so the
    stage under test behaves exactly as it would unspied.
    """
    real_connect = module.duckdb.connect
    statements = []

    class _RecordingConn:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *a, **kw):
            statements.append(sql)
            return self._real.execute(sql, *a, **kw)

        def __getattr__(self, name):
            return getattr(self._real, name)

    monkeypatch.setattr(
        module.duckdb, "connect", lambda *a, **kw: _RecordingConn(real_connect(*a, **kw))
    )
    return statements


def _max_temp_stmts(statements):
    return [s for s in statements if "SET max_temp_directory_size" in s]


def _build_export_inputs(fsq_parquet, tmp_path, subdir):
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


class TestAssertDensityParquetUniqueMaxTempDirectorySize:
    """_assert_density_parquet_unique opens its own duckdb.connect(":memory:")
    ahead of stage_import's dispatch (I3's single-placement guard) and must
    bound its spill the same way every other import-pipeline connection
    does, threaded from stage_import's own temp_directory/
    max_temp_directory_size."""

    @staticmethod
    def _statements_before_density_check(statements):
        """Statements recorded strictly before _assert_density_parquet_unique's
        own uniqueness query -- isolates ITS connection's SET calls from
        stage_import's own (later) connection, since _spy_on_duckdb_connect
        records every execute() across every connection into one flat,
        time-ordered list."""
        create_idx = next(
            i for i, s in enumerate(statements) if "count(DISTINCT tile_qk15)" in s
        )
        return statements[:create_idx]

    def test_applies_caller_supplied_value(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        import garganorn.stages as stages_mod

        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)
        scratch = tmp_path / "scratch_adpu"
        scratch.mkdir()

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path / "places_adpu.parquet"), memory_limit="4GB",
                     force=True, density_parquet=density_parquet,
                     temp_directory=str(scratch), max_temp_directory_size="8GB")

        before = self._statements_before_density_check(statements)
        stmts = _max_temp_stmts(before)
        assert stmts, (
            f"_assert_density_parquet_unique's OWN connection must bound "
            f"spill when stage_import redirects temp_directory; no SET "
            f"max_temp_directory_size recorded before its CREATE TEMP "
            f"TABLE. Statements before: {before}"
        )
        assert "8GB" in stmts[0], (
            f"must apply the caller-supplied bound (8GB); got: {stmts[0]!r}"
        )

    def test_bound_applied_when_temp_directory_omitted(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        """max_temp_directory_size is an independent DuckDB setting: it must
        bound spill even when temp_directory is never redirected, since spill
        then goes to DuckDB's own default location -- unbounded there is
        exactly the incident this setting exists to prevent."""
        import garganorn.stages as stages_mod

        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path / "places_adpu_notd.parquet"), memory_limit="4GB",
                     force=True, density_parquet=density_parquet)

        before = self._statements_before_density_check(statements)
        stmts = _max_temp_stmts(before)
        assert stmts, (
            f"with temp_directory omitted, _assert_density_parquet_unique must "
            f"still bound spill; no SET max_temp_directory_size recorded. "
            f"Statements before: {before}"
        )
        assert "250GB" in stmts[0], (
            f"default bound must be 250GB; got: {stmts[0]!r}"
        )


class TestStageImportMaxTempDirectorySize:
    """stage_import bounds spill on its own connection whenever it redirects
    spill (stages.py:1006-1009)."""

    def test_applies_caller_supplied_value(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        import garganorn.stages as stages_mod

        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)
        scratch = tmp_path / "scratch_mtds"
        scratch.mkdir()

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path / "places_mtds.parquet"), memory_limit="4GB",
                     force=True, density_parquet=density_parquet,
                     temp_directory=str(scratch), max_temp_directory_size="7GB")

        stmts = _max_temp_stmts(statements)
        assert stmts, (
            f"stage_import must bound spill when it redirects temp_directory; "
            f"no SET max_temp_directory_size recorded. Statements: {statements}"
        )
        assert "7GB" in stmts[0], (
            f"must apply the caller-supplied bound (7GB); got: {stmts[0]!r}"
        )

    def test_applies_default_when_not_specified(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        """The default is a real bound, not None -- an unbounded default would
        make the setting opt-in and leave every existing caller exposed."""
        import garganorn.stages as stages_mod

        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)
        scratch = tmp_path / "scratch_mtds_def"
        scratch.mkdir()

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path / "places_mtds_def.parquet"), memory_limit="4GB",
                     force=True, density_parquet=density_parquet,
                     temp_directory=str(scratch))

        stmts = _max_temp_stmts(statements)
        assert stmts, f"expected a default bound; statements: {statements}"
        assert "250GB" in stmts[0], (
            f"default bound must be 250GB, matching config.yaml.example and "
            f"every stage signature; got: {stmts[0]!r}"
        )

    def test_explicit_none_disables_the_bound(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        """`if max_temp_directory_size:` lets a caller opt out deliberately."""
        import garganorn.stages as stages_mod

        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)
        scratch = tmp_path / "scratch_mtds_none"
        scratch.mkdir()

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path / "places_mtds_none.parquet"), memory_limit="4GB",
                     force=True, density_parquet=density_parquet,
                     temp_directory=str(scratch), max_temp_directory_size=None)

        assert not _max_temp_stmts(statements), (
            f"max_temp_directory_size=None must issue no bound; got: "
            f"{_max_temp_stmts(statements)}"
        )

    def test_bound_applied_when_temp_directory_omitted(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        """max_temp_directory_size must bound spill independently of
        temp_directory: a caller that never redirects spill still gets the
        bound applied, so DuckDB's own default temp location is capped
        rather than limited only by free disk -- the exact shape of the
        production incident (506 GiB filled on /) this setting exists to
        prevent."""
        import garganorn.stages as stages_mod

        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)

        stage_import("foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
                     str(tmp_path / "places_mtds_notd.parquet"), memory_limit="4GB",
                     force=True, density_parquet=density_parquet)

        stmts = _max_temp_stmts(statements)
        assert stmts, (
            f"with temp_directory omitted, stage_import must still bound "
            f"spill; no SET max_temp_directory_size recorded. Statements: "
            f"{statements}"
        )
        assert "250GB" in stmts[0], (
            f"default bound must be 250GB; got: {stmts[0]!r}"
        )


class TestStageExportMaxTempDirectorySize:
    """stage_export always owns a spill dir, so it always bounds it."""

    def test_bounds_spill_even_without_temp_directory(
        self, fsq_parquet, tmp_path, monkeypatch
    ):
        """Unlike stage_import, stage_export redirects spill unconditionally
        (to run_dir + '.spill'), so it must bound it unconditionally too."""
        import garganorn.stages as stages_mod

        places_parquet, ta_parquet, containment_dir = _build_export_inputs(
            fsq_parquet, tmp_path, "inputs_mtds1"
        )
        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)

        stage_export("foursquare", places_parquet, ta_parquet, containment_dir,
                     str(tmp_path / "tiles_mtds1"), time.monotonic(),
                     memory_limit="4GB", force=True)

        stmts = _max_temp_stmts(statements)
        assert stmts, (
            f"stage_export spills to its own dir on every call and must bound "
            f"it even when no temp_directory is supplied. Statements: {statements}"
        )
        assert "250GB" in stmts[0], f"expected the 250GB default; got: {stmts[0]!r}"

    def test_applies_caller_supplied_value(
        self, fsq_parquet, tmp_path, monkeypatch
    ):
        import garganorn.stages as stages_mod

        places_parquet, ta_parquet, containment_dir = _build_export_inputs(
            fsq_parquet, tmp_path, "inputs_mtds2"
        )
        scratch = tmp_path / "scratch_export_mtds"
        scratch.mkdir()
        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)

        stage_export("foursquare", places_parquet, ta_parquet, containment_dir,
                     str(tmp_path / "tiles_mtds2"), time.monotonic(),
                     memory_limit="4GB", force=True, temp_directory=str(scratch),
                     max_temp_directory_size="9GB")

        stmts = _max_temp_stmts(statements)
        assert stmts, f"no bound recorded; statements: {statements}"
        assert "9GB" in stmts[0], (
            f"must apply the caller-supplied bound (9GB); got: {stmts[0]!r}"
        )


class TestWriteManifestDbMaxTempDirectorySize:
    """write_manifest_db opens its own bare connection and runs a
    CREATE TABLE ... ORDER BY over the full tile_assignments parquet, AFTER
    stage_export's own bounded connection has closed. It must accept and
    apply the same temp_directory/max_temp_directory_size contract as
    stage_import (bound only applied when temp_directory is supplied)."""

    def test_applies_caller_supplied_value(self, fsq_parquet, tmp_path, monkeypatch):
        import garganorn.stages as stages_mod

        places_parquet, ta_parquet, containment_dir = _build_export_inputs(
            fsq_parquet, tmp_path, "inputs_wmdb1"
        )
        scratch = tmp_path / "scratch_wmdb1"
        scratch.mkdir()
        out_dir = tmp_path / "manifest_out1"
        out_dir.mkdir()
        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)

        write_manifest_db(ta_parquet, str(out_dir), "foursquare",
                          temp_directory=str(scratch), max_temp_directory_size="6GB")

        stmts = _max_temp_stmts(statements)
        assert stmts, (
            f"write_manifest_db must bound spill when it redirects "
            f"temp_directory; no SET max_temp_directory_size recorded. "
            f"Statements: {statements}"
        )
        assert "6GB" in stmts[0], (
            f"must apply the caller-supplied bound (6GB); got: {stmts[0]!r}"
        )

    def test_applies_default_when_not_specified(self, fsq_parquet, tmp_path, monkeypatch):
        import garganorn.stages as stages_mod

        places_parquet, ta_parquet, containment_dir = _build_export_inputs(
            fsq_parquet, tmp_path, "inputs_wmdb2"
        )
        scratch = tmp_path / "scratch_wmdb2"
        scratch.mkdir()
        out_dir = tmp_path / "manifest_out2"
        out_dir.mkdir()
        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)

        write_manifest_db(ta_parquet, str(out_dir), "foursquare",
                          temp_directory=str(scratch))

        stmts = _max_temp_stmts(statements)
        assert stmts, f"expected a default bound; statements: {statements}"
        assert "250GB" in stmts[0], (
            f"default bound must be 250GB, matching every other stage "
            f"signature; got: {stmts[0]!r}"
        )

    def test_explicit_none_disables_the_bound(self, fsq_parquet, tmp_path, monkeypatch):
        import garganorn.stages as stages_mod

        places_parquet, ta_parquet, containment_dir = _build_export_inputs(
            fsq_parquet, tmp_path, "inputs_wmdb3"
        )
        scratch = tmp_path / "scratch_wmdb3"
        scratch.mkdir()
        out_dir = tmp_path / "manifest_out3"
        out_dir.mkdir()
        statements = _spy_on_duckdb_connect(monkeypatch, stages_mod)

        write_manifest_db(ta_parquet, str(out_dir), "foursquare",
                          temp_directory=str(scratch), max_temp_directory_size=None)

        assert not _max_temp_stmts(statements), (
            f"max_temp_directory_size=None must issue no bound; got: "
            f"{_max_temp_stmts(statements)}"
        )

    def test_stage_export_threads_its_own_values_to_write_manifest_db(
        self, fsq_parquet, tmp_path, monkeypatch
    ):
        """stage_export must pass its own temp_directory/max_temp_directory_size
        through to write_manifest_db so the manifest step spills where the
        export did, rather than to DuckDB's unbounded default location."""
        import garganorn.stages as stages_mod

        places_parquet, ta_parquet, containment_dir = _build_export_inputs(
            fsq_parquet, tmp_path, "inputs_wmdb4"
        )
        scratch = tmp_path / "scratch_wmdb4"
        scratch.mkdir()

        calls = []
        real_write_manifest_db = stages_mod.write_manifest_db

        def spy_write_manifest_db(*a, **kw):
            calls.append(kw)
            return real_write_manifest_db(*a, **kw)

        monkeypatch.setattr(stages_mod, "write_manifest_db", spy_write_manifest_db)

        stage_export("foursquare", places_parquet, ta_parquet, containment_dir,
                     str(tmp_path / "tiles_wmdb4"), time.monotonic(),
                     memory_limit="4GB", force=True, temp_directory=str(scratch),
                     max_temp_directory_size="12GB")

        assert calls, "stage_export must have called write_manifest_db"
        assert calls[-1].get("temp_directory") == str(scratch), (
            f"stage_export's temp_directory must reach write_manifest_db; "
            f"got kwargs={calls[-1]}"
        )
        assert calls[-1].get("max_temp_directory_size") == "12GB", (
            f"stage_export's max_temp_directory_size must reach "
            f"write_manifest_db; got kwargs={calls[-1]}"
        )


class TestMaxTempDirectorySizeThreading:
    """The bound is useless if a caller in the chain drops it. Every path that
    reaches a spilling stage must carry it through: run_pipeline, the config
    file, and the CLI flags."""

    def test_run_pipeline_threads_it_to_stage_export(
        self, fsq_parquet, density_parquet, tmp_path, monkeypatch
    ):
        import garganorn.quadtree as quadtree_mod

        calls = []
        monkeypatch.setattr(
            quadtree_mod, "stage_export",
            lambda *a, **kw: (calls.append(kw), str(tmp_path / "fake_run_dir"))[1],
        )

        quadtree_mod.run_pipeline(
            "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85),
            str(tmp_path / "output_mtds"), memory_limit="4GB", max_per_tile=100,
            density_parquet=density_parquet, max_temp_directory_size="11GB",
        )

        assert calls, "stage_export must have been called"
        assert calls[-1].get("max_temp_directory_size") == "11GB", (
            f"run_pipeline must thread max_temp_directory_size through to "
            f"stage_export; got kwargs={calls[-1]}"
        )

    def test_covering_cli_flag_reaches_stage_covering(self, tmp_path, monkeypatch):
        """--max-temp-directory-size on the covering subcommand must reach
        stage_covering, not be parsed and dropped."""
        import garganorn.quadtree as quadtree_mod

        calls = []
        monkeypatch.setattr(
            quadtree_mod, "stage_covering", lambda *a, **kw: calls.append(kw)
        )

        args = argparse.Namespace(
            boundaries=str(tmp_path / "b.duckdb"), output=str(tmp_path / "cov"),
            force=True, memory_limit=None, temp_directory=None,
            max_temp_directory_size="13GB", min_zoom=None, max_zoom=None,
        )
        quadtree_mod._cmd_covering(args)

        assert calls, "stage_covering must have been called"
        assert calls[-1].get("max_temp_directory_size") == "13GB", (
            f"the covering subcommand parses --max-temp-directory-size but did "
            f"not pass it on; got kwargs={calls[-1]}"
        )

    def test_config_file_value_reaches_run_pipeline(self, tmp_path, monkeypatch):
        """`_cmd_all` is the global-run entry point -- the one that crashed at
        506 GiB. Its bound comes from the config file, so a dropped value there
        is the highest-consequence version of this defect."""
        import garganorn.quadtree as quadtree_mod

        config = tmp_path / "config.yaml"
        config.write_text(
            "pipeline:\n"
            f"  output: {tmp_path / 'out'}\n"
            "  memory_limit: 4GB\n"
            "  max_temp_directory_size: 17GB\n"
            "  sources:\n"
            "    foursquare:\n"
            "      parquet: /nonexistent/*.parquet\n"
        )

        calls = []
        monkeypatch.setattr(
            quadtree_mod, "run_pipeline", lambda *a, **kw: calls.append(kw)
        )
        for name in ("stage_density_extract", "stage_idf"):
            if hasattr(quadtree_mod, name):
                monkeypatch.setattr(quadtree_mod, name, lambda *a, **kw: None)

        quadtree_mod._cmd_all(argparse.Namespace(config=str(config), force=False))

        assert calls, "_cmd_all must have called run_pipeline"
        assert calls[-1].get("max_temp_directory_size") == "17GB", (
            f"the configured max_temp_directory_size must reach run_pipeline; "
            f"got kwargs={calls[-1]}"
        )

    def test_config_default_when_absent(self, tmp_path, monkeypatch):
        """A config that predates the setting must still get a bound."""
        import garganorn.quadtree as quadtree_mod

        config = tmp_path / "config_nodefault.yaml"
        config.write_text(
            "pipeline:\n"
            f"  output: {tmp_path / 'out2'}\n"
            "  memory_limit: 4GB\n"
            "  sources:\n"
            "    foursquare:\n"
            "      parquet: /nonexistent/*.parquet\n"
        )

        calls = []
        monkeypatch.setattr(
            quadtree_mod, "run_pipeline", lambda *a, **kw: calls.append(kw)
        )
        for name in ("stage_density_extract", "stage_idf"):
            if hasattr(quadtree_mod, name):
                monkeypatch.setattr(quadtree_mod, name, lambda *a, **kw: None)

        quadtree_mod._cmd_all(argparse.Namespace(config=str(config), force=False))

        assert calls, "_cmd_all must have called run_pipeline"
        assert calls[-1].get("max_temp_directory_size") == "250GB", (
            f"a config without max_temp_directory_size must fall back to the "
            f"250GB default, not to no bound; got kwargs={calls[-1]}"
        )

    @staticmethod
    def _cmd_run_args(**overrides):
        """Minimal argparse.Namespace for _cmd_run with a foursquare source,
        matching what the `run` subparser would produce."""
        defaults = dict(
            source="foursquare", parquet="/nonexistent/*.parquet",
            parquet_dir=None, division_parquet=None, division_area_parquet=None,
            output="/nonexistent/out", bbox=None, config=None,
            memory_limit=None, max_per_tile=None, boundaries=None,
            export_workers=None, density_parquet=None, idf_parquet=None,
            temp_directory=None, max_temp_directory_size=None, force=False,
        )
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def test_cmd_run_config_file_temp_directory_reaches_run_pipeline(
        self, tmp_path, monkeypatch
    ):
        """`_cmd_run` is a separate entry point from `_cmd_all` and must read
        temp_directory from the pipeline: config section on its own, not only
        via --temp-directory."""
        import garganorn.quadtree as quadtree_mod

        scratch = tmp_path / "cfg_scratch"
        config = tmp_path / "config.yaml"
        config.write_text(
            "pipeline:\n"
            f"  temp_directory: {scratch}\n"
        )

        calls = []
        monkeypatch.setattr(
            quadtree_mod, "run_pipeline", lambda *a, **kw: calls.append(kw)
        )

        args = self._cmd_run_args(config=str(config))
        quadtree_mod._cmd_run(args, run_parser=None)

        assert calls, "_cmd_run must have called run_pipeline"
        assert calls[-1].get("temp_directory") == str(scratch), (
            f"the configured temp_directory must reach run_pipeline via "
            f"_cmd_run; got kwargs={calls[-1]}"
        )

    def test_cmd_run_config_file_max_temp_directory_size_reaches_run_pipeline(
        self, tmp_path, monkeypatch
    ):
        import garganorn.quadtree as quadtree_mod

        config = tmp_path / "config2.yaml"
        config.write_text(
            "pipeline:\n"
            "  max_temp_directory_size: 19GB\n"
        )

        calls = []
        monkeypatch.setattr(
            quadtree_mod, "run_pipeline", lambda *a, **kw: calls.append(kw)
        )

        args = self._cmd_run_args(config=str(config))
        quadtree_mod._cmd_run(args, run_parser=None)

        assert calls, "_cmd_run must have called run_pipeline"
        assert calls[-1].get("max_temp_directory_size") == "19GB", (
            f"the configured max_temp_directory_size must reach run_pipeline "
            f"via _cmd_run; got kwargs={calls[-1]}"
        )

    def test_cmd_run_cli_flag_overrides_config(self, tmp_path, monkeypatch):
        """Explicit CLI args must win over the config file, matching the
        precedence already used for memory_limit/max_per_tile in _cmd_run."""
        import garganorn.quadtree as quadtree_mod

        config = tmp_path / "config3.yaml"
        config.write_text(
            "pipeline:\n"
            "  temp_directory: /from/config\n"
            "  max_temp_directory_size: 19GB\n"
        )

        calls = []
        monkeypatch.setattr(
            quadtree_mod, "run_pipeline", lambda *a, **kw: calls.append(kw)
        )

        args = self._cmd_run_args(
            config=str(config), temp_directory="/from/cli",
            max_temp_directory_size="5GB",
        )
        quadtree_mod._cmd_run(args, run_parser=None)

        assert calls, "_cmd_run must have called run_pipeline"
        assert calls[-1].get("temp_directory") == "/from/cli"
        assert calls[-1].get("max_temp_directory_size") == "5GB"

    def test_cmd_run_config_default_when_absent(self, tmp_path, monkeypatch):
        """A config that predates the setting -- or no --config at all --
        must still get the 250GB bound."""
        import garganorn.quadtree as quadtree_mod

        calls = []
        monkeypatch.setattr(
            quadtree_mod, "run_pipeline", lambda *a, **kw: calls.append(kw)
        )

        args = self._cmd_run_args()
        quadtree_mod._cmd_run(args, run_parser=None)

        assert calls, "_cmd_run must have called run_pipeline"
        assert calls[-1].get("max_temp_directory_size") == "250GB", (
            f"absent config and CLI flag must fall back to the 250GB "
            f"default, not to no bound; got kwargs={calls[-1]}"
        )

    def test_cmd_run_empty_string_cli_flag_disables_bound(
        self, tmp_path, monkeypatch
    ):
        """C3: `--max-temp-directory-size ""` must disable the bound, not
        silently become 250GB. The `or "250GB"` form in _cmd_run treated
        empty string as falsy; `is not None` (matching _cmd_density,
        _cmd_idf, _cmd_covering) preserves the caller's explicit choice."""
        import garganorn.quadtree as quadtree_mod

        calls = []
        monkeypatch.setattr(
            quadtree_mod, "run_pipeline", lambda *a, **kw: calls.append(kw)
        )

        args = self._cmd_run_args(max_temp_directory_size="")
        quadtree_mod._cmd_run(args, run_parser=None)

        assert calls, "_cmd_run must have called run_pipeline"
        assert calls[-1].get("max_temp_directory_size") == "", (
            f"an explicit empty string must reach run_pipeline unchanged, "
            f"not be replaced with the 250GB default; got kwargs={calls[-1]}"
        )
