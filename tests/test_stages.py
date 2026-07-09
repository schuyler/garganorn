"""Tests for garganorn.stages module functions.

Tests are organized by stage function:
- TestStageTileAssignment: tests for stage_tile_assignment()
- TestStageContainment: tests for compute_containment()
- TestStageExport: tests for stage_export()
- TestStageManifest: tests for write_manifest() and write_manifest_db()
- TestStageDensityExtractMtime: mtime-based caching tests for stage_density_extract()
"""
import time
import json
import gzip
import logging
import os
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
