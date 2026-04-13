"""Tests for garganorn.stages module functions.

Phase 1 tests: these tests FAIL because garganorn.stages doesn't exist yet.
After implementation, they should pass.

Tests are organized by stage function:
- TestStageImport: tests for stage_import()
- TestStageImportance: tests for stage_importance()
- TestStageTileAssignment: tests for stage_tile_assignment()
- TestStageOrchestration: integration test verifying stages match run_pipeline()
"""
import time
import json
import gzip
import logging
import os
from pathlib import Path

import pytest
import duckdb

# These imports will fail with ImportError until garganorn.stages is implemented
from garganorn.stages import (
    stage_import,
    stage_importance,
    stage_variants,
    stage_tile_assignment,
    stage_containment,
    stage_export,
    stage_manifest,
    stage_boundary_export,
)
from garganorn.database import FoursquareOSP, OverturePlaces, OpenStreetMap, OvertureDivisions
from garganorn.quadtree import run_pipeline, SOURCES


# ---------------------------------------------------------------------------
# TestStageImport
# ---------------------------------------------------------------------------

class TestStageImport:
    """Tests for stage_import function which loads parquet data into places table."""

    def test_creates_places_table_fsq(self, fsq_parquet, tmp_path):
        """stage_import with source='foursquare' creates a populated places table."""
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        stage_import(con, "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)

        count = con.execute("SELECT count(*) FROM places").fetchone()[0]
        assert count > 0, "places table should have rows after import"
        con.close()

    def test_creates_places_table_overture(self, overture_parquet, tmp_path):
        """stage_import with source='overture_place' creates a populated places table."""
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        stage_import(con, "overture_place", overture_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)

        count = con.execute("SELECT count(*) FROM places").fetchone()[0]
        assert count > 0, "places table should have rows after import"
        con.close()

    def test_creates_places_table_osm(self, osm_parquet, tmp_path):
        """stage_import with source='osm' creates a populated places table.

        OSM import requires a tuple of (node_parquet, way_parquet) paths.
        """
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        parquet_tuple = (osm_parquet["node"], osm_parquet["way"])
        stage_import(con, "osm", parquet_tuple, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)

        count = con.execute("SELECT count(*) FROM places").fetchone()[0]
        assert count > 0, "places table should have rows after import"
        con.close()

    def test_bbox_filter_fsq(self, fsq_parquet, density_parquet, tmp_path):
        """stage_import with bbox filters records to bounding box.

        Verify that the count matches what run_pipeline produces with the same bbox.
        Uses manifest.json to get the pipeline's total record count since run_pipeline
        deletes the working DuckDB after completion.
        """
        bbox = (-122.55, 37.60, -122.30, 37.85)

        # First, run stage_import and count
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()
        stage_import(con, "foursquare", fsq_parquet, bbox, "4GB", t0)
        stage_count = con.execute("SELECT count(*) FROM places").fetchone()[0]
        con.close()

        # Then, run full pipeline and read total count from manifest.duckdb
        output_dir = str(tmp_path / "output")
        run_pipeline("foursquare", fsq_parquet, bbox, output_dir, memory_limit="4GB", density_parquet=density_parquet)

        current_link = Path(output_dir) / "foursquare" / "current"
        timestamp_dir = os.readlink(str(current_link))
        manifest_db_path = Path(output_dir) / "foursquare" / timestamp_dir / "manifest.duckdb"
        con_manifest = duckdb.connect(str(manifest_db_path), read_only=True)
        pipeline_count = con_manifest.execute("SELECT count(*) FROM record_tiles").fetchone()[0]
        con_manifest.close()

        assert stage_count == pipeline_count, (
            f"stage_import count ({stage_count}) should match "
            f"run_pipeline count ({pipeline_count})"
        )


# ---------------------------------------------------------------------------
# TestStageImportance
# ---------------------------------------------------------------------------

class TestStageImportance:
    """Tests for stage_importance function which adds importance column."""

    def test_adds_importance_fsq(self, fsq_parquet, density_parquet, tmp_path):
        """stage_importance for foursquare adds importance column with non-zero values."""
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        # First import
        stage_import(con, "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)

        # Then add importance
        stage_importance(con, "foursquare", t0, density_parquet)

        # Verify importance column exists and has non-zero values
        rows = con.execute(
            "SELECT COUNT(*) FROM places WHERE importance > 0"
        ).fetchone()[0]
        assert rows > 0, "Some places should have importance > 0"
        con.close()

    def test_skipped_for_division(self, overture_parquet, density_parquet, tmp_path):
        """stage_importance works correctly for non-division sources.

        The guard is in the caller (run_pipeline), but stage_importance itself
        should work when called for any source that has a places table.
        """
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        # Import overture_place data
        stage_import(con, "overture_place", overture_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)

        # Add importance (this should work)
        stage_importance(con, "overture_place", t0, density_parquet)

        # Verify importance column exists
        rows = con.execute(
            "SELECT COUNT(*) FROM places WHERE importance IS NOT NULL"
        ).fetchone()[0]
        assert rows > 0, "All places should have importance set"
        con.close()


# ---------------------------------------------------------------------------
# TestStageTileAssignment
# ---------------------------------------------------------------------------

class TestStageTileAssignment:
    """Tests for stage_tile_assignment function."""

    def test_creates_tile_assignments(self, fsq_parquet, density_parquet, tmp_path):
        """stage_tile_assignment creates tile_assignments table after import + importance."""
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        # Run import + importance
        stage_import(con, "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)
        stage_importance(con, "foursquare", t0, density_parquet)
        stage_variants(con, "foursquare", t0)

        # Run tile assignment
        pk_expr = SOURCES["foursquare"].source_pk  # "fsq_place_id"
        stage_tile_assignment(con, "foursquare", pk_expr, 100, t0)

        # Verify tile_assignments table exists
        count = con.execute("SELECT count(*) FROM tile_assignments").fetchone()[0]
        assert count > 0, "tile_assignments table should have rows"
        con.close()


# ---------------------------------------------------------------------------
# TestStageOrchestration
# ---------------------------------------------------------------------------

class TestStageOrchestration:
    """Integration tests verifying that calling stages in sequence matches run_pipeline."""

    def _read_tile_records(self, tile_dir):
        """Read all tile JSON files and return a dict of qk -> records list."""
        records_by_qk = {}
        for json_path in Path(tile_dir).rglob("*.json.gz"):
            with gzip.open(json_path, "rt") as f:
                data = json.load(f)
                qk = json_path.stem
                records_by_qk[qk] = data["records"]
        return records_by_qk

    def test_stages_match_run_pipeline(self, fsq_parquet, density_parquet, tmp_path):
        """Calling stages manually produces tile record content identical to run_pipeline.

        Manifest timestamps may differ, but tile record content must match.
        """
        bbox = (-122.55, 37.60, -122.30, 37.85)

        # Create two output directories
        stages_dir = tmp_path / "stages_output"
        pipeline_dir = tmp_path / "pipeline_output"
        os.makedirs(stages_dir, exist_ok=True)
        os.makedirs(pipeline_dir, exist_ok=True)

        # Run stages manually
        stages_tile_dir = os.path.join(stages_dir, "foursquare", "20260411T120000")
        os.makedirs(stages_tile_dir, exist_ok=True)
        stages_db = os.path.join(stages_tile_dir, ".foursquare_work.duckdb")
        con = duckdb.connect(stages_db)
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        # Import + importance + variants
        stage_import(con, "foursquare", fsq_parquet, bbox, "4GB", t0)
        stage_importance(con, "foursquare", t0, density_parquet)
        stage_variants(con, "foursquare", t0)

        # Tile assignment
        pk_expr = SOURCES["foursquare"].source_pk
        stage_tile_assignment(con, "foursquare", pk_expr, 100, t0)

        # Containment (no boundaries_db for this test)
        lon_expr, lat_expr = "p.longitude", "p.latitude"
        stage_containment(con, "foursquare", pk_expr, lon_expr, lat_expr, None, t0)

        # Export
        manifest = stage_export(con, "foursquare", stages_tile_dir, t0, None)

        # Manifest
        stage_manifest(con, manifest, "foursquare", stages_tile_dir, t0)

        con.close()

        # Run full pipeline for comparison
        run_pipeline("foursquare", fsq_parquet, bbox, str(pipeline_dir), memory_limit="4GB", density_parquet=density_parquet)

        # Compare tile records
        stages_records = self._read_tile_records(stages_tile_dir)

        pipeline_current = Path(pipeline_dir) / "foursquare" / "current"
        pipeline_timestamp = os.readlink(str(pipeline_current))
        pipeline_tile_dir = Path(pipeline_dir) / "foursquare" / pipeline_timestamp
        pipeline_records = self._read_tile_records(pipeline_tile_dir)

        # Compare record counts per tile
        stages_qks = set(stages_records.keys())
        pipeline_qks = set(pipeline_records.keys())

        assert stages_qks == pipeline_qks, (
            f"Tile quadkeys differ: stages has {stages_qks - pipeline_qks}, "
            f"pipeline has {pipeline_qks - stages_qks}"
        )

        # Compare record content for each tile
        for qk in sorted(stages_qks):
            stages_count = len(stages_records[qk])
            pipeline_count = len(pipeline_records[qk])
            assert stages_count == pipeline_count, (
                f"Tile {qk}: stages has {stages_count} records, "
                f"pipeline has {pipeline_count} records"
            )

            # Verify at least one record matches (spot check)
            if stages_records[qk] and pipeline_records[qk]:
                stages_rkey = stages_records[qk][0].get("rkey")
                pipeline_rkey = pipeline_records[qk][0].get("rkey")
                assert stages_rkey == pipeline_rkey, (
                    f"Tile {qk}: first record rkey differs: "
                    f"{stages_rkey} vs {pipeline_rkey}"
                )


# ---------------------------------------------------------------------------
# TestStageContainment
# ---------------------------------------------------------------------------

class TestStageContainment:
    """Tests for stage_containment function."""

    def test_creates_place_containment(self, fsq_parquet, density_parquet, division_db_path, tmp_path):
        """stage_containment creates place_containment table when boundaries_db is provided."""
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        # Run up to tile assignment
        stage_import(con, "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)
        stage_importance(con, "foursquare", t0, density_parquet)
        stage_variants(con, "foursquare", t0)
        pk_expr = SOURCES["foursquare"].source_pk
        stage_tile_assignment(con, "foursquare", pk_expr, 100, t0)

        # Run containment with boundaries
        lon_expr, lat_expr = "p.longitude", "p.latitude"
        stage_containment(con, "foursquare", pk_expr, lon_expr, lat_expr, str(division_db_path), t0)

        # Verify place_containment table exists
        count = con.execute("SELECT count(*) FROM place_containment").fetchone()[0]
        assert count >= 0, "place_containment table should exist"
        con.close()

    def test_no_boundaries_creates_empty_table(self, fsq_parquet, density_parquet, tmp_path):
        """stage_containment with boundaries_db=None creates empty place_containment table."""
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        # Run up to tile assignment
        stage_import(con, "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)
        stage_importance(con, "foursquare", t0, density_parquet)
        stage_variants(con, "foursquare", t0)
        pk_expr = SOURCES["foursquare"].source_pk
        stage_tile_assignment(con, "foursquare", pk_expr, 100, t0)

        # Run containment without boundaries
        lon_expr, lat_expr = "p.longitude", "p.latitude"
        stage_containment(con, "foursquare", pk_expr, lon_expr, lat_expr, None, t0)

        # Verify place_containment table exists but is empty
        count = con.execute("SELECT count(*) FROM place_containment").fetchone()[0]
        assert count == 0, "place_containment should be empty when boundaries_db is None"
        con.close()


# ---------------------------------------------------------------------------
# TestStageExport
# ---------------------------------------------------------------------------

class TestStageExport:
    """Tests for stage_export function."""

    def test_writes_gzipped_tiles(self, fsq_parquet, density_parquet, tmp_path):
        """stage_export writes gzipped JSON tile files to output directory."""
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        # Run up to containment
        stage_import(con, "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)
        stage_importance(con, "foursquare", t0, density_parquet)
        stage_variants(con, "foursquare", t0)
        pk_expr = SOURCES["foursquare"].source_pk
        stage_tile_assignment(con, "foursquare", pk_expr, 100, t0)
        lon_expr, lat_expr = "p.longitude", "p.latitude"
        stage_containment(con, "foursquare", pk_expr, lon_expr, lat_expr, None, t0)

        # Export tiles
        tile_dir = str(tmp_path / "tiles")
        os.makedirs(tile_dir, exist_ok=True)
        manifest = stage_export(con, "foursquare", tile_dir, t0, None)

        # Verify manifest is non-empty
        assert len(manifest) > 0, "manifest should have at least one tile"
        assert all(isinstance(k, str) for k in manifest.keys()), "manifest keys should be strings (quadkeys)"
        assert all(isinstance(v, int) for v in manifest.values()), "manifest values should be ints (record counts)"

        # Verify at least one .json.gz file exists
        gz_files = list(Path(tile_dir).rglob("*.json.gz"))
        assert len(gz_files) > 0, "at least one .json.gz file should be written"
        con.close()


# ---------------------------------------------------------------------------
# TestStageManifest
# ---------------------------------------------------------------------------

class TestStageManifest:
    """Tests for stage_manifest function."""

    def test_writes_manifest_json(self, fsq_parquet, density_parquet, tmp_path):
        """stage_manifest writes manifest.json with expected structure."""
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        # Run up to export
        stage_import(con, "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)
        stage_importance(con, "foursquare", t0, density_parquet)
        stage_variants(con, "foursquare", t0)
        pk_expr = SOURCES["foursquare"].source_pk
        stage_tile_assignment(con, "foursquare", pk_expr, 100, t0)
        lon_expr, lat_expr = "p.longitude", "p.latitude"
        stage_containment(con, "foursquare", pk_expr, lon_expr, lat_expr, None, t0)
        tile_dir = str(tmp_path / "tiles")
        os.makedirs(tile_dir, exist_ok=True)
        manifest = stage_export(con, "foursquare", tile_dir, t0, None)

        # Write manifest
        stage_manifest(con, manifest, "foursquare", tile_dir, t0)

        # Verify manifest.json exists
        manifest_path = Path(tile_dir) / "manifest.json"
        assert manifest_path.exists(), "manifest.json should be written"

        # Verify structure
        with open(manifest_path) as f:
            data = json.load(f)
        assert "source" in data, "manifest should have 'source' field"
        assert "generated_at" in data, "manifest should have 'generated_at' field"
        assert "quadkeys" in data, "manifest should have 'quadkeys' field"
        assert data["source"] == "foursquare", "source should match"
        assert isinstance(data["quadkeys"], list), "quadkeys should be a list"
        con.close()

    def test_writes_manifest_duckdb(self, fsq_parquet, density_parquet, tmp_path):
        """stage_manifest writes manifest.duckdb with record_tiles table."""
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        # Run up to export
        stage_import(con, "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)
        stage_importance(con, "foursquare", t0, density_parquet)
        stage_variants(con, "foursquare", t0)
        pk_expr = SOURCES["foursquare"].source_pk
        stage_tile_assignment(con, "foursquare", pk_expr, 100, t0)
        lon_expr, lat_expr = "p.longitude", "p.latitude"
        stage_containment(con, "foursquare", pk_expr, lon_expr, lat_expr, None, t0)
        tile_dir = str(tmp_path / "tiles")
        os.makedirs(tile_dir, exist_ok=True)
        manifest = stage_export(con, "foursquare", tile_dir, t0, None)

        # Write manifest
        stage_manifest(con, manifest, "foursquare", tile_dir, t0)

        # Verify manifest.duckdb exists
        manifest_db = Path(tile_dir) / "manifest.duckdb"
        assert manifest_db.exists(), "manifest.duckdb should be written"

        # Verify record_tiles table exists
        con_check = duckdb.connect(str(manifest_db))
        count = con_check.execute("SELECT count(*) FROM record_tiles").fetchone()[0]
        assert count > 0, "record_tiles table should have rows"
        con_check.close()
        con.close()


# ---------------------------------------------------------------------------
# TestStageDensityExtractMtime
# ---------------------------------------------------------------------------


class TestStageDensityExtractMtime:
    """Tests for mtime-based caching in stage_density_extract().

    These tests FAIL because the force parameter and mtime skip logic
    don't exist yet. This is TDD red phase.
    """

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
        """stage_density_extract skips when output mtime is newer than input.

        This test FAILS because mtime skip logic doesn't exist yet.
        """
        import time
        from garganorn.stages import stage_density_extract

        # First run: create output
        with caplog.at_level(logging.INFO):
            stage_density_extract(small_overture_parquet, density_output, time.monotonic())

        assert os.path.exists(density_output), "First run should create output"

        # Set output mtime to the future
        future_time = time.time() + 3600  # 1 hour in the future
        os.utime(density_output, (future_time, future_time))

        # Second run: should skip
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_density_extract(small_overture_parquet, density_output, time.monotonic())

        # Verify skip message was logged
        assert any("skip" in record.message.lower() for record in caplog.records), (
            "Should log skip message when output is fresh"
        )

    def test_runs_when_output_missing(self, small_overture_parquet, density_output, caplog):
        """stage_density_extract runs normally when output doesn't exist.

        This test FAILS because mtime skip logic doesn't exist yet.
        """
        import time
        from garganorn.stages import stage_density_extract

        assert not os.path.exists(density_output), "Output should not exist initially"

        with caplog.at_level(logging.INFO):
            stage_density_extract(small_overture_parquet, density_output, time.monotonic())

        assert os.path.exists(density_output), "Should create output when missing"
        # Verify it ran (not skipped)
        assert any("starting" in record.message.lower() for record in caplog.records), (
            "Should log starting message when output is missing"
        )

    def test_runs_when_input_newer(self, small_overture_parquet, density_output, caplog):
        """stage_density_extract re-runs when input parquet is newer than output.

        This test FAILS because mtime skip logic doesn't exist yet.
        """
        import time
        from garganorn.stages import stage_density_extract

        # First run: create output
        stage_density_extract(small_overture_parquet, density_output, time.monotonic())
        assert os.path.exists(density_output)

        # Touch input to make it newer
        time.sleep(0.01)  # Small delay to ensure mtime difference
        past_time = time.time() - 3600  # 1 hour ago
        os.utime(density_output, (past_time, past_time))

        # Second run: should re-run because input is newer
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_density_extract(small_overture_parquet, density_output, time.monotonic())

        # Verify it re-ran (not skipped)
        assert any("starting" in record.message.lower() for record in caplog.records), (
            "Should log starting message when input is newer"
        )

    def test_force_overrides_fresh(self, small_overture_parquet, density_output, caplog):
        """stage_density_extract with force=True re-runs even when output is fresh.

        This test FAILS because the force parameter doesn't exist yet.
        """
        import time
        from garganorn.stages import stage_density_extract

        # First run: create output
        stage_density_extract(small_overture_parquet, density_output, time.monotonic())
        assert os.path.exists(density_output)

        # Set output mtime to the future
        future_time = time.time() + 3600
        os.utime(density_output, (future_time, future_time))

        # Second run with force=True: should re-run despite fresh output
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_density_extract(small_overture_parquet, density_output, time.monotonic(), force=True)

        # Verify it re-ran (not skipped)
        assert any("starting" in record.message.lower() for record in caplog.records), (
            "Should log starting message when force=True"
        )


# ---------------------------------------------------------------------------
# TestStageBoundaryExport
# ---------------------------------------------------------------------------

class TestStageBoundaryExport:
    """Tests for stage_boundary_export function."""

    def test_writes_boundaries_duckdb(self, tmp_path, division_db_path):
        """stage_boundary_export writes boundaries.duckdb for overture_division source.

        This test creates a minimal places table with geometry and admin_level,
        then calls stage_boundary_export to verify boundaries.duckdb is created.
        """
        # Create a test places table with division-like data
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")

        # Create a minimal places table
        con.execute("""
            CREATE TABLE places (
                id VARCHAR,
                geometry GEOMETRY,
                admin_level INTEGER,
                names STRUCT("primary" VARCHAR),
                subtype VARCHAR,
                country VARCHAR,
                region VARCHAR,
                wikidata VARCHAR,
                population BIGINT,
                min_latitude DOUBLE,
                max_latitude DOUBLE,
                min_longitude DOUBLE,
                max_longitude DOUBLE,
                importance INTEGER,
                variants STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
            )
        """)

        # Insert test data
        con.execute("""
            INSERT INTO places VALUES (
                'test_div_1',
                ST_GeomFromText('POLYGON((-122.5 37.7, -122.5 37.8, -122.4 37.8, -122.4 37.7, -122.5 37.7))'),
                2,
                {'primary': 'Test Division'},
                'region',
                'US',
                'US-CA',
                'Q62',
                1000000,
                37.7, 37.8, -122.5, -122.4,
                50,
                []
            )
        """)

        con.close()

        # Now run boundary export
        source_dir = str(tmp_path / "output")
        os.makedirs(source_dir, exist_ok=True)
        t0 = time.monotonic()

        # Reconnect for the stage function
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        stage_boundary_export(con, "overture_division", source_dir, t0)
        con.close()

        # Verify boundaries.duckdb was created
        boundaries_path = Path(source_dir) / "boundaries.duckdb"
        assert boundaries_path.exists(), "boundaries.duckdb should be created"

        # Verify the boundaries DB has the expected structure
        con_check = duckdb.connect(str(boundaries_path))
        # Check places table exists
        count = con_check.execute("SELECT count(*) FROM places").fetchone()[0]
        assert count == 1, "boundaries.duckdb should have 1 place"
        # Check R-tree index exists
        indexes = con_check.execute("SELECT index_name FROM duckdb_indexes()").fetchall()
        index_names = [row[0] for row in indexes]
        assert "bnd_places_rtree" in index_names, "R-tree index should be created"
        con_check.close()

    def test_no_op_for_non_division_sources(self, fsq_parquet, tmp_path):
        """stage_boundary_export is a no-op for non-division sources.

        The function should not write boundaries.duckdb for sources other
        than overture_division.
        """
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        # Import FSQ data
        stage_import(con, "foursquare", fsq_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)

        # Call boundary export (should be no-op)
        source_dir = str(tmp_path / "output")
        os.makedirs(source_dir, exist_ok=True)
        stage_boundary_export(con, "foursquare", source_dir, t0)

        # Verify boundaries.duckdb was NOT created
        boundaries_path = Path(source_dir) / "boundaries.duckdb"
        assert not boundaries_path.exists(), (
            "boundaries.duckdb should NOT be created for non-division sources"
        )
        con.close()
