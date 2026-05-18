"""Tests for Phase 3: Extract boundary export into export_boundaries_db() function.

These tests FAIL because the export_boundaries_db() function doesn't exist yet.
This is TDD red phase.

Phase 3 changes:
- Extract boundary export logic from stage_boundary_export() into export_boundaries_db()
- export_boundaries_db(work_db_path, source_dir, t0) creates boundaries.duckdb without requiring an open connection
- stage_boundary_export() becomes a thin wrapper calling export_boundaries_db()
- boundary_export is NOT in STAGE_ORDER (it's a special case called directly in run_pipeline)
"""

import time
import os
from pathlib import Path

import duckdb
import pytest

from garganorn.stages import stage_boundary_export, export_boundaries_db, quadkey_to_bbox
from garganorn.quadtree import STAGE_ORDER


# ---------------------------------------------------------------------------
# TestExportBoundariesDb (Phase 3)
# ---------------------------------------------------------------------------

class TestExportBoundariesDb:
    """Tests for export_boundaries_db() function (Phase 3).

    The export_boundaries_db() function should:
    - Take a work_db_path (not an open connection)
    - Create boundaries.duckdb in source_dir
    - Include R-tree index on geometry
    - Use atomic write-to-temp-then-rename pattern
    """

    def test_function_is_importable(self):
        """export_boundaries_db must be importable from garganorn.stages.

        This test FAILS because export_boundaries_db doesn't exist yet.
        """
        from garganorn.stages import export_boundaries_db  # noqa: F401
        assert callable(export_boundaries_db), "export_boundaries_db should be callable"

    def test_creates_boundaries_duckdb_from_path(self, tmp_path):
        """export_boundaries_db creates boundaries.duckdb from a working DB path.

        This test FAILS because export_boundaries_db doesn't exist yet.

        The function should take a work_db_path (string path to a DuckDB file)
        and create boundaries.duckdb without requiring an open connection.
        """
        # Create a working DB with division places
        work_db_path = tmp_path / "test_work.duckdb"
        con = duckdb.connect(str(work_db_path))
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

        # Call export_boundaries_db with path (not connection)
        source_dir = str(tmp_path / "output")
        os.makedirs(source_dir, exist_ok=True)
        t0 = time.monotonic()

        export_boundaries_db(str(work_db_path), source_dir, t0)

        # Verify boundaries.duckdb was created
        boundaries_path = Path(source_dir) / "boundaries.duckdb"
        assert boundaries_path.exists(), "boundaries.duckdb should be created"

    def test_boundaries_has_rtree_index(self, tmp_path):
        """export_boundaries_db creates boundaries.duckdb with R-tree index.

        This test FAILS because export_boundaries_db doesn't exist yet.
        """
        # Create a working DB with division places
        work_db_path = tmp_path / "test_work.duckdb"
        con = duckdb.connect(str(work_db_path))
        con.execute("INSTALL spatial; LOAD spatial;")
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

        # Export boundaries
        source_dir = str(tmp_path / "output")
        os.makedirs(source_dir, exist_ok=True)
        export_boundaries_db(str(work_db_path), source_dir, time.monotonic())

        # Verify R-tree index exists
        boundaries_path = Path(source_dir) / "boundaries.duckdb"
        con_check = duckdb.connect(str(boundaries_path))
        indexes = con_check.execute("SELECT index_name FROM duckdb_indexes()").fetchall()
        index_names = [row[0] for row in indexes]
        assert "bnd_places_rtree" in index_names, "R-tree index should be created"
        con_check.close()

    def test_stage_boundary_export_wrapper(self, tmp_path):
        """stage_boundary_export still works as a thin wrapper.

        This test FAILS because stage_boundary_export signature needs to change.
        After Phase 3, stage_boundary_export(con, source, source_dir, t0) should
        extract the DB path from the connection and call export_boundaries_db().
        """
        # Create a working DB with division places
        work_db_path = tmp_path / "test_work.duckdb"
        con = duckdb.connect(str(work_db_path))
        con.execute("INSTALL spatial; LOAD spatial;")
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

        # Call stage_boundary_export with connection (backward compat)
        source_dir = str(tmp_path / "output")
        os.makedirs(source_dir, exist_ok=True)
        stage_boundary_export(con, "overture_division", source_dir, time.monotonic())
        con.close()

        # Verify boundaries.duckdb was created
        boundaries_path = Path(source_dir) / "boundaries.duckdb"
        assert boundaries_path.exists(), "boundaries.duckdb should be created by stage_boundary_export"

    def test_boundary_export_not_in_stage_order(self):
        """boundary_export should NOT be in STAGE_ORDER.

        This test FAILS because boundary_export might still be in STAGE_ORDER.
        After Phase 3, boundary_export is a special case called directly in
        run_pipeline, not part of the standard stage sequence.
        """
        assert "boundary_export" not in STAGE_ORDER, (
            "boundary_export should NOT be in STAGE_ORDER - it's a special case"
        )
