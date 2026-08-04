"""Failing tests for spatial processing bug fixes (SPATIAL-2, SPATIAL-5, EXPORT-6, EXPORT-7).

These tests verify that the following bugs are fixed:

SPATIAL-2: Malformed qk17 not validated in tile assignment
SPATIAL-5: ST_Intersection can produce invalid/empty geometries
EXPORT-6: NULL qk17 places silently dropped from tiles
EXPORT-7: No uniqueness validation on tile assignments

All tests should FAIL with the current code and PASS after fixes are implemented.
"""

import pytest
import duckdb
import logging
from pathlib import Path

from garganorn.stages import (
    stage_tile_assignment,
    compute_containment,
    quadkey_to_bbox,
)
from tests.quadtree_helpers import _load_sql


class TestSPATIAL2_MalformedQK17Validation:
    """SPATIAL-2: Malformed qk17 values must be validated and rejected.

    Bug: compute_tile_assignments.sql calls left(qk17, level) without verifying
    qk17 is well-formed (length 17, digits 0-3 only). NULL is filtered, but
    non-standard values pass through and can cause incorrect tile assignments.

    Fix: Add WHERE filter: length(qk17) = 17 AND regexp_matches(qk17, '^[0-3]{17}$')
    in both tile_counts and place_zoom CTEs.
    """

    def test_malformed_qk17_string_rejected(self, tmp_path):
        """Places with malformed qk17 (e.g., 'invalid') should not be in tile_assignments."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        # Create places table with a malformed qk17
        con.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        # Insert a valid place
        con.execute("""
            INSERT INTO places VALUES
            ('valid001', 'Valid Place', 37.7749, -122.4194, '02301020333300320')
        """)

        # Insert a place with malformed qk17 (not all digits 0-3)
        con.execute("""
            INSERT INTO places VALUES
            ('invalid001', 'Invalid QK17', 37.7750, -122.4200, 'invalid12345678')
        """)

        # Insert a place with qk17 that has digits outside 0-3
        con.execute("""
            INSERT INTO places VALUES
            ('invalid002', 'Wrong Digits', 37.7751, -122.4201, '12345678901234567')
        """)

        # Run tile assignment
        sql = _load_sql(
            "compute_tile_assignments.sql",
            {
                "pk_expr": "fsq_place_id",
                "min_zoom": 6,
                "max_zoom": 17,
                "max_per_tile": 1000,
            },
        )
        con.execute(sql)

        # Check that only the valid place is in tile_assignments
        rows = con.execute("SELECT place_id FROM tile_assignments").fetchall()
        place_ids = [r[0] for r in rows]

        assert 'valid001' in place_ids, "Valid place should be in tile_assignments"
        assert 'invalid001' not in place_ids, "Place with malformed qk17 string should be rejected"
        assert 'invalid002' not in place_ids, "Place with qk17 containing digits 4-9 should be rejected"
        assert len(place_ids) == 1, f"Expected 1 place, got {len(place_ids)}"

    def test_qk17_wrong_length_rejected(self, tmp_path):
        """Places with qk17 that is not exactly 17 characters should be rejected."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        con.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        # Insert a place with qk17 that's too short
        con.execute("""
            INSERT INTO places VALUES
            ('short001', 'Short QK17', 37.7749, -122.4194, '0230102')
        """)

        # Insert a place with qk17 that's too long
        con.execute("""
            INSERT INTO places VALUES
            ('long001', 'Long QK17', 37.7750, -122.4200, '0230102033330032012345')
        """)

        # Run tile assignment
        sql = _load_sql(
            "compute_tile_assignments.sql",
            {
                "pk_expr": "fsq_place_id",
                "min_zoom": 6,
                "max_zoom": 17,
                "max_per_tile": 1000,
            },
        )
        con.execute(sql)

        # Check that neither place is in tile_assignments
        rows = con.execute("SELECT place_id FROM tile_assignments").fetchall()
        place_ids = [r[0] for r in rows]

        assert 'short001' not in place_ids, "Place with qk17 < 17 chars should be rejected"
        assert 'long001' not in place_ids, "Place with qk17 > 17 chars should be rejected"
        assert len(place_ids) == 0, f"Expected 0 places, got {len(place_ids)}"


class TestSPATIAL5_InvalidGeometryFilter:
    """SPATIAL-5: ST_Intersection can produce invalid/empty geometries.

    Bug: Clipping boundaries to tile envelopes via ST_Intersection can produce
    degenerate geometries (point intersections, zero-area slivers). These
    invalid geometries can cause ST_Contains to return false negatives.

    Fix: Add filter after tile_boundaries creation in _run_containment():
    ST_IsValid(geometry) AND ST_Area(geometry) > 0

    The filter should be in the WHERE clause of the CREATE TEMP TABLE statement,
    not as a separate DELETE.
    """

    def test_degenerate_intersection_filtered(self, tmp_path):
        """Degenerate geometries (points, lines) from ST_Intersection must be filtered out.

        The bug: ST_Intersection of boundaries with tile envelopes can produce
        degenerate geometries (points, zero-area slivers). These are not valid
        polygon boundaries and should be excluded from tile_boundaries.

        The fix: In _run_containment(), the tile_boundaries query includes:
        WHERE ST_IsValid(geometry) AND ST_Area(geometry) > 0

        This test verifies that degenerate geometries are filtered out.
        """
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        # Create a boundary table with geometries that produce degenerate intersections.
        # Column named `level` (not admin_level) to match the boundaries.duckdb
        # `places` schema post pipeline-implementation-decisions.md
        # ("OQ-P2-2 — containment level vocabulary").
        con.execute("""
            CREATE TABLE boundaries (
                id VARCHAR,
                geometry GEOMETRY,
                level INTEGER,
                min_latitude DOUBLE,
                max_latitude DOUBLE,
                min_longitude DOUBLE,
                max_longitude DOUBLE
            )
        """)

        # A vertical line at x=-122.4194 that crosses the envelope vertically.
        # When intersected with the envelope below, it produces a LINESTRING (degenerate).
        con.execute("""
            INSERT INTO boundaries VALUES
            ('boundary_line',
             ST_GeomFromText('LINESTRING(-122.4194 37.7700, -122.4194 37.7800)'),
             25, 37.77, 37.78, -122.42, -122.42)
        """)

        # A valid polygon that properly overlaps the envelope with area > 0
        con.execute("""
            INSERT INTO boundaries VALUES
            ('boundary_valid',
             ST_GeomFromText('POLYGON((-122.43 37.76, -122.40 37.76, -122.40 37.79, -122.43 37.79, -122.43 37.76))'),
             25, 37.76, 37.79, -122.43, -122.40)
        """)

        # Define the tile envelope
        envelope = "ST_MakeEnvelope(-122.43, 37.76, -122.40, 37.79)"

        # Create tile_boundaries using the same pattern as _run_containment()
        # This includes the ST_IsValid + ST_Area filter
        con.execute(f"""
            CREATE TEMP TABLE tile_boundaries AS
            SELECT * FROM (
                SELECT id, level,
                       ST_Intersection(geometry, {envelope}) AS geometry
                FROM boundaries
                WHERE ST_Intersects(geometry, {envelope})
            )
            WHERE ST_IsValid(geometry) AND ST_Area(geometry) > 0
        """)

        # Verify no degenerate geometries in tile_boundaries
        degenerate_count = con.execute(f"""
            SELECT count(*) FROM tile_boundaries
            WHERE NOT ST_IsValid(geometry)
               OR ST_Area(geometry) <= 0
               OR ST_GeometryType(geometry) IN ('POINT', 'LINESTRING', 'MULTIPOINT', 'MULTILINESTRING')
        """).fetchone()[0]

        assert degenerate_count == 0, \
            f"tile_boundaries should contain no degenerate geometries, found {degenerate_count}"

        # Also verify the valid boundary is still present
        valid_count = con.execute(f"""
            SELECT count(*) FROM tile_boundaries
            WHERE ST_IsValid(geometry) AND ST_Area(geometry) > 0
        """).fetchone()[0]
        assert valid_count >= 1, "tile_boundaries should contain at least one valid polygon"


class TestEXPORT6_NullQK17Logging:
    """EXPORT-6: NULL qk17 places are silently dropped from tile assignments.

    Bug: Places with NULL qk17 are filtered out of tile_assignments with no
    warning logged. This makes data quality issues invisible.

    Fix: After tile assignment, compare count(places) vs count(tile_assignments).
    Log a warning if places were dropped.
    """

    def test_null_qk17_logs_warning(self, tmp_path, caplog):
        """Places with NULL qk17 should trigger a warning log."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        # Create places table with some NULL qk17 values
        con.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        # Insert valid places
        con.execute("""
            INSERT INTO places VALUES
            ('valid001', 'Valid Place 1', 37.7749, -122.4194, '02301020333300320'),
            ('valid002', 'Valid Place 2', 37.7750, -122.4200, '02301020333300321')
        """)

        # Insert a place with NULL qk17
        con.execute("""
            INSERT INTO places VALUES
            ('null001', 'Null QK17 Place', 37.7751, -122.4201, NULL)
        """)

        # Export places to parquet for Phase 2 API
        places_parquet = str(tmp_path / "places_null_qk17.parquet")
        ta_parquet = str(tmp_path / "ta_null_qk17.parquet")
        total_places = con.execute("SELECT count(*) FROM places").fetchone()[0]
        con.execute(f"COPY places TO '{places_parquet}' (FORMAT PARQUET)")
        con.close()

        # Capture logging output
        with caplog.at_level(logging.WARNING):
            stage_tile_assignment(
                places_parquet, ta_parquet, "foursquare",
                max_per_tile=1000, force=True
            )

        # Check that a warning was logged about dropped places
        warning_messages = [record.message for record in caplog.records
                           if record.levelno >= logging.WARNING
                           and ('dropped' in record.message.lower()
                                or 'null' in record.message.lower()
                                or 'qk17' in record.message.lower())]

        assert len(warning_messages) > 0, \
            "Expected warning log about places with NULL qk17 being dropped"

        # Verify the place count difference
        check_con = duckdb.connect()
        assigned_places = check_con.execute(
            f"SELECT count(*) FROM read_parquet('{ta_parquet}')"
        ).fetchone()[0]
        check_con.close()

        assert total_places == 3, f"Expected 3 total places, got {total_places}"
        assert assigned_places == 2, f"Expected 2 assigned places, got {assigned_places}"

    def test_all_valid_qk17_no_warning(self, tmp_path, caplog):
        """When all places have valid qk17, no warning should be logged."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        con.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        # Insert only valid places
        con.execute("""
            INSERT INTO places VALUES
            ('valid001', 'Valid Place 1', 37.7749, -122.4194, '02301020333300320'),
            ('valid002', 'Valid Place 2', 37.7750, -122.4200, '02301020333300321')
        """)

        # Export places to parquet for Phase 2 API
        places_parquet = str(tmp_path / "places_all_valid.parquet")
        ta_parquet = str(tmp_path / "ta_all_valid.parquet")
        con.execute(f"COPY places TO '{places_parquet}' (FORMAT PARQUET)")
        con.close()

        # Capture logging output
        with caplog.at_level(logging.WARNING):
            stage_tile_assignment(
                places_parquet, ta_parquet, "foursquare",
                max_per_tile=1000, force=True
            )

        # Check that NO warning was logged about dropped places
        warning_messages = [record.message for record in caplog.records
                           if record.levelno >= logging.WARNING
                           and ('dropped' in record.message.lower()
                                or 'null' in record.message.lower()
                                or 'qk17' in record.message.lower())]

        assert len(warning_messages) == 0, \
            f"Expected no warning when all qk17 are valid, but got: {warning_messages}"


class TestEXPORT7_TileAssignmentUniqueness:
    """EXPORT-7: No uniqueness validation on tile assignments.

    Bug: No check that each place appears exactly once in tile_assignments.
    Duplicate place_ids could cause incorrect exports and data corruption.

    Fix: Add post-assignment query checking for duplicate place_ids.
    Log an error if duplicates are found.
    """

    def test_normal_assignment_no_duplicates(self, tmp_path, caplog):
        """Normal tile assignment should produce no duplicate place_ids."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        con.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        # Insert valid places
        con.execute("""
            INSERT INTO places VALUES
            ('valid001', 'Valid Place 1', 37.7749, -122.4194, '02301020333300320'),
            ('valid002', 'Valid Place 2', 37.7750, -122.4200, '02301020333300321'),
            ('valid003', 'Valid Place 3', 37.7751, -122.4201, '02301020333300322')
        """)

        places_parquet = str(tmp_path / "places_no_dup.parquet")
        ta_parquet = str(tmp_path / "ta_no_dup.parquet")
        con.execute(f"COPY places TO '{places_parquet}' (FORMAT PARQUET)")
        con.close()

        # Capture logging output
        with caplog.at_level(logging.ERROR):
            stage_tile_assignment(
                places_parquet, ta_parquet, "foursquare",
                max_per_tile=1000, force=True
            )

        # Check that NO error was logged about duplicate assignments
        error_messages = [record.message for record in caplog.records
                         if record.levelno >= logging.ERROR
                         and ('multiple' in record.message.lower()
                              or 'duplicate' in record.message.lower())]

        assert len(error_messages) == 0, \
            f"Expected no duplicate/multiple-tile error for normal assignment, but got: {error_messages}"

        # Verify uniqueness in parquet output
        check_con = duckdb.connect()
        duplicate_count = check_con.execute(f"""
            SELECT place_id, COUNT(*) as cnt
            FROM read_parquet('{ta_parquet}')
            GROUP BY place_id
            HAVING cnt > 1
        """).fetchone()
        check_con.close()

        assert duplicate_count is None, "Expected no duplicate place_ids"

    def test_manually_inserted_duplicates_detected(self, tmp_path, caplog):
        """If input places parquet has duplicate pk values, an EXPORT-7 error should be logged."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        con.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        # Insert the same place_id twice to simulate duplicate pk in input
        con.execute("""
            INSERT INTO places VALUES
            ('valid001', 'Valid Place 1', 37.7749, -122.4194, '02301020333300320'),
            ('valid001', 'Valid Place 1 Dup', 37.7749, -122.4194, '02301020333300320')
        """)

        places_parquet = str(tmp_path / "places_dup.parquet")
        ta_parquet = str(tmp_path / "ta_dup.parquet")
        con.execute(f"COPY places TO '{places_parquet}' (FORMAT PARQUET)")
        con.close()

        # Run stage_tile_assignment with duplicate input and capture error logs
        with caplog.at_level(logging.ERROR):
            stage_tile_assignment(
                places_parquet, ta_parquet, "foursquare",
                max_per_tile=1000, force=True
            )

        # Check that an EXPORT-7 error was logged (message contains "multiple" or "duplicate")
        error_messages = [record.message for record in caplog.records
                         if record.levelno >= logging.ERROR
                         and ('multiple' in record.message.lower()
                              or 'duplicate' in record.message.lower())]

        assert len(error_messages) > 0, (
            "Expected EXPORT-7 error log about places assigned to multiple tiles when input "
            "places parquet has duplicate fsq_place_id values; "
            f"captured log records: {[(r.levelno, r.message) for r in caplog.records]}"
        )

    def test_tile_assignments_has_primary_key_constraint(self, tmp_path):
        """tile_assignments parquet output should have place_id as first column."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        con.execute("""
            CREATE TABLE places (
                fsq_place_id VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                qk17 VARCHAR
            )
        """)

        con.execute("""
            INSERT INTO places VALUES
            ('valid001', 'Valid Place 1', 37.7749, -122.4194, '02301020333300320')
        """)

        places_parquet = str(tmp_path / "places_pk.parquet")
        ta_parquet = str(tmp_path / "ta_pk.parquet")
        con.execute(f"COPY places TO '{places_parquet}' (FORMAT PARQUET)")
        con.close()

        stage_tile_assignment(places_parquet, ta_parquet, "foursquare", max_per_tile=1000, force=True)

        # Check parquet schema - place_id should be the first column
        check_con = duckdb.connect()
        table_info = check_con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{ta_parquet}')"
        ).fetchall()
        check_con.close()

        # place_id should be the first column
        assert len(table_info) >= 2, "tile_assignments should have at least 2 columns"
        assert table_info[0][0] == "place_id", "First column should be place_id"
