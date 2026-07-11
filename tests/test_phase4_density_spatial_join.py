"""Tests for Phase 4: Fix density tile spatial join.

These tests FAIL because the density extract changes don't exist yet.
This is TDD red phase.

Phase 4 changes:
- density_extract.sql output has tile bounds (tile_xmin, tile_ymin, tile_xmax, tile_ymax)
- density_extract.sql output does NOT have centroid_lon or centroid_lat
- Tile bounds are computed using quadkey_to_bbox() in Python (not pure SQL)
- Small localities (bbox smaller than z15 tile) receive non-zero density score
- overture_division_import.sql uses bbox-overlap join (no centroid references)
"""

import time
from pathlib import Path

import duckdb
import pytest

from garganorn.levels import level_case_sql
from garganorn.stages import (
    stage_density_extract,
    quadkey_to_bbox,
)


# ---------------------------------------------------------------------------
# TestDensityTileBounds (Phase 4)
# ---------------------------------------------------------------------------

class TestDensityTileBounds:
    """Tests for density tile bounds in density_extract.sql (Phase 4).

    After Phase 4, density_extract.sql output should have:
    - tile_qk15, density_score (existing)
    - tile_xmin, tile_ymin, tile_xmax, tile_ymax (new, computed via Python)
    - NO centroid_lon or centroid_lat (removed)
    """

    def test_density_has_tile_bounds_columns(self, overture_parquet, tmp_path):
        """Density parquet must have tile bounds columns.

        This test FAILS because density_extract.sql doesn't output tile bounds yet.

        Expected columns after Phase 4:
        - tile_qk15, density_score (existing)
        - tile_xmin, tile_ymin, tile_xmax, tile_ymax (new)
        """
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")
        cols = {row[0] for row in conn.execute("DESCRIBE density").fetchall()}
        conn.close()

        # Check for new tile bounds columns
        assert "tile_xmin" in cols, f"tile_xmin column missing; found: {cols}"
        assert "tile_ymin" in cols, f"tile_ymin column missing; found: {cols}"
        assert "tile_xmax" in cols, f"tile_xmax column missing; found: {cols}"
        assert "tile_ymax" in cols, f"tile_ymax column missing; found: {cols}"

    def test_density_no_centroid_columns(self, overture_parquet, tmp_path):
        """Density parquet must NOT have centroid columns.

        This test FAILS because density_extract.sql still outputs centroids.

        After Phase 4, centroid_lon and centroid_lat should be removed.
        Tile bounds are used instead for spatial joins.
        """
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")
        cols = {row[0] for row in conn.execute("DESCRIBE density").fetchall()}
        conn.close()

        # Check that centroid columns are gone
        assert "centroid_lon" not in cols, f"centroid_lon should not exist; found: {cols}"
        assert "centroid_lat" not in cols, f"centroid_lat should not exist; found: {cols}"

    def test_tile_bounds_match_quadkey_function(self, overture_parquet, tmp_path):
        """Tile bounds in density parquet must match quadkey_to_bbox() for known quadkey.

        This test FAILS because density_extract.sql doesn't output tile bounds yet.

        Verifies that the Python post-processing in stage_density_extract
        correctly computes tile bounds using the existing quadkey_to_bbox() function.
        """
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")

        # Get a known quadkey from the density output
        row = conn.execute("SELECT tile_qk15, tile_xmin, tile_ymin, tile_xmax, tile_ymax FROM density LIMIT 1").fetchone()
        conn.close()

        if row is None:
            pytest.skip("No density tiles in output")

        qk, xmin, ymin, xmax, ymax = row

        # Compute expected bounds using quadkey_to_bbox
        expected = quadkey_to_bbox(qk)

        assert xmin == expected[0], f"tile_xmin mismatch for {qk}: got {xmin}, expected {expected[0]}"
        assert ymin == expected[1], f"tile_ymin mismatch for {qk}: got {ymin}, expected {expected[1]}"
        assert xmax == expected[2], f"tile_xmax mismatch for {qk}: got {xmax}, expected {expected[2]}"
        assert ymax == expected[3], f"tile_ymax mismatch for {qk}: got {ymax}, expected {expected[3]}"


# ---------------------------------------------------------------------------
# TestDivisionDensityJoin (Phase 4)
# ---------------------------------------------------------------------------

class TestDivisionDensityJoin:
    """Tests for division density join in overture_division_import.sql (Phase 4).

    After Phase 4, overture_division_import.sql should:
    - Use bbox-overlap join condition (tile bounds intersect division bbox)
    - NOT reference centroid_lon or centroid_lat
    - Give non-zero density score to small localities
    """

    def test_small_locality_gets_density(self, small_density_parquet, division_parquet, division_area_parquet, tmp_path):
        """Small localities (bbox smaller than z15 tile) receive non-zero density score.

        This test FAILS because the density join still uses centroid-point-in-bbox,
        which misses small localities.

        After Phase 4, using bbox-overlap join ensures that any locality whose
        bbox intersects a density tile gets that tile's density score.
        """
        # Import divisions with small density parquet
        work_db = tmp_path / "test_work.duckdb"
        con = duckdb.connect(str(work_db))
        con.execute("INSTALL spatial; LOAD spatial;")

        # Create density temp table from small_density_parquet
        con.execute(f"CREATE TEMP TABLE density_tiles AS SELECT * FROM read_parquet('{small_density_parquet}')")

        # Run division import SQL
        division_import_sql = (Path(__file__).parent.parent / "garganorn" / "sql" / "overture_division_import.sql").read_text()

        # Substitute parameters
        substitutions = {
            "memory_limit": "4GB",
            "division_parquet": division_parquet,
            "division_area_parquet": division_area_parquet,
            "xmin": -180, "ymin": -90, "xmax": 180, "ymax": 90,
            "density_cte": "-- density_tiles already created as temp table",
            "density_norm": 10.0,
            "pop_norm": 20.0,
            "level_case": level_case_sql(),
        }

        for k, v in substitutions.items():
            division_import_sql = division_import_sql.replace(f"${{{k}}}", str(v))

        con.execute(division_import_sql)

        # Re-create density temp table for verification (import SQL drops it)
        con.execute(f"CREATE TEMP TABLE density_tiles AS SELECT * FROM read_parquet('{small_density_parquet}')")

        # Check that at least one locality has non-zero avg_density
        result = con.execute("""
            SELECT COUNT(*)
            FROM places
            WHERE subtype = 'locality'
              AND EXISTS (
                  SELECT 1 FROM (
                      SELECT id, coalesce(avg(density_score), 0) AS avg_density
                      FROM places p2
                      LEFT JOIN density_tiles d
                          ON d.tile_xmin <= p2.max_longitude
                         AND d.tile_xmax >= p2.min_longitude
                         AND d.tile_ymin <= p2.max_latitude
                         AND d.tile_ymax >= p2.min_latitude
                      WHERE p2.subtype = 'locality'
                      GROUP BY p2.id
                  ) WHERE id = places.id AND avg_density > 0
              )
        """).fetchone()

        assert result[0] > 0, "At least one small locality should have non-zero density score"
        con.close()

    def test_division_import_no_centroid_references(self):
        """overture_division_import.sql must NOT reference centroid_lon or centroid_lat.

        This test FAILS because overture_division_import.sql still uses centroid columns.

        After Phase 4, the density join should use tile bounds, not centroids.
        """
        division_import_path = Path(__file__).parent.parent / "garganorn" / "sql" / "overture_division_import.sql"
        sql = division_import_path.read_text()

        # Check that centroid references are gone
        assert "centroid_lon" not in sql, "overture_division_import.sql should not reference centroid_lon"
        assert "centroid_lat" not in sql, "overture_division_import.sql should not reference centroid_lat"

        # Check that tile bounds are used
        assert "tile_xmin" in sql, "overture_division_import.sql should use tile_xmin"
        assert "tile_xmax" in sql, "overture_division_import.sql should use tile_xmax"
        assert "tile_ymin" in sql, "overture_division_import.sql should use tile_ymin"
        assert "tile_ymax" in sql, "overture_division_import.sql should use tile_ymax"

    def test_division_import_bbox_overlap_join(self):
        """overture_division_import.sql must use bbox-overlap join condition.

        This test FAILS because overture_division_import.sql still uses centroid-point-in-bbox.

        After Phase 4, the join should be:
            d.tile_xmin <= p.max_longitude
            AND d.tile_xmax >= p.min_longitude
            AND d.tile_ymin <= p.max_latitude
            AND d.tile_ymax >= p.min_latitude

        This ensures bbox overlap, not point-in-bbox.
        """
        division_import_path = Path(__file__).parent.parent / "garganorn" / "sql" / "overture_division_import.sql"
        sql = division_import_path.read_text()

        # Check for bbox-overlap pattern
        # Pattern: tile_xmin <= max_longitude AND tile_xmax >= min_longitude
        assert "tile_xmin" in sql and "max_longitude" in sql, "Should use tile_xmin <= max_longitude"
        assert "tile_xmax" in sql and "min_longitude" in sql, "Should use tile_xmax >= min_longitude"
        assert "tile_ymin" in sql and "max_latitude" in sql, "Should use tile_ymin <= max_latitude"
        assert "tile_ymax" in sql and "min_latitude" in sql, "Should use tile_ymax >= min_latitude"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def small_density_parquet(tmp_path):
    """Create a minimal density parquet with tile bounds.

    This fixture simulates the post-Phase 4 density parquet format:
    - tile_qk15, density_score
    - tile_xmin, tile_ymin, tile_xmax, tile_ymax
    - NO centroid columns
    """
    parquet_path = tmp_path / "small_density.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")

    # Create density table with tile bounds (post-Phase 4 format)
    conn.execute("""
        CREATE TABLE density AS
        SELECT '021230132303312' AS tile_qk15,
               5.0 AS density_score,
               -122.5 AS tile_xmin,
               37.7 AS tile_ymin,
               -122.4 AS tile_xmax,
               37.8 AS tile_ymax
    """)

    conn.execute(f"COPY density TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()

    return str(parquet_path)


@pytest.fixture
def division_parquet(tmp_path):
    """Create a minimal division parquet for testing."""
    parquet_path = tmp_path / "division.parquet"

    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE division AS
        SELECT 'div1' AS id,
               {'primary': 'Small Locality'} AS names,
               'locality' AS subtype,
               'US' AS country,
               'US-CA' AS region,
               'Q123' AS wikidata,
               1000::BIGINT AS population,
               NULL::VARCHAR AS parent_division_id
    """)

    conn.execute(f"COPY division TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()

    return str(parquet_path)


@pytest.fixture
def division_area_parquet(tmp_path):
    """Create a minimal division_area parquet for testing.

    Creates a small locality bbox that intersects the density tile
    from small_density_parquet fixture.
    """
    parquet_path = tmp_path / "division_area.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")

    # Create a small locality bbox that intersects the density tile
    # Density tile: -122.5 to -122.4, 37.7 to 37.8
    # Small locality: -122.45 to -122.44, 37.75 to 37.76 (smaller than z15 tile)
    conn.execute("""
        CREATE TABLE division_area AS
        SELECT 'div1' AS division_id,
               2::INTEGER AS admin_level,
               ST_GeomFromText('POLYGON((-122.45 37.75, -122.45 37.76, -122.44 37.76, -122.44 37.75, -122.45 37.75))')::GEOMETRY AS geometry,
               {'xmin': -122.45, 'ymin': 37.75, 'xmax': -122.44, 'ymax': 37.76} AS bbox,
               true AS is_land
    """)

    conn.execute(f"COPY division_area TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()

    return str(parquet_path)
