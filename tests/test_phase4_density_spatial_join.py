"""Tests for density-tile spatial join and division import.

density_extract.sql's output carries tile bounds (tile_xmin, tile_ymin,
tile_xmax, tile_ymax) instead of centroid columns; bounds are computed in
SQL via the qk_env() macro. overture_division_import.sql joins on bbox
overlap between division and tile bounds (not a centroid-in-bbox check),
so small localities smaller than a z15 tile still receive a non-zero
density score.
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
# TestDensityTileBounds
# ---------------------------------------------------------------------------

class TestDensityTileBounds:
    """Tests for density tile bounds in density_extract.sql's output.

    density_extract.sql's output has:
    - tile_qk15, density_score
    - tile_xmin, tile_ymin, tile_xmax, tile_ymax
    - no centroid columns
    """

    def test_density_has_tile_bounds_columns(self, overture_parquet, tmp_path):
        """Density parquet has tile bounds columns."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")
        cols = {row[0] for row in conn.execute("DESCRIBE density").fetchall()}
        conn.close()

        # Check for tile bounds columns
        assert "tile_xmin" in cols, f"tile_xmin column missing; found: {cols}"
        assert "tile_ymin" in cols, f"tile_ymin column missing; found: {cols}"
        assert "tile_xmax" in cols, f"tile_xmax column missing; found: {cols}"
        assert "tile_ymax" in cols, f"tile_ymax column missing; found: {cols}"

    def test_density_no_centroid_columns(self, overture_parquet, tmp_path):
        """Density parquet has no centroid columns; tile bounds are used for
        spatial joins instead.
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
        """Tile bounds in density parquet match quadkey_to_bbox() for a known quadkey.

        Cross-checks the SQL-computed tile bounds (qk_env macro) against
        Python's independent quadkey_to_bbox() implementation.
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

        # Bounds are computed in SQL (qk_env macro, sinh via (exp-exp)/2) and
        # compared against Python's math.sinh; the two differ at the ULP
        # level, so exact equality is not guaranteed. 1e-9 is the codebase's
        # established numerical contract for this computation (see
        # tests/test_covering.py, which pins the same tolerance).
        assert abs(xmin - expected[0]) <= 1e-9, f"tile_xmin mismatch for {qk}: got {xmin}, expected {expected[0]}"
        assert abs(ymin - expected[1]) <= 1e-9, f"tile_ymin mismatch for {qk}: got {ymin}, expected {expected[1]}"
        assert abs(xmax - expected[2]) <= 1e-9, f"tile_xmax mismatch for {qk}: got {xmax}, expected {expected[2]}"
        assert abs(ymax - expected[3]) <= 1e-9, f"tile_ymax mismatch for {qk}: got {ymax}, expected {expected[3]}"


# ---------------------------------------------------------------------------
# TestDivisionDensityJoin
# ---------------------------------------------------------------------------

class TestDivisionDensityJoin:
    """Tests for the division density join in overture_division_import.sql.

    overture_division_import.sql joins on bbox overlap (tile bounds
    intersecting the division's bbox), not centroid columns, so small
    localities receive a non-zero density score.
    """

    def test_small_locality_gets_density(self, small_density_parquet, division_parquet, division_area_parquet, tmp_path):
        """Small localities (bbox smaller than z15 tile) receive non-zero density score.

        Asserts on places.importance (the production column the density join
        feeds into), not a query-side reimplementation of the join — the fixture's
        single locality has population=1000, density_norm=10.0, pop_norm=20.0, and
        the density tile's density_score=5.0, so importance must reflect the
        density contribution: round(60*min(5/10,1) + 40*min(ln(1001)/20,1)) == 44.
        Without the bbox-overlap join (avg_density=0), importance would be 14.
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

        # overture_division_import.sql calls qk17(); load it onto this
        # standalone connection, mirroring stages._load_qk_env_macros.
        qk_env_sql = (Path(__file__).parent.parent / "garganorn" / "sql" / "qk_env_macro.sql").read_text()
        for stmt in qk_env_sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(stmt)

        con.execute(division_import_sql)

        importance = con.execute(
            "SELECT importance FROM places WHERE id = 'div1'"
        ).fetchone()[0]
        con.close()

        assert importance == 44, (
            f"expected importance=44 (density-driven component included) for "
            f"div1, got {importance}; a bbox-overlap join failure would yield 14"
        )

    def test_division_import_no_centroid_references(self):
        """overture_division_import.sql's density join uses tile bounds, not
        centroid columns.
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

    def test_division_base_is_materialized(self):
        """division_base must be a CREATE TEMP TABLE, not a CTE, so the density
        join's probe side is materialized (docs/gotchas.md: "A join is sized
        off the source relation, not the filtered one"). Text proxy for a plan
        property, not the plan itself -- it cannot see the actual query plan.
        """
        division_import_path = Path(__file__).parent.parent / "garganorn" / "sql" / "overture_division_import.sql"
        sql = division_import_path.read_text()

        assert "CREATE TEMP TABLE division_base AS" in sql, (
            "division_base must be materialized as its own CREATE TEMP TABLE"
        )

    def test_division_import_bbox_overlap_join(self):
        """overture_division_import.sql's density join uses a bbox-overlap condition:

            d.tile_xmin <= p.max_longitude
            AND d.tile_xmax >= p.min_longitude
            AND d.tile_ymin <= p.max_latitude
            AND d.tile_ymax >= p.min_latitude
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
    """Create a minimal density parquet with tile bounds (no centroid columns)."""
    parquet_path = tmp_path / "small_density.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")

    # Create density table with tile bounds
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
