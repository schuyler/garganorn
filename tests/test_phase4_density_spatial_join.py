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
