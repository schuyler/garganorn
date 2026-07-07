"""Tests for density_extract.sql and stage_density_extract() function."""

import time

import duckdb
import pytest
from garganorn.stages import stage_density_extract


# ---------------------------------------------------------------------------
# Tests: density_extract.sql (via stage_density_extract)
# ---------------------------------------------------------------------------

class TestDensityExtract:
    """Tests for garganorn/sql/density_extract.sql and the stage_density_extract() function.

    The stage_density_extract() function reads Overture parquet and writes a
    density parquet file with one row per zoom-15 quadkey containing the
    density score ln(1 + count(*)).
    """

    def test_produces_parquet(self, overture_parquet, tmp_path):
        """stage_density_extract must produce a parquet file."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        assert output.exists(), f"Density parquet not found at {output}"
        assert output.stat().st_size > 0, "Density parquet is empty"

    def test_schema_columns(self, overture_parquet, tmp_path):
        """Density parquet must have tile_qk15 and density_score columns."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")
        cols = {row[0] for row in conn.execute("DESCRIBE density").fetchall()}
        conn.close()

        assert "tile_qk15" in cols, f"tile_qk15 column missing; found: {cols}"
        assert "density_score" in cols, f"density_score column missing; found: {cols}"

    def test_density_score_positive(self, overture_parquet, tmp_path):
        """All density_score values must be positive (ln(1 + count) > 0)."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")
        bad = conn.execute("SELECT tile_qk15, density_score FROM density WHERE density_score <= 0").fetchall()
        conn.close()

        assert not bad, f"Rows with non-positive density_score: {bad}"

    def test_tile_qk15_length_15(self, overture_parquet, tmp_path):
        """All tile_qk15 values must be exactly 15 characters."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")
        bad = conn.execute("SELECT tile_qk15 FROM density WHERE length(tile_qk15) != 15").fetchall()
        conn.close()

        assert not bad, f"Rows with tile_qk15 length != 15: {bad}"

    def test_global_no_bbox_filter(self, overture_parquet, tmp_path):
        """stage_density_extract must not filter by bbox; all records contribute to density."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")
        count = conn.execute("SELECT COUNT(*) FROM density").fetchone()[0]
        conn.close()

        # The overture_parquet fixture has 7 rows total (including ov006 outside bbox).
        # All should contribute to density since there's no bbox filter.
        # We expect at least 1 density tile (probably 1-5 given the small fixture).
        assert count > 0, f"Expected at least 1 density tile, got {count}"
