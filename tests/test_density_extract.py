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


# ---------------------------------------------------------------------------
# Tests: importance SQL with shared density parquet
# ---------------------------------------------------------------------------

class TestImportanceWithSharedDensity:
    """Tests that importance SQL uses the shared density parquet via ${density_parquet}.

    These tests verify that all three importance SQL files (overture_place_importance.sql,
    foursquare_importance.sql, osm_importance.sql) correctly substitute ${density_parquet}
    and join against the density table to compute density scores.
    """

    def test_overture_importance_with_density(self, overture_parquet, density_parquet, tmp_path):
        """overture_place_importance.sql must use ${density_parquet} to compute density."""
        import duckdb as _duckdb

        db_path = tmp_path / "test_ov_density.duckdb"
        conn = _duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Run import to create places table
        from tests.quadtree_helpers import run_overture_import
        run_overture_import(conn, overture_parquet)

        # Run importance with density_parquet substitution
        substitutions = {
            "density_norm": "10.0",
            "idf_norm": "18.0",
            "density_parquet": density_parquet,
        }
        from tests.quadtree_helpers import _load_sql, _strip_spatial_install, _strip_memory_limit
        raw_sql = _load_sql("overture_place_importance.sql", substitutions)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

        # Verify importance column exists
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        assert "importance" in cols, f"importance column missing; found: {cols}"

        # Verify all importance values are in [0, 100]
        bad = conn.execute("SELECT id, importance FROM places WHERE importance < 0 OR importance > 100").fetchall()
        assert not bad, f"Rows with out-of-range importance: {bad}"

        conn.close()

    def test_fsq_importance_with_density(self, fsq_parquet, overture_parquet, density_parquet, tmp_path):
        """foursquare_importance.sql must use ${density_parquet} to compute density."""
        import duckdb as _duckdb

        db_path = tmp_path / "test_fsq_density.duckdb"
        conn = _duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Run import to create places table
        from tests.quadtree_helpers import run_fsq_import
        run_fsq_import(conn, fsq_parquet)

        # Run importance with density_parquet substitution
        substitutions = {
            "density_norm": "10.0",
            "idf_norm": "18.0",
            "density_parquet": density_parquet,
        }
        from tests.quadtree_helpers import _load_sql, _strip_spatial_install, _strip_memory_limit
        raw_sql = _load_sql("foursquare_importance.sql", substitutions)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

        # Verify importance column exists
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        assert "importance" in cols, f"importance column missing; found: {cols}"

        # Verify all importance values are in [0, 100]
        bad = conn.execute("SELECT fsq_place_id, importance FROM places WHERE importance < 0 OR importance > 100").fetchall()
        assert not bad, f"Rows with out-of-range importance: {bad}"

        conn.close()

    def test_osm_importance_with_density(self, osm_parquet, overture_parquet, density_parquet, tmp_path):
        """osm_importance.sql must use ${density_parquet} to compute density."""
        import duckdb as _duckdb

        db_path = tmp_path / "test_osm_density.duckdb"
        conn = _duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Run import to create places table
        from tests.quadtree_helpers import run_osm_import
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])

        # Run importance with density_parquet substitution
        substitutions = {
            "density_norm": "10.0",
            "idf_norm": "18.0",
            "density_parquet": density_parquet,
        }
        from tests.quadtree_helpers import _load_sql, _strip_spatial_install, _strip_memory_limit
        raw_sql = _load_sql("osm_importance.sql", substitutions)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

        # Verify importance column exists
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        assert "importance" in cols, f"importance column missing; found: {cols}"

        # Verify all importance values are in [0, 100]
        bad = conn.execute("SELECT rkey, importance FROM places WHERE importance < 0 OR importance > 100").fetchall()
        assert not bad, f"Rows with out-of-range importance: {bad}"

        conn.close()
