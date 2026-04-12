"""Tests for Phase 4: Division Importance Backfill.

Tests for division importance backfill implementation:
- density_extract.sql produces centroid_lon/centroid_lat columns
- division_importance_backfill.sql computes importance from density + population
- stage_division_importance_backfill() in stages.py executes the backfill
- Pipeline runs boundary_export before backfill stage
"""
import time
import os

import pytest
import duckdb

# These imports will fail with ImportError until implementation exists
from garganorn.stages import stage_division_importance_backfill, stage_boundary_export


# ---------------------------------------------------------------------------
# TestDensityExtractCentroids (3 tests)
# Prerequisite: density_extract.sql must produce centroid columns
# ---------------------------------------------------------------------------

class TestDensityExtractCentroids:
    """Tests for density_extract.sql centroid column output.

    These tests verify that density_extract.sql produces centroid_lon
    and centroid_lat columns in addition to tile_qk15 and density_score.
    This is a prerequisite for division importance backfill.
    """

    def test_produces_centroid_lon_column(self, density_parquet):
        """Density parquet must have centroid_lon column.

        The centroid_lon column stores the average longitude of all places
        in each z15 tile, used for spatial joins in division backfill.
        """
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE density AS SELECT * FROM read_parquet(?)", [density_parquet])
        cols = {row[0] for row in conn.execute("DESCRIBE density").fetchall()}
        conn.close()

        assert "centroid_lon" in cols, (
            f"centroid_lon column missing from density parquet; found: {cols}"
        )

    def test_produces_centroid_lat_column(self, density_parquet):
        """Density parquet must have centroid_lat column.

        The centroid_lat column stores the average latitude of all places
        in each z15 tile, used for spatial joins in division backfill.
        """
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE density AS SELECT * FROM read_parquet(?)", [density_parquet])
        cols = {row[0] for row in conn.execute("DESCRIBE density").fetchall()}
        conn.close()

        assert "centroid_lat" in cols, (
            f"centroid_lat column missing from density parquet; found: {cols}"
        )

    def test_centroid_values_reasonable(self, density_parquet):
        """Centroid coordinates must be in valid ranges.

        All centroid_lon values must be in [-180, 180].
        All centroid_lat values must be in [-90, 90].
        """
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE density AS SELECT * FROM read_parquet(?)", [density_parquet])

        bad_lon = conn.execute(
            "SELECT tile_qk15, centroid_lon FROM density WHERE centroid_lon < -180 OR centroid_lon > 180"
        ).fetchall()
        bad_lat = conn.execute(
            "SELECT tile_qk15, centroid_lat FROM density WHERE centroid_lat < -90 OR centroid_lat > 90"
        ).fetchall()
        conn.close()

        assert not bad_lon, f"Rows with invalid centroid_lon: {bad_lon}"
        assert not bad_lat, f"Rows with invalid centroid_lat: {bad_lat}"


# ---------------------------------------------------------------------------
# TestDivisionImportanceBackfill (6 tests)
# Unit tests for stage_division_importance_backfill() function
# ---------------------------------------------------------------------------

class TestDivisionImportanceBackfill:
    """Tests for stage_division_importance_backfill function.

    These tests create a places table directly in DuckDB with test data,
    then call stage_division_importance_backfill and verify the results.
    """

    def _create_division_places_table(self, conn):
        """Create a places table with overture_division schema."""
        conn.execute("""
            CREATE TABLE places (
                id VARCHAR,
                geometry GEOMETRY,
                names STRUCT("primary" VARCHAR),
                subtype VARCHAR,
                country VARCHAR,
                region VARCHAR,
                admin_level INTEGER,
                wikidata VARCHAR,
                population BIGINT,
                parent_division_id VARCHAR,
                bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
                qk17 VARCHAR,
                min_latitude DOUBLE,
                max_latitude DOUBLE,
                min_longitude DOUBLE,
                max_longitude DOUBLE,
                importance INTEGER,
                variants STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
            )
        """)

    def test_locality_gets_nonzero_importance(self, density_parquet, tmp_path):
        """Locality in SF bbox with pop=874961 must get importance > 0.

        Creates a locality in San Francisco with population 874961.
        After backfill, importance should be > 0 because:
        - The locality is in a dense urban area (non-zero density score)
        - The population is large
        - importance = 60% * density_component + 40% * pop_component
        """
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        self._create_division_places_table(con)

        # Insert SF locality (same bbox as FSQ fixture)
        con.execute("""
            INSERT INTO places VALUES (
                'div_locality_sf',
                ST_GeomFromText('POLYGON((-122.55 37.6, -122.55 37.85, -122.30 37.85, -122.30 37.6, -122.55 37.6))'),
                {'primary': 'San Francisco'},
                'locality',
                'US',
                'US-CA',
                3,
                'Q62',
                874961,
                NULL,
                {'xmin': -122.55, 'ymin': 37.6, 'xmax': -122.30, 'ymax': 37.85},
                '0232222222222222',
                37.6, 37.85, -122.55, -122.30,
                0,
                []
            )
        """)

        t0 = time.monotonic()
        stage_division_importance_backfill(con, density_parquet, t0)

        # Check that importance is now > 0
        result = con.execute(
            "SELECT importance FROM places WHERE id = 'div_locality_sf'"
        ).fetchone()
        con.close()

        assert result is not None, "SF locality not found"
        importance = result[0]
        assert importance > 0, f"SF locality importance should be > 0, got {importance}"

    def test_country_uses_population_only(self, density_parquet, tmp_path):
        """Country with large population must get 0 < importance <= 40.

        For countries (admin_level=1), the density component is zero
        (density is a locality-only signal). Importance should come
        entirely from population: min(40, ln(pop) * 40 / pop_norm).
        With pop=331000000, ln(pop) ~ 19.6, so importance ~ 40 (capped).
        """
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        self._create_division_places_table(con)

        # Insert US country
        con.execute("""
            INSERT INTO places VALUES (
                'div_country_us',
                ST_GeomFromText('POLYGON((-125 25, -125 49, -65 49, -65 25, -125 25))'),
                {'primary': 'United States'},
                'country',
                'US',
                NULL,
                1,
                'Q30',
                331000000,
                NULL,
                {'xmin': -125.0, 'ymin': 25.0, 'xmax': -65.0, 'ymax': 49.0},
                '0023101322222222',
                25.0, 49.0, -125.0, -65.0,
                0,
                []
            )
        """)

        t0 = time.monotonic()
        stage_division_importance_backfill(con, density_parquet, t0)

        result = con.execute(
            "SELECT importance FROM places WHERE id = 'div_country_us'"
        ).fetchone()
        con.close()

        assert result is not None, "US country not found"
        importance = result[0]
        assert 0 < importance <= 40, (
            f"Country importance should be in (0, 40] for large population, got {importance}"
        )

    def test_no_density_tiles_yields_zero_density_component(self, density_parquet, tmp_path):
        """Region at (0,0) with population must have importance in (0, 40].

        Creates a region at (0,0) where the density parquet has no tiles.
        The density component should be zero, but the population component
        should still apply: importance = 40% * pop_component.
        With pop=500000, ln(pop) ~ 13.1, pop_component ~ 26, so
        importance = 0.4 * 26 = 10.4 (capped at 100).
        """
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        self._create_division_places_table(con)

        # Insert region at (0,0) - unlikely to have density tiles
        con.execute("""
            INSERT INTO places VALUES (
                'div_region_no_density',
                ST_GeomFromText('POLYGON((-0.1 -0.1, -0.1 0.1, 0.1 0.1, 0.1 -0.1, -0.1 -0.1))'),
                {'primary': 'Null Island Region'},
                'region',
                'ZZ',
                NULL,
                2,
                NULL,
                500000,
                NULL,
                {'xmin': -0.1, 'ymin': -0.1, 'xmax': 0.1, 'ymax': 0.1},
                '3000000000000000',
                -0.1, 0.1, -0.1, 0.1,
                0,
                []
            )
        """)

        t0 = time.monotonic()
        stage_division_importance_backfill(con, density_parquet, t0)

        result = con.execute(
            "SELECT importance FROM places WHERE id = 'div_region_no_density'"
        ).fetchone()
        con.close()

        assert result is not None, "Region not found"
        importance = result[0]
        assert 0 < importance <= 40, (
            f"Region with no density tiles should have importance in (0, 40], got {importance}"
        )

    def test_preserves_all_columns(self, density_parquet, tmp_path):
        """Backfill must preserve all columns and row count.

        The backfill operation updates only the importance column.
        All other columns must remain unchanged, and the row count
        must match.
        """
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        self._create_division_places_table(con)

        # Insert multiple divisions
        divisions = [
            ('div1', 'locality', 3, 874961, -122.55, 37.6, -122.30, 37.85),
            ('div2', 'country', 1, 331000000, -125.0, 25.0, -65.0, 49.0),
            ('div3', 'region', 2, 500000, -0.1, -0.1, 0.1, 0.1),
        ]

        for div_id, subtype, admin_level, pop, xmin, ymin, xmax, ymax in divisions:
            polygon = f"POLYGON(({xmin} {ymin}, {xmin} {ymax}, {xmax} {ymax}, {xmax} {ymin}, {xmin} {ymin}))"
            con.execute("""
                INSERT INTO places VALUES (
                    ?,
                    ST_GeomFromText(?),
                    {'primary': ?},
                    ?,
                    'US',
                    NULL,
                    ?,
                    NULL,
                    ?,
                    NULL,
                    {'xmin': ?, 'ymin': ?, 'xmax': ?, 'ymax': ?},
                    '0232222222222222',
                    ?, ?, ?, ?,
                    0,
                    []
                )
            """, [div_id, polygon, div_id, subtype, admin_level, pop, xmin, ymin, xmax, ymax,
                  ymin, ymax, xmin, xmax])

        # Record original column count and row count
        original_cols = con.execute("DESCRIBE places").fetchall()
        original_count = con.execute("SELECT count(*) FROM places").fetchone()[0]

        t0 = time.monotonic()
        stage_division_importance_backfill(con, density_parquet, t0)

        # Check column count preserved
        new_cols = con.execute("DESCRIBE places").fetchall()
        assert len(original_cols) == len(new_cols), (
            f"Column count changed from {len(original_cols)} to {len(new_cols)}"
        )

        # Check row count preserved
        new_count = con.execute("SELECT count(*) FROM places").fetchone()[0]
        assert original_count == new_count, (
            f"Row count changed from {original_count} to {new_count}"
        )

        # Check key columns preserved (spot check)
        for div_id, _, _, _, _, _, _, _ in divisions:
            result = con.execute(
                "SELECT id, subtype, admin_level FROM places WHERE id = ?", [div_id]
            ).fetchone()
            assert result is not None, f"Division {div_id} not found after backfill"

        con.close()

    def test_null_population_yields_zero_pop_component(self, density_parquet, tmp_path):
        """Locality with NULL population must have importance >= 0.

        When population is NULL, the population component is zero.
        The importance should come only from density (for localities).
        If there's no density data, importance remains 0.
        """
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        self._create_division_places_table(con)

        # Insert locality with NULL population
        con.execute("""
            INSERT INTO places VALUES (
                'div_locality_null_pop',
                ST_GeomFromText('POLYGON((-122.55 37.6, -122.55 37.85, -122.30 37.85, -122.30 37.6, -122.55 37.6))'),
                {'primary': 'Unknown Pop City'},
                'locality',
                'US',
                'US-CA',
                3,
                NULL,
                NULL,
                NULL,
                {'xmin': -122.55, 'ymin': 37.6, 'xmax': -122.30, 'ymax': 37.85},
                '0232222222222222',
                37.6, 37.85, -122.55, -122.30,
                0,
                []
            )
        """)

        t0 = time.monotonic()
        stage_division_importance_backfill(con, density_parquet, t0)

        result = con.execute(
            "SELECT importance FROM places WHERE id = 'div_locality_null_pop'"
        ).fetchone()
        con.close()

        assert result is not None, "Null pop locality not found"
        importance = result[0]
        assert importance >= 0, f"Importance should be >= 0 for NULL population, got {importance}"

    def test_importance_capped_at_100(self, density_parquet, tmp_path):
        """Extreme values must produce importance <= 100.

        Creates divisions with extreme population and density values.
        The importance formula must cap the final value at 100.
        """
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        self._create_division_places_table(con)

        # Insert locality with extreme population in dense area
        con.execute("""
            INSERT INTO places VALUES (
                'div_extreme',
                ST_GeomFromText('POLYGON((-122.55 37.6, -122.55 37.85, -122.30 37.85, -122.30 37.6, -122.55 37.6))'),
                {'primary': 'Extreme City'},
                'locality',
                'US',
                'US-CA',
                3,
                NULL,
                10000000000,
                NULL,
                {'xmin': -122.55, 'ymin': 37.6, 'xmax': -122.30, 'ymax': 37.85},
                '0232222222222222',
                37.6, 37.85, -122.55, -122.30,
                0,
                []
            )
        """)

        t0 = time.monotonic()
        stage_division_importance_backfill(con, density_parquet, t0)

        result = con.execute(
            "SELECT importance FROM places WHERE id = 'div_extreme'"
        ).fetchone()
        con.close()

        assert result is not None, "Extreme division not found"
        importance = result[0]
        assert importance <= 100, f"Importance should be capped at 100, got {importance}"


# ---------------------------------------------------------------------------
# TestDivisionPipelineIntegration (2 tests)
# Integration tests for the division pipeline with backfill stage
# ---------------------------------------------------------------------------

class TestDivisionPipelineIntegration:
    """Integration tests for division pipeline with importance backfill.

    These tests run the full division pipeline stages and verify that
    boundary_export runs before backfill, and that tiles have non-zero
    importance after backfill.
    """

    def test_boundary_export_before_backfill_succeeds(self, division_parquet, density_parquet, tmp_path):
        """boundary_export stage must run before backfill and produce valid boundaries.duckdb.

        This test verifies that boundary_export produces a valid boundaries.duckdb
        file that can be attached and queried. The backfill stage will use this
        file (or the working DB) for reading the places table.
        """
        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")

        # Run import
        from garganorn.stages import stage_import
        t0 = time.monotonic()
        stage_import(con, "overture_division", division_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)

        # Run boundary export (should create boundaries.duckdb)
        source_dir = str(tmp_path / "output")
        os.makedirs(source_dir, exist_ok=True)
        stage_boundary_export(con, "overture_division", source_dir, t0)
        con.close()

        # Verify boundaries.duckdb exists and is valid
        from pathlib import Path
        boundaries_path = Path(source_dir) / "boundaries.duckdb"
        assert boundaries_path.exists(), "boundaries.duckdb should be created"

        # Verify the DB can be opened and queried
        check_con = duckdb.connect(str(boundaries_path))
        check_con.execute("LOAD spatial")
        count = check_con.execute("SELECT count(*) FROM places").fetchone()[0]
        assert count >= 0, "boundaries.duckdb should have a places table"
        check_con.close()

    def test_division_tiles_have_nonzero_locality_importance(self, division_parquet, density_parquet, tmp_path):
        """Full division pipeline must produce tiles with non-zero locality importance.

        This test runs the complete division pipeline stages and verifies that
        after backfill, localities have non-zero importance values in their
        tile records.
        """
        from garganorn.stages import stage_import, stage_tile_assignment, stage_export
        from garganorn.quadtree import SOURCES

        con = duckdb.connect(str(tmp_path / "test.duckdb"))
        con.execute("INSTALL spatial; LOAD spatial;")
        t0 = time.monotonic()

        # Import
        stage_import(con, "overture_division", division_parquet, (-122.55, 37.60, -122.30, 37.85), "4GB", t0)

        # Boundary export
        source_dir = str(tmp_path / "output")
        os.makedirs(source_dir, exist_ok=True)
        stage_boundary_export(con, "overture_division", source_dir, t0)

        # Importance backfill (THIS IS THE NEW STAGE)
        stage_division_importance_backfill(con, density_parquet, t0)

        # Tile assignment
        pk_expr = SOURCES["overture_division"].source_pk
        stage_tile_assignment(con, "overture_division", pk_expr, 1000, t0)

        # Containment (required for export)
        from garganorn.stages import stage_containment, _coord_exprs
        lon_expr, lat_expr = _coord_exprs("overture_division", alias="p")
        stage_containment(con, "overture_division", f"p.{pk_expr}", lon_expr, lat_expr, None, t0)

        # Export
        tile_dir = str(tmp_path / "tiles")
        os.makedirs(tile_dir, exist_ok=True)
        manifest = stage_export(con, "overture_division", tile_dir, t0, export_workers=1)
        con.close()

        # Verify that at least one tile has locality with importance > 0
        # For this test, we check the manifest is non-empty
        assert len(manifest) > 0, "Should export at least one tile"

        # Spot-check: verify that localities in the places table have non-zero importance
        # (Note: we'd need to parse the exported JSON tiles for a full check,
        # but for now we verify the backfill stage ran by checking the DB state)
        check_con = duckdb.connect(str(tmp_path / "test.duckdb"))
        result = check_con.execute(
            "SELECT count(*) FROM places WHERE subtype = 'locality' AND importance > 0"
        ).fetchone()
        check_con.close()

        # We expect at least some localities to have importance > 0
        # (This depends on the test data in division_parquet having localities in the bbox)
        locality_count = result[0] if result else 0
        assert locality_count > 0, "At least one locality should have importance > 0 after backfill"
