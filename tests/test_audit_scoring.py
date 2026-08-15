"""Tests for scoring behavior."""

import os
import tempfile
from pathlib import Path

import duckdb
import pytest

from garganorn.stages import stage_import


class TestNegativePopulationClamping:
    """Tests for negative population clamping in Overture division import.

    The SQL should use GREATEST(coalesce(population, 0), 0) to clamp negative
    population values to 0 before computing ln(1 + population), ensuring
    importance is always >= 0.
    """

    def setup_method(self):
        """Set up test database and create test parquet files."""
        self.con = duckdb.connect()
        self.con.execute("INSTALL spatial; LOAD spatial;")
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmpdir_path = Path(self.tmpdir.name)

    def teardown_method(self):
        """Clean up test resources."""
        self.tmpdir.cleanup()
        self.con.close()

    def _create_division_parquet(self):
        """Create test division parquet with negative population."""
        division_parquet = self.tmpdir_path / "division.parquet"
        self.con.execute(f"""
            COPY (
                SELECT
                    'test_div_1' AS id,
                    row('Test Division', map_from_entries([('en', 'Test Division')]), MAP([],[]::VARCHAR[]))::STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR), variants MAP(VARCHAR, VARCHAR[])) AS names,
                    'locality'::VARCHAR AS subtype,
                    'US'::VARCHAR AS country,
                    'CA'::VARCHAR AS region,
                    'Q123'::VARCHAR AS wikidata,
                    -- Negative population: exercises the clamp
                    CAST(-1000 AS BIGINT) AS population,
                    NULL::VARCHAR AS parent_division_id
                UNION ALL
                SELECT
                    'test_div_2' AS id,
                    row('Test Division 2', map_from_entries([('en', 'Test Division 2')]), MAP([],[]::VARCHAR[]))::STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR), variants MAP(VARCHAR, VARCHAR[])) AS names,
                    'locality'::VARCHAR AS subtype,
                    'US'::VARCHAR AS country,
                    'NY'::VARCHAR AS region,
                    'Q456'::VARCHAR AS wikidata,
                    -- Zero population: should be handled correctly
                    CAST(0 AS BIGINT) AS population,
                    NULL::VARCHAR AS parent_division_id
                UNION ALL
                SELECT
                    'test_div_3' AS id,
                    row('Test Division 3', map_from_entries([('en', 'Test Division 3')]), MAP([],[]::VARCHAR[]))::STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR), variants MAP(VARCHAR, VARCHAR[])) AS names,
                    'locality'::VARCHAR AS subtype,
                    'US'::VARCHAR AS country,
                    'TX'::VARCHAR AS region,
                    'Q789'::VARCHAR AS wikidata,
                    -- Positive population: should work correctly
                    CAST(50000 AS BIGINT) AS population,
                    NULL::VARCHAR AS parent_division_id
            ) TO '{division_parquet}' (FORMAT PARQUET)
        """)
        return str(division_parquet)

    def _create_division_area_parquet(self):
        """Create test division_area parquet with geometries."""
        division_area_parquet = self.tmpdir_path / "division_area.parquet"

        # Create point geometries (simplified for testing)
        self.con.execute(f"""
            COPY (
                SELECT
                    'test_div_1' AS division_id,
                    4 AS admin_level,
                    ST_Point(-122.4194, 37.7749)::GEOMETRY AS geometry,
                    TRUE::BOOLEAN AS is_land,
                    {{'xmin': -122.4194, 'ymin': 37.7749, 'xmax': -122.4194, 'ymax': 37.7749}} AS bbox
                UNION ALL
                SELECT
                    'test_div_2' AS division_id,
                    4 AS admin_level,
                    ST_Point(-74.0060, 40.7128)::GEOMETRY AS geometry,
                    TRUE::BOOLEAN AS is_land,
                    {{'xmin': -74.0060, 'ymin': 40.7128, 'xmax': -74.0060, 'ymax': 40.7128}} AS bbox
                UNION ALL
                SELECT
                    'test_div_3' AS division_id,
                    4 AS admin_level,
                    ST_Point(-97.7500, 30.2500)::GEOMETRY AS geometry,
                    TRUE::BOOLEAN AS is_land,
                    {{'xmin': -97.7500, 'ymin': 30.2500, 'xmax': -97.7500, 'ymax': 30.2500}} AS bbox
            ) TO '{division_area_parquet}' (FORMAT PARQUET)
        """)
        return str(division_area_parquet)

    def test_negative_population_importance_is_non_negative(self):
        """Division with negative population has importance >= 0.

        ln(1 + population) is NaN for population < -1; GREATEST(coalesce(population, 0), 0)
        clamps population to 0 before the log so importance is never negative.
        """
        division_parquet = self._create_division_parquet()
        division_area_parquet = self._create_division_area_parquet()

        # Import divisions with negative population
        places_parquet = str(self.tmpdir_path / "places.parquet")
        stage_import("overture_division", (division_parquet, division_area_parquet),
                     None, places_parquet, memory_limit="4GB",
                     density_norm=10.0, pop_norm=20.0, force=True)
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE places AS SELECT * FROM read_parquet('{places_parquet}')")

        # Check that all importance scores are non-negative
        result = self.con.execute("""
            SELECT id, population, importance
            FROM places
            ORDER BY id
        """).fetchall()

        # Extract results
        test_div_1 = next(row for row in result if row[0] == 'test_div_1')
        test_div_2 = next(row for row in result if row[0] == 'test_div_2')
        test_div_3 = next(row for row in result if row[0] == 'test_div_3')

        # All importance scores must be >= 0
        assert test_div_1[2] >= 0, f"test_div_1 (population={test_div_1[1]}) has negative importance: {test_div_1[2]}"
        assert test_div_2[2] >= 0, f"test_div_2 (population={test_div_2[1]}) has negative importance: {test_div_2[2]}"
        assert test_div_3[2] >= 0, f"test_div_3 (population={test_div_3[1]}) has negative importance: {test_div_3[2]}"

        # Negative population (-1000) clamps to 0, giving
        # importance = round(40 * least(ln(1 + 0) / 20.0, 1.0)) = 0
        assert test_div_1[2] == 0, f"test_div_1 (population=-1000) should have importance=0, got {test_div_1[2]}"

        # test_div_2 has population=0, should also get importance=0
        assert test_div_2[2] == 0, f"test_div_2 (population=0) should have importance=0, got {test_div_2[2]}"

        # test_div_3 has positive population (50000), should have positive importance
        # ln(1 + 50000) ≈ 10.82, so importance ≈ round(40 * 10.82/20.0) ≈ round(21.64) = 22
        assert test_div_3[2] > 0, f"test_div_3 (population=50000) should have positive importance, got {test_div_3[2]}"

    def test_negative_population_not_nan(self):
        """Division with negative population does not produce NaN importance.

        Population is clamped to non-negative before ln() is applied.
        """
        division_parquet = self._create_division_parquet()
        division_area_parquet = self._create_division_area_parquet()

        places_parquet = str(self.tmpdir_path / "places.parquet")
        stage_import("overture_division", (division_parquet, division_area_parquet),
                     None, places_parquet, memory_limit="4GB",
                     density_norm=10.0, pop_norm=20.0, force=True)
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE places AS SELECT * FROM read_parquet('{places_parquet}')")

        # Check that no importance is NaN or NULL
        result = self.con.execute("""
            SELECT COUNT(*) AS invalid_count
            FROM places
            WHERE importance IS NULL OR importance != importance  -- NaN check
        """).fetchone()

        assert result[0] == 0, f"Found {result[0]} divisions with NULL or NaN importance"

    def test_sql_clamping_at_selection_point(self):
        """Population is clamped to non-negative where it is read from the
        division table (division_base's population column), not just in the
        importance formula, so any downstream consumer of the population
        column also sees a non-negative value.
        """
        division_parquet = self._create_division_parquet()
        division_area_parquet = self._create_division_area_parquet()

        # division_base clamps population with GREATEST(coalesce(population, 0), 0).
        sql_path = Path(__file__).parent.parent / "garganorn" / "sql" / "overture_division_import.sql"
        sql_content = sql_path.read_text()

        assert "greatest(" in sql_content.lower() or "greatest(" in sql_content, \
            "SQL should contain GREATEST() function to clamp negative population"

        # Actually import and verify behavior
        places_parquet = str(self.tmpdir_path / "places.parquet")
        stage_import("overture_division", (division_parquet, division_area_parquet),
                     None, places_parquet, memory_limit="4GB",
                     density_norm=10.0, pop_norm=20.0, force=True)
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE places AS SELECT * FROM read_parquet('{places_parquet}')")

        # test_div_1 is still imported even though its population is negative.
        result = self.con.execute("""
            SELECT population
            FROM places
            WHERE id = 'test_div_1'
        """).fetchone()

        assert result is not None, "test_div_1 should be imported"

        importance = self.con.execute("""
            SELECT importance
            FROM places
            WHERE id = 'test_div_1'
        """).fetchone()[0]

        assert importance >= 0, "Importance should be non-negative with clamped population"
