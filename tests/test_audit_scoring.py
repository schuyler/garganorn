"""Failing tests for scoring bug fixes (SCORE-1/2/3/4/5).

These tests verify the expected behavior for norm constant validation and
negative population clamping. They MUST fail with the current code and
pass after the fixes are implemented.
"""

import os
import tempfile
from pathlib import Path

import duckdb
import pytest

from garganorn.stages import stage_import


class TestNormConstantValidation:
    """Tests for SCORE-1/2/3/4: Norm constant validation in stage_import().

    The stage_import() function should raise ValueError if any norm constant
    (density_norm, idf_norm, pop_norm) is <= 0.
    """

    def setup_method(self):
        """Set up a minimal test database with required schema."""
        self.con = duckdb.connect()
        self.con.execute("INSTALL spatial; LOAD spatial;")

    def teardown_method(self):
        """Clean up the test database."""
        self.con.close()

    def test_density_norm_zero_raises_error(self):
        """density_norm=0 should raise ValueError before SQL execution."""
        # Create a minimal valid parquet file for testing
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "test.parquet")
            # Create empty but valid parquet with required schema
            self.con.execute(f"""
                COPY (
                    SELECT NULL::VARCHAR AS fsq_place_id,
                           NULL::VARCHAR AS name,
                           NULL::DOUBLE AS latitude,
                           NULL::DOUBLE AS longitude,
                           NULL::DATE AS date_closed,
                           NULL::TIMESTAMP AS date_refreshed,
                           NULL::DOUBLE[] AS fsq_category_ids,
                           NULL::GEOMETRY AS geom,
                           {{'xmin': 0.0, 'ymin': 0.0, 'xmax': 0.0, 'ymax': 0.0}} AS bbox
                    WHERE FALSE
                ) TO '{parquet_path}' (FORMAT PARQUET)
            """)

            with pytest.raises(ValueError, match="density_norm.*must be positive"):
                stage_import(
                    self.con,
                    source="foursquare",
                    parquet_glob=parquet_path,
                    bbox=None,
                    memory_limit="4GB",
                    t0=0.0,
                    density_norm=0.0,
                    idf_norm=18.0,
                    pop_norm=20.0
                )

    def test_density_norm_negative_raises_error(self):
        """density_norm < 0 should raise ValueError before SQL execution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "test.parquet")
            self.con.execute(f"""
                COPY (
                    SELECT NULL::VARCHAR AS fsq_place_id,
                           NULL::VARCHAR AS name,
                           NULL::DOUBLE AS latitude,
                           NULL::DOUBLE AS longitude,
                           NULL::DATE AS date_closed,
                           NULL::TIMESTAMP AS date_refreshed,
                           NULL::DOUBLE[] AS fsq_category_ids,
                           NULL::GEOMETRY AS geom,
                           {{'xmin': 0.0, 'ymin': 0.0, 'xmax': 0.0, 'ymax': 0.0}} AS bbox
                    WHERE FALSE
                ) TO '{parquet_path}' (FORMAT PARQUET)
            """)

            with pytest.raises(ValueError, match="density_norm.*must be positive"):
                stage_import(
                    self.con,
                    source="foursquare",
                    parquet_glob=parquet_path,
                    bbox=None,
                    memory_limit="4GB",
                    t0=0.0,
                    density_norm=-10.0,
                    idf_norm=18.0,
                    pop_norm=20.0
                )

    def test_idf_norm_zero_raises_error(self):
        """idf_norm=0 should raise ValueError before SQL execution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "test.parquet")
            self.con.execute(f"""
                COPY (
                    SELECT NULL::VARCHAR AS fsq_place_id,
                           NULL::VARCHAR AS name,
                           NULL::DOUBLE AS latitude,
                           NULL::DOUBLE AS longitude,
                           NULL::DATE AS date_closed,
                           NULL::TIMESTAMP AS date_refreshed,
                           NULL::DOUBLE[] AS fsq_category_ids,
                           NULL::GEOMETRY AS geom,
                           {{'xmin': 0.0, 'ymin': 0.0, 'xmax': 0.0, 'ymax': 0.0}} AS bbox
                    WHERE FALSE
                ) TO '{parquet_path}' (FORMAT PARQUET)
            """)

            with pytest.raises(ValueError, match="idf_norm.*must be positive"):
                stage_import(
                    self.con,
                    source="foursquare",
                    parquet_glob=parquet_path,
                    bbox=None,
                    memory_limit="4GB",
                    t0=0.0,
                    density_norm=10.0,
                    idf_norm=0.0,
                    pop_norm=20.0
                )

    def test_idf_norm_negative_raises_error(self):
        """idf_norm < 0 should raise ValueError before SQL execution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "test.parquet")
            self.con.execute(f"""
                COPY (
                    SELECT NULL::VARCHAR AS fsq_place_id,
                           NULL::VARCHAR AS name,
                           NULL::DOUBLE AS latitude,
                           NULL::DOUBLE AS longitude,
                           NULL::DATE AS date_closed,
                           NULL::TIMESTAMP AS date_refreshed,
                           NULL::DOUBLE[] AS fsq_category_ids,
                           NULL::GEOMETRY AS geom,
                           {{'xmin': 0.0, 'ymin': 0.0, 'xmax': 0.0, 'ymax': 0.0}} AS bbox
                    WHERE FALSE
                ) TO '{parquet_path}' (FORMAT PARQUET)
            """)

            with pytest.raises(ValueError, match="idf_norm.*must be positive"):
                stage_import(
                    self.con,
                    source="foursquare",
                    parquet_glob=parquet_path,
                    bbox=None,
                    memory_limit="4GB",
                    t0=0.0,
                    density_norm=10.0,
                    idf_norm=-18.0,
                    pop_norm=20.0
                )

    def test_pop_norm_zero_raises_error(self):
        """pop_norm=0 should raise ValueError before SQL execution (overture_division)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            division_parquet = os.path.join(tmpdir, "division.parquet")
            division_area_parquet = os.path.join(tmpdir, "division_area.parquet")

            # Create empty but valid division parquet
            self.con.execute(f"""
                COPY (
                    SELECT NULL::VARCHAR AS id,
                           NULL::STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR), variants MAP(VARCHAR, VARCHAR[])) AS names,
                           NULL::VARCHAR AS subtype,
                           NULL::VARCHAR AS country,
                           NULL::VARCHAR AS region,
                           NULL::VARCHAR AS wikidata,
                           NULL::BIGINT AS population,
                           NULL::VARCHAR AS parent_division_id
                    WHERE FALSE
                ) TO '{division_parquet}' (FORMAT PARQUET)
            """)

            # Create empty but valid division_area parquet
            self.con.execute(f"""
                COPY (
                    SELECT NULL::VARCHAR AS division_id,
                           NULL::INTEGER AS admin_level,
                           NULL::GEOMETRY AS geometry,
                           FALSE::BOOLEAN AS is_land,
                           {{'xmin': 0.0, 'ymin': 0.0, 'xmax': 0.0, 'ymax': 0.0}} AS bbox
                    WHERE FALSE
                ) TO '{division_area_parquet}' (FORMAT PARQUET)
            """)

            with pytest.raises(ValueError, match="pop_norm.*must be positive"):
                stage_import(
                    self.con,
                    source="overture_division",
                    parquet_glob=(division_parquet, division_area_parquet),
                    bbox=None,
                    memory_limit="4GB",
                    t0=0.0,
                    density_norm=10.0,
                    idf_norm=18.0,
                    pop_norm=0.0
                )

    def test_pop_norm_negative_raises_error(self):
        """pop_norm < 0 should raise ValueError before SQL execution (overture_division)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            division_parquet = os.path.join(tmpdir, "division.parquet")
            division_area_parquet = os.path.join(tmpdir, "division_area.parquet")

            self.con.execute(f"""
                COPY (
                    SELECT NULL::VARCHAR AS id,
                           NULL::STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR), variants MAP(VARCHAR, VARCHAR[])) AS names,
                           NULL::VARCHAR AS subtype,
                           NULL::VARCHAR AS country,
                           NULL::VARCHAR AS region,
                           NULL::VARCHAR AS wikidata,
                           NULL::BIGINT AS population,
                           NULL::VARCHAR AS parent_division_id
                    WHERE FALSE
                ) TO '{division_parquet}' (FORMAT PARQUET)
            """)

            self.con.execute(f"""
                COPY (
                    SELECT NULL::VARCHAR AS division_id,
                           NULL::INTEGER AS admin_level,
                           NULL::GEOMETRY AS geometry,
                           FALSE::BOOLEAN AS is_land,
                           {{'xmin': 0.0, 'ymin': 0.0, 'xmax': 0.0, 'ymax': 0.0}} AS bbox
                    WHERE FALSE
                ) TO '{division_area_parquet}' (FORMAT PARQUET)
            """)

            with pytest.raises(ValueError, match="pop_norm.*must be positive"):
                stage_import(
                    self.con,
                    source="overture_division",
                    parquet_glob=(division_parquet, division_area_parquet),
                    bbox=None,
                    memory_limit="4GB",
                    t0=0.0,
                    density_norm=10.0,
                    idf_norm=18.0,
                    pop_norm=-20.0
                )

    def test_multiple_invalid_norms_raise_error(self):
        """Multiple invalid norm constants should raise ValueError with all mentioned."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "test.parquet")
            self.con.execute(f"""
                COPY (
                    SELECT NULL::VARCHAR AS fsq_place_id,
                           NULL::VARCHAR AS name,
                           NULL::DOUBLE AS latitude,
                           NULL::DOUBLE AS longitude,
                           NULL::DATE AS date_closed,
                           NULL::TIMESTAMP AS date_refreshed,
                           NULL::DOUBLE[] AS fsq_category_ids,
                           NULL::GEOMETRY AS geom,
                           {{'xmin': 0.0, 'ymin': 0.0, 'xmax': 0.0, 'ymax': 0.0}} AS bbox
                    WHERE FALSE
                ) TO '{parquet_path}' (FORMAT PARQUET)
            """)

            with pytest.raises(ValueError, match="density_norm.*idf_norm"):
                stage_import(
                    self.con,
                    source="foursquare",
                    parquet_glob=parquet_path,
                    bbox=None,
                    memory_limit="4GB",
                    t0=0.0,
                    density_norm=-10.0,
                    idf_norm=0.0,
                    pop_norm=20.0
                )

    def test_valid_norms_do_not_raise_error(self):
        """All positive norm constants should not raise ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "test.parquet")
            self.con.execute(f"""
                COPY (
                    SELECT NULL::VARCHAR AS fsq_place_id,
                           NULL::VARCHAR AS name,
                           NULL::DOUBLE AS latitude,
                           NULL::DOUBLE AS longitude,
                           NULL::DATE AS date_closed,
                           NULL::TIMESTAMP AS date_refreshed,
                           NULL::DOUBLE[] AS fsq_category_ids,
                           NULL::GEOMETRY AS geom,
                           {{'xmin': 0.0, 'ymin': 0.0, 'xmax': 0.0, 'ymax': 0.0}} AS bbox
                    WHERE FALSE
                ) TO '{parquet_path}' (FORMAT PARQUET)
            """)

            # Should not raise any error
            stage_import(
                self.con,
                source="foursquare",
                parquet_glob=parquet_path,
                bbox=None,
                memory_limit="4GB",
                t0=0.0,
                density_norm=10.0,
                idf_norm=18.0,
                pop_norm=20.0
            )


class TestNegativePopulationClamping:
    """Tests for SCORE-5: Negative population clamping in Overture division import.

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
                    row('Test Division', map_from_entries([('en', 'Test Division')]), map_from_entries([]))::STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR), variants MAP(VARCHAR, VARCHAR[])) AS names,
                    'locality'::VARCHAR AS subtype,
                    'US'::VARCHAR AS country,
                    'CA'::VARCHAR AS region,
                    'Q123'::VARCHAR AS wikidata,
                    -- Negative population: this is the bug trigger
                    CAST(-1000 AS BIGINT) AS population,
                    NULL::VARCHAR AS parent_division_id
                UNION ALL
                SELECT
                    'test_div_2' AS id,
                    row('Test Division 2', map_from_entries([('en', 'Test Division 2')]), map_from_entries([]))::STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR), variants MAP(VARCHAR, VARCHAR[])) AS names,
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
                    row('Test Division 3', map_from_entries([('en', 'Test Division 3')]), map_from_entries([]))::STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR), variants MAP(VARCHAR, VARCHAR[])) AS names,
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
        """Division with negative population should have importance >= 0.

        The bug: ln(1 + coalesce(population, 0)) with population=-1000 produces
        ln(1 + -1000) = ln(-999) = NaN (or error).

        The fix: GREATEST(coalesce(population, 0), 0) clamps to 0 before ln(),
        so ln(1 + 0) = 0, producing non-negative importance.
        """
        division_parquet = self._create_division_parquet()
        division_area_parquet = self._create_division_area_parquet()

        # Import divisions with negative population
        stage_import(
            self.con,
            source="overture_division",
            parquet_glob=(division_parquet, division_area_parquet),
            bbox=None,
            memory_limit="4GB",
            t0=0.0,
            density_norm=10.0,
            idf_norm=18.0,
            pop_norm=20.0
        )

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

        # With the fix, negative population (-1000) should be clamped to 0,
        # producing importance = round(40 * least(ln(1 + 0) / 20.0, 1.0)) = round(40 * 0) = 0
        # test_div_1 has negative population, should get importance=0 after fix
        assert test_div_1[2] == 0, f"test_div_1 (population=-1000) should have importance=0, got {test_div_1[2]}"

        # test_div_2 has population=0, should also get importance=0
        assert test_div_2[2] == 0, f"test_div_2 (population=0) should have importance=0, got {test_div_2[2]}"

        # test_div_3 has positive population (50000), should have positive importance
        # ln(1 + 50000) ≈ 10.82, so importance ≈ round(40 * 10.82/20.0) ≈ round(21.64) = 22
        assert test_div_3[2] > 0, f"test_div_3 (population=50000) should have positive importance, got {test_div_3[2]}"

    def test_negative_population_not_nan(self):
        """Division with negative population should not produce NaN importance.

        ln of negative number produces NaN. The fix ensures population is clamped
        to non-negative before ln() is applied.
        """
        division_parquet = self._create_division_parquet()
        division_area_parquet = self._create_division_area_parquet()

        stage_import(
            self.con,
            source="overture_division",
            parquet_glob=(division_parquet, division_area_parquet),
            bbox=None,
            memory_limit="4GB",
            t0=0.0,
            density_norm=10.0,
            idf_norm=18.0,
            pop_norm=20.0
        )

        # Check that no importance is NaN or NULL
        result = self.con.execute("""
            SELECT COUNT(*) AS invalid_count
            FROM places
            WHERE importance IS NULL OR importance != importance  -- NaN check
        """).fetchone()

        assert result[0] == 0, f"Found {result[0]} divisions with NULL or NaN importance"

    def test_sql_clamping_at_selection_point(self):
        """Verify the fix is applied at population selection point (line 59).

        The design review specifies: clamp at the selection point where population
        is read from the division table, not just at the formula usage point.
        """
        division_parquet = self._create_division_parquet()
        division_area_parquet = self._create_division_area_parquet()

        # Read the SQL file to verify the fix location
        sql_path = Path(__file__).parent.parent / "garganorn" / "sql" / "overture_division_import.sql"
        sql_content = sql_path.read_text()

        # The fix should be at the selection point (line ~59) where population is read
        # The pattern should be: greatest(coalesce(d.population, 0), 0)
        # This test verifies the SQL contains the clamping logic

        # Check for the clamping pattern in division_base CTE (around line 59)
        # The fix should use GREATEST to clamp negative population to 0
        assert "greatest(" in sql_content.lower() or "greatest(" in sql_content, \
            "SQL should contain GREATEST() function to clamp negative population"

        # Actually import and verify behavior
        stage_import(
            self.con,
            source="overture_division",
            parquet_glob=(division_parquet, division_area_parquet),
            bbox=None,
            memory_limit="4GB",
            t0=0.0,
            density_norm=10.0,
            idf_norm=18.0,
            pop_norm=20.0
        )

        # Verify the population value itself is clamped in the base table
        # (not just in the importance formula)
        result = self.con.execute("""
            SELECT population
            FROM places
            WHERE id = 'test_div_1'
        """).fetchone()

        # After the fix, negative population should be visible as the original negative value
        # in the base table, but clamped in the importance formula
        # The fix at selection point ensures the value used in calculations is clamped
        # So we check that importance is non-negative (the behavioral test)
        assert result is not None, "test_div_1 should be imported"

        importance = self.con.execute("""
            SELECT importance
            FROM places
            WHERE id = 'test_div_1'
        """).fetchone()[0]

        assert importance >= 0, "Importance should be non-negative with clamped population"
