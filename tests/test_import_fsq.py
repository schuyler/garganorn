"""Tests for fsq_import.sql, fsq_importance.sql, and fsq_variants.sql."""

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    SF_BBOX, FSQ_ROWS, run_fsq_import,
)


# ---------------------------------------------------------------------------
# Tests: fsq_import.sql
# ---------------------------------------------------------------------------

class TestFsqImport:
    """Tests for garganorn/sql/fsq_import.sql."""

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "fsq_import.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def test_places_table_created(self, fsq_parquet, tmp_path):
        """After import, the `places` table must exist."""
        db_path = tmp_path / "test_import.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        assert "places" in tables

    def test_places_has_qk17_column(self, fsq_parquet, tmp_path):
        """After import, `places` must have a qk17 column."""
        db_path = tmp_path / "test_qk17_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "qk17" in cols, f"qk17 column missing; found columns: {cols}"

    def test_places_expected_columns(self, fsq_parquet, tmp_path):
        """After import, `places` must include the columns expected downstream."""
        required = {"fsq_place_id", "name", "latitude", "longitude", "geom",
                    "date_refreshed", "date_closed", "bbox", "fsq_category_ids", "qk17"}
        db_path = tmp_path / "test_cols.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        missing = required - cols
        assert not missing, f"Missing columns: {missing}"

    def test_bbox_filter_excludes_out_of_range(self, fsq_parquet, tmp_path):
        """Rows outside the bbox must be excluded."""
        db_path = tmp_path / "test_bbox.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        assert "fsq003" not in ids, "Out-of-bbox row must be excluded"

    def test_closed_places_excluded(self, fsq_parquet, tmp_path):
        """Rows with date_closed IS NOT NULL must be excluded."""
        db_path = tmp_path / "test_closed.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        assert "fsq004" not in ids, "Closed place must be excluded"

    def test_zero_longitude_excluded(self, fsq_parquet, tmp_path):
        """Rows with longitude == 0 must be excluded."""
        db_path = tmp_path / "test_zero_lon.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        assert "fsq005" not in ids, "Zero-longitude row must be excluded"

    def test_null_geom_excluded(self, fsq_parquet, tmp_path):
        """Rows with geom IS NULL must be excluded."""
        db_path = tmp_path / "test_null_geom.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        assert "fsq006" not in ids, "Null-geom row must be excluded"

    def test_stale_date_refreshed_excluded(self, fsq_parquet, tmp_path):
        """Rows with date_refreshed <= 2020-03-15 must be excluded."""
        db_path = tmp_path / "test_stale.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        assert "fsq007" not in ids, "Stale place must be excluded"

    def test_good_rows_included(self, fsq_parquet, tmp_path):
        """All rows that pass filters must appear in the result."""
        expected = {row[0] for row in FSQ_ROWS if row[-1]}
        db_path = tmp_path / "test_good.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        missing = expected - ids
        assert not missing, f"Expected rows missing after import: {missing}"

    def test_qk17_is_17_chars(self, fsq_parquet, tmp_path):
        """qk17 values must be 17-character strings for all surviving rows."""
        db_path = tmp_path / "test_qk17_len.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        bad = conn.execute("""
            SELECT fsq_place_id, qk17
            FROM places
            WHERE qk17 IS NULL OR length(qk17) != 17
        """).fetchall()
        conn.close()
        assert not bad, f"Rows with invalid qk17: {bad}"

    def test_qk17_contains_only_valid_chars(self, fsq_parquet, tmp_path):
        """qk17 values must consist only of digits 0-3 (quadkey alphabet)."""
        db_path = tmp_path / "test_qk17_chars.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        # regexp_full_match: entire string must be [0-3]{17}
        bad = conn.execute("""
            SELECT fsq_place_id, qk17
            FROM places
            WHERE NOT regexp_matches(qk17, '^[0-3]{17}$')
        """).fetchall()
        conn.close()
        assert not bad, f"qk17 values with unexpected characters: {bad}"

    def test_geom_column_is_geometry_type(self, fsq_parquet, tmp_path):
        """The geom column must be of GEOMETRY type after import."""
        db_path = tmp_path / "test_geom_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "geom" in describe, "geom column missing from places"
        assert describe["geom"] == "GEOMETRY", (
            f"Expected geom to be GEOMETRY, got {describe['geom']}"
        )

    def test_qk17_computed_in_ctas_not_alter_update(self):
        """qk17 must be computed inline in the CTAS, not via ALTER TABLE + UPDATE.

        The ALTER TABLE / UPDATE two-pass approach causes a full second scan of
        the places table. Once qk17 is folded into the CTAS SELECT list those
        two statements must not appear in the SQL file.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "fsq_import.sql"
        sql = sql_path.read_text()
        assert "ALTER TABLE places ADD COLUMN qk17" not in sql, (
            "fsq_import.sql still uses ALTER TABLE to add qk17; "
            "fold qk17 into the CTAS SELECT list instead"
        )
        assert "UPDATE places SET qk17" not in sql, (
            "fsq_import.sql still uses UPDATE to populate qk17; "
            "fold qk17 into the CTAS SELECT list instead"
        )


# ---------------------------------------------------------------------------
# Tests: fsq_importance.sql
# ---------------------------------------------------------------------------

class TestFsqImportance:
    """Tests for garganorn/sql/fsq_importance.sql.

    Each test creates a fresh DuckDB connection, runs fsq_import.sql first,
    then runs fsq_importance.sql.
    """

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "fsq_importance.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def _run_importance(self, conn):
        """Load, substitute, and execute fsq_importance.sql on `conn`."""
        substitutions: dict = {"density_norm": "10.0", "idf_norm": "18.0"}
        raw_sql = _load_sql("fsq_importance.sql", substitutions)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

    def test_importance_column_added(self, fsq_parquet, tmp_path):
        """After fsq_importance.sql, `places` must have an `importance` column."""
        db_path = tmp_path / "test_importance_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        self._run_importance(conn)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "importance" in cols, f"importance column missing; found: {cols}"

    def test_importance_column_is_integer(self, fsq_parquet, tmp_path):
        """The `importance` column must be INTEGER type."""
        db_path = tmp_path / "test_importance_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        self._run_importance(conn)
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert describe.get("importance") in ("INTEGER", "INT", "INT4", "SIGNED"), (
            f"importance column type unexpected: {describe.get('importance')}"
        )

    def test_importance_values_in_range(self, fsq_parquet, tmp_path):
        """All importance values must be in [0, 100]."""
        db_path = tmp_path / "test_importance_range.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        self._run_importance(conn)
        bad = conn.execute("""
            SELECT fsq_place_id, importance
            FROM places
            WHERE importance < 0 OR importance > 100
        """).fetchall()
        conn.close()
        assert not bad, f"Rows with out-of-range importance: {bad}"

    def test_importance_not_null(self, fsq_parquet, tmp_path):
        """All surviving rows must have a non-NULL importance value."""
        db_path = tmp_path / "test_importance_notnull.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        self._run_importance(conn)
        nulls = conn.execute("""
            SELECT fsq_place_id FROM places WHERE importance IS NULL
        """).fetchall()
        conn.close()
        assert not nulls, f"Rows with NULL importance: {nulls}"

    def test_diverse_venue_scores_higher_than_single_category(self, fsq_parquet, tmp_path):
        """fsq008 has 4 category IDs; fsq001 has 1. fsq008 should score > fsq001.

        Strict inequality is guaranteed by the fixture:
        - fsq001's sole category (13065143) appears in 2 of 3 surviving rows
          (fsq001, fsq008) → IDF = ln(3/2) ≈ 0.405
        - fsq008 has two exclusive categories (10000001, 10000002) each
          appearing in only 1 of 3 rows → IDF = ln(3/1) ≈ 1.099
        max(idf) for fsq008 (1.099) > max(idf) for fsq001 (0.405), and
        density is equal (each isolated in its own zoom-15 tile), so
        fsq008 scores strictly higher.
        """
        db_path = tmp_path / "test_importance_diversity.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        self._run_importance(conn)
        rows = conn.execute("""
            SELECT fsq_place_id, importance
            FROM places
            WHERE fsq_place_id IN ('fsq001', 'fsq008')
            ORDER BY fsq_place_id
        """).fetchall()
        conn.close()
        assert len(rows) == 2, f"Expected 2 rows, got {rows}"
        imp = {r[0]: r[1] for r in rows}
        # Strict inequality: fsq008's exclusive categories (IDF ≈ 1.099) beat
        # fsq001's partially-shared category (IDF ≈ 0.405); density is equal.
        assert imp["fsq008"] > imp["fsq001"], (
            f"Diverse venue (fsq008={imp['fsq008']}) should score > "
            f"single-category venue (fsq001={imp['fsq001']})"
        )

    def test_density_scoring_path(self, tmp_path):
        """High-density group scores higher than low-density isolated place.

        Creates a fresh parquet fixture with:
        - Group A: 5 places clustered near SF (share the same zoom-15 quadkey)
        - Group B: 1 place in Tokyo

        Both groups use the same single category ID so IDF scores are equal.
        The density term (ln(1+count)) dominates: Group A ≈ ln(6) ≈ 1.79,
        Group B ≈ ln(2) ≈ 0.69.  Any Group A place must outscore Group B.
        """
        import duckdb as _duckdb

        # Build a fresh parquet file for this test only.
        parquet_path = tmp_path / "density_test.parquet"

        conn = _duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE tmp_density (
                fsq_place_id    VARCHAR,
                name            VARCHAR,
                latitude        DOUBLE,
                longitude       DOUBLE,
                bbox            STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
                geom            VARCHAR,
                date_refreshed  DATE,
                date_closed     DATE,
                fsq_category_ids VARCHAR[]
            )
        """)

        # Group A: 5 places clustered in SF (within ~100m, same zoom-15 quadkey).
        # All tiny offsets keep them within the same ~1.2km zoom-15 cell.
        group_a = [
            ("a001", "SF Place 1", 37.7700, -122.4100),
            ("a002", "SF Place 2", 37.7701, -122.4101),
            ("a003", "SF Place 3", 37.7702, -122.4099),
            ("a004", "SF Place 4", 37.7699, -122.4102),
            ("a005", "SF Place 5", 37.7700, -122.4098),
        ]
        # Group B: 1 place in Tokyo.
        group_b = [
            ("b001", "Tokyo Place", 35.6800, 139.6900),
        ]

        for fsq_id, name, lat, lon in group_a + group_b:
            conn.execute(f"""
                INSERT INTO tmp_density VALUES (
                    '{fsq_id}', '{name}', {lat}, {lon},
                    {{'xmin': {lon - 0.001}, 'ymin': {lat - 0.001},
                      'xmax': {lon + 0.001}, 'ymax': {lat + 0.001}}},
                    'POINT({lon} {lat})',
                    '2022-01-01',
                    NULL,
                    ['catA']::VARCHAR[]
                )
            """)

        conn.execute(f"COPY tmp_density TO '{parquet_path}' (FORMAT PARQUET)")
        conn.close()

        parquet_glob = str(tmp_path / "*.parquet")

        # Global bbox covering both SF and Tokyo.
        global_bbox = dict(xmin=-180, xmax=180, ymin=-90, ymax=90)

        db_path = tmp_path / "test_density.duckdb"
        conn = _duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, parquet_glob, bbox=global_bbox)
        self._run_importance(conn)
        rows = conn.execute("""
            SELECT fsq_place_id, importance
            FROM places
            ORDER BY fsq_place_id
        """).fetchall()
        conn.close()

        imp = {r[0]: r[1] for r in rows}
        assert "b001" in imp, f"Group B place missing from results; got: {list(imp.keys())}"
        for aid in ("a001", "a002", "a003", "a004", "a005"):
            assert aid in imp, f"Group A place {aid} missing from results"
            assert imp[aid] > imp["b001"], (
                f"High-density place {aid} (importance={imp[aid]}) should score "
                f"higher than low-density Tokyo place b001 (importance={imp['b001']})"
            )


# ---------------------------------------------------------------------------
# Tests: fsq_variants.sql
# ---------------------------------------------------------------------------

class TestFsqVariants:
    """Tests for garganorn/sql/fsq_variants.sql.

    Each test runs fsq_import.sql first, then fsq_variants.sql.
    """

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "fsq_variants.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def _run_variants(self, conn):
        """Load, substitute, and execute fsq_variants.sql on `conn`."""
        substitutions: dict = {}
        raw_sql = _load_sql("fsq_variants.sql", substitutions)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

    def test_variants_column_added(self, fsq_parquet, tmp_path):
        """After fsq_variants.sql, `places` must have a `variants` column."""
        db_path = tmp_path / "test_variants_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        self._run_variants(conn)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "variants" in cols, f"variants column missing; found: {cols}"

    def test_variants_column_is_struct_array(self, fsq_parquet, tmp_path):
        """The `variants` column must be STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]."""
        db_path = tmp_path / "test_variants_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        self._run_variants(conn)
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        col_type = describe.get("variants", "")
        # DuckDB may report type as STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
        assert "STRUCT" in col_type, (
            f"variants column type should be a STRUCT array; got: {col_type}"
        )
        assert "[]" in col_type or "ARRAY" in col_type.upper(), (
            f"variants column should be an array type; got: {col_type}"
        )

    def test_variants_defaults_to_empty_array(self, fsq_parquet, tmp_path):
        """All rows must have variants defaulting to an empty array []."""
        db_path = tmp_path / "test_variants_default.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        self._run_variants(conn)
        # len() on a list/array column returns the length of each element
        non_empty = conn.execute("""
            SELECT fsq_place_id, len(variants)
            FROM places
            WHERE len(variants) != 0
        """).fetchall()
        conn.close()
        assert not non_empty, (
            f"Expected all variants to default to empty array; "
            f"got non-empty rows: {non_empty}"
        )

    def test_variants_struct_has_expected_fields(self, fsq_parquet, tmp_path):
        """The variants struct must have name, type, and language fields.

        Verified by attempting to access each field — an error means the field
        is absent.
        """
        db_path = tmp_path / "test_variants_fields.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_fsq_import(conn, fsq_parquet)
        self._run_variants(conn)

        # Insert a test row with a populated variant to verify field access
        conn.execute("""
            UPDATE places
            SET variants = [{'name': 'Test Variant', 'type': 'alternate', 'language': 'en'}]
                ::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
            WHERE fsq_place_id = 'fsq001'
        """)

        row = conn.execute("""
            SELECT variants[1].name, variants[1].type, variants[1].language
            FROM places
            WHERE fsq_place_id = 'fsq001'
        """).fetchone()
        conn.close()

        assert row is not None, "No row returned for fsq001"
        assert row[0] == "Test Variant", f"Unexpected variant name: {row[0]}"
        assert row[1] == "alternate", f"Unexpected variant type: {row[1]}"
        assert row[2] == "en", f"Unexpected variant language: {row[2]}"
