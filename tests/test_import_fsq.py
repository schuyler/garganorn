"""Tests for foursquare_import.sql."""

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    SF_BBOX, FSQ_ROWS, run_fsq_import,
)


# ---------------------------------------------------------------------------
# Tests: foursquare_import.sql
# ---------------------------------------------------------------------------

class TestFsqImport:
    """Tests for garganorn/sql/foursquare_import.sql."""

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "foursquare_import.sql"
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
        sql_path = REPO_ROOT / "garganorn" / "sql" / "foursquare_import.sql"
        sql = sql_path.read_text()
        assert "ALTER TABLE places ADD COLUMN qk17" not in sql, (
            "foursquare_import.sql still uses ALTER TABLE to add qk17; "
            "fold qk17 into the CTAS SELECT list instead"
        )
        assert "UPDATE places SET qk17" not in sql, (
            "foursquare_import.sql still uses UPDATE to populate qk17; "
            "fold qk17 into the CTAS SELECT list instead"
        )
