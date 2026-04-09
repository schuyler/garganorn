"""Tests for overture_import.sql, overture_importance.sql, and overture_variants.sql."""

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    OV_BBOX, run_overture_import,
)


# ---------------------------------------------------------------------------
# Tests: overture_import.sql
# ---------------------------------------------------------------------------

class TestOvertureImport:
    """Tests for garganorn/sql/overture_import.sql."""

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_import.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def test_places_table_created(self, overture_parquet, tmp_path):
        """After import, the `places` table must exist."""
        db_path = tmp_path / "test_ov_import.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        assert "places" in tables

    def test_places_has_id_column(self, overture_parquet, tmp_path):
        """After import, `places` must have an `id` column."""
        db_path = tmp_path / "test_ov_id_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "id" in cols, f"id column missing; found columns: {cols}"

    def test_places_has_qk17_column(self, overture_parquet, tmp_path):
        """After import, `places` must have a `qk17` column."""
        db_path = tmp_path / "test_ov_qk17_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "qk17" in cols, f"qk17 column missing; found columns: {cols}"

    def test_places_expected_columns(self, overture_parquet, tmp_path):
        """After import, `places` must include the columns expected downstream."""
        required = {"id", "bbox", "qk17"}
        db_path = tmp_path / "test_ov_cols.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        missing = required - cols
        assert not missing, f"Missing columns: {missing}"

    def test_bbox_filter_excludes_out_of_range(self, overture_parquet, tmp_path):
        """Rows outside the bbox must be excluded."""
        db_path = tmp_path / "test_ov_bbox.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        ids = {row[0] for row in conn.execute("SELECT id FROM places").fetchall()}
        conn.close()
        assert "ov006" not in ids, "Out-of-bbox row (ov006) must be excluded"

    def test_null_geometry_excluded(self, overture_parquet, tmp_path):
        """Rows with geometry IS NULL must be excluded."""
        db_path = tmp_path / "test_ov_null_geom.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        ids = {row[0] for row in conn.execute("SELECT id FROM places").fetchall()}
        conn.close()
        assert "ov007" not in ids, "Null-geometry row (ov007) must be excluded"

    def test_good_rows_included(self, overture_parquet, tmp_path):
        """All in-bbox, non-null-geometry rows must survive the import filters."""
        expected = {"ov001", "ov002", "ov003", "ov004", "ov005"}
        db_path = tmp_path / "test_ov_good.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        ids = {row[0] for row in conn.execute("SELECT id FROM places").fetchall()}
        conn.close()
        missing = expected - ids
        assert not missing, f"Expected rows missing after import: {missing}"

    def test_qk17_is_17_chars(self, overture_parquet, tmp_path):
        """qk17 values must be 17-character strings for all surviving rows."""
        db_path = tmp_path / "test_ov_qk17_len.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        bad = conn.execute("""
            SELECT id, qk17
            FROM places
            WHERE qk17 IS NULL OR length(qk17) != 17
        """).fetchall()
        conn.close()
        assert not bad, f"Rows with invalid qk17: {bad}"

    def test_qk17_contains_only_valid_chars(self, overture_parquet, tmp_path):
        """qk17 values must consist only of digits 0-3 (quadkey alphabet)."""
        db_path = tmp_path / "test_ov_qk17_chars.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        bad = conn.execute("""
            SELECT id, qk17
            FROM places
            WHERE NOT regexp_matches(qk17, '^[0-3]{17}$')
        """).fetchall()
        conn.close()
        assert not bad, f"qk17 values with unexpected characters: {bad}"

    def test_geometry_column_is_geometry_type(self, overture_parquet, tmp_path):
        """The geometry column must be of GEOMETRY type after import."""
        db_path = tmp_path / "test_ov_geom_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "geometry" in describe, "geometry column missing from places"
        assert describe["geometry"] == "GEOMETRY", (
            f"Expected geometry to be GEOMETRY, got {describe['geometry']}"
        )


# ---------------------------------------------------------------------------
# Tests: overture_importance.sql
# ---------------------------------------------------------------------------

class TestOvertureImportance:
    """Tests for garganorn/sql/overture_importance.sql.

    Each test creates a fresh DuckDB connection, runs overture_import.sql first,
    then runs overture_importance.sql.
    """

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_importance.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def _run_importance(self, conn):
        """Load and execute overture_importance.sql on `conn`."""
        raw_sql = _load_sql("overture_importance.sql", {"density_norm": "10.0", "idf_norm": "18.0"})
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

    def test_importance_column_added(self, overture_parquet, tmp_path):
        """After overture_importance.sql, `places` must have an `importance` column."""
        db_path = tmp_path / "test_ov_imp_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        self._run_importance(conn)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "importance" in cols, f"importance column missing; found: {cols}"

    def test_importance_column_is_integer(self, overture_parquet, tmp_path):
        """The `importance` column must be INTEGER type."""
        db_path = tmp_path / "test_ov_imp_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        self._run_importance(conn)
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert describe.get("importance") in ("INTEGER", "INT", "INT4", "SIGNED"), (
            f"importance column type unexpected: {describe.get('importance')}"
        )

    def test_importance_values_in_range(self, overture_parquet, tmp_path):
        """All importance values must be in [0, 100]."""
        db_path = tmp_path / "test_ov_imp_range.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        self._run_importance(conn)
        bad = conn.execute("""
            SELECT id, importance
            FROM places
            WHERE importance < 0 OR importance > 100
        """).fetchall()
        conn.close()
        assert not bad, f"Rows with out-of-range importance: {bad}"

    def test_importance_not_null(self, overture_parquet, tmp_path):
        """All surviving rows must have a non-NULL importance value."""
        db_path = tmp_path / "test_ov_imp_notnull.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        self._run_importance(conn)
        nulls = conn.execute("""
            SELECT id FROM places WHERE importance IS NULL
        """).fetchall()
        conn.close()
        assert not nulls, f"Rows with NULL importance: {nulls}"

    def test_unique_category_scores_higher(self, overture_parquet, tmp_path):
        """A place with a unique category should score higher than one with a common category (IDF).

        ov001 has 'coffee_shop' which appears in 3 of 7 surviving rows
        (ov001, ov003, ov004) → IDF = ln(7/3) ≈ 0.847.
        ov005 has 'unique_venue' which appears in only 1 of 7 surviving rows
        → IDF = ln(7/1) ≈ 1.946.
        ov005 should score strictly higher than ov001.
        """
        db_path = tmp_path / "test_ov_idf.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        self._run_importance(conn)
        rows = conn.execute("""
            SELECT id, importance FROM places WHERE id IN ('ov001', 'ov005') ORDER BY id
        """).fetchall()
        conn.close()
        assert len(rows) == 2, f"Expected 2 rows, got {rows}"
        imp = {r[0]: r[1] for r in rows}
        assert imp["ov005"] > imp["ov001"], (
            f"Unique category place ov005 ({imp['ov005']}) should score "
            f"higher than common-category place ov001 ({imp['ov001']})"
        )

    def test_density_scoring_path(self, tmp_path):
        """High-density group scores higher than low-density isolated place.

        Creates a fresh parquet fixture with:
        - Group A: 5 places clustered near SF (share the same zoom-15 quadkey)
        - Group B: 1 place in Tokyo

        Both groups use the same single category so IDF scores are equal.
        The density term (ln(1+count)) dominates: Group A ≈ ln(6) ≈ 1.79,
        Group B ≈ ln(2) ≈ 0.69.  Any Group A place must outscore Group B.
        """
        import duckdb as _duckdb

        parquet_path = tmp_path / "ov_density_test.parquet"

        conn = _duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE tmp_ov_density (
                id          VARCHAR,
                bbox        STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
                geometry    VARCHAR,
                names       STRUCT(
                                common MAP(VARCHAR, VARCHAR),
                                rules  STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]
                            ),
                categories  STRUCT("primary" VARCHAR)
            )
        """)

        # Group A: 5 places clustered in SF (within ~100m, same zoom-15 quadkey).
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

        for ov_id, name, lat, lon in group_a + group_b:
            conn.execute(f"""
                INSERT INTO tmp_ov_density VALUES (
                    '{ov_id}',
                    {{'xmin': {lon - 0.001}, 'ymin': {lat - 0.001},
                      'xmax': {lon + 0.001}, 'ymax': {lat + 0.001}}},
                    'POINT({lon} {lat})',
                    NULL::STRUCT(common MAP(VARCHAR, VARCHAR),
                                 rules STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]),
                    {{'primary': 'catA'}}
                )
            """)

        conn.execute(f"COPY tmp_ov_density TO '{parquet_path}' (FORMAT PARQUET)")
        conn.close()

        parquet_glob = str(tmp_path / "*.parquet")

        # Global bbox covering both SF and Tokyo.
        global_bbox = dict(xmin=-180, xmax=180, ymin=-90, ymax=90)

        db_path = tmp_path / "test_ov_density.duckdb"
        conn = _duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, parquet_glob, bbox=global_bbox)
        self._run_importance(conn)
        rows = conn.execute("""
            SELECT id, importance
            FROM places
            ORDER BY id
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
# Tests: overture_variants.sql
# ---------------------------------------------------------------------------

class TestOvertureVariants:
    """Tests for garganorn/sql/overture_variants.sql.

    Each test runs overture_import.sql first, then overture_variants.sql.
    """

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_variants.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def _run_variants(self, conn):
        """Load and execute overture_variants.sql on `conn`."""
        raw_sql = _load_sql("overture_variants.sql", {})
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

    def test_variants_column_added(self, overture_parquet, tmp_path):
        """After overture_variants.sql, `places` must have a `variants` column."""
        db_path = tmp_path / "test_ov_var_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        self._run_variants(conn)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "variants" in cols, f"variants column missing; found: {cols}"

    def test_variants_column_is_struct_array(self, overture_parquet, tmp_path):
        """The `variants` column must be an array of STRUCTs."""
        db_path = tmp_path / "test_ov_var_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        self._run_variants(conn)
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        col_type = describe.get("variants", "")
        assert "STRUCT" in col_type, (
            f"variants column type should be a STRUCT array; got: {col_type}"
        )
        assert "[]" in col_type or "ARRAY" in col_type.upper(), (
            f"variants column should be an array type; got: {col_type}"
        )

    def test_variants_from_names_common(self, overture_parquet, tmp_path):
        """A row with names.common entries must get non-empty variants (ov001)."""
        db_path = tmp_path / "test_ov_var_common.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        self._run_variants(conn)
        row = conn.execute("""
            SELECT len(variants) FROM places WHERE id = 'ov001'
        """).fetchone()
        conn.close()
        assert row is not None, "ov001 not found in places after variants SQL"
        assert row[0] > 0, (
            f"ov001 has names.common but got empty variants (len={row[0]})"
        )

    def test_variants_from_names_rules(self, overture_parquet, tmp_path):
        """A row with names.rules entries must get non-empty variants (ov002).

        ov002 has a single rules entry: language='en', value='GG Park',
        variant='short'.  The CASE expression must map 'short' → type='short'.
        """
        db_path = tmp_path / "test_ov_var_rules.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        self._run_variants(conn)
        row = conn.execute("""
            SELECT variants[1].name, variants[1].type, variants[1].language
            FROM places WHERE id = 'ov002' AND len(variants) > 0
        """).fetchone()
        conn.close()
        assert row is not None, "ov002 not found or has empty variants after variants SQL"
        assert row[0] == "GG Park", f"Unexpected variant name: {row[0]}"
        assert row[1] == "short", f"Unexpected variant type (expected 'short' from CASE): {row[1]}"
        assert row[2] == "en", f"Unexpected variant language: {row[2]}"

    def test_variants_empty_when_no_names(self, overture_parquet, tmp_path):
        """A row with names IS NULL must get an empty variants array (ov003)."""
        db_path = tmp_path / "test_ov_var_empty.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        self._run_variants(conn)
        row = conn.execute("""
            SELECT len(variants) FROM places WHERE id = 'ov003'
        """).fetchone()
        conn.close()
        assert row is not None, "ov003 not found in places after variants SQL"
        assert row[0] == 0, (
            f"ov003 has no names but got non-empty variants (len={row[0]})"
        )

    def test_variants_struct_has_expected_fields(self, overture_parquet, tmp_path):
        """The variants struct must have name, type, and language fields.

        ov001 has a names.common entry ('en' → 'Blue Bottle Coffee').
        After overture_variants.sql, variants[1] must expose .name, .type,
        and .language.  The type for a names.common entry should be 'alternate'.
        """
        db_path = tmp_path / "test_ov_variants_fields.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        self._run_variants(conn)
        row = conn.execute("""
            SELECT variants[1].name, variants[1].type, variants[1].language
            FROM places
            WHERE id = 'ov001' AND len(variants) > 0
        """).fetchone()
        conn.close()
        assert row is not None, "No variant found for ov001 — expected names.common entry"
        assert row[0] == "Blue Bottle Coffee", f"Unexpected variant name: {row[0]}"
        assert row[1] == "alternate", f"Unexpected variant type: {row[1]}"
        assert row[2] == "en", f"Unexpected variant language: {row[2]}"
