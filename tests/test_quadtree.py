"""Tests for quadtree export SQL files.

These tests cover three SQL files that will live in garganorn/sql/:

  fsq_import.sql    — creates a `places` table from FSQ parquet with bbox/quality
                      filters, then adds a qk17 column (quadkey at zoom 17)
  fsq_importance.sql — adds an `importance` INTEGER column (0-100) to `places`
  fsq_variants.sql  — adds a `variants` STRUCT[] column to `places`

Tests are in the Red phase: all SQL files are absent, so every test that
attempts to load one will raise FileNotFoundError.  That is the expected
failure mode.
"""

import os
import pathlib
import string
import tempfile

import duckdb
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REPO_ROOT = pathlib.Path(__file__).parent.parent

def _load_sql(filename: str, substitutions: dict) -> str:
    """Read a SQL file from garganorn/sql/ and substitute ${var} placeholders.

    Raises FileNotFoundError when the SQL file does not yet exist (Red phase).
    """
    sql_path = REPO_ROOT / "garganorn" / "sql" / filename
    # Let FileNotFoundError propagate — that's the expected Red-phase failure.
    raw = sql_path.read_text()
    # Use string.Template for ${var} substitution.
    return string.Template(raw).substitute(substitutions)


def _strip_spatial_install(sql: str) -> str:
    """Remove INSTALL/LOAD spatial lines; extension is already available in tests."""
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("INSTALL spatial") or stripped.startswith("LOAD spatial"):
            continue
        lines.append(line)
    return "\n".join(lines)


def _strip_memory_limit(sql: str) -> str:
    """Remove SET memory_limit lines; not needed in tests."""
    lines = [
        line for line in sql.splitlines()
        if not line.strip().startswith("SET memory_limit")
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Fixtures: synthetic FSQ parquet file
# ---------------------------------------------------------------------------

# All rows share latitude/longitude inside SF bbox unless noted otherwise.
#
# in_bbox rows  — should survive the bbox filter
# out_bbox row  — longitude outside xmax, should be excluded
# closed row    — date_closed IS NOT NULL, should be excluded
# zero_coords   — longitude == 0, should be excluded
# null_geom     — geom IS NULL, should be excluded
# stale_refresh — date_refreshed <= 2020-03-15, should be excluded

_SF_BBOX = dict(xmin=-122.55, xmax=-122.30, ymin=37.60, ymax=37.85)

# (fsq_place_id, name, latitude, longitude, date_refreshed, date_closed, geom_wkt,
#  fsq_category_ids, expected_in_result)
_FSQ_ROWS = [
    # In-bbox, good quality — should survive
    ("fsq001", "Blue Bottle Coffee",  37.7749, -122.4194, "2022-01-01", None,
     "POINT(-122.4194 37.7749)", ["13065143"], True),
    ("fsq002", "Golden Gate Park",    37.7694, -122.4862, "2021-06-15", None,
     "POINT(-122.4862 37.7694)", ["16000178", "16000179"], True),
    # Out of bbox (longitude < xmin)
    ("fsq003", "Faraway Place",       37.7500, -123.0000, "2022-01-01", None,
     "POINT(-123.0000 37.7500)", ["13065143"], False),
    # date_closed is not null — should be excluded
    ("fsq004", "Closed Cafe",         37.7600, -122.4000, "2022-01-01", "2023-01-01",
     "POINT(-122.4000 37.7600)", ["13065143"], False),
    # longitude == 0 — should be excluded
    ("fsq005", "Zero Lon Place",      37.7600,   0.0000,  "2022-01-01", None,
     "POINT(0.0 37.7600)", ["13065143"], False),
    # geom IS NULL — should be excluded
    ("fsq006", "Null Geom Place",     37.7700, -122.4100, "2022-01-01", None,
     None, ["13065143"], False),
    # date_refreshed too old — should be excluded
    ("fsq007", "Stale Place",         37.7710, -122.4110, "2019-01-01", None,
     "POINT(-122.4110 37.7710)", ["13065143"], False),
    # A second good in-bbox place with multiple categories (higher diversity)
    ("fsq008", "Diverse Venue",       37.7800, -122.4300, "2023-03-01", None,
     "POINT(-122.4300 37.7800)",
     ["13065143", "16000178", "10000001", "10000002"], True),
]


@pytest.fixture(scope="module")
def fsq_parquet(tmp_path_factory):
    """Write a single FSQ-schema parquet file and return a glob path for it."""
    base = tmp_path_factory.mktemp("fsq_parquet")
    parquet_path = base / "fsq_data.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE tmp_fsq (
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

    for row in _FSQ_ROWS:
        fsq_id, name, lat, lon, date_ref, date_closed, geom_wkt, cat_ids, _ = row
        # Build bbox from coordinates
        bbox_xmin = lon - 0.001
        bbox_xmax = lon + 0.001
        bbox_ymin = lat - 0.001
        bbox_ymax = lat + 0.001
        # Represent categories as a list literal
        cat_str = "[" + ", ".join(f"'{c}'" for c in cat_ids) + "]"

        closed_val = f"'{date_closed}'" if date_closed else "NULL"
        geom_val = f"'{geom_wkt}'" if geom_wkt else "NULL"

        conn.execute(f"""
            INSERT INTO tmp_fsq VALUES (
                '{fsq_id}', '{name}', {lat}, {lon},
                {{'xmin': {bbox_xmin}, 'ymin': {bbox_ymin},
                  'xmax': {bbox_xmax}, 'ymax': {bbox_ymax}}},
                {geom_val},
                '{date_ref}',
                {closed_val},
                {cat_str}::VARCHAR[]
            )
        """)

    conn.execute(f"COPY tmp_fsq TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()

    return str(base / "*.parquet")


# ---------------------------------------------------------------------------
# Helper: run fsq_import.sql against the parquet fixture
# ---------------------------------------------------------------------------

def _run_import(conn, parquet_glob, bbox=None):
    """Load, substitute, and execute fsq_import.sql on `conn`.

    bbox defaults to the SF bbox defined in _SF_BBOX.
    Strips INSTALL/LOAD spatial and SET memory_limit lines for test isolation.
    """
    if bbox is None:
        bbox = _SF_BBOX

    substitutions = {
        "memory_limit": "4GB",
        "parquet_glob": parquet_glob,
        "xmin": bbox["xmin"],
        "xmax": bbox["xmax"],
        "ymin": bbox["ymin"],
        "ymax": bbox["ymax"],
    }
    raw_sql = _load_sql("fsq_import.sql", substitutions)
    sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
    conn.execute(sql)


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
        _run_import(conn, fsq_parquet)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        assert "places" in tables

    def test_places_has_qk17_column(self, fsq_parquet, tmp_path):
        """After import, `places` must have a qk17 column."""
        db_path = tmp_path / "test_qk17_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        missing = required - cols
        assert not missing, f"Missing columns: {missing}"

    def test_bbox_filter_excludes_out_of_range(self, fsq_parquet, tmp_path):
        """Rows outside the bbox must be excluded."""
        db_path = tmp_path / "test_bbox.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
        ids = {row[0] for row in conn.execute(
            "SELECT fsq_place_id FROM places"
        ).fetchall()}
        conn.close()
        assert "fsq007" not in ids, "Stale place must be excluded"

    def test_good_rows_included(self, fsq_parquet, tmp_path):
        """All rows that pass filters must appear in the result."""
        expected = {row[0] for row in _FSQ_ROWS if row[-1]}
        db_path = tmp_path / "test_good.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "geom" in describe, "geom column missing from places"
        assert describe["geom"] == "GEOMETRY", (
            f"Expected geom to be GEOMETRY, got {describe['geom']}"
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
        _run_import(conn, fsq_parquet)
        self._run_importance(conn)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "importance" in cols, f"importance column missing; found: {cols}"

    def test_importance_column_is_integer(self, fsq_parquet, tmp_path):
        """The `importance` column must be INTEGER type."""
        db_path = tmp_path / "test_importance_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, parquet_glob, bbox=global_bbox)
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
        _run_import(conn, fsq_parquet)
        self._run_variants(conn)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "variants" in cols, f"variants column missing; found: {cols}"

    def test_variants_column_is_struct_array(self, fsq_parquet, tmp_path):
        """The `variants` column must be STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]."""
        db_path = tmp_path / "test_variants_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
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
        _run_import(conn, fsq_parquet)
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


# ---------------------------------------------------------------------------
# Fixtures: synthetic Overture parquet file
# ---------------------------------------------------------------------------

# Overture parquet columns used by the import pipeline:
#   id        VARCHAR          — primary key
#   bbox      STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)
#   geometry  VARCHAR          — WKT string (import SQL casts to GEOMETRY)
#   names     STRUCT(common MAP(VARCHAR, VARCHAR),
#                    rules  STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[])
#   categories STRUCT(primary VARCHAR)
#
# Row legend (id → outcome):
#   ov001  in-bbox, has names.common entry         → survives
#   ov002  in-bbox, has names.rules entry           → survives
#   ov003  in-bbox, no names (NULL)                 → survives (empty variants)
#   ov004  in-bbox, has categories.primary          → survives
#   ov005  in-bbox, different categories.primary    → survives
#   ov006  bbox.xmin (-123.001) < filter xmin (-122.55) → excluded by bbox filter
#   ov007  geometry IS NULL                         → excluded by null-geometry filter

_OV_BBOX = dict(xmin=-122.55, xmax=-122.30, ymin=37.60, ymax=37.85)


@pytest.fixture(scope="module")
def overture_parquet(tmp_path_factory):
    """Write a single Overture-schema parquet file and return a glob path for it."""
    base = tmp_path_factory.mktemp("overture_parquet")
    parquet_path = base / "overture_data.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")

    # Build the table row-by-row using VALUES.  The names column uses DuckDB
    # struct/map literals; geometry is VARCHAR WKT (import SQL casts it).
    conn.execute("""
        CREATE TABLE tmp_ov (
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

    # ov001 — in-bbox, names.common has one entry (language 'en')
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov001',
            {'xmin': -122.420, 'ymin': 37.774, 'xmax': -122.418, 'ymax': 37.776},
            'POINT(-122.419 37.775)',
            {'common': map(['en'], ['Blue Bottle Coffee']),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'coffee_shop'}
        )
    """)

    # ov002 — in-bbox, names.rules has one entry
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov002',
            {'xmin': -122.487, 'ymin': 37.768, 'xmax': -122.485, 'ymax': 37.770},
            'POINT(-122.486 37.769)',
            {'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  [{'language': 'en', 'value': 'GG Park', 'variant': 'short'}]},
            {'primary': 'park'}
        )
    """)

    # ov003 — in-bbox, names IS NULL → empty variants after variants SQL
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov003',
            {'xmin': -122.411, 'ymin': 37.769, 'xmax': -122.409, 'ymax': 37.771},
            'POINT(-122.410 37.770)',
            NULL,
            {'primary': 'coffee_shop'}
        )
    """)

    # ov004 — in-bbox, same category as ov001 (coffee_shop)
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov004',
            {'xmin': -122.431, 'ymin': 37.779, 'xmax': -122.429, 'ymax': 37.781},
            'POINT(-122.430 37.780)',
            {'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'coffee_shop'}
        )
    """)

    # ov005 — in-bbox, unique category (gets higher IDF than coffee_shop)
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov005',
            {'xmin': -122.401, 'ymin': 37.779, 'xmax': -122.399, 'ymax': 37.781},
            'POINT(-122.400 37.780)',
            {'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'unique_venue'}
        )
    """)

    # ov006 — out of bbox (longitude < xmin = -122.55)
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov006',
            {'xmin': -123.001, 'ymin': 37.749, 'xmax': -122.999, 'ymax': 37.751},
            'POINT(-123.000 37.750)',
            {'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'coffee_shop'}
        )
    """)

    # ov007 — geometry IS NULL (in-bbox coordinates, but geometry absent)
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov007',
            {'xmin': -122.411, 'ymin': 37.769, 'xmax': -122.409, 'ymax': 37.771},
            NULL,
            {'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'coffee_shop'}
        )
    """)

    conn.execute(f"COPY tmp_ov TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()

    return str(base / "*.parquet")


# ---------------------------------------------------------------------------
# Helper: run overture_import.sql against the parquet fixture
# ---------------------------------------------------------------------------

def _run_overture_import(conn, parquet_glob, bbox=None):
    """Load, substitute, and execute overture_import.sql on `conn`.

    bbox defaults to the SF bbox defined in _OV_BBOX.
    Strips INSTALL/LOAD spatial and SET memory_limit lines for test isolation.
    """
    if bbox is None:
        bbox = _OV_BBOX

    substitutions = {
        "memory_limit": "4GB",
        "parquet_glob": parquet_glob,
        "xmin": bbox["xmin"],
        "xmax": bbox["xmax"],
        "ymin": bbox["ymin"],
        "ymax": bbox["ymax"],
    }
    raw_sql = _load_sql("overture_import.sql", substitutions)
    sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
    conn.execute(sql)


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
        _run_overture_import(conn, overture_parquet)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        assert "places" in tables

    def test_places_has_id_column(self, overture_parquet, tmp_path):
        """After import, `places` must have an `id` column."""
        db_path = tmp_path / "test_ov_id_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "id" in cols, f"id column missing; found columns: {cols}"

    def test_places_has_qk17_column(self, overture_parquet, tmp_path):
        """After import, `places` must have a `qk17` column."""
        db_path = tmp_path / "test_ov_qk17_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "qk17" in cols, f"qk17 column missing; found columns: {cols}"

    def test_places_expected_columns(self, overture_parquet, tmp_path):
        """After import, `places` must include the columns expected downstream."""
        required = {"id", "bbox", "qk17"}
        db_path = tmp_path / "test_ov_cols.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        missing = required - cols
        assert not missing, f"Missing columns: {missing}"

    def test_bbox_filter_excludes_out_of_range(self, overture_parquet, tmp_path):
        """Rows outside the bbox must be excluded."""
        db_path = tmp_path / "test_ov_bbox.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_overture_import(conn, overture_parquet)
        ids = {row[0] for row in conn.execute("SELECT id FROM places").fetchall()}
        conn.close()
        assert "ov006" not in ids, "Out-of-bbox row (ov006) must be excluded"

    def test_null_geometry_excluded(self, overture_parquet, tmp_path):
        """Rows with geometry IS NULL must be excluded."""
        db_path = tmp_path / "test_ov_null_geom.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_overture_import(conn, overture_parquet)
        ids = {row[0] for row in conn.execute("SELECT id FROM places").fetchall()}
        conn.close()
        assert "ov007" not in ids, "Null-geometry row (ov007) must be excluded"

    def test_good_rows_included(self, overture_parquet, tmp_path):
        """All in-bbox, non-null-geometry rows must survive the import filters."""
        expected = {"ov001", "ov002", "ov003", "ov004", "ov005"}
        db_path = tmp_path / "test_ov_good.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_overture_import(conn, overture_parquet)
        ids = {row[0] for row in conn.execute("SELECT id FROM places").fetchall()}
        conn.close()
        missing = expected - ids
        assert not missing, f"Expected rows missing after import: {missing}"

    def test_qk17_is_17_chars(self, overture_parquet, tmp_path):
        """qk17 values must be 17-character strings for all surviving rows."""
        db_path = tmp_path / "test_ov_qk17_len.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_overture_import(conn, overture_parquet)
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
        _run_overture_import(conn, overture_parquet)
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
        _run_overture_import(conn, overture_parquet)
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
        _run_overture_import(conn, overture_parquet)
        self._run_importance(conn)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "importance" in cols, f"importance column missing; found: {cols}"

    def test_importance_column_is_integer(self, overture_parquet, tmp_path):
        """The `importance` column must be INTEGER type."""
        db_path = tmp_path / "test_ov_imp_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_overture_import(conn, overture_parquet)
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
        _run_overture_import(conn, overture_parquet)
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
        _run_overture_import(conn, overture_parquet)
        self._run_importance(conn)
        nulls = conn.execute("""
            SELECT id FROM places WHERE importance IS NULL
        """).fetchall()
        conn.close()
        assert not nulls, f"Rows with NULL importance: {nulls}"

    def test_unique_category_scores_higher(self, overture_parquet, tmp_path):
        """A place with a unique category should score higher than one with a common category (IDF).

        ov001 has 'coffee_shop' which appears in 3 of 5 surviving rows
        (ov001, ov003, ov004) → IDF = ln(5/3) ≈ 0.511.
        ov005 has 'unique_venue' which appears in only 1 of 5 surviving rows
        → IDF = ln(5/1) ≈ 1.609.
        ov005 should score strictly higher than ov001.
        """
        db_path = tmp_path / "test_ov_idf.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_overture_import(conn, overture_parquet)
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
        _run_overture_import(conn, parquet_glob, bbox=global_bbox)
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
        _run_overture_import(conn, overture_parquet)
        self._run_variants(conn)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "variants" in cols, f"variants column missing; found: {cols}"

    def test_variants_column_is_struct_array(self, overture_parquet, tmp_path):
        """The `variants` column must be an array of STRUCTs."""
        db_path = tmp_path / "test_ov_var_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_overture_import(conn, overture_parquet)
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
        _run_overture_import(conn, overture_parquet)
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
        _run_overture_import(conn, overture_parquet)
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
        _run_overture_import(conn, overture_parquet)
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
        _run_overture_import(conn, overture_parquet)
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


# ---------------------------------------------------------------------------
# Fixtures: synthetic OSM parquet files
# ---------------------------------------------------------------------------
#
# Node rows (all inside SF bbox -122.55 to -122.30, 37.60 to 37.85 unless noted):
#
#   n1001  name='Tartine Manufactory', amenity='cafe'   lat=37.7612, lon=-122.4195  → IN
#   n1002  name='Dolores Park',        leisure='park'   lat=37.7596, lon=-122.4269  → IN
#   n1003  name=None,                  amenity='cafe'   lat=37.7700, lon=-122.4100  → excluded (no name)
#   n1004  name='Faraway Place',       shop='bakery'    lat=37.9000, lon=-123.5000  → excluded (out of bbox)
#   n1005  name='Alt Name Cafe',       amenity='cafe'   lat=37.7750, lon=-122.4200  → IN (has alt_name)
#           tags: name, alt_name='The Old Spot', name:fr='Café Alt'
#   n9001  id=9001, lat=37.8199, lon=-122.4786  (centroid node, no name)
#   n9002  id=9002, lat=37.8197, lon=-122.4788  (centroid node, no name)
#
# Way rows:
#   w2001  name='Golden Gate Bridge', bridge='yes'  nds=[{ref:9001},{ref:9002}]  → IN (if centroids found)
#
# NOTE: Way import requires centroid nodes in the node parquet. In tests, we pass
# the same node parquet for both node_parquet and way_parquet substitutions.
# The way parquet schema differs from node parquet, so passing node parquet for
# way_parquet will produce 0 way rows — this is acceptable.  The core behaviors
# (correct schema, qk17, tag filtering, importance, variants) are all testable
# with nodes alone.

_OSM_SF_BBOX = dict(xmin=-122.55, xmax=-122.30, ymin=37.60, ymax=37.85)


@pytest.fixture(scope="module")
def osm_parquet(tmp_path_factory):
    """Write OSM-schema node and way parquet files; return dict with 'node' and 'way' globs."""
    base = tmp_path_factory.mktemp("osm_parquet")
    node_path = base / "node_data.parquet"
    way_path = base / "way_data.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")

    # --- Node parquet ---
    conn.execute("""
        CREATE TABLE tmp_nodes (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            lat     DOUBLE,
            lon     DOUBLE
        )
    """)

    # n1001: Tartine Manufactory — amenity=cafe, IN bbox
    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1001,
            map(['name','amenity'], ['Tartine Manufactory','cafe']),
            37.7612, -122.4195
        )
    """)

    # n1002: Dolores Park — leisure=park, IN bbox
    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1002,
            map(['name','leisure'], ['Dolores Park','park']),
            37.7596, -122.4269
        )
    """)

    # n1003: no name tag — should be excluded
    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1003,
            map(['amenity'], ['cafe']),
            37.7700, -122.4100
        )
    """)

    # n1004: out of bbox (lon=-123.5) — should be excluded
    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1004,
            map(['name','shop'], ['Faraway Place','bakery']),
            37.9000, -123.5000
        )
    """)

    # n1005: Alt Name Cafe — has alt_name and name:fr, IN bbox
    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1005,
            map(['name','amenity','alt_name','name:fr'],
                ['Alt Name Cafe','cafe','The Old Spot','Café Alt']),
            37.7750, -122.4200
        )
    """)

    # n9001: centroid node for way (no name tag)
    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            9001,
            map([]::VARCHAR[], []::VARCHAR[]),
            37.8199, -122.4786
        )
    """)

    # n9002: centroid node for way (no name tag)
    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            9002,
            map([]::VARCHAR[], []::VARCHAR[]),
            37.8197, -122.4788
        )
    """)

    # n1006: has name but no quality tag (highway=crossing only) → excluded by quality filter
    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1006,
            map(['name','highway'], ['No Category Node','crossing']),
            37.7760, -122.4150
        )
    """)

    # n1007: has all variant tag types + amenity=cafe so it passes quality filter
    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1007,
            map(['name','old_name','official_name','short_name','loc_name','int_name','amenity'],
                ['Multi Variant Place','Former Name','Official Title','MVP','Local Spot','International Name','cafe']),
            37.7770, -122.4160
        )
    """)

    conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")

    # --- Way parquet ---
    conn.execute("""
        CREATE TABLE tmp_ways (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            nds     STRUCT(ref BIGINT)[]
        )
    """)

    # w2001: Golden Gate Bridge
    conn.execute("""
        INSERT INTO tmp_ways VALUES (
            2001,
            map(['name','bridge','tourism'], ['Golden Gate Bridge','yes','attraction']),
            [{'ref': 9001}, {'ref': 9002}]::STRUCT(ref BIGINT)[]
        )
    """)

    conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
    conn.close()

    return {
        "node": str(base / "node_data.parquet"),
        "way": str(base / "way_data.parquet"),
    }


# ---------------------------------------------------------------------------
# Helper: run osm_import.sql against the parquet fixtures
# ---------------------------------------------------------------------------

def _run_osm_import(conn, node_glob, way_glob=None, bbox=None):
    """Load, substitute, and execute osm_import.sql on `conn`.

    If way_glob is None, the node_glob is used for both substitutions.
    bbox defaults to the SF bbox defined in _OSM_SF_BBOX.
    Strips INSTALL/LOAD spatial and SET memory_limit lines for test isolation.
    """
    if bbox is None:
        bbox = _OSM_SF_BBOX
    if way_glob is None:
        way_glob = node_glob

    substitutions = {
        "memory_limit": "4GB",
        "node_parquet": node_glob,
        "way_parquet": way_glob,
        "xmin": bbox["xmin"],
        "xmax": bbox["xmax"],
        "ymin": bbox["ymin"],
        "ymax": bbox["ymax"],
    }
    raw_sql = _load_sql("osm_import.sql", substitutions)
    sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
    conn.execute(sql)


# ---------------------------------------------------------------------------
# Tests: osm_import.sql
# ---------------------------------------------------------------------------

class TestOsmImport:
    """Tests for garganorn/sql/osm_import.sql."""

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "osm_import.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def test_places_table_created(self, osm_parquet, tmp_path):
        """After import, the `places` table must exist."""
        db_path = tmp_path / "test_osm_import.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        assert "places" in tables

    def test_places_has_qk17_column(self, osm_parquet, tmp_path):
        """After import, `places` must have a qk17 column."""
        db_path = tmp_path / "test_osm_qk17_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "qk17" in cols, f"qk17 column missing; found columns: {cols}"

    def test_places_has_rkey_column(self, osm_parquet, tmp_path):
        """After import, `places` must have an rkey column (not id)."""
        db_path = tmp_path / "test_osm_rkey_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "rkey" in cols, f"rkey column missing; found columns: {cols}"

    def test_places_expected_columns(self, osm_parquet, tmp_path):
        """After import, `places` must include all expected columns."""
        required = {
            "osm_type", "osm_id", "rkey", "name", "latitude", "longitude",
            "geom", "primary_category", "tags", "bbox", "qk17", "importance",
        }
        db_path = tmp_path / "test_osm_cols.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        missing = required - cols
        assert not missing, f"Missing columns: {missing}"

    def test_bbox_filter_excludes_out_of_range(self, osm_parquet, tmp_path):
        """Node n1004 (lon=-123.5) must be excluded by bbox filter."""
        db_path = tmp_path / "test_osm_bbox.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1004" not in rkeys, "Out-of-bbox node n1004 must be excluded"

    def test_no_name_excluded(self, osm_parquet, tmp_path):
        """Node n1003 (no name tag) must not appear in places."""
        db_path = tmp_path / "test_osm_noname.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1003" not in rkeys, "No-name node n1003 must be excluded"

    def test_surviving_places(self, osm_parquet, tmp_path):
        """Nodes n1001 and n1002 must appear by rkey."""
        db_path = tmp_path / "test_osm_survive.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1001" in rkeys, f"n1001 (Tartine Manufactory) missing; got: {rkeys}"
        assert "n1002" in rkeys, f"n1002 (Dolores Park) missing; got: {rkeys}"

    def test_qk17_populated(self, osm_parquet, tmp_path):
        """All rows must have non-null qk17 values."""
        db_path = tmp_path / "test_osm_qk17_pop.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        nulls = conn.execute("SELECT rkey FROM places WHERE qk17 IS NULL").fetchall()
        conn.close()
        assert not nulls, f"Rows with NULL qk17: {nulls}"

    def test_rkey_format(self, osm_parquet, tmp_path):
        """rkey values for nodes must start with 'n' and match 'n' || osm_id exactly."""
        db_path = tmp_path / "test_osm_rkey_fmt.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        # All places imported from nodes in this fixture should have rkey like 'n<id>'
        node_rkeys = conn.execute(
            "SELECT rkey FROM places WHERE osm_type = 'n'"
        ).fetchall()
        for (rkey,) in node_rkeys:
            assert rkey.startswith("n"), f"Node rkey must start with 'n', got: {rkey!r}"
        # Assert exact rkey values for known surviving nodes
        surviving_ids = [1001, 1002, 1005]
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        for osm_id in surviving_ids:
            expected = f"n{osm_id}"
            assert expected in rkeys, f"Expected rkey '{expected}' not found in places"

    def test_geom_column_is_geometry_type(self, osm_parquet, tmp_path):
        """geom column must be GEOMETRY type after import."""
        db_path = tmp_path / "test_geom_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "geom" in describe, "geom column not found in places"
        assert describe["geom"] == "GEOMETRY", f"geom column type should be GEOMETRY, got {describe['geom']!r}"

    def test_tags_column_is_map_type(self, osm_parquet, tmp_path):
        """tags column must be MAP(VARCHAR, VARCHAR) type."""
        db_path = tmp_path / "test_tags_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "tags" in describe, "tags column not found in places"
        assert "MAP" in describe["tags"].upper(), (
            f"tags column should be MAP type, got {describe['tags']!r}"
        )

    def test_quality_filter_excludes_uncategorized(self, osm_parquet, tmp_path):
        """Nodes with name but no recognized quality tag must be excluded."""
        db_path = tmp_path / "test_quality_filter.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1006" not in rkeys, "Node with name but no quality tag must be excluded by quality filter"

    def test_way_import_survives(self, osm_parquet, tmp_path):
        """A way with a recognized quality tag survives import with rkey 'w' + osm_id."""
        db_path = tmp_path / "test_way_import.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "w2001" in rkeys, (
            "Way w2001 (tourism=attraction) should survive import via centroid computation"
        )


# ---------------------------------------------------------------------------
# Tests: osm_importance.sql
# ---------------------------------------------------------------------------

class TestOsmImportance:
    """Tests for garganorn/sql/osm_importance.sql.

    Each test creates a fresh DuckDB connection, runs osm_import.sql first,
    then runs osm_importance.sql.
    """

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "osm_importance.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def _run_importance(self, conn):
        """Load and execute osm_importance.sql on `conn`."""
        raw_sql = _load_sql("osm_importance.sql", {"density_norm": "10.0", "idf_norm": "18.0"})
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

    def test_importance_column_exists(self, osm_parquet, tmp_path):
        """After osm_importance.sql, `places` must have an `importance` column."""
        db_path = tmp_path / "test_osm_imp_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_importance(conn)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "importance" in cols, f"importance column missing; found: {cols}"

    def test_importance_is_integer(self, osm_parquet, tmp_path):
        """The `importance` column must be INTEGER type."""
        db_path = tmp_path / "test_osm_imp_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_importance(conn)
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert describe.get("importance") in ("INTEGER", "INT", "INT4", "SIGNED"), (
            f"importance column type unexpected: {describe.get('importance')}"
        )

    def test_importance_range(self, osm_parquet, tmp_path):
        """All importance values must be in [0, 100]."""
        db_path = tmp_path / "test_osm_imp_range.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_importance(conn)
        bad = conn.execute("""
            SELECT rkey, importance
            FROM places
            WHERE importance < 0 OR importance > 100
        """).fetchall()
        conn.close()
        assert not bad, f"Rows with out-of-range importance: {bad}"

    def test_importance_positive_for_clustered(self, tmp_path):
        """Multiple places in the same qk15 cell get importance > 0 from density.

        Creates 5 places clustered in SF using a global-bbox import so none are
        filtered out.  After importance scoring, each place's density_score =
        ln(1 + 5) ≈ 1.79 which maps to a positive importance value.
        """
        import duckdb as _duckdb

        node_path = tmp_path / "cluster_nodes.parquet"

        conn = _duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE tmp_cluster (
                id      BIGINT,
                tags    MAP(VARCHAR, VARCHAR),
                lat     DOUBLE,
                lon     DOUBLE
            )
        """)

        cluster_places = [
            (3001, "Cafe Alpha",   37.7700, -122.4100, "amenity", "cafe"),
            (3002, "Cafe Beta",    37.7701, -122.4101, "amenity", "cafe"),
            (3003, "Cafe Gamma",   37.7702, -122.4099, "amenity", "cafe"),
            (3004, "Cafe Delta",   37.7699, -122.4102, "amenity", "cafe"),
            (3005, "Cafe Epsilon", 37.7700, -122.4098, "amenity", "cafe"),
        ]

        for nid, name, lat, lon, tag_k, tag_v in cluster_places:
            conn.execute(f"""
                INSERT INTO tmp_cluster VALUES (
                    {nid},
                    map(['name','{tag_k}'], ['{name}','{tag_v}']),
                    {lat}, {lon}
                )
            """)

        conn.execute(f"COPY tmp_cluster TO '{node_path}' (FORMAT PARQUET)")

        # Write an empty way parquet with the correct schema so the way INSERT
        # runs without error but produces 0 rows.
        way_path = tmp_path / "cluster_ways.parquet"
        conn.execute("""
            CREATE TABLE tmp_cluster_ways (
                id   BIGINT,
                tags MAP(VARCHAR, VARCHAR),
                nds  STRUCT(ref BIGINT)[]
            )
        """)
        conn.execute(f"COPY tmp_cluster_ways TO '{way_path}' (FORMAT PARQUET)")
        conn.close()

        global_bbox = dict(xmin=-180, xmax=180, ymin=-90, ymax=90)
        db_path = tmp_path / "test_osm_cluster.duckdb"
        conn = _duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, str(node_path), str(way_path), bbox=global_bbox)
        raw_sql = _load_sql("osm_importance.sql", {"density_norm": "10.0", "idf_norm": "18.0"})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw_sql)))
        rows = conn.execute("SELECT rkey, importance FROM places ORDER BY rkey").fetchall()
        conn.close()

        assert len(rows) == 5, f"Expected 5 clustered rows, got: {rows}"
        for rkey, imp in rows:
            assert imp > 0, f"Clustered place {rkey} has importance=0, expected > 0"


# ---------------------------------------------------------------------------
# Tests: osm_variants.sql
# ---------------------------------------------------------------------------

class TestOsmVariants:
    """Tests for garganorn/sql/osm_variants.sql.

    Each test runs osm_import.sql first, then osm_variants.sql.
    """

    def test_sql_file_exists(self):
        """The SQL file must exist on disk (will fail until Green phase)."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "osm_variants.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def _run_variants(self, conn, node_glob, way_glob=None):
        """Load and execute osm_variants.sql on `conn`."""
        if way_glob is None:
            way_glob = node_glob
        substitutions = {
            "node_parquet": node_glob,
            "way_parquet": way_glob,
        }
        raw_sql = _load_sql("osm_variants.sql", substitutions)
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

    def test_variants_column_exists(self, osm_parquet, tmp_path):
        """After osm_variants.sql, `places` must have a `variants` column."""
        db_path = tmp_path / "test_osm_var_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn, osm_parquet["node"], osm_parquet["way"])
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "variants" in cols, f"variants column missing; found: {cols}"

    def test_variants_is_list(self, osm_parquet, tmp_path):
        """All rows must have a variants column that is a list (not NULL)."""
        db_path = tmp_path / "test_osm_var_list.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn, osm_parquet["node"], osm_parquet["way"])
        nulls = conn.execute(
            "SELECT rkey FROM places WHERE variants IS NULL"
        ).fetchall()
        conn.close()
        assert not nulls, f"Rows with NULL variants: {nulls}"

    def test_alt_name_produces_variant(self, osm_parquet, tmp_path):
        """n1005 has alt_name='The Old Spot'; must produce a variant with type='alternate'."""
        db_path = tmp_path / "test_osm_var_alt.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn, osm_parquet["node"], osm_parquet["way"])
        # Find the variant with name='The Old Spot' for n1005
        row = conn.execute("""
            SELECT v.name, v.type, v.language
            FROM (
                SELECT rkey, UNNEST(variants) AS v
                FROM places
                WHERE rkey = 'n1005'
            ) sub
            WHERE v.name = 'The Old Spot'
        """).fetchone()
        conn.close()
        assert row is not None, "No variant with name='The Old Spot' found for n1005"
        assert row[1] == "alternate", f"Expected type='alternate', got {row[1]!r}"

    def test_name_lang_produces_variant(self, osm_parquet, tmp_path):
        """n1005 has name:fr='Café Alt'; must produce a variant with type='alternate', language='fr'."""
        db_path = tmp_path / "test_osm_var_lang.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn, osm_parquet["node"], osm_parquet["way"])
        row = conn.execute("""
            SELECT v.name, v.type, v.language
            FROM (
                SELECT rkey, UNNEST(variants) AS v
                FROM places
                WHERE rkey = 'n1005'
            ) sub
            WHERE v.name = 'Café Alt'
        """).fetchone()
        conn.close()
        assert row is not None, "No variant with name='Café Alt' found for n1005"
        assert row[1] == "alternate", f"Expected type='alternate', got {row[1]!r}"
        assert row[2] == "fr", f"Expected language='fr', got {row[2]!r}"

    def test_no_variants_is_empty_list(self, osm_parquet, tmp_path):
        """n1001 (Tartine Manufactory) has no alt names; variants must be []."""
        db_path = tmp_path / "test_osm_var_empty.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn, osm_parquet["node"], osm_parquet["way"])
        row = conn.execute(
            "SELECT len(variants) FROM places WHERE rkey = 'n1001'"
        ).fetchone()
        conn.close()
        assert row is not None, "n1001 not found in places after variants SQL"
        assert row[0] == 0, f"Expected empty variants for n1001, got len={row[0]}"

    def _get_n1007_variant(self, osm_parquet, tmp_path, db_name, variant_name):
        """Helper: run import + variants for n1007 and return variant row matching variant_name."""
        db_path = tmp_path / db_name
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn, osm_parquet["node"], osm_parquet["way"])
        row = conn.execute(f"""
            SELECT v.name, v.type, v.language
            FROM (
                SELECT rkey, UNNEST(variants) AS v
                FROM places
                WHERE rkey = 'n1007'
            ) sub
            WHERE v.name = '{variant_name}'
        """).fetchone()
        conn.close()
        return row

    def test_old_name_produces_variant(self, osm_parquet, tmp_path):
        """old_name tag produces a variant with type='historical'."""
        row = self._get_n1007_variant(osm_parquet, tmp_path, "test_osm_var_old.duckdb", "Former Name")
        assert row is not None, "No variant with name='Former Name' found for n1007"
        assert row[1] == "historical", f"Expected type='historical' for old_name, got {row[1]!r}"

    def test_official_name_produces_variant(self, osm_parquet, tmp_path):
        """official_name tag produces a variant with type='official'."""
        row = self._get_n1007_variant(osm_parquet, tmp_path, "test_osm_var_official.duckdb", "Official Title")
        assert row is not None, "No variant with name='Official Title' found for n1007"
        assert row[1] == "official", f"Expected type='official' for official_name, got {row[1]!r}"

    def test_short_name_produces_variant(self, osm_parquet, tmp_path):
        """short_name tag produces a variant with type='short'."""
        row = self._get_n1007_variant(osm_parquet, tmp_path, "test_osm_var_short.duckdb", "MVP")
        assert row is not None, "No variant with name='MVP' found for n1007"
        assert row[1] == "short", f"Expected type='short' for short_name, got {row[1]!r}"

    def test_loc_name_produces_variant(self, osm_parquet, tmp_path):
        """loc_name tag produces a variant with type='colloquial'."""
        row = self._get_n1007_variant(osm_parquet, tmp_path, "test_osm_var_loc.duckdb", "Local Spot")
        assert row is not None, "No variant with name='Local Spot' found for n1007"
        assert row[1] == "colloquial", f"Expected type='colloquial' for loc_name, got {row[1]!r}"

    def test_int_name_produces_variant(self, osm_parquet, tmp_path):
        """int_name tag produces a variant with type='alternate'."""
        row = self._get_n1007_variant(osm_parquet, tmp_path, "test_osm_var_int.duckdb", "International Name")
        assert row is not None, "No variant with name='International Name' found for n1007"
        assert row[1] == "alternate", f"Expected type='alternate' for int_name, got {row[1]!r}"


# ---------------------------------------------------------------------------
# Tests: compute_tile_assignments.sql
# ---------------------------------------------------------------------------

def _run_tile_assignments(conn, pk_expr="fsq_place_id", min_zoom=6, max_zoom=17, max_per_tile=1000):
    """Load, substitute, and execute compute_tile_assignments.sql on `conn`.

    Raises FileNotFoundError when the SQL file does not yet exist (Red phase).
    """
    sql = _load_sql(
        "compute_tile_assignments.sql",
        {
            "pk_expr": pk_expr,
            "min_zoom": min_zoom,
            "max_zoom": max_zoom,
            "max_per_tile": max_per_tile,
        },
    )
    conn.execute(sql)


def _make_tile_assignment_db(conn, places):
    """Populate an in-memory DuckDB connection with a minimal places table.

    `places` is a list of (fsq_place_id, latitude, longitude) tuples.
    The qk17 column is computed directly via ST_QuadKey so the fixture
    matches what the real import pipeline would produce.
    """
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE places (
            fsq_place_id VARCHAR,
            name         VARCHAR,
            latitude     DOUBLE,
            longitude    DOUBLE,
            qk17         VARCHAR
        )
    """)
    for fsq_id, lat, lon in places:
        conn.execute(
            "INSERT INTO places VALUES (?, ?, ?, ?, ST_QuadKey(?, ?, 17))",
            [fsq_id, f"Place {fsq_id}", lat, lon, lon, lat],
        )


class TestComputeTileAssignments:
    """Tests for garganorn/sql/compute_tile_assignments.sql.

    All tests fail at Red phase with FileNotFoundError because the SQL file
    does not exist yet.
    """

    def test_sql_file_exists(self):
        sql_path = REPO_ROOT / "garganorn" / "sql" / "compute_tile_assignments.sql"
        assert sql_path.exists(), f"SQL file not found: {sql_path}"

    def test_main_case_table_exists_with_expected_columns(self, tmp_path):
        """After running the SQL, tile_assignments must exist with place_id and tile_qk.

        Fixture: 5 SF places + 1 NYC place, max_per_tile=2.  The 5 SF places
        share the same zoom-6 quadkey (count=5 > 2), so the SQL must subdivide
        them to a finer zoom level where tiles have ≤ 2 places.  The NYC place
        is isolated (count=1 ≤ 2) so it gets a coarse tile.
        """
        # SF cluster — 5 places very close together (same zoom-6 quadkey)
        # NYC place — isolated in a different zoom-6 quadkey
        places = [
            ("sf001", 37.7749, -122.4194),
            ("sf002", 37.7750, -122.4195),
            ("sf003", 37.7748, -122.4193),
            ("sf004", 37.7751, -122.4196),
            ("sf005", 37.7747, -122.4192),
            ("nyc001", 40.7128, -74.0060),
        ]

        max_per_tile = 2

        db_path = tmp_path / "test_tile_main.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_tile_assignment_db(conn, places)
        _run_tile_assignments(conn, pk_expr="fsq_place_id", min_zoom=6, max_zoom=17, max_per_tile=max_per_tile)

        # tile_assignments table must exist
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        assert "tile_assignments" in tables, (
            f"tile_assignments table not found; tables: {tables}"
        )

        # Must have place_id and tile_qk columns
        cols = {row[0] for row in conn.execute("DESCRIBE tile_assignments").fetchall()}
        assert "place_id" in cols, f"place_id column missing; found: {cols}"
        assert "tile_qk" in cols, f"tile_qk column missing; found: {cols}"

        # Every place with non-null qk17 must appear exactly once
        assigned_ids = {row[0] for row in conn.execute(
            "SELECT place_id FROM tile_assignments"
        ).fetchall()}
        expected_ids = {p[0] for p in places}
        assert assigned_ids == expected_ids, (
            f"Assigned IDs differ from expected.\n"
            f"  Missing: {expected_ids - assigned_ids}\n"
            f"  Extra:   {assigned_ids - expected_ids}"
        )
        assert conn.execute("SELECT count(*) FROM tile_assignments").fetchone()[0] == len(places)

        # No tile_qk (at zooms < 17) may have more records than max_per_tile.
        # Zoom-17 tiles are accepted as-is per the spec.
        overflow = conn.execute(f"""
            SELECT tile_qk, count(*) AS cnt
            FROM tile_assignments
            WHERE length(tile_qk) < 17
            GROUP BY tile_qk
            HAVING count(*) > {max_per_tile}
        """).fetchall()
        assert not overflow, (
            f"Tiles at zoom < 17 exceed max_per_tile={max_per_tile}: {overflow}"
        )

        # At least one place must have been assigned a coarse tile (zoom < 17).
        coarse_count = conn.execute(
            "SELECT count(*) FROM tile_assignments WHERE length(tile_qk) < 17"
        ).fetchone()[0]
        assert coarse_count >= 1, (
            "Expected at least one place assigned a tile at zoom < 17, "
            f"but coarse_count={coarse_count}"
        )

        # The NYC place (isolated at zoom 6) must receive a zoom-6 tile.
        nyc_qk = conn.execute(
            "SELECT tile_qk FROM tile_assignments WHERE place_id = 'nyc001'"
        ).fetchone()
        assert nyc_qk is not None, "nyc001 not found in tile_assignments"
        assert len(nyc_qk[0]) == 6, (
            f"nyc001 expected a zoom-6 tile (length 6), got tile_qk={nyc_qk[0]!r} "
            f"(length {len(nyc_qk[0])})"
        )

        conn.close()

    def test_zoom17_fallback_when_all_in_one_cell(self, tmp_path):
        """When max_per_tile=1, all places must fall back to zoom-17 tiles.

        Fixture: 4 places all within the same zoom-17 tile (~1m apart, 0.00003°
        spread, well within the ~0.00274° zoom-17 tile width).  With
        max_per_tile=1 the shared zoom-17 tile has count=4 > 1, so no zoom
        level — including 17 — can satisfy the density constraint.  Because
        zoom 17 is the unconditional fallback, every place must still receive a
        tile_qk of length 17.
        """
        # All in the same zoom-17 tile (~1m apart) — count=4 > max_per_tile=1
        # at every zoom level including 17, triggering the fallback.
        places = [
            ("p001", 37.77000, -122.42000),
            ("p002", 37.77001, -122.42001),
            ("p003", 37.77002, -122.42002),
            ("p004", 37.77003, -122.42003),
        ]

        db_path = tmp_path / "test_tile_z17.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_tile_assignment_db(conn, places)
        _run_tile_assignments(conn, pk_expr="fsq_place_id", min_zoom=6, max_zoom=17, max_per_tile=1)

        rows = conn.execute(
            "SELECT place_id, tile_qk FROM tile_assignments ORDER BY place_id"
        ).fetchall()
        conn.close()

        assert len(rows) == len(places), (
            f"Expected {len(places)} rows in tile_assignments, got {len(rows)}"
        )

        assigned_ids = {row[0] for row in rows}
        expected_ids = {p[0] for p in places}
        assert assigned_ids == expected_ids, f"place_id mismatch: assigned={assigned_ids}, expected={expected_ids}"

        non_z17 = [(pid, qk) for pid, qk in rows if len(qk) != 17]
        assert not non_z17, (
            f"With max_per_tile=1 all places should fall back to zoom-17 tiles, "
            f"but these did not: {non_z17}"
        )

        # All 4 places must be in the SAME zoom-17 tile (len==1 unique qk), confirming
        # the fixture geometry is correct and the fallback is actually exercised.
        qk_values = [row[1] for row in rows]  # row is (place_id, tile_qk)
        assert len(set(qk_values)) == 1, (
            f"All 4 places should share the same zoom-17 tile; got {len(set(qk_values))} distinct tiles: {set(qk_values)}"
        )

    def test_null_qk17_excluded(self):
        """Places with qk17=NULL must be excluded from tile_assignments."""
        places = [
            ("a001", 37.7749, -122.4194),
            ("a002", 37.7750, -122.4195),
        ]

        conn = duckdb.connect()
        _make_tile_assignment_db(conn, places)
        conn.execute(
            "INSERT INTO places (fsq_place_id, name, latitude, longitude, qk17) "
            "VALUES ('null001', 'Null Place', 37.77, -122.42, NULL)"
        )
        _run_tile_assignments(conn, pk_expr='fsq_place_id', max_per_tile=10)

        null_rows = conn.execute(
            "SELECT place_id FROM tile_assignments WHERE place_id = 'null001'"
        ).fetchall()
        assert null_rows == [], "Place with null qk17 should be excluded from tile_assignments"

        present_ids = {row[0] for row in conn.execute("SELECT place_id FROM tile_assignments").fetchall()}
        assert len(present_ids) == len(places), (
            f"Expected {len(places)} non-null places in tile_assignments, got {len(present_ids)}: {present_ids}"
        )
        assert "a001" in present_ids, "a001 should be present in tile_assignments"
        assert "a002" in present_ids, "a002 should be present in tile_assignments"

        conn.close()
