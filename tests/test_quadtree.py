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
        substitutions: dict = {}
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
#   ov006  longitude outside xmax (out of bbox)     → excluded by bbox filter
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
        raw_sql = _load_sql("overture_importance.sql", {})
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
        """A row with names.rules entries must get non-empty variants (ov002)."""
        db_path = tmp_path / "test_ov_var_rules.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        _run_overture_import(conn, overture_parquet)
        self._run_variants(conn)
        row = conn.execute("""
            SELECT len(variants) FROM places WHERE id = 'ov002'
        """).fetchone()
        conn.close()
        assert row is not None, "ov002 not found in places after variants SQL"
        assert row[0] > 0, (
            f"ov002 has names.rules but got empty variants (len={row[0]})"
        )

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
