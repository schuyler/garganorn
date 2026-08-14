"""Red tests: garganorn/covering.py.

Until garganorn/covering.py exists and exports the required symbols, tests surface
as either FAILED (tests that call _check_covering() directly in the test body) or
ERROR at setup for 14 tests whose class-scoped covering_dir fixture calls
_check_covering() — pytest reports fixture-setup failures as ERROR, not FAILED.
Collection succeeds so the full existing test suite can still run.
"""
import json
import math
import os
import random
import time

import duckdb
import pytest

# Attempt module-level import; if garganorn.covering does not exist, record the
# error and define None stubs. Tests call _check_covering() to surface the error.
try:
    from garganorn.covering import (
        COVER_MIN_ZOOM,
        COVER_MAX_ZOOM,
        lonlat_to_tile,
        bbox_to_quadkeys,
        stage_covering,
    )
    _COVERING_ERROR = None
except ImportError as _exc:
    COVER_MIN_ZOOM = COVER_MAX_ZOOM = None
    lonlat_to_tile = bbox_to_quadkeys = stage_covering = None
    _COVERING_ERROR = _exc


def _check_covering():
    """Call at the start of every test; fails with the ImportError if module is absent."""
    if _COVERING_ERROR is not None:
        pytest.fail(str(_COVERING_ERROR), pytrace=False)


from garganorn.stages import containment_arms_sql, quadkey_to_bbox
from tests.duckdb_spy import spy_on_duckdb_connect

# ---------------------------------------------------------------------------
# Minimal boundaries DB shared by covering tests
# ---------------------------------------------------------------------------

_COVERING_TEST_BOUNDARIES = [
    # (id, level, wkt, min_lat, min_lon, max_lat, max_lon)
    # level values are the atgeo containment vocabulary (garganorn.levels.LEVEL_VOCAB):
    # country=10, region=25, locality=50. cov_continent has no vocabulary entry
    # (continent has no producer entry in the containment level vocabulary);
    # 0 is a synthetic sentinel
    # kept only to preserve this pre-built boundaries.duckdb-shaped fixture's
    # ascending order.
    (
        "cov_continent",
        0,
        "POLYGON((-130 20, -130 55, -60 55, -60 20, -130 20))",
        20.0, -130.0, 55.0, -60.0,
    ),
    (
        "cov_country",
        10,
        "POLYGON((-125 24, -125 50, -66 50, -66 24, -125 24))",
        24.0, -125.0, 50.0, -66.0,
    ),
    (
        "cov_region",
        25,
        "POLYGON((-125 34, -125 42, -118 42, -118 34, -125 34))",
        34.0, -125.0, 42.0, -118.0,
    ),
    (
        "cov_locality",
        50,
        "POLYGON((-122.55 37.6, -122.55 37.85, -122.3 37.85, -122.3 37.6, -122.55 37.6))",
        37.6, -122.55, 37.85, -122.3,
    ),
]


def _create_boundaries_db(db_path, boundary_rows):
    """Create a boundaries.duckdb from an explicit list of boundary rows.

    Each row: (id, level, wkt, min_lat, min_lon, max_lat, max_lon).
    """
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE places (
            id VARCHAR,
            geometry GEOMETRY,
            level INTEGER,
            min_latitude DOUBLE,
            max_latitude DOUBLE,
            min_longitude DOUBLE,
            max_longitude DOUBLE
        )
    """)
    for bid, level, wkt, min_lat, min_lon, max_lat, max_lon in boundary_rows:
        conn.execute(
            "INSERT INTO places VALUES (?, ST_GeomFromText(?), ?, ?, ?, ?, ?)",
            [bid, wkt, level, min_lat, max_lat, min_lon, max_lon],
        )
    conn.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
    conn.close()


def _create_covering_test_db(db_path):
    """Create a minimal boundaries.duckdb for covering tests."""
    _create_boundaries_db(db_path, _COVERING_TEST_BOUNDARIES)


@pytest.fixture(scope="module")
def covering_test_db(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("cov_bnd") / "boundaries.duckdb"
    _create_covering_test_db(db_path)
    return db_path


# ---------------------------------------------------------------------------
# qk_env SQL macro agrees with quadkey_to_bbox Python (10k keys)
# ---------------------------------------------------------------------------

_QK_ENV_SQL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "garganorn", "sql", "qk_env_macro.sql"
)


def _load_qk_env_macro(con):
    """Load qk_env_macro.sql into a DuckDB connection."""
    with open(_QK_ENV_SQL_PATH) as f:
        sql = f.read()
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


class TestQkEnvMacro:
    """qk_env DuckDB macro agrees with quadkey_to_bbox (10,000 quadkeys, 1e-9)."""

    def test_qk_env_agrees_with_python_10k_quadkeys(self):
        """All four envelope coordinates agree to 1e-9 for 10,000 random quadkeys."""
        _check_covering()
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        _load_qk_env_macro(con)

        rng = random.Random(42)
        mismatches = []
        for _ in range(10_000):
            length = rng.randint(1, 17)
            qk = "".join(rng.choice("0123") for _ in range(length))
            py_xmin, py_ymin, py_xmax, py_ymax = quadkey_to_bbox(qk)

            row = con.execute(
                """SELECT ST_XMin(qk_env(?)), ST_YMin(qk_env(?)),
                          ST_XMax(qk_env(?)), ST_YMax(qk_env(?))""",
                [qk, qk, qk, qk],
            ).fetchone()
            sql_xmin, sql_ymin, sql_xmax, sql_ymax = row

            coords = [
                (py_xmin, sql_xmin, "xmin"),
                (py_ymin, sql_ymin, "ymin"),
                (py_xmax, sql_xmax, "xmax"),
                (py_ymax, sql_ymax, "ymax"),
            ]
            for py_val, sql_val, name in coords:
                if abs(py_val - sql_val) > 1e-9:
                    mismatches.append((qk, name, py_val, sql_val))
                    break

        assert len(mismatches) == 0, (
            f"qk_env disagrees with quadkey_to_bbox on {len(mismatches)} quadkeys; "
            f"first mismatch: {mismatches[0]}"
        )

    def test_qk_env_z1_sanity(self):
        """qk_env('0') covers the NW quadrant; spot-check x/y bounds."""
        _check_covering()
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        _load_qk_env_macro(con)

        xmin, ymin, xmax, ymax = con.execute(
            "SELECT ST_XMin(qk_env(?)), ST_YMin(qk_env(?)), ST_XMax(qk_env(?)), ST_YMax(qk_env(?))",
            ["0", "0", "0", "0"],
        ).fetchone()
        assert xmin == pytest.approx(-180.0, abs=0.01)
        assert xmax == pytest.approx(0.0, abs=0.01)
        assert ymin == pytest.approx(0.0, abs=0.01)
        assert ymax == pytest.approx(85.05112877980659, abs=0.01)


# ---------------------------------------------------------------------------
# bbox_to_quadkeys / lonlat_to_tile
# ---------------------------------------------------------------------------

class TestBboxToQuadkeys:
    """Known bboxes, world=256 tiles, lat clamping, antimeridian
    (`gotchas.md`, "Antimeridian bboxes are two lobes")."""

    def test_whole_world_z4_is_256_tiles(self):
        """bbox_to_quadkeys for the whole world at z4 returns exactly 256 tiles."""
        _check_covering()
        tiles = bbox_to_quadkeys(-180.0, -90.0, 180.0, 90.0, 4)
        assert len(tiles) == 256, f"Expected 256 z4 tiles, got {len(tiles)}"
        assert all(len(t) == 4 for t in tiles)
        assert all(c in "0123" for t in tiles for c in t)

    def test_whole_world_z4_all_unique(self):
        _check_covering()
        tiles = bbox_to_quadkeys(-180.0, -90.0, 180.0, 90.0, 4)
        assert len(set(tiles)) == 256

    def test_known_bbox_sf_z4_nonempty(self):
        """SF bbox at z4 returns at least one tile."""
        _check_covering()
        tiles = bbox_to_quadkeys(-122.55, 37.6, -122.3, 37.85, 4)
        assert len(tiles) >= 1
        assert all(len(t) == 4 for t in tiles)

    def test_zoom_controls_tile_length(self):
        _check_covering()
        tiles_z6 = bbox_to_quadkeys(-10.0, -10.0, 10.0, 10.0, 6)
        tiles_z9 = bbox_to_quadkeys(-10.0, -10.0, 10.0, 10.0, 9)
        assert all(len(t) == 6 for t in tiles_z6)
        assert all(len(t) == 9 for t in tiles_z9)

    def test_latitude_clamped_above_merc_max(self):
        """Latitudes beyond ±85.05… are clamped; result is non-empty and valid."""
        _check_covering()
        tiles = bbox_to_quadkeys(-10.0, 85.1, 10.0, 90.0, 4)
        assert len(tiles) > 0
        assert all(all(c in "0123" for c in t) for t in tiles)

    def test_antimeridian_bbox_both_lobes(self):
        """min_lon > max_lon (the two-lobe rule): returns tiles from BOTH lobes (near ±180°)."""
        _check_covering()
        tiles = bbox_to_quadkeys(170.0, -15.0, -170.0, 15.0, 4)
        assert len(tiles) > 0, "Antimeridian bbox should return tiles"
        bboxes = [quadkey_to_bbox(t) for t in tiles]
        has_east_lobe = any(xmin >= 160.0 or xmax > 160.0 for xmin, _, xmax, _ in bboxes)
        # Unpacking corrected during green (was `for _, _, xmax, _`): xmin was
        # unbound in this generator, raising NameError. Sanctioned test-defect fix.
        has_west_lobe = any(xmax <= -160.0 or xmin < -160.0 for xmin, _, xmax, _ in bboxes)
        assert has_east_lobe, "No tiles near +170° (east lobe missing)"
        assert has_west_lobe, "No tiles near -170° (west lobe missing)"

    def test_antimeridian_gap_tiles_absent(self):
        """No tile returned by the [170,-170] bbox is entirely within the gap."""
        _check_covering()
        tiles = bbox_to_quadkeys(170.0, -15.0, -170.0, 15.0, 4)
        for qk in tiles:
            xmin, ymin, xmax, ymax = quadkey_to_bbox(qk)
            in_gap = xmax < 170.0 and xmin > -170.0 and xmin < xmax
            assert not in_gap, (
                f"Tile {qk!r} (xmin={xmin}, xmax={xmax}) "
                "appears to be entirely in the antimeridian gap"
            )

    def test_lonlat_to_tile_sf(self):
        """lonlat_to_tile for SF at z10 returns a valid (x, y) pair."""
        _check_covering()
        x, y = lonlat_to_tile(-122.4194, 37.7749, 10)
        assert isinstance(x, int)
        assert isinstance(y, int)
        assert 0 <= x < 2 ** 10
        assert 0 <= y < 2 ** 10

    def test_lonlat_to_tile_lat_clamping(self):
        """lat > 85.05 is clamped; should not raise and should return valid tile."""
        _check_covering()
        x, y = lonlat_to_tile(0.0, 90.0, 4)
        assert 0 <= y < 2 ** 4


# ---------------------------------------------------------------------------
# stage_covering output schema / sort invariants
# ---------------------------------------------------------------------------

class TestStageCoveringSchema:
    """Parquet schema, sort, _meta.json."""

    @pytest.fixture(scope="class")
    def covering_dir(self, covering_test_db, tmp_path_factory):
        _check_covering()
        out_dir = str(tmp_path_factory.mktemp("schema_cov") / "covering")
        stage_covering(
            str(covering_test_db),
            out_dir,
            cover_min_zoom=4,
            cover_max_zoom=7,
        )
        return out_dir

    def _parquet_paths(self, covering_dir):
        return [
            os.path.join(covering_dir, f)
            for f in os.listdir(covering_dir)
            if f.endswith(".parquet")
        ]

    def test_files_named_qk4_parquet(self, covering_dir):
        _check_covering()
        parquets = [f for f in os.listdir(covering_dir) if f.endswith(".parquet")]
        assert len(parquets) > 0, "No parquet files written by stage_covering"
        for fname in parquets:
            stem = fname[:-8]
            assert len(stem) == 4, f"{fname!r} stem is not 4 chars (expected qk4)"
            assert all(c in "0123" for c in stem), f"{fname!r} contains non-quadkey chars"

    def test_parquet_has_required_columns(self, covering_dir):
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        cols = {
            row[0]
            for row in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [parquets[:1]]
            ).fetchall()
        }
        assert "tile_qk" in cols
        assert "boundary_id" in cols
        assert "level" in cols
        assert "geom" in cols

    def test_tile_qk_lengths_in_zoom_range(self, covering_dir):
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        min_len, max_len = con.execute(
            "SELECT MIN(length(tile_qk)), MAX(length(tile_qk)) FROM read_parquet(?)",
            [parquets],
        ).fetchone()
        assert min_len >= 4, (
            f"tile_qk shorter than cover_min_zoom=4 (fixture override): min_len={min_len}"
        )
        assert max_len <= 7, (
            f"tile_qk longer than cover_max_zoom=7: max_len={max_len}"
        )

    def test_rows_sorted_tile_qk_then_boundary_id(self, covering_dir):
        _check_covering()
        parquet_files = sorted(
            f for f in os.listdir(covering_dir) if f.endswith(".parquet")
        )
        con = duckdb.connect(":memory:")
        for fname in parquet_files:
            path = os.path.join(covering_dir, fname)
            rows = con.execute(
                "SELECT tile_qk, boundary_id FROM read_parquet(?)", [path]
            ).fetchall()
            assert rows == sorted(rows), (
                f"{fname}: rows not sorted by (tile_qk, boundary_id)"
            )

    def test_level_equals_boundary_vocab_level(self, covering_dir, covering_test_db):
        """covering.level must equal bnd.places.level (the atgeo vocabulary value).

        The invariant level == raw admin_level does not hold.
        covering_seed.sql:21 selects b.level directly (the vocabulary
        column already produced by the import CTAS from subtype), so this fixture's
        bnd.places.level values (garganorn.levels.LEVEL_VOCAB-derived, set in
        _COVERING_TEST_BOUNDARIES) are themselves the expected values -- the
        covering-seed copy must reproduce them verbatim, with no admin_level
        involved anywhere in the path.
        """
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        con.execute(f"ATTACH '{covering_test_db}' AS bnd (READ_ONLY)")
        mismatches = con.execute(
            """
            SELECT c.boundary_id, c.level, b.level
            FROM read_parquet(?) c
            JOIN bnd.places b ON b.id = c.boundary_id
            WHERE c.level IS DISTINCT FROM b.level
            """,
            [parquets],
        ).fetchall()
        assert len(mismatches) == 0, (
            f"level != bnd.places.level for {len(mismatches)} covering rows: {mismatches[:3]}"
        )

    def test_meta_json_written_last_with_params(self, covering_dir):
        _check_covering()
        meta_path = os.path.join(covering_dir, "_meta.json")
        assert os.path.exists(meta_path), "_meta.json not found in covering_dir"
        with open(meta_path) as f:
            meta = json.load(f)
        assert "cover_min_zoom" in meta, "_meta.json missing cover_min_zoom"
        assert "cover_max_zoom" in meta, "_meta.json missing cover_max_zoom"
        assert meta["cover_min_zoom"] == COVER_MIN_ZOOM
        assert meta["cover_max_zoom"] == 7


class TestStageCoveringProgressBar:
    """stage_covering's long-running iterative per-zoom loop must disable
    the DuckDB progress bar so it doesn't pollute build logs."""

    def test_disables_progress_bar(self, covering_test_db, tmp_path_factory, monkeypatch):
        _check_covering()
        import garganorn.covering as covering_mod

        statements = spy_on_duckdb_connect(monkeypatch, covering_mod)
        out_dir = str(tmp_path_factory.mktemp("progress_cov") / "covering")
        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=5)

        assert any("SET enable_progress_bar = false" in s for s in statements), (
            "stage_covering must disable the DuckDB progress bar"
        )


# ---------------------------------------------------------------------------
# Semantic interior/edge invariants
# ---------------------------------------------------------------------------

class TestCoveringSemanticInvariants:
    """Every row covered by its boundary and intersects it; no prefix chains."""

    @pytest.fixture(scope="class")
    def covering_dir(self, covering_test_db, tmp_path_factory):
        _check_covering()
        out_dir = str(tmp_path_factory.mktemp("sem_cov") / "covering")
        stage_covering(
            str(covering_test_db),
            out_dir,
            cover_min_zoom=4,
            cover_max_zoom=7,
        )
        return out_dir

    def _parquet_paths(self, covering_dir):
        return [
            os.path.join(covering_dir, f)
            for f in os.listdir(covering_dir)
            if f.endswith(".parquet")
        ]

    def _load_env_macro(self, con):
        with open(_QK_ENV_SQL_PATH) as f:
            for stmt in f.read().split(";"):
                s = stmt.strip()
                if s:
                    con.execute(s)

    def test_rows_covered_by_and_intersect_boundary(self, covering_dir, covering_test_db):
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"ATTACH '{covering_test_db}' AS bnd (READ_ONLY)")
        self._load_env_macro(con)
        violations = con.execute(
            """
            SELECT c.tile_qk, c.boundary_id
            FROM read_parquet(?) c
            JOIN bnd.places b ON b.id = c.boundary_id
            WHERE NOT ST_Covers(b.geometry, c.geom)
               OR NOT ST_Intersects(b.geometry, qk_env(c.tile_qk))
            """,
            [parquets],
        ).fetchall()
        assert len(violations) == 0, (
            f"Covering rows not covered by / intersecting their boundary: {violations[:5]}"
        )


# ---------------------------------------------------------------------------
# Point classification property
# ---------------------------------------------------------------------------

class TestPointClassification:
    """Covering arm ⟺ direct ST_Contains(boundary, point)."""

    @pytest.fixture(scope="class")
    def covering_dir(self, covering_test_db, tmp_path_factory):
        _check_covering()
        out_dir = str(tmp_path_factory.mktemp("ptcls_cov") / "covering")
        stage_covering(
            str(covering_test_db),
            out_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        return out_dir

    def _parquet_paths(self, covering_dir):
        return [
            os.path.join(covering_dir, f)
            for f in os.listdir(covering_dir)
            if f.endswith(".parquet")
        ]

    def test_sf_point_covering_arm_parity(self, covering_dir, covering_test_db):
        """Covering-arm matches for a deep-interior point equal direct ST_Contains."""
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"ATTACH '{covering_test_db}' AS bnd (READ_ONLY)")

        lat, lon = 37.78, -122.42
        qk17 = con.execute("SELECT ST_QuadKey(?, ?, 17)", [lon, lat]).fetchone()[0]

        covering_ids = {
            r[0]
            for r in con.execute(
                """
                SELECT DISTINCT boundary_id FROM read_parquet(?)
                WHERE left(?, length(tile_qk)) = tile_qk
                  AND ST_Covers(geom, ST_Point(?, ?))
                """,
                [parquets, qk17, lon, lat],
            ).fetchall()
        }
        direct_ids = {
            r[0]
            for r in con.execute(
                "SELECT id FROM bnd.places WHERE ST_Contains(geometry, ST_Point(?, ?))",
                [lon, lat],
            ).fetchall()
        }

        assert covering_ids == direct_ids, (
            f"Covering arm {covering_ids} != direct ST_Contains {direct_ids} "
            f"for point ({lat}, {lon})"
        )

    def test_pacific_point_no_covering_match(self, covering_dir):
        """Point in the Pacific Ocean (outside all test boundaries) hits no covering tiles."""
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        lat, lon = 30.0, -155.0
        qk17 = con.execute("SELECT ST_QuadKey(?, ?, 17)", [lon, lat]).fetchone()[0]

        count = con.execute(
            """
            SELECT COUNT(*) FROM read_parquet(?)
            WHERE left(?, length(tile_qk)) = tile_qk
            """,
            [parquets, qk17],
        ).fetchone()[0]
        assert count == 0, f"Pacific point hit {count} covering tiles (expected 0)"

    def test_edge_point_covering_arm_parity(self, covering_dir, covering_test_db):
        """For a point near a boundary edge, the covering arm matches direct ST_Contains."""
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"ATTACH '{covering_test_db}' AS bnd (READ_ONLY)")

        lat, lon = 37.61, -122.40
        qk17 = con.execute("SELECT ST_QuadKey(?, ?, 17)", [lon, lat]).fetchone()[0]

        covering_ids = {
            r[0]
            for r in con.execute(
                """
                SELECT DISTINCT boundary_id FROM read_parquet(?)
                WHERE left(?, length(tile_qk)) = tile_qk
                  AND ST_Covers(geom, ST_Point(?, ?))
                """,
                [parquets, qk17, lon, lat],
            ).fetchall()
        }

        direct_ids = {
            r[0]
            for r in con.execute(
                "SELECT id FROM bnd.places WHERE ST_Contains(geometry, ST_Point(?, ?))",
                [lon, lat],
            ).fetchall()
        }

        assert covering_ids == direct_ids, (
            f"Covering arm {covering_ids} != direct ST_Contains {direct_ids} "
            f"for point ({lat}, {lon})"
        )


# ---------------------------------------------------------------------------
# Zoom parameter overrides
# ---------------------------------------------------------------------------

class TestZoomParameters:
    """cover_min_zoom / cover_max_zoom overrides honored."""

    def test_cover_max_zoom_7_limits_depth(self, covering_test_db, tmp_path):
        _check_covering()
        out_dir = str(tmp_path / "z7")
        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=7)
        parquets = [
            os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.endswith(".parquet")
        ]
        con = duckdb.connect(":memory:")
        max_len = con.execute(
            "SELECT MAX(length(tile_qk)) FROM read_parquet(?)", [parquets]
        ).fetchone()[0]
        assert max_len == 7, f"cover_max_zoom=7 but max tile_qk length is {max_len}"

    def test_cover_min_zoom_5_respected(self, covering_test_db, tmp_path):
        _check_covering()
        out_dir = str(tmp_path / "z5")
        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=5, cover_max_zoom=7)
        parquets = [
            os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.endswith(".parquet")
        ]
        assert len(parquets) > 0
        con = duckdb.connect(":memory:")
        min_len = con.execute(
            "SELECT MIN(length(tile_qk)) FROM read_parquet(?)", [parquets]
        ).fetchone()[0]
        assert min_len >= 5, f"cover_min_zoom=5 but shortest tile_qk is length {min_len}"

    def test_meta_json_records_overridden_params(self, covering_test_db, tmp_path):
        _check_covering()
        out_dir = str(tmp_path / "z46")
        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=6)
        meta = json.load(open(os.path.join(out_dir, "_meta.json")))
        assert meta["cover_min_zoom"] == 4
        assert meta["cover_max_zoom"] == 6

    def test_higher_max_zoom_produces_more_rows(self, covering_test_db, tmp_path):
        _check_covering()
        out_z6 = str(tmp_path / "hz6")
        out_z8 = str(tmp_path / "hz8")
        stage_covering(str(covering_test_db), out_z6, cover_min_zoom=4, cover_max_zoom=6)
        stage_covering(str(covering_test_db), out_z8, cover_min_zoom=4, cover_max_zoom=8)
        con = duckdb.connect(":memory:")
        paths_z6 = [os.path.join(out_z6, f) for f in os.listdir(out_z6) if f.endswith(".parquet")]
        paths_z8 = [os.path.join(out_z8, f) for f in os.listdir(out_z8) if f.endswith(".parquet")]
        n_z6 = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [paths_z6]).fetchone()[0]
        n_z8 = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [paths_z8]).fetchone()[0]
        assert n_z8 >= n_z6, (
            f"Higher max zoom should produce >= rows: z8={n_z8} < z6={n_z6}"
        )


# ---------------------------------------------------------------------------
# Freshness and atomicity
# ---------------------------------------------------------------------------

class TestFreshnessAtomicity:
    """Freshness gate (no-op), force=True, param change, crash recovery."""

    def test_second_call_is_noop(self, covering_test_db, tmp_path):
        """Second stage_covering call with same params is a no-op (no mtime change)."""
        _check_covering()
        out_dir = str(tmp_path / "noop")
        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=6)
        meta_path = os.path.join(out_dir, "_meta.json")
        mtime1 = os.path.getmtime(meta_path)
        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=6)
        mtime2 = os.path.getmtime(meta_path)
        assert mtime1 == mtime2, (
            "Second stage_covering call should not update _meta.json (output is fresh)"
        )

    def test_force_true_rebuilds(self, covering_test_db, tmp_path):
        """force=True rebuilds even when fresh."""
        _check_covering()
        out_dir = str(tmp_path / "force")
        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=6)
        meta_path = os.path.join(out_dir, "_meta.json")
        mtime1 = os.path.getmtime(meta_path)
        time.sleep(0.05)
        stage_covering(
            str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=6, force=True
        )
        mtime2 = os.path.getmtime(meta_path)
        assert mtime2 > mtime1, "force=True should rebuild and produce a newer _meta.json"

    def test_changed_zoom_param_triggers_rebuild(self, covering_test_db, tmp_path):
        """Changed cover_max_zoom (with fresh source) forces a rebuild."""
        _check_covering()
        out_dir = str(tmp_path / "param_change")
        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=6)
        meta1 = json.load(open(os.path.join(out_dir, "_meta.json")))
        assert meta1["cover_max_zoom"] == 6
        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=7)
        meta2 = json.load(open(os.path.join(out_dir, "_meta.json")))
        assert meta2["cover_max_zoom"] == 7, (
            "cover_max_zoom param change did not trigger rebuild"
        )

    def test_leftover_tmp_dir_removed_at_build_start(self, covering_test_db, tmp_path):
        """Stale .tmp directory from a crashed previous run is removed at build start."""
        _check_covering()
        out_dir = str(tmp_path / "stale_tmp")
        tmp_dir = out_dir + ".tmp"
        os.makedirs(tmp_dir)
        stale_file = os.path.join(tmp_dir, "stale.parquet")
        open(stale_file, "w").close()

        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=6)
        assert os.path.exists(os.path.join(out_dir, "_meta.json")), (
            "stage_covering should succeed after removing stale .tmp"
        )
        assert not os.path.exists(stale_file), (
            "Stale .tmp/stale.parquet should have been removed before build"
        )

    def test_leftover_old_dir_no_covering_dir_recovered(self, covering_test_db, tmp_path):
        """Crash between the two renames (.old present, covering_dir absent) recovers.

        Simulates the state after a crash between rename(covering_dir → .old) and
        rename(.tmp → covering_dir): .old exists, covering_dir does not.
        Next call should remove .old at build start and rebuild from scratch.
        """
        _check_covering()
        out_dir = str(tmp_path / "old_crash")
        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=6)
        old_dir = out_dir + ".old"
        os.rename(out_dir, old_dir)
        assert not os.path.exists(out_dir)
        assert os.path.exists(old_dir)

        stage_covering(str(covering_test_db), out_dir, cover_min_zoom=4, cover_max_zoom=6)
        assert os.path.exists(os.path.join(out_dir, "_meta.json")), (
            "Recovery failed: covering_dir should have been rebuilt"
        )
        assert not os.path.exists(old_dir), (
            "Recovery failed: .old dir should have been cleaned up at build start"
        )

    def test_explicit_temp_directory_not_destroyed(self, covering_test_db, tmp_path):
        """A caller-supplied --temp-directory must not be deleted or crash on reuse.

        BUG: stage_covering shutil.rmtree's the caller's temp_directory at build
        start (leftover-cleanup loop), then os.makedirs(temp_directory) (which
        silently recreates it as an empty directory, discarding any pre-existing
        contents), and shutil.rmtree's it again at the end of the build. This
        destroys any pre-existing, caller-owned directory pointed to by
        --temp-directory -- including its unrelated contents -- rather than
        treating it as pipeline-owned scratch space (which is only true of the
        default `covering_dir + ".spill"` path).

        This test creates a pre-existing temp_directory containing a sentinel
        file, calls stage_covering with that directory as temp_directory, and
        asserts: (1) no exception is raised, and (2) the sentinel file still
        exists afterward. It MUST FAIL at Red (currently deletes the sentinel).
        """
        _check_covering()
        out_dir = str(tmp_path / "explicit_temp_out")
        explicit_temp_dir = str(tmp_path / "caller_owned_temp")
        os.makedirs(explicit_temp_dir)
        sentinel = os.path.join(explicit_temp_dir, "DO_NOT_DELETE.txt")
        with open(sentinel, "w") as f:
            f.write("caller-owned data, must survive stage_covering\n")

        stage_covering(
            str(covering_test_db),
            out_dir,
            cover_min_zoom=4,
            cover_max_zoom=6,
            temp_directory=explicit_temp_dir,
        )

        assert os.path.exists(sentinel), (
            "Caller-supplied temp_directory's pre-existing sentinel file was "
            "deleted by stage_covering; explicit temp_directory must not be "
            "treated as pipeline-owned scratch space"
        )


# ---------------------------------------------------------------------------
# Fragment-containment: shared fixture helpers
# (docs/fragment-containment-design.md)
# ---------------------------------------------------------------------------

# Bing's Mercator-projectable latitude bound (docs/fragment-containment-design.md,
# "Decided"). Pinned as a literal rather than imported from garganorn.covering:
# an assertion that reads its expected value back out of the code under test
# cannot catch that code changing the value.
_MERC_LAT_MAX = 85.05112877980659


def _ellipse_wkt(cx, cy, rx, ry, n):
    """WKT for an n-vertex ellipse centered at (cx, cy).

    Used to force a high vertex count after clipping, for the
    depth-cap/capacity synthetic fixtures below.  Callers scale rx and ry to
    their tile's width and height independently, so the shape tracks the
    tile's aspect ratio -- the expected leaf counts in those tests were
    derived against that geometry.
    """
    pts = [
        (cx + rx * math.cos(2 * math.pi * i / n), cy + ry * math.sin(2 * math.pi * i / n))
        for i in range(n)
    ]
    pts.append(pts[0])
    coords = ", ".join(f"{x} {y}" for x, y in pts)
    return f"POLYGON(({coords}))"


# ---------------------------------------------------------------------------
# Fragment-containment structural invariants
# (docs/fragment-containment-design.md, "Verification" section, items 1-5)
# ---------------------------------------------------------------------------

class TestFragmentContainmentInvariants:
    """Structural invariants: mass balance, no orphaned boundary, generalized
    antichain, leaf depth, capacity.

    All fail RED because stage_covering does not yet accept
    cover_min_leaf_zoom/cover_vertex_capacity (TypeError at the fixture's
    first stage_covering call, before any assertion runs).
    """

    @pytest.fixture(scope="class")
    def fc_covering_dir(self, covering_test_db, tmp_path_factory):
        _check_covering()
        out_dir = str(tmp_path_factory.mktemp("fc_invariants") / "covering")
        stage_covering(
            str(covering_test_db),
            out_dir,
            cover_min_zoom=4,
            cover_min_leaf_zoom=5,
            cover_max_zoom=7,
            cover_vertex_capacity=5000,
        )
        return out_dir

    def _parquet_paths(self, covering_dir):
        return [
            os.path.join(covering_dir, f)
            for f in os.listdir(covering_dir)
            if f.endswith(".parquet")
        ]

    def test_mass_balance_per_boundary(self, fc_covering_dir, covering_test_db):
        """Summed row area == boundary area clipped to the Mercator extent,
        within relative tolerance 1e-6. Leaves are pairwise disjoint
        (antichain), so areas sum -- no ST_Union_Agg, per the design's
        explicit prohibition."""
        _check_covering()
        parquets = self._parquet_paths(fc_covering_dir)
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"ATTACH '{covering_test_db}' AS bnd (READ_ONLY)")

        rows = con.execute(
            """
            WITH per_boundary AS (
                SELECT boundary_id, SUM(ST_Area(geom)) AS covered_area
                FROM read_parquet(?)
                GROUP BY boundary_id
            )
            SELECT pb.boundary_id,
                   pb.covered_area,
                   ST_Area(ST_Intersection(b.geometry,
                       ST_MakeEnvelope(-180, ?, 180, ?))) AS expected_area
            FROM per_boundary pb
            JOIN bnd.places b ON b.id = pb.boundary_id
            """,
            [parquets, -_MERC_LAT_MAX, _MERC_LAT_MAX],
        ).fetchall()

        assert len(rows) == len(_COVERING_TEST_BOUNDARIES), (
            f"Expected a mass-balance row for each of {len(_COVERING_TEST_BOUNDARIES)} "
            f"boundaries, got {len(rows)}"
        )
        for boundary_id, covered_area, expected_area in rows:
            rel_err = abs(covered_area - expected_area) / expected_area
            assert rel_err <= 1e-6, (
                f"{boundary_id}: mass balance off by relative {rel_err:.2e} "
                f"(covered={covered_area}, expected={expected_area})"
            )

    def test_no_orphaned_boundary(self, fc_covering_dir, covering_test_db):
        """Every boundary intersecting the Mercator extent has >=1 covering row."""
        _check_covering()
        parquets = self._parquet_paths(fc_covering_dir)
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"ATTACH '{covering_test_db}' AS bnd (READ_ONLY)")

        orphans = con.execute(
            """
            SELECT b.id
            FROM bnd.places b
            WHERE ST_Intersects(b.geometry, ST_MakeEnvelope(-180, ?, 180, ?))
              AND b.id NOT IN (SELECT DISTINCT boundary_id FROM read_parquet(?))
            """,
            [-_MERC_LAT_MAX, _MERC_LAT_MAX, parquets],
        ).fetchall()
        assert orphans == [], f"Boundaries with no covering row: {orphans}"

    def test_antichain_over_all_rows(self, fc_covering_dir):
        """No covering row is a quadkey-prefix descendant of any other row of
        the same boundary."""
        _check_covering()
        parquets = self._parquet_paths(fc_covering_dir)
        con = duckdb.connect(":memory:")
        violations = con.execute(
            """
            SELECT child.tile_qk, child.boundary_id, parent.tile_qk AS ancestor
            FROM read_parquet(?) child
            JOIN read_parquet(?) parent
              ON child.boundary_id = parent.boundary_id
             AND length(child.tile_qk) > length(parent.tile_qk)
             AND left(child.tile_qk, length(parent.tile_qk)) = parent.tile_qk
            """,
            [parquets, parquets],
        ).fetchall()
        assert violations == [], (
            f"Found {len(violations)} covering rows descending from another row "
            f"of the same boundary: {violations[:5]}"
        )

    def test_fragment_leaf_depth_within_floor_and_cap(self, fc_covering_dir):
        """No fragment row (geom != its own tile envelope) is shallower than
        cover_min_leaf_zoom(5) or deeper than cover_max_zoom(7); at least one
        fragment row sits strictly above the floor and below the cap."""
        _check_covering()
        parquets = self._parquet_paths(fc_covering_dir)
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        _load_qk_env_macro(con)
        min_len, max_len = con.execute(
            "SELECT MIN(length(tile_qk)), MAX(length(tile_qk)) FROM read_parquet(?) "
            "WHERE NOT ST_Equals(geom, qk_env(tile_qk))",
            [parquets],
        ).fetchone()
        assert min_len >= 5, f"fragment row shallower than cover_min_leaf_zoom=5: {min_len}"
        assert max_len <= 7, f"fragment row deeper than cover_max_zoom=7: {max_len}"

        below_cap = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE NOT ST_Equals(geom, qk_env(tile_qk)) AND length(tile_qk) < 7",
            [parquets],
        ).fetchone()[0]
        assert below_cap > 0, (
            "Expected at least one fragment row shallower than cover_max_zoom=7 "
            "(floor-triggered emission); found none"
        )

    def test_capacity_and_no_row_has_null_geom(self, fc_covering_dir):
        """Every fragment row shallower than cover_max_zoom has
        ST_NPoints(geom) <= V; no row anywhere has NULL geom."""
        _check_covering()
        parquets = self._parquet_paths(fc_covering_dir)
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        _load_qk_env_macro(con)
        over_capacity = con.execute(
            """
            SELECT COUNT(*) FROM read_parquet(?)
            WHERE NOT ST_Equals(geom, qk_env(tile_qk)) AND length(tile_qk) < 7
              AND ST_NPoints(geom) > 5000
            """,
            [parquets],
        ).fetchone()[0]
        assert over_capacity == 0, (
            f"{over_capacity} fragment rows below cover_max_zoom exceed V=5000"
        )

        null_geom = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) WHERE geom IS NULL",
            [parquets],
        ).fetchone()[0]
        assert null_geom == 0, f"{null_geom} rows have NULL geom"


# ---------------------------------------------------------------------------
# Fragment-containment synthetic unit tests
# (docs/fragment-containment-design.md, "Verification" section)
# ---------------------------------------------------------------------------

class TestFragmentContainmentSynthetics:
    """Synthetic fixtures built at reduced zoom so a fragment split is
    reachable in test time.  All fail RED with TypeError (stage_covering does
    not yet accept cover_min_leaf_zoom/cover_vertex_capacity)."""

    def test_seam_point_matched_exactly_once_by_covers_not_contains(self, tmp_path):
        """A point on the internal seam introduced by splitting (not on the
        boundary's own true edge) is matched by ST_Covers via the tile-prefix
        join, exactly once; ST_Contains would miss it entirely."""
        _check_covering()
        tile_a = quadkey_to_bbox("120030")  # NW child of a common z5 parent
        tile_b = quadkey_to_bbox("120031")  # NE child -- shares tile_a's east edge
        axmin, aymin, axmax, aymax = tile_a
        bxmin, bymin, bxmax, bymax = tile_b
        assert axmax == bxmin, "fixture assumption: tiles must share an edge"

        margin = 0.2 * (aymax - aymin)
        y_lo, y_hi = aymin + margin, aymax - margin
        rect_wkt = (
            f"POLYGON(({axmin} {y_lo}, {axmin} {y_hi}, "
            f"{bxmax} {y_hi}, {bxmax} {y_lo}, {axmin} {y_lo}))"
        )
        db_path = tmp_path / "seam_boundaries.duckdb"
        _create_boundaries_db(db_path, [
            ("seam_boundary", 50, rect_wkt, y_lo, axmin, y_hi, bxmax),
        ])

        out_dir = str(tmp_path / "seam_covering")
        stage_covering(
            str(db_path), out_dir,
            cover_min_zoom=4, cover_min_leaf_zoom=6, cover_max_zoom=6,
        )
        parquets = [
            os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.endswith(".parquet")
        ]

        seam_x, seam_y = axmax, (y_lo + y_hi) / 2
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        qk17 = con.execute("SELECT ST_QuadKey(?, ?, 17)", [seam_x, seam_y]).fetchone()[0]

        covers_matches = con.execute(
            """
            SELECT COUNT(*) FROM read_parquet(?)
            WHERE length(tile_qk) = 6
              AND left(?, 6) = tile_qk
              AND ST_Covers(geom, ST_Point(?, ?))
            """,
            [parquets, qk17, seam_x, seam_y],
        ).fetchone()[0]
        assert covers_matches == 1, (
            f"Seam point should be matched exactly once via ST_Covers; got {covers_matches}"
        )

        contains_matches = con.execute(
            """
            SELECT COUNT(*) FROM read_parquet(?)
            WHERE length(tile_qk) = 6
              AND left(?, 6) = tile_qk
              AND ST_Contains(geom, ST_Point(?, ?))
            """,
            [parquets, qk17, seam_x, seam_y],
        ).fetchone()[0]
        assert contains_matches == 0, (
            "ST_Contains should miss a point exactly on the fragment boundary "
            f"(demonstrating why ST_Covers is required); got {contains_matches}"
        )

    def test_grid_aligned_border_no_zero_area_fragment(self, tmp_path):
        """A boundary edge that lies exactly along a cell edge stores no
        zero-area fragment for the neighboring cell, and produces no covering
        row there."""
        _check_covering()
        tile_a = quadkey_to_bbox("120030")
        tile_b_qk = "120031"
        axmin, aymin, axmax, aymax = tile_a

        margin = 0.2 * (aymax - aymin)
        y_lo, y_hi = aymin + margin, aymax - margin
        rect_wkt = (
            f"POLYGON(({axmin} {y_lo}, {axmin} {y_hi}, "
            f"{axmax} {y_hi}, {axmax} {y_lo}, {axmin} {y_lo}))"
        )
        db_path = tmp_path / "grid_boundaries.duckdb"
        _create_boundaries_db(db_path, [
            ("grid_boundary", 50, rect_wkt, y_lo, axmin, y_hi, axmax),
        ])

        out_dir = str(tmp_path / "grid_covering")
        stage_covering(
            str(db_path), out_dir,
            cover_min_zoom=4, cover_min_leaf_zoom=6, cover_max_zoom=6,
        )
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        parquets = [
            os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.endswith(".parquet")
        ]
        rows = con.execute(
            "SELECT tile_qk, ST_Area(geom) FROM read_parquet(?) WHERE boundary_id = 'grid_boundary'",
            [parquets],
        ).fetchall()
        tile_qks = {r[0] for r in rows}
        assert tile_b_qk not in tile_qks, (
            f"Neighboring tile {tile_b_qk!r} (grid-aligned, zero-area clip) "
            f"must not appear as a covering row; got rows: {rows}"
        )
        assert all(area > 0 for _, area in rows), f"Zero-area fragment stored: {rows}"

    def test_antimeridian_lobes_matched_gap_absent(self, tmp_path):
        """Two-lobe boundary: a point in each lobe matches via the tile-prefix
        + ST_Covers join; a point in the gap does not (the import-side bbox
        filter drops ±180-crossers, so this fixture is built, not found)."""
        _check_covering()
        ami_wkt = (
            "MULTIPOLYGON("
            "((170 -15, 180 -15, 180 15, 170 15, 170 -15)),"
            "((-180 -15, -170 -15, -170 15, -180 15, -180 -15)))"
        )
        db_path = tmp_path / "ami_boundaries.duckdb"
        _create_boundaries_db(db_path, [
            ("ami_boundary", 10, ami_wkt, -15.0, 170.0, 15.0, -170.0),
        ])

        out_dir = str(tmp_path / "ami_covering")
        stage_covering(
            str(db_path), out_dir,
            cover_min_zoom=4, cover_min_leaf_zoom=6, cover_max_zoom=6,
        )
        parquets = [
            os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.endswith(".parquet")
        ]
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        def _matches(lon, lat):
            """Mirror compute_containment's single arm: prefix membership
            plus ST_Covers against the row's stored geometry."""
            qk17 = con.execute("SELECT ST_QuadKey(?, ?, 17)", [lon, lat]).fetchone()[0]
            return con.execute(
                """
                SELECT COUNT(*) FROM read_parquet(?)
                WHERE left(?, length(tile_qk)) = tile_qk
                  AND ST_Covers(geom, ST_Point(?, ?))
                """,
                [parquets, qk17, lon, lat],
            ).fetchone()[0]

        assert _matches(175.0, 0.0) >= 1, "East lobe point (lon=175) should match"
        assert _matches(-175.0, 0.0) >= 1, "West lobe point (lon=-175) should match"
        assert _matches(0.0, 0.0) == 0, "Gap point (lon=0) should not match"

    def test_polar_boundary_clipped_at_merc_extent(self, tmp_path):
        """A boundary extending past +/-_MERC_LAT_MAX still produces >=1
        covering row for its in-extent part, and that part's covered area
        mass-balances against the boundary clipped to the Mercator extent,
        not its full area.

        _COVERING_TEST_BOUNDARIES (used by test_mass_balance_per_boundary
        and test_no_orphaned_boundary) sits entirely within 20-55N, so the
        +/-_MERC_LAT_MAX clip in those tests' expected-area computation is
        a no-op -- neither test can catch a broken clip. This builds a
        dedicated polar fixture via _create_boundaries_db instead of
        extending _COVERING_TEST_BOUNDARIES, which is a module-scoped
        fixture shared by every existing covering test; adding a row to it
        would perturb their row counts and per-level stats.
        """
        _check_covering()
        polar_wkt = "POLYGON((-10 70, -10 89, 10 89, 10 70, -10 70))"
        db_path = tmp_path / "polar_boundaries.duckdb"
        _create_boundaries_db(db_path, [
            ("polar_boundary", 50, polar_wkt, 70.0, -10.0, 89.0, 10.0),
        ])

        out_dir = str(tmp_path / "polar_covering")
        stage_covering(
            str(db_path), out_dir,
            cover_min_zoom=4, cover_min_leaf_zoom=5, cover_max_zoom=6,
            cover_vertex_capacity=5000,
        )
        parquets = [
            os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.endswith(".parquet")
        ]
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"ATTACH '{db_path}' AS bnd (READ_ONLY)")

        row_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) WHERE boundary_id = 'polar_boundary'",
            [parquets],
        ).fetchone()[0]
        assert row_count > 0, (
            "Polar boundary's in-extent part (lat 70 to _MERC_LAT_MAX) "
            "produced no covering row"
        )

        covered_area, expected_area, full_area = con.execute(
            """
            WITH per_boundary AS (
                SELECT SUM(ST_Area(geom)) AS covered_area
                FROM read_parquet(?)
                WHERE boundary_id = 'polar_boundary'
            )
            SELECT pb.covered_area,
                   ST_Area(ST_Intersection(b.geometry, ST_MakeEnvelope(-180, ?, 180, ?))),
                   ST_Area(b.geometry)
            FROM per_boundary pb, bnd.places b
            WHERE b.id = 'polar_boundary'
            """,
            [parquets, -_MERC_LAT_MAX, _MERC_LAT_MAX],
        ).fetchone()

        assert expected_area < full_area, (
            "fixture assumption violated: polar_boundary must extend past "
            "_MERC_LAT_MAX, else this test cannot distinguish a clipped "
            "mass balance from an unclipped one"
        )
        rel_err = abs(covered_area - expected_area) / expected_area
        assert rel_err <= 1e-6, (
            f"polar_boundary: covered area off by relative {rel_err:.2e} "
            f"from the Mercator-clipped area (covered={covered_area}, "
            f"clipped_expected={expected_area}, full={full_area})"
        )

    def test_containment_arms_have_no_boundary_table_join(self):
        """Regression guard: the generated arms test the stored covering row
        and never join the boundary table.

        Asserts against `containment_arms_sql`'s output, not against
        compute_containment.sql -- the template holds only `${arms}`, so a
        text check there passes no matter what the generator emits. The
        template's final SELECT does keep a `JOIN bnd.places bp` for
        `names."primary"`, which the design retains; scoping to the
        generator's output excludes it without special-casing.

        Catches a CASE-only fix: deleting the antimeridian min_longitude CASE while
        leaving `JOIN bnd.places b ...` and `ST_Contains(b.geometry, ...)`
        would satisfy a "no min_longitude" grep while keeping the
        whole-polygon test this unit exists to remove. The positive
        assertions below fail the converse case, where the arms stop
        testing the stored row at all.
        """
        arms = containment_arms_sql(4, 16)

        assert "bnd.places" not in arms, (
            "arms must not join bnd.places -- the geometry stored in `cov` "
            "IS the geometry to test against, so the boundary table join "
            f"(and the bbox pre-filter it carries) must not appear:\n{arms}"
        )
        assert "min_longitude" not in arms, (
            "arms must not reference min_longitude -- the antimeridian bbox "
            f"pre-filter is deleted along with the join it lived on:\n{arms}"
        )
        assert "ST_Contains" not in arms, (
            f"arms must test with ST_Covers, not ST_Contains:\n{arms}"
        )
        assert "kind" not in arms, (
            f"arms must not reference the removed kind column:\n{arms}"
        )
        assert arms.count("ST_Covers(c.geom") == 13, (
            "expected one ST_Covers arm per zoom over [4, 16]; got "
            f"{arms.count('ST_Covers(c.geom')}:\n{arms}"
        )

    def test_depth_cap_over_capacity_leaves_counted(self, tmp_path):
        """A high-vertex fragment that still exceeds V at cover_max_zoom is
        emitted anyway (no error path); stats['over_capacity_leaves'] counts
        it."""
        _check_covering()
        bxmin, bymin, bxmax, bymax = quadkey_to_bbox("120031")
        cx, cy = (bxmin + bxmax) / 2, (bymin + bymax) / 2
        rx, ry = (bxmax - bxmin) * 0.35, (bymax - bymin) * 0.35
        ellipse_wkt = _ellipse_wkt(cx, cy, rx, ry, 2000)

        db_path = tmp_path / "depthcap_boundaries.duckdb"
        _create_boundaries_db(db_path, [
            ("depthcap_boundary", 50, ellipse_wkt, bymin, bxmin, bymax, bxmax),
        ])

        out_dir = str(tmp_path / "depthcap_covering")
        stats = stage_covering(
            str(db_path), out_dir,
            cover_min_zoom=4, cover_min_leaf_zoom=6, cover_max_zoom=9,
            cover_vertex_capacity=50,
        )

        parquets = [
            os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.endswith(".parquet")
        ]
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        over_cap_rows = con.execute(
            """
            SELECT COUNT(*) FROM read_parquet(?)
            WHERE length(tile_qk) = 9 AND ST_NPoints(geom) > 50
            """,
            [parquets],
        ).fetchone()[0]
        assert over_cap_rows > 0, (
            "Expected at least one leaf at cover_max_zoom still above V=50"
        )
        assert stats["over_capacity_leaves"] == over_cap_rows, (
            f"stats['over_capacity_leaves']={stats.get('over_capacity_leaves')} "
            f"!= actual over-capacity row count {over_cap_rows}"
        )

    def test_small_capacity_splits_one_fragment_into_several_all_under_capacity(self, tmp_path):
        """With a small V, a boundary that would be a single fragment at the
        design's default V=5000 instead produces several fragments, each
        under capacity."""
        _check_covering()
        bxmin, bymin, bxmax, bymax = quadkey_to_bbox("120031")
        cx, cy = (bxmin + bxmax) / 2, (bymin + bymax) / 2
        rx, ry = (bxmax - bxmin) * 0.35, (bymax - bymin) * 0.35
        ellipse_wkt = _ellipse_wkt(cx, cy, rx, ry, 500)

        db_path = tmp_path / "capsplit_boundaries.duckdb"
        _create_boundaries_db(db_path, [
            ("capsplit_boundary", 50, ellipse_wkt, bymin, bxmin, bymax, bxmax),
        ])

        def _build(vertex_capacity, dirname):
            out_dir = str(tmp_path / dirname)
            stage_covering(
                str(db_path), out_dir,
                cover_min_zoom=4, cover_min_leaf_zoom=6, cover_max_zoom=12,
                cover_vertex_capacity=vertex_capacity,
            )
            parquets = [
                os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.endswith(".parquet")
            ]
            con = duckdb.connect(":memory:")
            con.execute("INSTALL spatial; LOAD spatial;")
            _load_qk_env_macro(con)
            return con.execute(
                "SELECT length(tile_qk), ST_NPoints(geom) FROM read_parquet(?) "
                "WHERE NOT ST_Equals(geom, qk_env(tile_qk))",
                [parquets],
            ).fetchall()

        large_v_rows = _build(5000, "capsplit_large")
        small_v_rows = _build(50, "capsplit_small")

        assert len(large_v_rows) == 1, (
            f"At V=5000 expected exactly one fragment; got {len(large_v_rows)}: {large_v_rows}"
        )
        assert len(small_v_rows) > 1, (
            f"At V=50 expected several fragments; got {len(small_v_rows)}: {small_v_rows}"
        )
        over_capacity = [n for _, n in small_v_rows if n > 50]
        assert over_capacity == [], (
            f"At V=50 every fragment must be under capacity; over: {over_capacity}"
        )

    def test_freshness_leaf_zoom_and_vertex_capacity_change_rebuilds(
        self, covering_test_db, tmp_path
    ):
        """Changing either cover_vertex_capacity or cover_min_leaf_zoom alone
        rebuilds the covering; an identical repeat call is a no-op.

        The design's "Params and freshness" section requires BOTH new params
        in the freshness gate's comparison. An implementation that adds
        cover_vertex_capacity to the gate but forgets cover_min_leaf_zoom
        would pass a test that only ever varies vertex_capacity -- so this
        test varies each param independently, holding the other fixed.
        """
        _check_covering()
        out_dir = str(tmp_path / "params_freshness")
        stage_covering(
            str(covering_test_db), out_dir,
            cover_min_zoom=4, cover_min_leaf_zoom=5, cover_max_zoom=6,
            cover_vertex_capacity=1000,
        )
        meta_path = os.path.join(out_dir, "_meta.json")
        mtime1 = os.path.getmtime(meta_path)

        time.sleep(0.05)
        stage_covering(
            str(covering_test_db), out_dir,
            cover_min_zoom=4, cover_min_leaf_zoom=5, cover_max_zoom=6,
            cover_vertex_capacity=1000,
        )
        mtime2 = os.path.getmtime(meta_path)
        assert mtime2 == mtime1, "Unchanged params must not rebuild"

        time.sleep(0.05)
        stage_covering(
            str(covering_test_db), out_dir,
            cover_min_zoom=4, cover_min_leaf_zoom=5, cover_max_zoom=6,
            cover_vertex_capacity=2000,
        )
        mtime3 = os.path.getmtime(meta_path)
        assert mtime3 > mtime2, "Changed cover_vertex_capacity must trigger a rebuild"

        time.sleep(0.05)
        stage_covering(
            str(covering_test_db), out_dir,
            cover_min_zoom=4, cover_min_leaf_zoom=6, cover_max_zoom=6,
            cover_vertex_capacity=2000,
        )
        mtime4 = os.path.getmtime(meta_path)
        assert mtime4 > mtime3, "Changed cover_min_leaf_zoom must trigger a rebuild"


# ---------------------------------------------------------------------------
# R1: geom is never NULL, even for an interior-only partition
# ---------------------------------------------------------------------------

class TestInteriorOnlyPartitionGeometry:
    """A z4 partition holding only interior rows still binds geom as
    GEOMETRY (docs/tranche2-design.md, R1)."""

    def test_interior_only_partition_reads_back_as_geometry(self, tmp_path):
        """DESCRIBE reports GEOMETRY for a partition whose sole row is a
        whole-tile interior row -- today it's all-NULL, so no GeoParquet
        `geo` metadata key is written and the column reads back as BLOB."""
        _check_covering()
        qk4 = "0231"
        xmin, ymin, xmax, ymax = quadkey_to_bbox(qk4)
        mx, my = 0.2 * (xmax - xmin), 0.2 * (ymax - ymin)
        bxmin, bxmax = xmin - mx, xmax + mx
        bymin, bymax = ymin - my, ymax + my
        wkt = (
            f"POLYGON(({bxmin} {bymin}, {bxmin} {bymax}, "
            f"{bxmax} {bymax}, {bxmax} {bymin}, {bxmin} {bymin}))"
        )
        db_path = tmp_path / "spill_boundaries.duckdb"
        _create_boundaries_db(db_path, [
            ("spill_boundary", 50, wkt, bymin, bxmin, bymax, bxmax),
        ])

        out_dir = str(tmp_path / "spill_covering")
        stage_covering(str(db_path), out_dir, cover_min_zoom=4, cover_max_zoom=7)
        target = os.path.join(out_dir, f"{qk4}.parquet")

        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        row_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [target]
        ).fetchone()[0]
        assert row_count == 1, (
            f"fixture assumption violated: {qk4}.parquet should hold exactly "
            f"one interior row, got {row_count}"
        )

        geom_type = con.execute(
            "DESCRIBE SELECT geom FROM read_parquet(?)", [target]
        ).fetchone()[1]
        assert geom_type == "GEOMETRY", (
            f"{qk4}.parquet's geom column reads back as {geom_type}, not GEOMETRY"
        )
