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
        ensure_covering,
    )
    _COVERING_ERROR = None
except ImportError as _exc:
    COVER_MIN_ZOOM = COVER_MAX_ZOOM = None
    lonlat_to_tile = bbox_to_quadkeys = stage_covering = ensure_covering = None
    _COVERING_ERROR = _exc


def _check_covering():
    """Call at the start of every test; fails with the ImportError if module is absent."""
    if _COVERING_ERROR is not None:
        pytest.fail(str(_COVERING_ERROR), pytrace=False)


from garganorn.stages import quadkey_to_bbox

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


def _create_covering_test_db(db_path):
    """Create a minimal boundaries.duckdb for covering tests."""
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
    for bid, level, wkt, min_lat, min_lon, max_lat, max_lon in _COVERING_TEST_BOUNDARIES:
        conn.execute(
            "INSERT INTO places VALUES (?, ST_GeomFromText(?), ?, ?, ?, ?, ?)",
            [bid, wkt, level, min_lat, max_lat, min_lon, max_lon],
        )
    conn.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
    conn.close()


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
    """Known bboxes, world=256 tiles, lat clamping, antimeridian (D7)."""

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
        """min_lon > max_lon (D7): returns tiles from BOTH lobes (near ±180°)."""
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
# stage_covering output schema / sort / kind invariants
# ---------------------------------------------------------------------------

class TestStageCoveringSchema:
    """Parquet schema, sort, kind='edge' only at max zoom, _meta.json."""

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
        assert "kind" in cols

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

    def test_edge_kind_only_at_max_zoom(self, covering_dir):
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        edge_non_max = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) WHERE kind = 'edge' AND length(tile_qk) < 7",
            [parquets],
        ).fetchone()[0]
        assert edge_non_max == 0, (
            f"Found {edge_non_max} 'edge' rows at zoom < 7 (max zoom)"
        )

    def test_kind_values_are_interior_or_edge(self, covering_dir):
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        invalid_kinds = con.execute(
            "SELECT DISTINCT kind FROM read_parquet(?) WHERE kind NOT IN ('interior', 'edge')",
            [parquets],
        ).fetchall()
        assert len(invalid_kinds) == 0, f"Unexpected kind values: {invalid_kinds}"

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


# ---------------------------------------------------------------------------
# Semantic interior/edge invariants
# ---------------------------------------------------------------------------

class TestCoveringSemanticInvariants:
    """Interior tiles contained; edge tiles intersect; no prefix chains."""

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

    def test_interior_tiles_contained_by_boundary(self, covering_dir, covering_test_db):
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
            WHERE c.kind = 'interior'
              AND NOT ST_Contains(b.geometry, qk_env(c.tile_qk))
            """,
            [parquets],
        ).fetchall()
        assert len(violations) == 0, (
            f"Interior tiles not contained by their boundary: {violations[:5]}"
        )

    def test_edge_tiles_intersect_boundary(self, covering_dir, covering_test_db):
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
            WHERE c.kind = 'edge'
              AND NOT ST_Intersects(b.geometry, qk_env(c.tile_qk))
            """,
            [parquets],
        ).fetchall()
        assert len(violations) == 0, (
            f"Edge tiles do not intersect their boundary: {violations[:5]}"
        )

    def test_no_covering_tile_descends_from_interior_tile(self, covering_dir):
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        violations = con.execute(
            """
            SELECT child.tile_qk, child.boundary_id, parent.tile_qk AS interior_ancestor
            FROM read_parquet(?) child
            JOIN read_parquet(?) parent
              ON child.boundary_id = parent.boundary_id
             AND parent.kind = 'interior'
             AND length(child.tile_qk) > length(parent.tile_qk)
             AND left(child.tile_qk, length(parent.tile_qk)) = parent.tile_qk
            """,
            [parquets, parquets],
        ).fetchall()
        assert len(violations) == 0, (
            f"Found {len(violations)} covering tiles descending from interior tiles: "
            f"{violations[:5]}"
        )


# ---------------------------------------------------------------------------
# Point classification property
# ---------------------------------------------------------------------------

class TestPointClassification:
    """Covering interior/edge arms ⟺ direct ST_Contains(boundary, point)."""

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

    def test_sf_point_interior_arm_subset_of_direct(self, covering_dir, covering_test_db):
        """Interior-arm matches for SF point are a subset of direct ST_Contains results."""
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"ATTACH '{covering_test_db}' AS bnd (READ_ONLY)")

        lat, lon = 37.78, -122.42
        qk17 = con.execute("SELECT ST_QuadKey(?, ?, 17)", [lon, lat]).fetchone()[0]

        interior_ids = {
            r[0]
            for r in con.execute(
                """
                SELECT DISTINCT boundary_id FROM read_parquet(?)
                WHERE kind = 'interior'
                  AND left(?, length(tile_qk)) = tile_qk
                """,
                [parquets, qk17],
            ).fetchall()
        }
        direct_ids = {
            r[0]
            for r in con.execute(
                "SELECT id FROM bnd.places WHERE ST_Contains(geometry, ST_Point(?, ?))",
                [lon, lat],
            ).fetchall()
        }

        spurious = interior_ids - direct_ids
        assert len(spurious) == 0, (
            f"Interior arm returned boundaries not confirmed by direct ST_Contains: {spurious}"
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

    def test_edge_point_combined_arm_parity(self, covering_dir, covering_test_db):
        """For a point near a boundary edge, combined interior+edge arm matches direct ST_Contains."""
        _check_covering()
        parquets = self._parquet_paths(covering_dir)
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"ATTACH '{covering_test_db}' AS bnd (READ_ONLY)")

        lat, lon = 37.61, -122.40
        qk17 = con.execute("SELECT ST_QuadKey(?, ?, 17)", [lon, lat]).fetchone()[0]

        interior_ids = {
            r[0]
            for r in con.execute(
                """
                SELECT DISTINCT boundary_id FROM read_parquet(?)
                WHERE kind = 'interior'
                  AND left(?, length(tile_qk)) = tile_qk
                """,
                [parquets, qk17],
            ).fetchall()
        }
        edge_ids = {
            r[0]
            for r in con.execute(
                """
                SELECT DISTINCT c.boundary_id
                FROM read_parquet(?) c
                JOIN bnd.places b ON b.id = c.boundary_id
                WHERE c.kind = 'edge'
                  AND left(?, length(c.tile_qk)) = c.tile_qk
                  AND ST_Contains(b.geometry, ST_Point(?, ?))
                """,
                [parquets, qk17, lon, lat],
            ).fetchall()
        }
        covering_ids = interior_ids | edge_ids

        direct_ids = {
            r[0]
            for r in con.execute(
                "SELECT id FROM bnd.places WHERE ST_Contains(geometry, ST_Point(?, ?))",
                [lon, lat],
            ).fetchall()
        }

        assert covering_ids == direct_ids, (
            f"Covering combined arm {covering_ids} != direct ST_Contains {direct_ids} "
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
        import time
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
