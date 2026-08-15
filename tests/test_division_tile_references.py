"""garganorn.stages.stage_division_tile_references.

Fixtures build a real boundaries.duckdb + covering artifact via
garganorn.covering.stage_covering, and a hand-written
tile_assignments.parquet (the stage only reads its distinct tile_qk set,
so the grid's provenance doesn't matter).

Completeness is checked against an independent ST_Intersects +
positive-area oracle over the raw geometry, never against the stage's own
prefix arithmetic -- a test that reused it would be circular.
"""
import os
import time

import duckdb

from garganorn.stages import stage_division_tile_references
from garganorn.covering import stage_covering
from garganorn.stages import quadkey_to_bbox

_QK_ENV_SQL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "garganorn", "sql", "qk_env_macro.sql"
)


def _load_qk_env(con):
    with open(_QK_ENV_SQL_PATH) as f:
        for stmt in f.read().split(";"):
            s = stmt.strip()
            if s:
                con.execute(s)


def _create_boundaries_db(db_path, boundary_rows):
    """Each row: (id, level, wkt, min_lat, min_lon, max_lat, max_lon)."""
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


def _write_tile_assignments(path, rows):
    """rows: list of (place_id, tile_qk)."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE t (place_id VARCHAR, tile_qk VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?, ?)", rows)
    con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    con.close()


def _rect_wkt(xmin, ymin, xmax, ymax):
    return (
        f"POLYGON(({xmin} {ymin}, {xmin} {ymax}, "
        f"{xmax} {ymax}, {xmax} {ymin}, {xmin} {ymin}))"
    )


def _covering_parquets(covering_dir):
    return [
        os.path.join(covering_dir, f)
        for f in os.listdir(covering_dir)
        if f.endswith(".parquet")
    ]


def _two_tile_fixture(tmp_path):
    """One division spanning two grid tiles. Returns (covering_dir, ta_path).

    The grid and the references must not coincide: the grid holds
    (div1, 12300) and (g2, 12301) while the references hold div1 against
    both tiles. A fixture where they match cannot detect the stage writing
    its output over the grid artifact.
    """
    nw = quadkey_to_bbox("12300")
    ne = quadkey_to_bbox("12301")
    my = 0.2 * (nw[3] - nw[1])
    wkt = _rect_wkt(nw[0], nw[1] + my, ne[2], nw[3] - my)

    db_path = tmp_path / "boundaries.duckdb"
    _create_boundaries_db(db_path, [
        ("div1", 50, wkt, nw[1] + my, nw[0], nw[3] - my, ne[2]),
    ])

    covering_dir = str(tmp_path / "covering")
    stage_covering(str(db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=5)

    ta_path = str(tmp_path / "tile_assignments.parquet")
    _write_tile_assignments(ta_path, [("div1", "12300"), ("g2", "12301")])
    return covering_dir, ta_path


def _output_rows(output_path):
    con = duckdb.connect(":memory:")
    rows = con.execute(
        "SELECT place_id, tile_qk FROM read_parquet(?)", [output_path]
    ).fetchall()
    con.close()
    return rows


# ---------------------------------------------------------------------------
# Exhaustive completeness against an independent ST_Intersects +
# positive-area oracle -- not the stage's own prefix arithmetic.
# ---------------------------------------------------------------------------

class TestCompleteness:
    def test_every_geometry_tile_overlap_appears_in_output(self, tmp_path):
        """div_wide spans two grid tiles (plus a small sliver into the other
        two, via a margin); div_small sits well inside a single tile. Every
        true overlap the independent oracle finds must appear in the
        stage's output."""
        parent_qk = "1230"
        children = {d: quadkey_to_bbox(parent_qk + d) for d in "0123"}
        nw, ne, sw = children["0"], children["1"], children["2"]
        midlat = sw[3]
        buffer = 0.1 * (nw[3] - midlat)
        wide_wkt = _rect_wkt(nw[0], midlat - buffer, ne[2], nw[3])

        mx, my = 0.3 * (sw[2] - sw[0]), 0.3 * (sw[3] - sw[1])
        small_wkt = _rect_wkt(sw[0] + mx, sw[1] + my, sw[2] - mx, sw[3] - my)

        db_path = tmp_path / "boundaries.duckdb"
        _create_boundaries_db(db_path, [
            ("div_wide", 50, wide_wkt, midlat - buffer, nw[0], nw[3], ne[2]),
            ("div_small", 50, small_wkt, sw[1] + my, sw[0] + mx, sw[3] - my, sw[2] - mx),
        ])

        covering_dir = str(tmp_path / "covering")
        stage_covering(str(db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=5)

        ta_path = str(tmp_path / "tile_assignments.parquet")
        grid = [(f"g{d}", parent_qk + d) for d in "0123"]
        _write_tile_assignments(ta_path, grid)

        out_path = str(tmp_path / "tile_references.parquet")
        stage_division_tile_references(covering_dir, ta_path, out_path)

        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"ATTACH '{db_path}' AS bnd (READ_ONLY)")
        _load_qk_env(con)
        oracle = set(con.execute(
            """
            SELECT b.id, g.tile_qk
            FROM bnd.places b, (SELECT DISTINCT tile_qk FROM read_parquet(?)) g
            WHERE ST_Intersects(b.geometry, qk_env(g.tile_qk))
              AND ST_Area(ST_Intersection(b.geometry, qk_env(g.tile_qk))) > 0
            """,
            [ta_path],
        ).fetchall())
        assert len(oracle) == 5, f"fixture drift: expected 5 oracle pairs, got {len(oracle)}"

        # Equality, not just containment. Grid depth equals leaf depth here, so
        # the truncation is the identity and no false positive is permitted --
        # which makes over-emission detectable too.
        actual = set(_output_rows(out_path))
        assert actual == oracle, (
            f"missing {sorted(oracle - actual)[:5]}, "
            f"spurious {sorted(actual - oracle)[:5]}"
        )


# ---------------------------------------------------------------------------
# An interior covering leaf shallower than the grid must appear in
# every grid tile beneath it (the tile_in_leaf arm).
# ---------------------------------------------------------------------------

class TestShallowCoveringArm:
    def test_shallow_interior_leaf_appears_in_every_grid_tile_beneath_it(self, tmp_path):
        """div_shallow's geometry covers its whole z4 tile with margin, so
        stage_covering emits a single interior leaf at z4 -- shallower than
        the z6 grid beneath it. It must appear in all 16 of those z6 tiles."""
        parent_qk = "1230"
        xmin, ymin, xmax, ymax = quadkey_to_bbox(parent_qk)
        mx, my = 0.2 * (xmax - xmin), 0.2 * (ymax - ymin)
        wkt = _rect_wkt(xmin - mx, ymin - my, xmax + mx, ymax + my)

        db_path = tmp_path / "boundaries.duckdb"
        _create_boundaries_db(db_path, [
            ("div_shallow", 50, wkt, ymin - my, xmin - mx, ymax + my, xmax + mx),
        ])

        covering_dir = str(tmp_path / "covering")
        stage_covering(str(db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=6)

        con = duckdb.connect(":memory:")
        parquets = _covering_parquets(covering_dir)
        cov_rows = con.execute(
            "SELECT tile_qk FROM read_parquet(?) "
            "WHERE boundary_id = 'div_shallow' AND left(tile_qk, 4) = ?",
            [parquets, parent_qk],
        ).fetchall()
        assert cov_rows == [(parent_qk,)], (
            f"fixture assumption violated: expected a single interior leaf "
            f"at {parent_qk!r} under its own prefix, got {cov_rows}"
        )

        grid_qks = [f"{parent_qk}{a}{b}" for a in "0123" for b in "0123"]
        ta_path = str(tmp_path / "tile_assignments.parquet")
        _write_tile_assignments(ta_path, [(f"g{i}", qk) for i, qk in enumerate(grid_qks)])

        out_path = str(tmp_path / "tile_references.parquet")
        stage_division_tile_references(covering_dir, ta_path, out_path)

        actual = set(_output_rows(out_path))
        expected = {("div_shallow", qk) for qk in grid_qks}
        missing = expected - actual
        assert missing == set(), (
            f"div_shallow's z4 interior leaf must appear in every z6 grid "
            f"tile beneath it; missing {len(missing)} of {len(grid_qks)}: "
            f"{sorted(missing)[:5]}"
        )


# ---------------------------------------------------------------------------
# Overlap is decided by geometry, not bounding box.
# ---------------------------------------------------------------------------

class TestGeometryNotBbox:
    def test_antimeridian_gap_tile_absent_despite_wraparound_bbox(self, tmp_path):
        """A two-lobe antimeridian division: its flattened extent
        (min_longitude=170, max_longitude=-170) reads as spanning the whole
        globe under a bbox-only test, but the real geometry occupies only
        two thin strips near +-180. The gap tile (lon~0) -- which that
        wraparound bbox touches -- must be absent; both lobe tiles present."""
        ami_wkt = (
            "MULTIPOLYGON("
            "((170 -15, 180 -15, 180 15, 170 15, 170 -15)),"
            "((-180 -15, -170 -15, -170 15, -180 15, -180 -15)))"
        )
        db_path = tmp_path / "boundaries.duckdb"
        _create_boundaries_db(db_path, [
            ("ami_div", 10, ami_wkt, -15.0, 170.0, 15.0, -170.0),
        ])

        covering_dir = str(tmp_path / "covering")
        stage_covering(str(db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=4)

        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        east_qk = con.execute("SELECT ST_QuadKey(?, ?, 4)", [175.0, 0.0]).fetchone()[0]
        west_qk = con.execute("SELECT ST_QuadKey(?, ?, 4)", [-175.0, 0.0]).fetchone()[0]
        gap_qk = con.execute("SELECT ST_QuadKey(?, ?, 4)", [0.0, 0.0]).fetchone()[0]
        assert len({east_qk, west_qk, gap_qk}) == 3, "fixture assumption: 3 distinct z4 tiles"

        ta_path = str(tmp_path / "tile_assignments.parquet")
        _write_tile_assignments(ta_path, [
            ("g_east", east_qk), ("g_west", west_qk), ("g_gap", gap_qk),
        ])

        out_path = str(tmp_path / "tile_references.parquet")
        stage_division_tile_references(covering_dir, ta_path, out_path)

        actual = set(_output_rows(out_path))
        assert ("ami_div", east_qk) in actual, "east lobe tile missing from output"
        assert ("ami_div", west_qk) in actual, "west lobe tile missing from output"
        assert ("ami_div", gap_qk) not in actual, (
            f"gap tile {gap_qk!r} present -- its bbox is touched by the "
            "wraparound extent but its geometry never reaches it"
        )


# ---------------------------------------------------------------------------
# The output is a SET -- no duplicate (place_id, tile_qk) pairs.
# ---------------------------------------------------------------------------

class TestSetValuedOutput:
    def test_no_duplicate_pair_when_division_has_multiple_leaves_under_one_grid_tile(self, tmp_path):
        """div_l is an L-shape covering three of a z4 tile's four z5
        children (missing the fourth). That's >=2 covering leaves collapsing
        under the single z4 grid tile; the output must still hold exactly
        one (div_l, z4-tile) pair."""
        parent_qk = "1230"
        children = {d: quadkey_to_bbox(parent_qk + d) for d in "0123"}
        nw, se, sw = children["0"], children["3"], children["2"]
        w, s = sw[0], sw[1]
        e = se[2]
        n = nw[3]
        midlon = nw[2]
        midlat = sw[3]
        l_wkt = (
            f"POLYGON(({w} {s}, {e} {s}, {e} {midlat}, "
            f"{midlon} {midlat}, {midlon} {n}, {w} {n}, {w} {s}))"
        )

        db_path = tmp_path / "boundaries.duckdb"
        _create_boundaries_db(db_path, [("div_l", 50, l_wkt, s, w, n, e)])

        covering_dir = str(tmp_path / "covering")
        stage_covering(str(db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=5)

        con = duckdb.connect(":memory:")
        parquets = _covering_parquets(covering_dir)
        leaf_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?) WHERE boundary_id = 'div_l'",
            [parquets],
        ).fetchone()[0]
        assert leaf_count >= 2, (
            f"fixture assumption violated: expected >=2 covering leaves for "
            f"div_l under {parent_qk!r}, got {leaf_count}"
        )

        ta_path = str(tmp_path / "tile_assignments.parquet")
        _write_tile_assignments(ta_path, [("grid_marker", parent_qk)])

        out_path = str(tmp_path / "tile_references.parquet")
        stage_division_tile_references(covering_dir, ta_path, out_path)

        matches = [r for r in _output_rows(out_path) if r == ("div_l", parent_qk)]
        assert len(matches) == 1, (
            f"expected exactly one (div_l, {parent_qk!r}) pair despite "
            f"{leaf_count} underlying covering leaves; got {len(matches)}"
        )


# ---------------------------------------------------------------------------
# Schema and sort order.
# ---------------------------------------------------------------------------

class TestSchemaAndSort:
    def test_columns_and_sort_order_tile_qk_then_place_id(self, tmp_path):
        """Ten references across two tiles, inserted in reverse sort order.

        Ten rather than three deliberately: with preserve_insertion_order
        off, an unordered DuckDB result lands in sorted order by chance
        often enough at three rows to let a dropped ORDER BY pass.
        """
        qk_a, qk_b = "31", "02"

        boundaries = []
        for qk in (qk_b, qk_a):
            xmin, ymin, xmax, ymax = quadkey_to_bbox(qk)
            h = 0.25 * (ymax - ymin)
            w = (xmax - xmin) / 6.0
            for i in range(5):
                x0 = xmin + (i + 0.15) * w
                x1 = x0 + 0.7 * w
                y0, y1 = ymin + h, ymax - h
                boundaries.append((
                    f"d{qk}{4 - i}", 50, _rect_wkt(x0, y0, x1, y1), y0, x0, y1, x1,
                ))

        db_path = tmp_path / "boundaries.duckdb"
        _create_boundaries_db(db_path, list(reversed(boundaries)))

        covering_dir = str(tmp_path / "covering")
        stage_covering(str(db_path), covering_dir, cover_min_zoom=2, cover_max_zoom=2)

        ta_path = str(tmp_path / "tile_assignments.parquet")
        _write_tile_assignments(ta_path, [("g1", qk_a), ("g2", qk_b)])

        out_path = str(tmp_path / "tile_references.parquet")
        stage_division_tile_references(covering_dir, ta_path, out_path)

        con = duckdb.connect(":memory:")
        described = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [out_path]).fetchall()
        cols = {row[0]: row[1] for row in described}
        assert list(cols.keys()) == ["place_id", "tile_qk"], f"unexpected columns: {cols}"
        assert cols["place_id"] == "VARCHAR"
        assert cols["tile_qk"] == "VARCHAR"

        expected = {(f"d{qk}{i}", qk) for qk in (qk_a, qk_b) for i in range(5)}
        assert set(_output_rows(out_path)) == expected

        # Read in sort-key order, so `sorted` keys the same way the artifact
        # claims to be sorted. Reading (place_id, tile_qk) would sort
        # place-id-major and pass an unsorted file.
        keyed = con.execute(
            "SELECT tile_qk, place_id FROM read_parquet(?)", [out_path]
        ).fetchall()
        con.close()
        assert keyed == sorted(keyed), f"rows not sorted by (tile_qk, place_id): {keyed}"


# ---------------------------------------------------------------------------
# Freshness -- a second run over unchanged inputs does no work.
# ---------------------------------------------------------------------------

class TestFreshness:
    def test_second_run_over_unchanged_inputs_is_noop(self, tmp_path):
        covering_dir, ta_path = _two_tile_fixture(tmp_path)

        out_path = str(tmp_path / "tile_references.parquet")
        stage_division_tile_references(covering_dir, ta_path, out_path)
        meta_path = out_path + ".meta.json"
        assert os.path.exists(meta_path), "stage did not write a .meta.json sidecar"
        mtime1 = os.path.getmtime(meta_path)

        time.sleep(0.05)
        stage_division_tile_references(covering_dir, ta_path, out_path)
        mtime2 = os.path.getmtime(meta_path)
        assert mtime2 == mtime1, (
            "second run over unchanged inputs must not rebuild "
            "tile_references.parquet (freshness gate not honored)"
        )


# ---------------------------------------------------------------------------
# The tile grid is unchanged by this stage.
# ---------------------------------------------------------------------------

class TestGridUntouched:
    def test_tile_assignments_parquet_unchanged_after_stage_runs(self, tmp_path):
        covering_dir, ta_path = _two_tile_fixture(tmp_path)

        with open(ta_path, "rb") as f:
            bytes_before = f.read()
        mtime_before = os.path.getmtime(ta_path)

        out_path = str(tmp_path / "tile_references.parquet")
        stage_division_tile_references(covering_dir, ta_path, out_path)

        with open(ta_path, "rb") as f:
            bytes_after = f.read()
        mtime_after = os.path.getmtime(ta_path)

        assert bytes_after == bytes_before, "tile_assignments.parquet bytes changed"
        assert mtime_after == mtime_before, "tile_assignments.parquet mtime changed"
