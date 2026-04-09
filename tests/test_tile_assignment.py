"""Tests for compute_tile_assignments.sql."""

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, run_tile_assignments, make_tile_assignment_db,
)


# ---------------------------------------------------------------------------
# Tests: compute_tile_assignments.sql
# ---------------------------------------------------------------------------

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
        make_tile_assignment_db(conn, places)
        run_tile_assignments(conn, pk_expr="fsq_place_id", min_zoom=6, max_zoom=17, max_per_tile=max_per_tile)

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
        make_tile_assignment_db(conn, places)
        run_tile_assignments(conn, pk_expr="fsq_place_id", min_zoom=6, max_zoom=17, max_per_tile=1)

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
        make_tile_assignment_db(conn, places)
        conn.execute(
            "INSERT INTO places (fsq_place_id, name, latitude, longitude, qk17) "
            "VALUES ('null001', 'Null Place', 37.77, -122.42, NULL)"
        )
        run_tile_assignments(conn, pk_expr='fsq_place_id', max_per_tile=10)

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
