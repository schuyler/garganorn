"""Tests for pending quadtree pipeline SQL fixes (regression guards)."""

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    run_overture_import, run_osm_import,
    make_tile_assignment_db, run_tile_assignments,
)


# ---------------------------------------------------------------------------
# Tests: quadtree pipeline SQL fixes (Red phase — fail against current code)
# ---------------------------------------------------------------------------

class TestQk17PipelineFixes:
    """Red-phase tests for pending fixes to the quadtree pipeline SQL files.

    Fix 1 — overture_import.sql: qk17 should be computed inline in the CTAS
             SELECT list; no ALTER TABLE or UPDATE statements.

    Fix 2 — compute_tile_assignments.sql: place_zoom should be an inline CTE,
             not a CREATE TEMP TABLE statement.

    Fix 3 — compute_tile_assignments.sql: tile_assignments CTAS should have
             ORDER BY tile_qk so the output is sorted.

    Fix 4 — overture_export_tiles.sql: place_addresses TEMP TABLE eliminated;
             addresses rendered inline via list_transform/list_filter in the VIEW.

    OSM Fix — osm_import.sql: qk17 should be in the CREATE TABLE schema and
              computed inline in each INSERT SELECT; no ALTER TABLE ADD COLUMN.

    Every test here FAILS against the current SQL files and PASSES after the
    corresponding fix is applied.
    """

    # ------------------------------------------------------------------
    # Fix 1: overture_import.sql — qk17 inline in CTAS, no ALTER/UPDATE
    # ------------------------------------------------------------------

    def test_fix1_overture_import_no_alter_table(self):
        """overture_import.sql must NOT contain ALTER TABLE after fix.

        Currently the script adds qk17 via ALTER TABLE + UPDATE after the CTAS.
        After the fix, qk17 is computed inline in the CTAS SELECT list.
        FAILS until the ALTER TABLE statement is removed.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_import.sql"
        sql = sql_path.read_text()
        assert "ALTER TABLE" not in sql.upper(), (
            "overture_import.sql still contains ALTER TABLE. "
            "Fix: compute qk17 inline in the CTAS SELECT list and remove "
            "the ALTER TABLE / UPDATE block."
        )

    def test_fix1_overture_import_no_update_qk17(self):
        """overture_import.sql must NOT contain UPDATE places SET qk17 after fix.

        FAILS until the UPDATE statement is removed.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_import.sql"
        sql = sql_path.read_text()
        assert "UPDATE PLACES SET QK17" not in sql.upper(), (
            "overture_import.sql still contains 'UPDATE places SET qk17'. "
            "Fix: compute qk17 inline in the CTAS SELECT list."
        )

    def test_fix1_overture_import_qk17_nonnull(self, overture_parquet, tmp_path):
        """After overture_import.sql runs, all rows must have a non-null qk17.

        This is a green regression guard: it passes both before and after the
        fix because both the current ALTER TABLE + UPDATE approach and the
        post-fix inline CTAS approach produce non-null qk17 values. It ensures
        that the inline approach doesn't accidentally leave qk17 null.
        """
        db_path = tmp_path / "test_fix1_qk17_nonnull.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        assert "qk17" in cols, "qk17 column missing from places after import"
        null_count = conn.execute(
            "SELECT count(*) FROM places WHERE qk17 IS NULL"
        ).fetchone()[0]
        conn.close()
        assert null_count == 0, (
            f"Expected 0 null qk17 values after fix; got {null_count}. "
            "Inline CTAS must compute qk17 for every row."
        )

    # ------------------------------------------------------------------
    # Fix 2: compute_tile_assignments.sql — place_zoom as inline CTE
    # ------------------------------------------------------------------

    def test_fix2_tile_assignments_no_create_temp_place_zoom(self):
        """compute_tile_assignments.sql must NOT contain CREATE TEMP TABLE place_zoom.

        Currently the script materializes place_zoom as a TEMP TABLE.
        After the fix, place_zoom is an inline CTE inside the tile_assignments CTAS.
        FAILS until CREATE TEMP TABLE place_zoom is removed.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "compute_tile_assignments.sql"
        sql = sql_path.read_text()
        assert "CREATE TEMP TABLE place_zoom" not in sql, (
            "compute_tile_assignments.sql still contains 'CREATE TEMP TABLE place_zoom'. "
            "Fix: convert place_zoom to an inline CTE inside the tile_assignments CTAS."
        )

    # ------------------------------------------------------------------
    # Fix 3: compute_tile_assignments.sql — tile_assignments sorted by tile_qk
    # ------------------------------------------------------------------

    def test_fix3_tile_assignments_sql_has_order_by(self):
        """Structural guard: compute_tile_assignments.sql must have ORDER BY tile_qk in the CTAS."""
        sql_path = REPO_ROOT / "garganorn" / "sql" / "compute_tile_assignments.sql"
        sql = sql_path.read_text()
        assert "ORDER BY tile_qk" in sql, (
            "compute_tile_assignments.sql missing ORDER BY tile_qk in tile_assignments CTAS."
        )

    def test_fix3_tile_assignments_ordered_by_tile_qk(self, tmp_path):
        """tile_assignments rows must be sorted by tile_qk after fix.

        The fix adds ORDER BY tile_qk to the tile_assignments CTAS so the
        output is sorted for deterministic downstream processing.
        FAILS until ORDER BY tile_qk is added to the CTAS.
        """
        places = [
            # Choose places in different quadkey regions so ordering is testable.
            ("p_nyc",  40.7128, -74.0060),   # NYC — starts with '0'
            ("p_sf1",  37.7749, -122.4194),   # SF  — starts with '0' but different prefix
            ("p_sf2",  37.7750, -122.4195),
            ("p_lon",  51.5074,  -0.1278),    # London — different prefix
        ]

        db_path = tmp_path / "test_fix3_sorted.duckdb"
        conn = duckdb.connect(str(db_path))
        make_tile_assignment_db(conn, places)
        run_tile_assignments(conn, pk_expr="fsq_place_id", min_zoom=6, max_zoom=17, max_per_tile=10)

        rows = conn.execute("SELECT tile_qk FROM tile_assignments").fetchall()
        conn.close()

        qk_values = [row[0] for row in rows]
        assert qk_values == sorted(qk_values), (
            f"tile_assignments is not sorted by tile_qk. "
            f"Got order: {qk_values}. "
            "Fix: add ORDER BY tile_qk to the tile_assignments CTAS."
        )

    # ------------------------------------------------------------------
    # Fix 4: overture_export_tiles.sql — eliminate place_addresses TEMP TABLE
    # ------------------------------------------------------------------

    def test_no_place_addresses_temp_table(self):
        """overture_export_tiles.sql must NOT define place_addresses as a TEMP TABLE.

        After the fix, place_addresses is eliminated entirely and replaced with
        inline list_transform/list_filter in the VIEW. This test FAILS until the
        TEMP TABLE is removed from the SQL file.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_export_tiles.sql"
        sql = sql_path.read_text()
        assert "CREATE TEMP TABLE place_addresses" not in sql, (
            "overture_export_tiles.sql still defines place_addresses as a TEMP TABLE. "
            "Fix: remove the TEMP TABLE and use inline list_transform/list_filter in the VIEW."
        )

    # ------------------------------------------------------------------
    # OSM Fix: osm_import.sql — qk17 inline, no ALTER TABLE ADD COLUMN
    # ------------------------------------------------------------------

    def test_osm_fix_no_alter_table_add_column_qk17(self):
        """osm_import.sql must NOT contain ALTER TABLE places ADD COLUMN qk17.

        Currently the script adds qk17 via ALTER TABLE after the INSERT statements.
        After the fix, qk17 is in the CREATE TABLE schema and computed inline
        in each INSERT SELECT list.
        FAILS until the ALTER TABLE ADD COLUMN qk17 statement is removed.
        """
        sql_path = REPO_ROOT / "garganorn" / "sql" / "osm_import.sql"
        sql = sql_path.read_text()
        assert "ALTER TABLE places ADD COLUMN qk17" not in sql, (
            "osm_import.sql still contains 'ALTER TABLE places ADD COLUMN qk17'. "
            "Fix: add qk17 to the CREATE TABLE schema and compute it inline "
            "in each INSERT SELECT list."
        )
        assert "UPDATE PLACES SET QK17" not in sql.upper(), (
            "osm_import.sql still contains UPDATE places SET qk17 — inline qk17 in INSERT SELECT lists instead."
        )

    def test_osm_fix_qk17_nonnull(self, tmp_path):
        """After osm_import.sql runs against a minimal synthetic fixture, all qk17 values must be non-null.

        This is a green regression guard: it passes both before and after the fix because
        both the current ALTER TABLE + UPDATE approach and the post-fix inline approach
        produce non-null qk17 values.
        """
        import duckdb as _duckdb

        base = tmp_path / "osm_fix_qk17_nonnull"
        base.mkdir()
        node_path = base / "node_data.parquet"
        way_path = base / "way_data.parquet"

        conn = _duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE tmp_nodes (
                id   BIGINT,
                tags MAP(VARCHAR, VARCHAR),
                lat  DOUBLE,
                lon  DOUBLE
            )
        """)
        conn.execute("""
            INSERT INTO tmp_nodes VALUES
                (1, map(['name','amenity'], ['Test Cafe','cafe']),        37.7612, -122.4195),
                (2, map(['name','leisure'], ['Test Park','park']),        37.7596, -122.4269),
                (3, map(['name','shop'],    ['Test Shop','bakery']),      37.7700, -122.4100)
        """)
        conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")
        conn.execute("""
            CREATE TABLE tmp_ways (
                id   BIGINT,
                tags MAP(VARCHAR, VARCHAR),
                nds  STRUCT(ref BIGINT)[]
            )
        """)
        conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
        conn.close()

        db_path = tmp_path / "test_osm_fix_qk17_nonnull.duckdb"
        conn2 = _duckdb.connect(str(db_path))
        conn2.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn2, str(node_path), str(way_path))
        null_count = conn2.execute(
            "SELECT count(*) FROM places WHERE qk17 IS NULL"
        ).fetchone()[0]
        conn2.close()
        assert null_count == 0, (
            f"Expected 0 null qk17 values after osm_import; got {null_count}. "
            "Both the current UPDATE approach and the post-fix inline approach must produce non-null qk17."
        )
