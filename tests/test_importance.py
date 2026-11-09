"""Tests for importance INTEGER schema and tiebreaking behavior."""
import pytest
import duckdb

from tests.quadtree_helpers import run_overture_import


# ---------------------------------------------------------------------------
# Schema type test: importance column must be INTEGER, not DOUBLE
# ---------------------------------------------------------------------------

def test_overture_places_importance_is_integer(overture_parquet, tmp_path):
    """places.importance must be INTEGER type in the Overture places table."""
    db_path = tmp_path / "overture_importance.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")
    run_overture_import(conn, overture_parquet)
    rows = conn.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'places' AND column_name = 'importance'"
    ).fetchall()
    conn.close()
    assert rows, "importance column not found in places"
    data_type = rows[0][0].upper()
    assert data_type == "INTEGER", (
        f"places.importance should be INTEGER, got {data_type}"
    )
