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


# ---------------------------------------------------------------------------
# places.importance exact-value characterization (overture_place_import.sql)
#
# The tests above only check type (INTEGER) and range/variety; nothing pins
# the actual arithmetic:
#     round(60 * least(density_score / density_norm, 1.0)
#         + 40 * least(idf_score / idf_norm, 1.0))::INTEGER
# so a rewrite that folds the density/idf joins into the final SELECT could
# silently change the computed value without any test noticing. This test
# pins one exact value for known density_score/idf_score inputs and known
# density_norm/idf_norm.
# ---------------------------------------------------------------------------

class TestOvertureImportanceCharacterization:
    """Characterization test pinning an exact `importance` value."""

    def test_importance_exact_value_for_known_density_and_idf(self, tmp_path):
        from tests.quadtree_helpers import run_overture_import, write_minimal_overture_parquet

        base = tmp_path / "importance_fixture"
        base.mkdir()
        ov_path = base / "ov_importance.parquet"

        lon, lat = -122.419, 37.775
        write_minimal_overture_parquet(ov_path, [
            ("impA", lon, lat, "coffee_shop"),
        ])

        probe = duckdb.connect(":memory:")
        probe.execute("INSTALL spatial; LOAD spatial;")
        qk17 = probe.execute("SELECT ST_QuadKey(?, ?, 17)", [lon, lat]).fetchone()[0]
        probe.close()
        tile_qk15 = qk17[:15]

        density_norm = 10.0
        idf_norm = 18.0
        # density_score and idf_score are each exactly half of their norm,
        # so the expected value is exact with no rounding ambiguity:
        #   round(60 * (5.0/10.0) + 40 * (9.0/18.0)) = round(30 + 20) = 50
        density_rows = [(tile_qk15, 5.0, -122.5, 37.7, -122.4, 37.8)]
        idf_rows = [("coffee_shop", 9.0)]

        db_path = tmp_path / "test_importance_exact.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(
            conn, str(base / "*.parquet"),
            density_rows=density_rows, idf_rows=idf_rows,
            density_norm=density_norm, idf_norm=idf_norm,
        )
        importance = conn.execute(
            "SELECT importance FROM places WHERE id = 'impA'"
        ).fetchone()[0]
        conn.close()

        assert importance == 50, (
            f"expected importance == 50 for density_score=5.0 (norm 10.0) and "
            f"idf_score=9.0 (norm 18.0); got {importance}"
        )
