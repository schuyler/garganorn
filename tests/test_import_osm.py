"""Tests for osm_import.sql."""

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    OSM_SF_BBOX, run_osm_import,
)


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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        assert "places" in tables

    def test_places_has_qk17_column(self, osm_parquet, tmp_path):
        """After import, `places` must have a qk17 column."""
        db_path = tmp_path / "test_osm_qk17_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "qk17" in cols, f"qk17 column missing; found columns: {cols}"

    def test_places_has_rkey_column(self, osm_parquet, tmp_path):
        """After import, `places` must have an rkey column (not id)."""
        db_path = tmp_path / "test_osm_rkey_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        missing = required - cols
        assert not missing, f"Missing columns: {missing}"

    def test_bbox_filter_excludes_out_of_range(self, osm_parquet, tmp_path):
        """Node n1004 (lon=-123.5) must be excluded by bbox filter."""
        db_path = tmp_path / "test_osm_bbox.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1004" not in rkeys, "Out-of-bbox node n1004 must be excluded"

    def test_no_name_excluded(self, osm_parquet, tmp_path):
        """Node n1003 (no name tag) must not appear in places."""
        db_path = tmp_path / "test_osm_noname.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1003" not in rkeys, "No-name node n1003 must be excluded"

    def test_surviving_places(self, osm_parquet, tmp_path):
        """Nodes n1001 and n1002 must appear by rkey."""
        db_path = tmp_path / "test_osm_survive.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1001" in rkeys, f"n1001 (Tartine Manufactory) missing; got: {rkeys}"
        assert "n1002" in rkeys, f"n1002 (Dolores Park) missing; got: {rkeys}"

    def test_qk17_populated(self, osm_parquet, tmp_path):
        """All rows must have non-null qk17 values."""
        db_path = tmp_path / "test_osm_qk17_pop.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        nulls = conn.execute("SELECT rkey FROM places WHERE qk17 IS NULL").fetchall()
        conn.close()
        assert not nulls, f"Rows with NULL qk17: {nulls}"

    def test_rkey_format(self, osm_parquet, tmp_path):
        """rkey values for nodes must start with 'n' and match 'n' || osm_id exactly."""
        db_path = tmp_path / "test_osm_rkey_fmt.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "geom" in describe, "geom column not found in places"
        assert describe["geom"] == "GEOMETRY", f"geom column type should be GEOMETRY, got {describe['geom']!r}"

    def test_tags_column_is_map_type(self, osm_parquet, tmp_path):
        """tags column must be MAP(VARCHAR, VARCHAR) type."""
        db_path = tmp_path / "test_tags_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1006" not in rkeys, "Node with name but no quality tag must be excluded by quality filter"

    def test_way_import_survives(self, osm_parquet, tmp_path):
        """A way with a recognized quality tag survives import with rkey 'w' + osm_id."""
        db_path = tmp_path / "test_way_import.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "w2001" in rkeys, (
            "Way w2001 (tourism=attraction) should survive import via centroid computation"
        )

    def test_import_preserves_variant_tags(self, osm_parquet, tmp_path):
        """n1005 has alt_name and name:fr; both must survive in places.tags after import."""
        db_path = tmp_path / "test_osm_variant_tags.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        row = conn.execute(
            "SELECT tags FROM places WHERE rkey = 'n1005'"
        ).fetchone()
        conn.close()
        assert row is not None, "n1005 not found in places"
        tags = dict(row[0])
        assert 'alt_name' in tags, f"alt_name missing from tags: {tags}"
        assert 'name:fr' in tags, f"name:fr missing from tags: {tags}"
