"""Tests for osm_import.sql, osm_importance.sql, and osm_variants.sql."""

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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_importance(conn)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "importance" in cols, f"importance column missing; found: {cols}"

    def test_importance_is_integer(self, osm_parquet, tmp_path):
        """The `importance` column must be INTEGER type."""
        db_path = tmp_path / "test_osm_imp_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
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
        run_osm_import(conn, str(node_path), str(way_path), bbox=global_bbox)
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

    def _run_variants(self, conn):
        """Load and execute osm_variants.sql on `conn`."""
        raw_sql = _load_sql("osm_variants.sql", {})
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

    def test_variants_column_exists(self, osm_parquet, tmp_path):
        """After osm_variants.sql, `places` must have a `variants` column."""
        db_path = tmp_path / "test_osm_var_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn)
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "variants" in cols, f"variants column missing; found: {cols}"

    def test_variants_is_list(self, osm_parquet, tmp_path):
        """All rows must have a variants column that is a list (not NULL)."""
        db_path = tmp_path / "test_osm_var_list.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn)
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn)
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn)
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn)
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        self._run_variants(conn)
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
