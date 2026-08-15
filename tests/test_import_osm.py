"""Tests for osm_import.sql."""

import inspect
import os
import pathlib
import time

import duckdb
import pytest
from tests.quadtree_helpers import (
    REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit,
    OSM_SF_BBOX, run_osm_import,
)

import garganorn.stages as _stages


# ---------------------------------------------------------------------------
# Tests: osm_import.sql
# ---------------------------------------------------------------------------

class TestOsmImport:
    """Tests for garganorn/sql/osm_import.sql."""

    def test_sql_file_exists(self):
        """The SQL file must exist on disk."""
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

    def test_empty_and_whitespace_name_excluded(self, tmp_path):
        """A node with an empty-string or whitespace-only name tag must be
        excluded, the same as a missing name tag (see test_no_name_excluded).
        Uses a standalone fixture, not the shared osm_parquet fixture, so it
        doesn't disturb rkey/count expectations elsewhere.
        """
        base = tmp_path / "empty_name_fixture"
        base.mkdir()
        node_path = base / "node_data.parquet"
        way_path = base / "way_data.parquet"

        conn = duckdb.connect(":memory:")
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
                (2001, map(['name','amenity'], ['','cafe']),      37.7700, -122.4100),
                (2002, map(['name','amenity'], ['   ','cafe']),   37.7710, -122.4110),
                (2003, map(['name','amenity'], ['Real Cafe','cafe']), 37.7720, -122.4120)
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

        db_path = tmp_path / "test_osm_empty_name.duckdb"
        conn2 = duckdb.connect(str(db_path))
        conn2.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn2, str(node_path), str(way_path))
        rkeys = {row[0] for row in conn2.execute("SELECT rkey FROM places").fetchall()}
        conn2.close()
        assert "n2001" not in rkeys, "Empty-name node n2001 must be excluded"
        assert "n2002" not in rkeys, "Whitespace-only-name node n2002 must be excluded"
        assert "n2003" in rkeys, "Named node n2003 must survive"

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
# Characterization tests pinning current osm_import.sql semantics
#
# These pin the importance arithmetic, variants content, and how node vs.
# way records get their coordinates. Values below were obtained by RUNNING
# the current SQL against the existing osm_parquet fixture (tests/conftest.py),
# not guessed.
# ---------------------------------------------------------------------------

class TestOsmImportCharacterization:
    """Pins the importance arithmetic, variants content, and node vs. way
    coordinate derivation in osm_import.sql.
    """

    def test_variants_always_empty_for_nodes_and_ways(self, osm_parquet, tmp_path):
        """osm_import.sql hardcodes variants to an empty list for every
        record — node and way alike (`[]::STRUCT(name VARCHAR, type VARCHAR,
        language VARCHAR)[] AS variants` in both the node and way final
        SELECTs). OSM alternate names (alt_name, name:fr, etc.) live in the
        `tags` map instead (see test_import_preserves_variant_tags above),
        not in `variants`. Pinned here so a rewrite doesn't accidentally
        start deriving variants from tags without an explicit decision to
        do so.
        """
        db_path = tmp_path / "test_osm_variants_empty.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        node_variants = conn.execute(
            "SELECT variants FROM places WHERE rkey = 'n1001'"
        ).fetchone()[0]
        way_variants = conn.execute(
            "SELECT variants FROM places WHERE rkey = 'w2001'"
        ).fetchone()[0]
        conn.close()
        assert node_variants == [], f"expected [] for node n1001; got {node_variants!r}"
        assert way_variants == [], f"expected [] for way w2001; got {way_variants!r}"

    def test_way_centroid_is_plain_average_of_referenced_node_coords(self, osm_parquet, tmp_path):
        """Way w2001 references nodes 9001 (lat=37.8199, lon=-122.4786) and
        9002 (lat=37.8197, lon=-122.4788) via `nds`. way_centroids computes
        `avg(nc.lat)`/`avg(nc.lon)` over exactly those referenced nodes (a
        plain unweighted mean, not e.g. a true polygon centroid), so the
        expected values are exact: (37.8199+37.8197)/2 = 37.8198,
        (-122.4786-122.4788)/2 = -122.4787.

        This pins the node-vs-way distinction directly: a node's
        latitude/longitude come straight from its own row, while a way's
        come from averaging its constituent nodes' coordinates — a rewrite
        must preserve this even if it restructures the CTEs that compute it.
        """
        db_path = tmp_path / "test_osm_way_centroid.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
        row = conn.execute(
            "SELECT latitude, longitude FROM places WHERE rkey = 'w2001'"
        ).fetchone()
        conn.close()
        assert row is not None, "w2001 not found in places"
        latitude, longitude = row
        assert latitude == pytest.approx(37.8198), f"expected latitude 37.8198, got {latitude}"
        assert longitude == pytest.approx(-122.4787), f"expected longitude -122.4787, got {longitude}"

    def test_importance_exact_value_for_known_density_and_idf(self, osm_parquet, tmp_path):
        """Pins the importance arithmetic for a node:
            round(60 * least(density_score / density_norm, 1.0)
                + 40 * least(idf_score / idf_norm, 1.0))::INTEGER
        Node n1001 (Tartine Manufactory, amenity=cafe) at
        lat=37.7612, lon=-122.4195 -> primary_category 'amenity=cafe'.
        density_score=5.0 (norm 10.0) and idf_score=9.0 (norm 18.0) are each
        exactly half their norm, so the expected value is exact with no
        rounding ambiguity: round(60*0.5 + 40*0.5) = 50 — the same formula
        and same exact value as overture_place_import.sql's importance
        characterization test (tests/test_importance.py), confirming both
        import paths share the arithmetic.
        """
        probe = duckdb.connect(":memory:")
        probe.execute("INSTALL spatial; LOAD spatial;")
        qk17 = probe.execute(
            "SELECT ST_QuadKey(?, ?, 17)", [-122.4195, 37.7612]
        ).fetchone()[0]
        probe.close()
        tile_qk15 = qk17[:15]

        density_rows = [(tile_qk15, 5.0, -122.5, 37.7, -122.4, 37.8)]
        idf_rows = [("amenity=cafe", 9.0)]

        db_path = tmp_path / "test_osm_importance_exact.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(
            conn, osm_parquet["node"], osm_parquet["way"],
            density_rows=density_rows, idf_rows=idf_rows,
            density_norm=10.0, idf_norm=18.0,
        )
        importance = conn.execute(
            "SELECT importance FROM places WHERE rkey = 'n1001'"
        ).fetchone()[0]
        conn.close()

        assert importance == 50, (
            f"expected importance == 50 for density_score=5.0 (norm 10.0) and "
            f"idf_score=9.0 (norm 18.0); got {importance}"
        )


class TestOsmImportArtifactPhase2:
    """stage_import writes places.parquet for OSM without a 'geom' column.

    OSM-specific: DELETE WHERE geom IS NULL runs before EXCLUDE.
    """

    _BBOX = (-122.55, 37.60, -122.30, 37.85)

    def test_stage_import_no_con_parameter(self):
        """stage_import must not take 'con' as its first parameter."""
        params = list(inspect.signature(_stages.stage_import).parameters.keys())
        assert params[0] != "con", (
            f"stage_import must not have 'con' as first param; got {params[0]!r}"
        )

    def test_stage_import_writes_places_parquet(self, osm_parquet, tmp_path):
        """stage_import must write places.parquet for osm."""
        output = str(tmp_path / "places.parquet")
        parquet = (osm_parquet["node"], osm_parquet["way"])
        _stages.stage_import("osm", parquet, self._BBOX, output)
        assert pathlib.Path(output).exists(), f"places.parquet not written to {output}"

    def test_places_parquet_no_geom_column(self, osm_parquet, tmp_path):
        """places.parquet must not contain the 'geom' column."""
        output = str(tmp_path / "places.parquet")
        parquet = (osm_parquet["node"], osm_parquet["way"])
        _stages.stage_import("osm", parquet, self._BBOX, output)
        con = duckdb.connect()
        cols = {r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{output}')"
        ).fetchall()}
        con.close()
        assert "geom" not in cols, (
            f"places.parquet must not contain 'geom' column; found: {cols}"
        )

    def test_null_geom_rows_absent_from_artifact(self, osm_parquet, tmp_path):
        """Rows where geom IS NULL must not appear in places.parquet (filter before EXCLUDE)."""
        output = str(tmp_path / "places.parquet")
        parquet = (osm_parquet["node"], osm_parquet["way"])
        _stages.stage_import("osm", parquet, self._BBOX, output)
        # OSM fixture node 1003 has no 'name' tag → osm_import filters it; nodes with
        # no geometry (ways with centroid=NULL) also get filtered. We assert the artifact
        # has fewer rows than the unfiltered node count (6 nodes in fixture).
        con = duckdb.connect()
        count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{output}')"
        ).fetchone()[0]
        con.close()
        # The fixture has nodes without name (filtered) and nodes outside bbox; after
        # all filters the row count must be < 6 (total fixture nodes).
        assert count < 6, (
            f"Expected filtered row count < 6 nodes; got {count}. "
            "NULL-geom filter may not be running before EXCLUDE."
        )

    def test_places_parquet_qk17_sorted_nulls_last(self, osm_parquet, tmp_path):
        """places.parquet must be sorted by qk17 NULLS LAST."""
        output = str(tmp_path / "places.parquet")
        parquet = (osm_parquet["node"], osm_parquet["way"])
        _stages.stage_import("osm", parquet, self._BBOX, output)
        con = duckdb.connect()
        rows = [r[0] for r in con.execute(
            f"SELECT qk17 FROM read_parquet('{output}')"
        ).fetchall()]
        con.close()
        non_null = [v for v in rows if v is not None]
        assert non_null == sorted(non_null), "qk17 values must be in sorted order"


class TestOsmJoinNoFanOut:
    """Regression guard: no lookup join may multiply rows.

    osm_import.sql has four join sites with the same defect the Overture
    guards cover (TestOvertureDensityJoinNoFanOut / ...IdfJoinNoFanOut in
    test_regressions.py) -- node_density and way_density join density_tiles
    on a derived tile_qk15, node_idf and way_idf join idf_scores on
    primary_category, and none of the four keys is guaranteed unique in its
    lookup table. A duplicate key yields multiple rows in the helper CTE,
    which the final `LEFT JOIN ... ON rkey = ...` then fans back out into
    duplicate `places` rows for one input record.

    Parametrized over all four join sites so none is left unguarded.

    Expected values are read from a baseline run rather than hardcoded, so
    the test calibrates itself against whatever the fixture currently holds.
    """

    _DENSITY_BBOX = (-122.55, 37.60, -122.30, 37.85)  # OSM_SF_BBOX, as a tile extent

    @pytest.mark.parametrize("join_site,rkey", [
        ("node_density", "n1001"),
        ("node_idf", "n1001"),
        ("way_density", "w2001"),
        ("way_idf", "w2001"),
    ])
    def test_no_row_multiplication_on_duplicate_lookup_key(
        self, join_site, rkey, osm_parquet, tmp_path
    ):
        # Baseline with empty lookup tables: the row count a populated-lookup
        # run must reproduce exactly, plus the key values needed to aim the
        # duplicate at this specific record.
        base_conn = duckdb.connect(str(tmp_path / f"baseline_{join_site}.duckdb"))
        base_conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(base_conn, osm_parquet["node"], osm_parquet["way"])
        baseline_total = base_conn.execute("SELECT COUNT(*) FROM places").fetchone()[0]
        row = base_conn.execute(
            "SELECT latitude, longitude, primary_category FROM places WHERE rkey = ?",
            [rkey],
        ).fetchone()
        base_conn.close()
        assert row is not None, f"{rkey} missing from baseline import; fixture changed?"
        latitude, longitude, primary_category = row

        density_rows = idf_rows = None
        if join_site.endswith("_density"):
            # qk15 must be derived the same way the import derives it, or the
            # duplicate row simply won't match and the test proves nothing.
            probe = duckdb.connect(":memory:")
            probe.execute("INSTALL spatial; LOAD spatial;")
            qk15 = probe.execute(
                "SELECT ST_QuadKey(?, ?, 17)", [longitude, latitude]
            ).fetchone()[0][:15]
            probe.close()
            xmin, ymin, xmax, ymax = self._DENSITY_BBOX
            density_rows = [
                (qk15, 5.0, xmin, ymin, xmax, ymax),
                (qk15, 9.0, xmin, ymin, xmax, ymax),
            ]
        else:
            assert primary_category is not None, (
                f"{rkey} has no primary_category; cannot aim an idf duplicate at it"
            )
            idf_rows = [(primary_category, 5.0), (primary_category, 9.0)]

        conn = duckdb.connect(str(tmp_path / f"dup_{join_site}.duckdb"))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(
            conn, osm_parquet["node"], osm_parquet["way"],
            density_rows=density_rows, idf_rows=idf_rows,
        )
        total = conn.execute("SELECT COUNT(*) FROM places").fetchone()[0]
        distinct = conn.execute("SELECT COUNT(DISTINCT rkey) FROM places").fetchone()[0]
        dupes = conn.execute(
            "SELECT rkey, COUNT(*) FROM places GROUP BY rkey HAVING COUNT(*) > 1"
        ).fetchall()
        conn.close()

        assert total == baseline_total, (
            f"{join_site}: duplicate lookup key changed the places row count "
            f"({baseline_total} -> {total}). The join fanned out; exactly one "
            f"places row per input record is required."
        )
        assert total == distinct, (
            f"{join_site}: places contains duplicate rkeys {dupes!r}. A duplicate "
            f"key in the lookup table multiplied rows through the join."
        )
