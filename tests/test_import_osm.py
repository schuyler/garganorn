"""Tests for osm_import.sql."""

import inspect
import os
import pathlib
import time

import duckdb
import pytest
from tests.quadtree_helpers import REPO_ROOT, run_osm_import

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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        assert "places" in tables

    def test_places_has_qk17_column(self, osm_parquet, tmp_path):
        """After import, `places` must have a qk17 column."""
        db_path = tmp_path / "test_osm_qk17_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "qk17" in cols, f"qk17 column missing; found columns: {cols}"

    def test_places_has_rkey_column(self, osm_parquet, tmp_path):
        """After import, `places` must have an rkey column (not id)."""
        db_path = tmp_path / "test_osm_rkey_col.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        cols = {row[0] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        missing = required - cols
        assert not missing, f"Missing columns: {missing}"

    def test_bbox_filter_excludes_out_of_range(self, osm_parquet, tmp_path):
        """Node n1004 (lon=-123.5) must be excluded by bbox filter."""
        db_path = tmp_path / "test_osm_bbox.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1004" not in rkeys, "Out-of-bbox node n1004 must be excluded"

    def test_no_name_excluded(self, osm_parquet, tmp_path):
        """Node n1003 (no name tag) must not appear in places."""
        db_path = tmp_path / "test_osm_noname.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1003" not in rkeys, "No-name node n1003 must be excluded"

    def test_empty_and_whitespace_name_excluded(self, tmp_path, empty_relation_parquet):
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
        run_osm_import(conn2, str(node_path), str(way_path), empty_relation_parquet)
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1001" in rkeys, f"n1001 (Tartine Manufactory) missing; got: {rkeys}"
        assert "n1002" in rkeys, f"n1002 (Dolores Park) missing; got: {rkeys}"

    def test_qk17_populated(self, osm_parquet, tmp_path):
        """All rows must have non-null qk17 values."""
        db_path = tmp_path / "test_osm_qk17_pop.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        nulls = conn.execute("SELECT rkey FROM places WHERE qk17 IS NULL").fetchall()
        conn.close()
        assert not nulls, f"Rows with NULL qk17: {nulls}"

    def test_rkey_format(self, osm_parquet, tmp_path):
        """rkey values for nodes must start with 'n' and match 'n' || osm_id exactly."""
        db_path = tmp_path / "test_osm_rkey_fmt.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        describe = {row[0]: row[1] for row in conn.execute("DESCRIBE places").fetchall()}
        conn.close()
        assert "geom" in describe, "geom column not found in places"
        assert describe["geom"] == "GEOMETRY", f"geom column type should be GEOMETRY, got {describe['geom']!r}"

    def test_tags_column_is_map_type(self, osm_parquet, tmp_path):
        """tags column must be MAP(VARCHAR, VARCHAR) type."""
        db_path = tmp_path / "test_tags_type.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
        conn.close()
        assert "n1006" not in rkeys, "Node with name but no quality tag must be excluded by quality filter"

    def test_way_import_survives(self, osm_parquet, tmp_path):
        """A way with a recognized quality tag survives import with rkey 'w' + osm_id."""
        db_path = tmp_path / "test_way_import.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
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

    def test_variants_empty_when_no_variant_tags_present(self, osm_parquet, tmp_path):
        """n1001 and w2001 carry no name-variant tags, so `variants` is empty
        for both — node and way alike. See tests/test_variants.py's
        TestOsmVariantsCharacterization for records that do carry variant
        tags and the type/language mapping those derive.
        """
        db_path = tmp_path / "test_osm_variants_empty.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
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
            conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"],
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
        parquet = (osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        _stages.stage_import("osm", parquet, self._BBOX, output)
        assert pathlib.Path(output).exists(), f"places.parquet not written to {output}"

    def test_places_parquet_no_geom_column(self, osm_parquet, tmp_path):
        """places.parquet must not contain the 'geom' column."""
        output = str(tmp_path / "places.parquet")
        parquet = (osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
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
        """Rows where geom IS NULL must not appear in places.parquet.

        Neither import path can currently produce a NULL-geom row to test
        against directly: nodes require non-NULL lat/lon before `filtered`'s
        SELECT, and way centroids are excluded by `HAVING avg(...) IS NOT
        NULL` before reaching way_base. So this pins the DELETE's presence
        in the SQL source (the only thing standing between a future
        regression in either of those upstream guards and a NULL-geom row
        reaching the artifact), plus the artifact's actual exclusions by
        name/bbox/quality-tag filters, asserted by rkey rather than by a
        total-count comparison that passes regardless of which filter ran.
        """
        osm_import_sql = (REPO_ROOT / "garganorn" / "sql" / "osm_import.sql").read_text()
        assert "DELETE FROM places WHERE geom IS NULL" in osm_import_sql, (
            "osm_import.sql must delete NULL-geom rows before the parquet artifact is written"
        )

        output = str(tmp_path / "places.parquet")
        parquet = (osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        _stages.stage_import("osm", parquet, self._BBOX, output)
        con = duckdb.connect()
        output_rkeys = {r[0] for r in con.execute(
            f"SELECT rkey FROM read_parquet('{output}')"
        ).fetchall()}
        con.close()
        for excluded, reason in [
            ("n1003", "no name tag"),
            ("n1004", "out of bbox"),
            ("n1006", "no quality tag"),
            ("n9001", "no name (way-centroid input only)"),
            ("n9002", "no name (way-centroid input only)"),
        ]:
            assert excluded not in output_rkeys, f"{excluded} should be excluded ({reason})"

    def test_places_parquet_qk17_sorted_nulls_last(self, osm_parquet, tmp_path):
        """places.parquet must be sorted by qk17 NULLS LAST."""
        output = str(tmp_path / "places.parquet")
        parquet = (osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
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

    osm_import.sql has six join sites with the same defect the Overture
    guards cover (TestOvertureDensityJoinNoFanOut / ...IdfJoinNoFanOut in
    test_regressions.py) -- node_density and way_density join density_tiles
    on a derived tile_qk15, node_idf and way_idf join idf_scores on
    primary_category, and the relation arm carries the same pair (`rd`/`ri`
    in osm_import.sql); none of the six keys is guaranteed unique in its
    lookup table. A duplicate key yields multiple rows in the helper CTE,
    which the final `LEFT JOIN ... ON rkey = ...` then fans back out into
    duplicate `places` rows for one input record.

    Parametrized over all six join sites: node, way, and relation, each with
    a density and an idf case.

    Expected values are read from a baseline run rather than hardcoded, so
    the test calibrates itself against whatever the fixture currently holds.
    """

    _DENSITY_BBOX = (-122.55, 37.60, -122.30, 37.85)  # OSM_SF_BBOX, as a tile extent

    @pytest.mark.parametrize("join_site,rkey,parquet_fixture", [
        ("node_density", "n1001", "osm_parquet"),
        ("node_idf", "n1001", "osm_parquet"),
        ("way_density", "w2001", "osm_parquet"),
        ("way_idf", "w2001", "osm_parquet"),
        # osm_parquet's relation partition is deliberately empty, so the
        # relation cases are aimed at relation_parquet instead.
        ("relation_density", "r40001", "relation_parquet"),
        ("relation_idf", "r40001", "relation_parquet"),
    ])
    def test_no_row_multiplication_on_duplicate_lookup_key(
        self, join_site, rkey, parquet_fixture, request, tmp_path
    ):
        parquet = request.getfixturevalue(parquet_fixture)

        # Baseline with empty lookup tables: the row count a populated-lookup
        # run must reproduce exactly, plus the key values needed to aim the
        # duplicate at this specific record.
        base_conn = duckdb.connect(str(tmp_path / f"baseline_{join_site}.duckdb"))
        base_conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(base_conn, parquet["node"], parquet["way"], parquet["relation"])
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
            conn, parquet["node"], parquet["way"], parquet["relation"],
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


# ---------------------------------------------------------------------------
# Tests: which OSM tags the whitelist accepts. The invariants these pin are
# recorded in docs/design-constraints.md.
# ---------------------------------------------------------------------------

# Key/value pairs the whitelist accepts beyond the fourteen keys the CASE
# opened with, one (key, value) pair per row.
WHITELISTED_KEY_VALUE_PAIRS = [
    ("place", "locality"), ("place", "isolated_dwelling"), ("place", "farm"),
    ("place", "islet"), ("place", "city_block"),
    ("natural", "water"), ("natural", "wood"), ("natural", "wetland"),
    ("natural", "cliff"), ("natural", "strait"), ("natural", "peninsula"),
    ("man_made", "bridge"), ("man_made", "wastewater_plant"),
    ("man_made", "water_works"), ("man_made", "pumping_station"),
    ("man_made", "adit"), ("man_made", "mineshaft"),
    ("historic", "wayside_shrine"), ("historic", "tomb"),
    ("historic", "citywalls"), ("historic", "monastery"),
    ("historic", "battlefield"), ("historic", "aircraft"),
    ("aeroway", "helipad"),
    ("railway", "yard"),
    ("leisure", "bird_hide"),
    ("landuse", "cemetery"), ("landuse", "industrial"), ("landuse", "quarry"),
    ("landuse", "allotments"), ("landuse", "military"),
    ("landuse", "winter_sports"),
    ("waterway", "dam"), ("waterway", "waterfall"),
    ("power", "plant"), ("power", "substation"),
    ("boundary", "national_park"), ("boundary", "protected_area"),
    ("boundary", "aboriginal_lands"),
    ("highway", "services"), ("highway", "rest_area"), ("highway", "trailhead"),
    ("barrier", "toll_booth"),
    ("emergency", "ambulance_station"),
    ("telecom", "data_center"),
]

# The fourteen keys _osm_category_case.sql recognizes today, one accepted
# value per key -- pins "no existing category loses records".
EXISTING_WHITELIST_KEY_VALUES = [
    ("amenity", "cafe"), ("shop", "bakery"), ("tourism", "attraction"),
    ("leisure", "park"), ("office", "coworking"), ("craft", "carpenter"),
    ("healthcare", "clinic"), ("historic", "castle"), ("natural", "peak"),
    ("man_made", "lighthouse"), ("aeroway", "aerodrome"),
    ("railway", "station"), ("public_transport", "station"),
    ("place", "city"),
]

# The building subtractive rule.
BUILDING_INCLUDE_VALUES = ["commercial", "yes", "apartments"]
BUILDING_EXCLUDE_VALUES = [
    "house", "detached", "semidetached_house", "terrace",
    "garage", "garages", "shed", "hut", "barn", "greenhouse",
    "static_caravan", "roof", "no", "construction", "ruins",
]


@pytest.fixture(scope="module")
def whitelisted_keys_parquet(tmp_path_factory, empty_relation_parquet):
    """One named node per WHITELISTED_KEY_VALUE_PAIRS entry; an empty way parquet
    (this fixture only exercises the node arm, which shares its whitelist
    predicate with the way arm apart from the building branch).
    """
    base = tmp_path_factory.mktemp("whitelisted_keys_parquet")
    node_path = base / "node_data.parquet"
    way_path = base / "way_data.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE tmp_nodes (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            lat     DOUBLE,
            lon     DOUBLE
        )
    """)
    ids = {}
    for i, (key, value) in enumerate(WHITELISTED_KEY_VALUE_PAIRS):
        node_id = 5000 + i
        ids[f"{key}={value}"] = node_id
        conn.execute(
            f"INSERT INTO tmp_nodes VALUES "
            f"({node_id}, map(['name', '{key}'], ['Test {key}={value}', '{value}']), "
            f"37.7800, -122.4200)"
        )
    conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")
    conn.execute("""
        CREATE TABLE tmp_ways (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            nds     STRUCT(ref BIGINT)[]
        )
    """)
    conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
    conn.close()

    return {"node": str(node_path), "way": str(way_path), "relation": empty_relation_parquet, "ids": ids}


@pytest.fixture(scope="module")
def whitelisted_keys_rkeys(whitelisted_keys_parquet):
    """Run the import once for all of WHITELISTED_KEY_VALUE_PAIRS and return the
    surviving rkeys, so the parametrized test below doesn't re-run the
    import per case.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    run_osm_import(
        conn, whitelisted_keys_parquet["node"], whitelisted_keys_parquet["way"],
        whitelisted_keys_parquet["relation"],
    )
    rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
    conn.close()
    return rkeys


@pytest.fixture(scope="module")
def existing_keys_parquet(tmp_path_factory, empty_relation_parquet):
    """One named node per EXISTING_WHITELIST_KEY_VALUES entry."""
    base = tmp_path_factory.mktemp("existing_keys_parquet")
    node_path = base / "node_data.parquet"
    way_path = base / "way_data.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE tmp_nodes (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            lat     DOUBLE,
            lon     DOUBLE
        )
    """)
    ids = {}
    for i, (key, value) in enumerate(EXISTING_WHITELIST_KEY_VALUES):
        node_id = 4000 + i
        ids[key] = node_id
        conn.execute(
            f"INSERT INTO tmp_nodes VALUES "
            f"({node_id}, map(['name', '{key}'], ['Existing {key}', '{value}']), "
            f"37.7800, -122.4200)"
        )
    conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")
    conn.execute("""
        CREATE TABLE tmp_ways (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            nds     STRUCT(ref BIGINT)[]
        )
    """)
    conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
    conn.close()

    return {"node": str(node_path), "way": str(way_path), "relation": empty_relation_parquet, "ids": ids}


@pytest.fixture(scope="module")
def existing_keys_categories(existing_keys_parquet):
    """Run the import once and return {rkey: primary_category}."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    run_osm_import(
        conn, existing_keys_parquet["node"], existing_keys_parquet["way"],
        existing_keys_parquet["relation"],
    )
    rows = conn.execute("SELECT rkey, primary_category FROM places").fetchall()
    conn.close()
    return {rkey: category for rkey, category in rows}


@pytest.fixture(scope="module")
def building_parquet(tmp_path_factory, empty_relation_parquet):
    """One named way per BUILDING_INCLUDE_VALUES/BUILDING_EXCLUDE_VALUES
    entry (the subtractive building rule), all referencing the same
    two nodes so every way's centroid resolves.
    """
    base = tmp_path_factory.mktemp("building_parquet")
    node_path = base / "node_data.parquet"
    way_path = base / "way_data.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE tmp_nodes (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            lat     DOUBLE,
            lon     DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO tmp_nodes VALUES
            (9101, map([]::VARCHAR[], []::VARCHAR[]), 37.7800, -122.4200),
            (9102, map([]::VARCHAR[], []::VARCHAR[]), 37.7801, -122.4201)
    """)
    conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")
    conn.execute("""
        CREATE TABLE tmp_ways (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            nds     STRUCT(ref BIGINT)[]
        )
    """)
    ids = {}
    way_id = 6000
    for value in BUILDING_INCLUDE_VALUES + BUILDING_EXCLUDE_VALUES:
        ids[value] = way_id
        conn.execute(
            f"INSERT INTO tmp_ways VALUES "
            f"({way_id}, map(['name', 'building'], ['Test building={value}', '{value}']), "
            f"[{{'ref': 9101}}, {{'ref': 9102}}]::STRUCT(ref BIGINT)[])"
        )
        way_id += 1
    conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
    conn.close()

    return {"node": str(node_path), "way": str(way_path), "relation": empty_relation_parquet, "ids": ids}


@pytest.fixture(scope="module")
def building_rkeys(building_parquet):
    """Run the import once and return the surviving rkeys."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    run_osm_import(
        conn, building_parquet["node"], building_parquet["way"],
        building_parquet["relation"],
    )
    rkeys = {row[0] for row in conn.execute("SELECT rkey FROM places").fetchall()}
    conn.close()
    return rkeys


class TestOsmImportKeyWhitelist:
    """Values under the existing fourteen keys, and values under the eight
    keys outside them.

    Table-driven over WHITELISTED_KEY_VALUE_PAIRS -- one named node per pair, and
    each must resolve to a places record. Also covers the acceptance
    spot-list (cemetery, waterfall, dam, locality, power plant) as a
    subset of the same table.
    """

    @pytest.mark.parametrize("key,value", WHITELISTED_KEY_VALUE_PAIRS)
    def test_whitelisted_key_value_resolves(self, key, value, whitelisted_keys_parquet, whitelisted_keys_rkeys):
        expected = f"n{whitelisted_keys_parquet['ids'][f'{key}={value}']}"
        assert expected in whitelisted_keys_rkeys, (
            f"{key}={value} (rkey {expected}) should resolve to a places record"
        )


class TestOsmImportBuildingWhitelist:
    """The building subtractive rule, and its category precedence."""

    @pytest.mark.parametrize("value", BUILDING_INCLUDE_VALUES)
    def test_building_value_resolves(self, value, building_parquet, building_rkeys):
        """building=commercial (the Seagram Building acceptance case),
        building=yes, and building=apartments must all resolve.
        """
        expected = f"w{building_parquet['ids'][value]}"
        assert expected in building_rkeys, (
            f"building={value} (rkey {expected}) should resolve to a places record"
        )

    @pytest.mark.parametrize("value", BUILDING_EXCLUDE_VALUES)
    def test_building_drop_list_value_excluded(self, value, building_parquet, building_rkeys):
        """building=house and the rest of the drop list must not resolve."""
        excluded = f"w{building_parquet['ids'][value]}"
        assert excluded not in building_rkeys, (
            f"building={value} (rkey {excluded}) must stay excluded"
        )

    def test_building_yields_to_amenity_precedence(self, tmp_path, empty_relation_parquet):
        """A way tagged building=yes *and* amenity=restaurant, with a name,
        must resolve as amenity=restaurant, not a building category --
        building is appended last in _osm_category_case.sql's CASE,
        so any key already in the CASE wins over it.
        """
        base = tmp_path / "precedence_fixture"
        base.mkdir()
        node_path = base / "node_data.parquet"
        way_path = base / "way_data.parquet"

        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE tmp_nodes (
                id      BIGINT,
                tags    MAP(VARCHAR, VARCHAR),
                lat     DOUBLE,
                lon     DOUBLE
            )
        """)
        conn.execute("""
            INSERT INTO tmp_nodes VALUES
                (9201, map([]::VARCHAR[], []::VARCHAR[]), 37.7800, -122.4200),
                (9202, map([]::VARCHAR[], []::VARCHAR[]), 37.7801, -122.4201)
        """)
        conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")
        conn.execute("""
            CREATE TABLE tmp_ways (
                id      BIGINT,
                tags    MAP(VARCHAR, VARCHAR),
                nds     STRUCT(ref BIGINT)[]
            )
        """)
        conn.execute("""
            INSERT INTO tmp_ways VALUES (
                7001,
                map(['name', 'building', 'amenity'],
                    ['Precedence Restaurant', 'yes', 'restaurant']),
                [{'ref': 9201}, {'ref': 9202}]::STRUCT(ref BIGINT)[]
            )
        """)
        conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
        conn.close()

        db_path = tmp_path / "test_precedence.duckdb"
        conn2 = duckdb.connect(str(db_path))
        conn2.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn2, str(node_path), str(way_path), empty_relation_parquet)
        row = conn2.execute(
            "SELECT primary_category FROM places WHERE rkey = 'w7001'"
        ).fetchone()
        conn2.close()
        assert row is not None, "w7001 not found in places"
        assert row[0] == "amenity=restaurant", (
            f"expected primary_category 'amenity=restaurant' for "
            f"building=yes+amenity=restaurant; got {row[0]!r}"
        )

    def test_building_excluded_from_node_arm(self, tmp_path, empty_relation_parquet):
        """A NODE tagged building=yes with a name and no other whitelisted
        tag must produce no record -- the building branch is present in the
        way and relation arms of the whitelist, but absent from the node arm.
        """
        base = tmp_path / "node_building_fixture"
        base.mkdir()
        node_path = base / "node_data.parquet"
        way_path = base / "way_data.parquet"

        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE tmp_nodes (
                id      BIGINT,
                tags    MAP(VARCHAR, VARCHAR),
                lat     DOUBLE,
                lon     DOUBLE
            )
        """)
        conn.execute("""
            INSERT INTO tmp_nodes VALUES (
                8001,
                map(['name', 'building'], ['Node Building', 'yes']),
                37.7800, -122.4200
            )
        """)
        conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")
        conn.execute("""
            CREATE TABLE tmp_ways (
                id      BIGINT,
                tags    MAP(VARCHAR, VARCHAR),
                nds     STRUCT(ref BIGINT)[]
            )
        """)
        conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
        conn.close()

        db_path = tmp_path / "test_node_building.duckdb"
        conn2 = duckdb.connect(str(db_path))
        conn2.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn2, str(node_path), str(way_path), empty_relation_parquet)
        rkeys = {row[0] for row in conn2.execute("SELECT rkey FROM places").fetchall()}
        conn2.close()
        assert "n8001" not in rkeys, (
            f"node-tagged building=yes must not resolve; got {rkeys}"
        )


class TestOsmImportSettledExclusions:
    """Requirement: settled exclusions (transit stops, leisure=pitch) stay
    out even as sibling keys/values are added to the whitelist.

    Each case is asserted for both element types: the node arm (`filtered`)
    and the way arm (`qualifying_ways`) carry the same whitelist predicate,
    now written out three times in `osm_import.sql` (node, way, and
    `qualifying_relations`), so a node-only fixture leaves the way arm
    unexercised. The relation arm is not exercised by this test.
    """

    @pytest.mark.parametrize("key,value", [
        ("leisure", "pitch"),
        ("highway", "bus_stop"),
        ("public_transport", "platform"),
        ("railway", "platform"),
    ])
    def test_settled_exclusion_stays_out(self, key, value, tmp_path, empty_relation_parquet):
        base = tmp_path / f"exclusion_{key}_{value}"
        base.mkdir()
        node_path = base / "node_data.parquet"
        way_path = base / "way_data.parquet"

        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE tmp_nodes (
                id      BIGINT,
                tags    MAP(VARCHAR, VARCHAR),
                lat     DOUBLE,
                lon     DOUBLE
            )
        """)
        conn.execute(
            f"INSERT INTO tmp_nodes VALUES "
            f"(8500, map(['name', '{key}'], ['Excluded Place', '{value}']), "
            f"37.7800, -122.4200), "
            f"(8600, map([]::VARCHAR[], []::VARCHAR[]), 37.7800, -122.4200), "
            f"(8601, map([]::VARCHAR[], []::VARCHAR[]), 37.7801, -122.4201)"
        )
        conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")
        conn.execute("""
            CREATE TABLE tmp_ways (
                id      BIGINT,
                tags    MAP(VARCHAR, VARCHAR),
                nds     STRUCT(ref BIGINT)[]
            )
        """)
        conn.execute(
            f"INSERT INTO tmp_ways VALUES "
            f"(8501, map(['name', '{key}'], ['Excluded Way', '{value}']), "
            f"[{{'ref': 8600}}, {{'ref': 8601}}]::STRUCT(ref BIGINT)[])"
        )
        conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
        conn.close()

        db_path = tmp_path / f"test_exclusion_{key}_{value}.duckdb"
        conn2 = duckdb.connect(str(db_path))
        conn2.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn2, str(node_path), str(way_path), empty_relation_parquet)
        rkeys = {row[0] for row in conn2.execute("SELECT rkey FROM places").fetchall()}
        conn2.close()
        assert "n8500" not in rkeys, f"{key}={value} must stay excluded (node); got {rkeys}"
        assert "w8501" not in rkeys, f"{key}={value} must stay excluded (way); got {rkeys}"


class TestOsmImportExistingCategoriesInvariant:
    """The 'no existing category loses records' invariant:
    appending keys after the existing fourteen in _osm_category_case.sql's
    CASE must not change any existing category's primary_category.
    """

    @pytest.mark.parametrize("key,value", EXISTING_WHITELIST_KEY_VALUES)
    def test_existing_key_keeps_primary_category(
        self, key, value, existing_keys_parquet, existing_keys_categories
    ):
        rkey = f"n{existing_keys_parquet['ids'][key]}"
        expected = f"{key}={value}"
        assert existing_keys_categories.get(rkey) == expected, (
            f"{rkey} ({key}={value}) primary_category changed; "
            f"expected {expected!r}, got {existing_keys_categories.get(rkey)!r}"
        )


# ---------------------------------------------------------------------------
# Tests: the relation branch of osm_import.sql -- multipolygon import,
# member-linked and name+location suppression, and nested-relation members.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def relation_parquet(tmp_path_factory):
    """Nodes, ways, and relations for the relation-import tests: a
    multipolygon relation with no incumbent; node- and way-member
    suppression, each paired with a same-member different-name control
    that must survive (isolating the name-equality clause from bare
    membership); name+location suppression of a non-member, paired with
    a same-extent different-name control; a same-name relation outside
    its extent that must survive; a relation reachable only through a
    nested relation member, paired with a partially-resolvable control;
    a building drop-list value paired with an include-list control; and
    a relation carrying alt_name for variants derivation.
    """
    base = tmp_path_factory.mktemp("relation_parquet")
    node_path = base / "node_data.parquet"
    way_path = base / "way_data.parquet"
    relation_path = base / "relation_data.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE tmp_nodes (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            lat     DOUBLE,
            lon     DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO tmp_nodes VALUES
            -- A: multipolygon ring nodes (member-only, no tags)
            (40201, map([]::VARCHAR[], []::VARCHAR[]), 37.7900, -122.4110),
            (40202, map([]::VARCHAR[], []::VARCHAR[]), 37.7900, -122.4090),
            (40203, map([]::VARCHAR[], []::VARCHAR[]), 37.7920, -122.4090),
            (40204, map([]::VARCHAR[], []::VARCHAR[]), 37.7920, -122.4110),
            -- B: existing node place, target of node-member suppression
            (51001, map(['name', 'leisure'], ['Redwood Grove', 'park']), 37.8000, -122.4300),
            -- C: nodes for the existing way place (member-only)
            (52101, map([]::VARCHAR[], []::VARCHAR[]), 37.7950, -122.4250),
            (52102, map([]::VARCHAR[], []::VARCHAR[]), 37.7952, -122.4252),
            -- D: existing node place NOT referenced as a member
            (53001, map(['name', 'tourism'], ['Sunset Overlook', 'attraction']), 37.8100, -122.4400),
            (53201, map([]::VARCHAR[], []::VARCHAR[]), 37.8080, -122.4420),
            (53202, map([]::VARCHAR[], []::VARCHAR[]), 37.8120, -122.4380),
            -- E: existing node place outside the relation's extent
            (54001, map(['name', 'tourism'], ['Hilltop Vista', 'attraction']), 37.8400, -122.3200),
            (54201, map([]::VARCHAR[], []::VARCHAR[]), 37.7900, -122.4600),
            (54202, map([]::VARCHAR[], []::VARCHAR[]), 37.7920, -122.4580),
            -- F: nested-relation, member-only nodes for the resolvable control
            (55201, map([]::VARCHAR[], []::VARCHAR[]), 37.8200, -122.4500),
            (55202, map([]::VARCHAR[], []::VARCHAR[]), 37.8210, -122.4510),
            -- G: building drop-list, member-only nodes
            (56201, map([]::VARCHAR[], []::VARCHAR[]), 37.8300, -122.4600),
            (56202, map([]::VARCHAR[], []::VARCHAR[]), 37.8302, -122.4602),
            -- H: variants, member-only nodes
            (57201, map([]::VARCHAR[], []::VARCHAR[]), 37.8400, -122.4700),
            (57202, map([]::VARCHAR[], []::VARCHAR[]), 37.8402, -122.4702)
    """)
    conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")

    conn.execute("""
        CREATE TABLE tmp_ways (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            nds     STRUCT(ref BIGINT)[]
        )
    """)
    conn.execute("""
        INSERT INTO tmp_ways VALUES
            (40101, map([]::VARCHAR[], []::VARCHAR[]),
             [{'ref': 40201}, {'ref': 40202}, {'ref': 40203}, {'ref': 40204}]::STRUCT(ref BIGINT)[]),
            (52001, map(['name', 'shop'], ['Market Row', 'mall']),
             [{'ref': 52101}, {'ref': 52102}]::STRUCT(ref BIGINT)[]),
            (53101, map([]::VARCHAR[], []::VARCHAR[]),
             [{'ref': 53201}, {'ref': 53202}]::STRUCT(ref BIGINT)[]),
            (54101, map([]::VARCHAR[], []::VARCHAR[]),
             [{'ref': 54201}, {'ref': 54202}]::STRUCT(ref BIGINT)[]),
            (55101, map([]::VARCHAR[], []::VARCHAR[]),
             [{'ref': 55201}, {'ref': 55202}]::STRUCT(ref BIGINT)[]),
            (56101, map([]::VARCHAR[], []::VARCHAR[]),
             [{'ref': 56201}, {'ref': 56202}]::STRUCT(ref BIGINT)[]),
            (57101, map([]::VARCHAR[], []::VARCHAR[]),
             [{'ref': 57201}, {'ref': 57202}]::STRUCT(ref BIGINT)[])
    """)
    conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")

    conn.execute("""
        CREATE TABLE tmp_relations (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            members STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]
        )
    """)
    conn.execute("""
        INSERT INTO tmp_relations VALUES
            -- A: multipolygon park, no incumbent
            (40001, map(['name', 'leisure'], ['Big City Park', 'park']),
             [{'type': 'way', 'ref': 40101, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            -- B: node-member suppression + same-member different-name control
            (51002, map(['name', 'leisure'], ['Redwood Grove', 'park']),
             [{'type': 'node', 'ref': 51001, 'role': 'label'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            (51003, map(['name', 'leisure'], ['Redwood Grove Annex', 'park']),
             [{'type': 'node', 'ref': 51001, 'role': 'label'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            -- C: way-member suppression + same-member different-name control
            (52002, map(['name', 'leisure'], ['Market Row', 'park']),
             [{'type': 'way', 'ref': 52001, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            (52003, map(['name', 'leisure'], ['Market Row Extension', 'park']),
             [{'type': 'way', 'ref': 52001, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            -- D: name+location suppression of a non-member + control
            (53002, map(['name', 'leisure'], ['Sunset Overlook', 'park']),
             [{'type': 'way', 'ref': 53101, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            (53003, map(['name', 'leisure'], ['Sunset Overlook North', 'park']),
             [{'type': 'way', 'ref': 53101, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            -- E: same name, outside the extent -- must NOT be suppressed
            (54002, map(['name', 'leisure'], ['Hilltop Vista', 'park']),
             [{'type': 'way', 'ref': 54101, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            -- F: reachable only via a relation member (55002 has no row
            -- in this table) imports nothing; the control has the same
            -- relation member plus one resolvable way member
            (55001, map(['name', 'boundary'], ['Region Super', 'protected_area']),
             [{'type': 'relation', 'ref': 55002, 'role': 'subarea'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            (55003, map(['name', 'boundary'], ['Region Super Real', 'protected_area']),
             [{'type': 'relation', 'ref': 55002, 'role': 'subarea'},
              {'type': 'way', 'ref': 55101, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            -- G: building drop-list value excluded, vs. an include-list control
            (56001, map(['name', 'building'], ['Storage Complex', 'shed']),
             [{'type': 'way', 'ref': 56101, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            (56002, map(['name', 'building'], ['Storage Complex Commercial', 'commercial']),
             [{'type': 'way', 'ref': 56101, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[]),
            -- H: variants populated from a relation name tag
            (57001, map(['name', 'alt_name', 'leisure'], ['Grand Central Park', 'GCP', 'park']),
             [{'type': 'way', 'ref': 57101, 'role': 'outer'}]::STRUCT(type VARCHAR, ref BIGINT, role VARCHAR)[])
    """)
    conn.execute(f"COPY tmp_relations TO '{relation_path}' (FORMAT PARQUET)")
    conn.close()

    return {"node": str(node_path), "way": str(way_path), "relation": str(relation_path)}


@pytest.fixture(scope="module")
def relation_import_rows(relation_parquet):
    """Run the import once against relation_parquet; return {rkey: row dict}."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    run_osm_import(
        conn, relation_parquet["node"], relation_parquet["way"], relation_parquet["relation"]
    )
    cols = ["rkey", "osm_type", "name", "latitude", "longitude", "primary_category", "variants"]
    rows = conn.execute(f"SELECT {', '.join(cols)} FROM places").fetchall()
    conn.close()
    return {row[0]: dict(zip(cols, row)) for row in rows}


class TestOsmRelationImport:
    """Tests for the relation branch of osm_import.sql: multipolygon
    import, member-linked and name+location suppression."""

    def test_multipolygon_relation_imports_with_expected_fields(self, relation_import_rows):
        """A named multipolygon relation with no incumbent imports as
        'r' + osm_id, with a representative point inside its member extent
        -- the flat average of the ring way's four node coords, the same
        estimator the way pipeline uses for its centroid.
        """
        row = relation_import_rows.get("r40001")
        assert row is not None, "r40001 (Big City Park) missing from places"
        assert row["osm_type"] == "r"
        assert row["name"] == "Big City Park"
        assert row["primary_category"] == "leisure=park"
        assert row["latitude"] == pytest.approx(37.7910)
        assert row["longitude"] == pytest.approx(-122.4100)
        # Representative point within the member extent (ring nodes span
        # lat [37.7900, 37.7920], lon [-122.4110, -122.4090]).
        assert 37.7900 <= row["latitude"] <= 37.7920
        assert -122.4110 <= row["longitude"] <= -122.4090

    def test_node_member_suppression_requires_name_match(self, relation_import_rows):
        """A relation sharing a name with a node member it links to is
        suppressed; a relation linking to the same node member under a
        different name is not -- membership alone is not identity.
        """
        assert "n51001" in relation_import_rows, "incumbent node n51001 missing"
        assert "r51002" not in relation_import_rows, (
            "r51002 (same name as its node member) must be suppressed"
        )
        assert "r51003" in relation_import_rows, (
            "r51003 (same node member, different name) must NOT be suppressed"
        )

    def test_way_member_suppression_requires_name_match(self, relation_import_rows):
        """Mirrors the node-member case for a way member."""
        assert "w52001" in relation_import_rows, "incumbent way w52001 missing"
        assert "r52002" not in relation_import_rows, (
            "r52002 (same name as its way member) must be suppressed"
        )
        assert "r52003" in relation_import_rows, (
            "r52003 (same way member, different name) must NOT be suppressed"
        )

    def test_name_location_suppression_of_a_non_member(self, relation_import_rows):
        """A same-name places row need not be a member to suppress a
        relation -- its point lying inside the relation's member extent is
        sufficient. A different-name relation with the identical extent is
        not suppressed.
        """
        assert "n53001" in relation_import_rows, "incumbent node n53001 missing"
        assert "r53002" not in relation_import_rows, (
            "r53002 (same name, incumbent point inside its extent) must be suppressed"
        )
        assert "r53003" in relation_import_rows, (
            "r53003 (different name, same extent) must NOT be suppressed"
        )

    def test_same_name_outside_extent_not_suppressed(self, relation_import_rows):
        """A same-name places row outside the relation's member extent does
        not suppress it."""
        assert "n54001" in relation_import_rows
        assert "r54002" in relation_import_rows, (
            "r54002 (same name as n54001, but n54001 is outside its extent) "
            "must NOT be suppressed"
        )

    def test_existing_nw_rkeys_unchanged_by_relation_insert(self, relation_parquet, empty_relation_parquet):
        """The relation INSERT must not alter which n/w rkeys survive."""
        baseline_conn = duckdb.connect(":memory:")
        baseline_conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(baseline_conn, relation_parquet["node"], relation_parquet["way"], empty_relation_parquet)
        baseline_rkeys = {row[0] for row in baseline_conn.execute("SELECT rkey FROM places").fetchall()}
        baseline_conn.close()

        full_conn = duckdb.connect(":memory:")
        full_conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(
            full_conn, relation_parquet["node"], relation_parquet["way"], relation_parquet["relation"]
        )
        full_rkeys = {row[0] for row in full_conn.execute("SELECT rkey FROM places").fetchall()}
        full_conn.close()

        nw_rkeys = {rk for rk in full_rkeys if not rk.startswith("r")}
        assert nw_rkeys == baseline_rkeys, "the relation INSERT changed which n/w rkeys survive"
        assert any(rk.startswith("r") for rk in full_rkeys), (
            "no relation rkey present at all -- the relation INSERT is missing"
        )

    def test_relation_reachable_only_via_nested_relation_imports_nothing(self, relation_import_rows):
        """A relation whose only member is another relation has no
        resolvable coordinates and imports nothing; a relation with the same
        relation member plus one resolvable way member still imports.
        """
        assert "r55001" not in relation_import_rows, (
            "r55001 (only a relation member) must import nothing"
        )
        assert "r55003" in relation_import_rows, (
            "r55003 (relation member ignored, way member resolvable) must import"
        )

    def test_building_relation_drop_list_value_excluded(self, relation_import_rows):
        """The way pipeline's building drop-list extends to relation
        multipolygons."""
        assert "r56001" not in relation_import_rows, (
            "r56001 (building=shed, drop-list) must be excluded"
        )
        assert "r56002" in relation_import_rows, (
            "r56002 (building=commercial, include-list) must resolve"
        )

    def test_variants_populated_from_relation_name_tags(self, relation_import_rows):
        """alt_name on a relation derives a variants entry, the same mapping
        the node/way pipelines use."""
        row = relation_import_rows.get("r57001")
        assert row is not None, "r57001 (Grand Central Park) missing from places"
        assert {"name": "GCP", "type": "alternate", "language": None} in row["variants"], (
            f"expected alt_name variant for GCP; got {row['variants']!r}"
        )


class TestOsmImportRelationParquetGlob:
    """stage_import's OSM parquet_glob is a (node, way, relation)
    triple, unpacked directly from parquet_glob."""

    _BBOX = (-122.55, 37.60, -122.30, 37.85)

    def test_stage_import_accepts_relation_glob_triple(self, relation_parquet, tmp_path):
        output = str(tmp_path / "places.parquet")
        parquet = (relation_parquet["node"], relation_parquet["way"], relation_parquet["relation"])
        _stages.stage_import("osm", parquet, self._BBOX, output)
        con = duckdb.connect()
        rkeys = {r[0] for r in con.execute(
            f"SELECT rkey FROM read_parquet('{output}')"
        ).fetchall()}
        con.close()
        assert "r40001" in rkeys, (
            "Big City Park relation should survive the full stage_import pipeline"
        )
