"""Tests for stage_idf() — standalone IDF computation stage.

Tests verify:
- Correct output schema: (category, n_places, idf_score)
- Multi-category unnesting (FSQ)
- Named-elements filter (unnamed places excluded from category counts)
- IDF scores are positive
- Mtime-based caching behavior
- Unsupported sources raise ValueError
"""
import logging
import os
import time

import duckdb
import pytest

# This import will fail until stage_idf is implemented
from garganorn.stages import stage_idf


# ---------------------------------------------------------------------------
# Fixtures — minimal parquet files with known category distributions
# ---------------------------------------------------------------------------


@pytest.fixture
def fsq_idf_parquet(tmp_path):
    """Minimal FSQ parquet with known category distribution.

    Schema matches osm-pbf-parquet / foursquare OSP:
        fsq_category_ids  VARCHAR[]
        name              VARCHAR

    Includes:
    - 3 named places with single categories (cat_A: 2, cat_B: 1)
    - 1 named place with multiple categories (cat_A + cat_C)
    - 1 unnamed place with category cat_D (should be excluded)
    """
    parquet_path = tmp_path / "fsq_idf.parquet"

    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE tmp_fsq (
            fsq_place_id    VARCHAR,
            name            VARCHAR,
            fsq_category_ids VARCHAR[]
        )
    """)

    conn.execute("""
        INSERT INTO tmp_fsq VALUES
            ('fsq001', 'Coffee Shop',        ['cat_A']::VARCHAR[]),
            ('fsq002', 'Another Coffee',      ['cat_A']::VARCHAR[]),
            ('fsq003', 'Park',                ['cat_B']::VARCHAR[]),
            ('fsq004', 'Multi Category Spot', ['cat_A', 'cat_C']::VARCHAR[]),
            ('fsq005', NULL,                  ['cat_D']::VARCHAR[])
    """)

    conn.execute(f"COPY tmp_fsq TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()

    return str(parquet_path)


@pytest.fixture
def overture_idf_parquet(tmp_path):
    """Minimal Overture parquet with known categories.

    Schema:
        categories  STRUCT("primary" VARCHAR)
        names       STRUCT("primary" VARCHAR)

    Includes:
    - 2 named places with 'cafe' category
    - 1 named place with 'park' category
    - 1 place with NULL names.primary (should be excluded)
    - 1 place with NULL categories.primary (should be excluded)
    """
    parquet_path = tmp_path / "overture_idf.parquet"

    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE tmp_ov (
            id          VARCHAR,
            categories  STRUCT("primary" VARCHAR),
            names       STRUCT("primary" VARCHAR)
        )
    """)

    conn.execute("""
        INSERT INTO tmp_ov VALUES
            ('ov001', {'primary': 'cafe'}, {'primary': 'Blue Bottle'}),
            ('ov002', {'primary': 'cafe'}, {'primary': 'Ritual'}),
            ('ov003', {'primary': 'park'}, {'primary': 'Dolores Park'}),
            ('ov004', {'primary': 'cafe'}, {'primary': NULL}),
            ('ov005', {'primary': NULL},   {'primary': 'Named Place'})
    """)

    conn.execute(f"COPY tmp_ov TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()

    return str(parquet_path)


@pytest.fixture
def osm_idf_parquets(tmp_path):
    """Minimal OSM node and way parquets with known tags.

    Schema (node):
        id      BIGINT
        tags    MAP(VARCHAR, VARCHAR)
        lat     DOUBLE
        lon     DOUBLE

    Schema (way):
        id      BIGINT
        tags    MAP(VARCHAR, VARCHAR)
        nds     STRUCT(ref BIGINT)[]

    Includes:
    - Named node with amenity=cafe
    - Named node with amenity=restaurant
    - Unnamed node with amenity=cafe (should be excluded)
    - Named way with tourism=attraction
    - Unnamed way with shop=supermarket (should be excluded)
    - Node with highway=crossing (no recognized OSM category key — no output)
    """
    node_path = tmp_path / "osm_nodes.parquet"
    way_path = tmp_path / "osm_ways.parquet"

    conn = duckdb.connect(":memory:")

    # --- Nodes ---
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
            (1001, map(['name', 'amenity'], ['Node Cafe', 'cafe']),       37.77, -122.42),
            (1002, map(['name', 'amenity'], ['Node Restaurant', 'rest']),  37.78, -122.43),
            (1003, map(['amenity'], ['cafe']),                             37.79, -122.44),
            (1004, map(['name', 'highway'], ['Crossing Spot', 'crossing']), 37.76, -122.40)
    """)

    conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")

    # --- Ways ---
    conn.execute("""
        CREATE TABLE tmp_ways (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            nds     STRUCT(ref BIGINT)[]
        )
    """)

    conn.execute("""
        INSERT INTO tmp_ways VALUES
            (2001, map(['name', 'tourism'], ['Famous Bridge', 'attraction']),
             [{'ref': 9001}, {'ref': 9002}]::STRUCT(ref BIGINT)[]),
            (2002, map(['shop'], ['supermarket']),
             [{'ref': 9003}]::STRUCT(ref BIGINT)[])
    """)

    conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
    conn.close()

    return (str(node_path), str(way_path))


# ---------------------------------------------------------------------------
# TestStageIdfFoursquare
# ---------------------------------------------------------------------------


class TestStageIdfFoursquare:
    """Tests for stage_idf with source='foursquare'."""

    def test_produces_parquet(self, fsq_idf_parquet, tmp_path):
        """stage_idf('foursquare', ...) produces a parquet with correct schema.

        Output must have columns: category (VARCHAR), n_places (BIGINT),
        idf_score (DOUBLE).
        """
        output_path = str(tmp_path / "fsq_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("foursquare", fsq_idf_parquet, output_path, t0)

        assert os.path.exists(output_path), "Output parquet should be created"

        con = duckdb.connect(":memory:")
        con.execute(f"CREATE TABLE result AS SELECT * FROM read_parquet('{output_path}')")
        schema = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'result' ORDER BY ordinal_position"
        ).fetchall()

        col_names = [row[0] for row in schema]
        assert "category" in col_names
        assert "n_places" in col_names
        assert "idf_score" in col_names
        con.close()

    def test_multi_category_unnest(self, fsq_idf_parquet, tmp_path):
        """FSQ place with multiple categories contributes to each.

        fsq004 has ['cat_A', 'cat_C']. cat_A should get contributions from
        fsq001, fsq002, and fsq004 (3 named places). cat_C should get
        1 contribution.
        """
        output_path = str(tmp_path / "fsq_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("foursquare", fsq_idf_parquet, output_path, t0)

        con = duckdb.connect(":memory:")
        rows = con.execute(
            f"SELECT category, n_places FROM read_parquet('{output_path}') "
            f"ORDER BY category"
        ).fetchall()
        con.close()

        by_cat = {row[0]: row[1] for row in rows}

        # cat_A: fsq001 + fsq002 + fsq004 = 3 named places
        assert by_cat.get("cat_A") == 3, f"cat_A should have 3 places, got {by_cat.get('cat_A')}"
        # cat_C: fsq004 = 1
        assert by_cat.get("cat_C") == 1, f"cat_C should have 1 place, got {by_cat.get('cat_C')}"

    def test_named_elements_filter(self, fsq_idf_parquet, tmp_path):
        """Unnamed places (NULL name) are excluded from category counts.

        fsq005 has name=NULL and category cat_D. cat_D should NOT appear
        in the output.
        """
        output_path = str(tmp_path / "fsq_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("foursquare", fsq_idf_parquet, output_path, t0)

        con = duckdb.connect(":memory:")
        categories = con.execute(
            f"SELECT category FROM read_parquet('{output_path}')"
        ).fetchall()
        con.close()

        cat_list = [row[0] for row in categories]
        assert "cat_D" not in cat_list, (
            f"cat_D should not appear (unnamed place), got {cat_list}"
        )

    def test_idf_scores_positive(self, fsq_idf_parquet, tmp_path):
        """All IDF scores must be > 0.

        IDF = ln(N_total / n_places). Since N_total > n_places for any
        non-trivial distribution, all scores should be positive.
        """
        output_path = str(tmp_path / "fsq_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("foursquare", fsq_idf_parquet, output_path, t0)

        con = duckdb.connect(":memory:")
        scores = con.execute(
            f"SELECT idf_score FROM read_parquet('{output_path}')"
        ).fetchall()
        con.close()

        for (score,) in scores:
            assert score > 0, f"IDF score should be positive, got {score}"


# ---------------------------------------------------------------------------
# TestStageIdfOverture
# ---------------------------------------------------------------------------


class TestStageIdfOverture:
    """Tests for stage_idf with source='overture_place'."""

    def test_produces_parquet(self, overture_idf_parquet, tmp_path):
        """stage_idf('overture_place', ...) produces output with correct schema."""
        output_path = str(tmp_path / "ov_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("overture_place", overture_idf_parquet, output_path, t0)

        assert os.path.exists(output_path), "Output parquet should be created"

        con = duckdb.connect(":memory:")
        con.execute(f"CREATE TABLE result AS SELECT * FROM read_parquet('{output_path}')")
        schema = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'result' ORDER BY ordinal_position"
        ).fetchall()
        con.close()

        col_names = [row[0] for row in schema]
        assert "category" in col_names
        assert "n_places" in col_names
        assert "idf_score" in col_names

    def test_named_elements_filter(self, overture_idf_parquet, tmp_path):
        """Places with NULL names.primary are excluded from category counts.

        ov004 has names.primary=NULL and category='cafe'. The cafe count
        should only include ov001 and ov002 (2 named places).
        """
        output_path = str(tmp_path / "ov_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("overture_place", overture_idf_parquet, output_path, t0)

        con = duckdb.connect(":memory:")
        rows = con.execute(
            f"SELECT category, n_places FROM read_parquet('{output_path}') "
            f"ORDER BY category"
        ).fetchall()
        con.close()

        by_cat = {row[0]: row[1] for row in rows}
        assert by_cat.get("cafe") == 2, (
            f"cafe should count only 2 named places, got {by_cat.get('cafe')}"
        )

    def test_null_category_excluded(self, overture_idf_parquet, tmp_path):
        """No NULL categories in output.

        ov005 has categories.primary=NULL. It should not produce a row.
        """
        output_path = str(tmp_path / "ov_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("overture_place", overture_idf_parquet, output_path, t0)

        con = duckdb.connect(":memory:")
        categories = con.execute(
            f"SELECT category FROM read_parquet('{output_path}')"
        ).fetchall()
        con.close()

        for (cat,) in categories:
            assert cat is not None, "NULL category should not appear in output"


# ---------------------------------------------------------------------------
# TestStageIdfOSM
# ---------------------------------------------------------------------------


class TestStageIdfOSM:
    """Tests for stage_idf with source='osm'."""

    def test_produces_parquet(self, osm_idf_parquets, tmp_path):
        """stage_idf('osm', ...) produces output with correct schema.

        OSM takes a tuple of (node_parquet, way_parquet).
        """
        output_path = str(tmp_path / "osm_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("osm", osm_idf_parquets, output_path, t0)

        assert os.path.exists(output_path), "Output parquet should be created"

        con = duckdb.connect(":memory:")
        con.execute(f"CREATE TABLE result AS SELECT * FROM read_parquet('{output_path}')")
        schema = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'result' ORDER BY ordinal_position"
        ).fetchall()
        con.close()

        col_names = [row[0] for row in schema]
        assert "category" in col_names
        assert "n_places" in col_names
        assert "idf_score" in col_names

    def test_includes_nodes_and_ways(self, osm_idf_parquets, tmp_path):
        """Categories from both node and way sources are present.

        Nodes contribute amenity=cafe and amenity=rest categories.
        Ways contribute tourism=attraction category.
        All three should appear in the output.
        """
        output_path = str(tmp_path / "osm_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("osm", osm_idf_parquets, output_path, t0)

        con = duckdb.connect(":memory:")
        categories = con.execute(
            f"SELECT category FROM read_parquet('{output_path}')"
        ).fetchall()
        con.close()

        cat_list = [row[0] for row in categories]
        # amenity=cafe should be present (from node 1001)
        assert "amenity=cafe" in cat_list, f"amenity=cafe should be present, got {cat_list}"
        # tourism=attraction should be present (from way 2001)
        assert "tourism=attraction" in cat_list, (
            f"tourism=attraction should be present, got {cat_list}"
        )

    def test_named_elements_filter(self, osm_idf_parquets, tmp_path):
        """Unnamed nodes/ways are excluded from category counts.

        Node 1003 has no 'name' tag but has amenity=cafe. It should not
        contribute to the amenity=cafe count.

        Way 2002 has no 'name' tag but has shop=supermarket. It should not
        appear in output.
        """
        output_path = str(tmp_path / "osm_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("osm", osm_idf_parquets, output_path, t0)

        con = duckdb.connect(":memory:")
        rows = con.execute(
            f"SELECT category, n_places FROM read_parquet('{output_path}')"
        ).fetchall()
        con.close()

        by_cat = {row[0]: row[1] for row in rows}

        # amenity=cafe: only node 1001 (named), not 1003 (unnamed)
        assert by_cat.get("amenity=cafe") == 1, (
            f"amenity=cafe should count 1 named place, got {by_cat.get('amenity=cafe')}"
        )

        # shop=supermarket: way 2002 is unnamed, should not appear
        assert "shop=supermarket" not in by_cat, (
            f"shop=supermarket should not appear (unnamed way), got {list(by_cat.keys())}"
        )

    def test_no_category_not_in_output(self, osm_idf_parquets, tmp_path):
        """Unrecognized tag keys produce no output rows.

        Node 1004 has highway=crossing. 'highway' is not a recognized OSM
        place category key, so it should not appear in the output.
        """
        output_path = str(tmp_path / "osm_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("osm", osm_idf_parquets, output_path, t0)

        con = duckdb.connect(":memory:")
        categories = con.execute(
            f"SELECT category FROM read_parquet('{output_path}')"
        ).fetchall()
        con.close()

        cat_list = [row[0] for row in categories]
        assert "highway=crossing" not in cat_list, (
            f"highway=crossing should not appear, got {cat_list}"
        )


# ---------------------------------------------------------------------------
# TestStageIdfMtimeCaching
# ---------------------------------------------------------------------------


class TestStageIdfMtimeCaching:
    """Tests for mtime-based caching in stage_idf().

    Mirrors the caching pattern from stage_density_extract: skip when
    output is fresh, re-run when input is newer, force overrides.
    """

    @pytest.fixture
    def small_fsq_parquet(self, tmp_path):
        """Minimal FSQ parquet for mtime tests."""
        parquet_path = tmp_path / "small_fsq.parquet"

        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE tmp_fsq (
                fsq_place_id    VARCHAR,
                name            VARCHAR,
                fsq_category_ids VARCHAR[]
            )
        """)
        conn.execute("""
            INSERT INTO tmp_fsq VALUES
                ('x001', 'Place A', ['cat1']::VARCHAR[]),
                ('x002', 'Place B', ['cat2']::VARCHAR[])
        """)
        conn.execute(f"COPY tmp_fsq TO '{parquet_path}' (FORMAT PARQUET)")
        conn.close()

        return str(parquet_path)

    def test_skips_when_output_fresh(self, small_fsq_parquet, tmp_path, caplog):
        """Second run with fresh output skips computation."""
        output_path = str(tmp_path / "idf_out.parquet")
        t0 = time.monotonic()

        # First run: create output
        stage_idf("foursquare", small_fsq_parquet, output_path, t0)
        assert os.path.exists(output_path)

        # Set output mtime to the future
        future_time = time.time() + 3600
        os.utime(output_path, (future_time, future_time))

        # Second run: should skip
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_idf("foursquare", small_fsq_parquet, output_path, t0)

        assert any("skip" in record.message.lower() for record in caplog.records), (
            "Should log skip message when output is fresh"
        )

    def test_runs_when_output_missing(self, small_fsq_parquet, tmp_path, caplog):
        """Runs normally when output doesn't exist."""
        output_path = str(tmp_path / "idf_out.parquet")
        t0 = time.monotonic()

        assert not os.path.exists(output_path)

        with caplog.at_level(logging.INFO):
            stage_idf("foursquare", small_fsq_parquet, output_path, t0)

        assert os.path.exists(output_path), "Should create output when missing"
        assert any("start" in record.message.lower() for record in caplog.records), (
            "Should log starting message when output is missing"
        )

    def test_runs_when_input_newer(self, small_fsq_parquet, tmp_path, caplog):
        """Re-runs when input parquet is newer than output."""
        output_path = str(tmp_path / "idf_out.parquet")
        t0 = time.monotonic()

        # First run
        stage_idf("foursquare", small_fsq_parquet, output_path, t0)
        assert os.path.exists(output_path)

        # Make output older than input
        past_time = time.time() - 3600
        os.utime(output_path, (past_time, past_time))

        # Second run: should re-run
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_idf("foursquare", small_fsq_parquet, output_path, t0)

        assert any("start" in record.message.lower() for record in caplog.records), (
            "Should log starting message when input is newer"
        )

    def test_force_overrides_fresh(self, small_fsq_parquet, tmp_path, caplog):
        """force=True re-runs despite fresh output."""
        output_path = str(tmp_path / "idf_out.parquet")
        t0 = time.monotonic()

        # First run
        stage_idf("foursquare", small_fsq_parquet, output_path, t0)
        assert os.path.exists(output_path)

        # Set output to the future
        future_time = time.time() + 3600
        os.utime(output_path, (future_time, future_time))

        # Second run with force=True
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_idf("foursquare", small_fsq_parquet, output_path, t0, force=True)

        assert any("start" in record.message.lower() for record in caplog.records), (
            "Should log starting message when force=True"
        )

    def test_osm_mtime_checks_both_parquets(self, osm_idf_parquets, tmp_path, caplog):
        """OSM mtime check considers both node and way parquets.

        If the way parquet is newer than the output, the stage should
        re-run even if the node parquet is older.
        """
        node_path, way_path = osm_idf_parquets
        output_path = str(tmp_path / "osm_idf_out.parquet")
        t0 = time.monotonic()

        # First run
        stage_idf("osm", osm_idf_parquets, output_path, t0)
        assert os.path.exists(output_path)

        # Make output older than way parquet but newer than node parquet
        now = time.time()
        os.utime(output_path, (now - 3600, now - 3600))  # output: 1 hour ago
        os.utime(node_path, (now - 7200, now - 7200))     # node: 2 hours ago (older than output)
        # way parquet stays at current mtime (newer than output)

        # Second run: should re-run because way parquet is newer
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_idf("osm", osm_idf_parquets, output_path, t0)

        assert any("start" in record.message.lower() for record in caplog.records), (
            "Should re-run when way parquet is newer than output"
        )


# ---------------------------------------------------------------------------
# TestStageIdfUnsupportedSource
# ---------------------------------------------------------------------------


class TestStageIdfUnsupportedSource:
    """Tests that stage_idf rejects unsupported sources."""

    def test_raises_for_overture_division(self, tmp_path):
        """overture_division is not a valid source for IDF computation."""
        output_path = str(tmp_path / "should_not_exist.parquet")
        t0 = time.monotonic()

        with pytest.raises(ValueError, match="overture_division|unsupported|invalid"):
            stage_idf("overture_division", "/dev/null", output_path, t0)

    def test_raises_for_unknown_source(self, tmp_path):
        """Unknown source raises ValueError."""
        output_path = str(tmp_path / "should_not_exist.parquet")
        t0 = time.monotonic()

        with pytest.raises(ValueError, match="unsupported|invalid|unknown"):
            stage_idf("nonexistent_source", "/dev/null", output_path, t0)
