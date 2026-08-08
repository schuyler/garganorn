"""Tests for stage_idf() — standalone IDF computation stage.

Tests verify:
- Correct output schema: (category, n_places, idf_score)
- Named-elements filter (unnamed places excluded from category counts)
- IDF scores are positive
- Mtime-based caching behavior
- Unsupported sources raise ValueError
"""
import json
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

    def test_idf_scores_positive(self, overture_idf_parquet, tmp_path):
        """All IDF scores must be > 0.

        IDF = ln(N_total / n_places). Since N_total > n_places for any
        non-trivial distribution, all scores should be positive.
        """
        output_path = str(tmp_path / "ov_idf_out.parquet")
        t0 = time.monotonic()

        stage_idf("overture_place", overture_idf_parquet, output_path, t0)

        con = duckdb.connect(":memory:")
        scores = con.execute(
            f"SELECT idf_score FROM read_parquet('{output_path}')"
        ).fetchall()
        con.close()

        for (score,) in scores:
            assert score > 0, f"IDF score should be positive, got {score}"


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
    def small_overture_parquet(self, tmp_path):
        """Minimal Overture parquet for mtime tests."""
        parquet_path = tmp_path / "small_overture.parquet"

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
                ('x001', {'primary': 'cat1'}, {'primary': 'Place A'}),
                ('x002', {'primary': 'cat2'}, {'primary': 'Place B'})
        """)
        conn.execute(f"COPY tmp_ov TO '{parquet_path}' (FORMAT PARQUET)")
        conn.close()

        return str(parquet_path)

    def test_skips_when_output_fresh(self, small_overture_parquet, tmp_path, caplog):
        """Second run skips when artifact+meta are present and both newer than input.

        Under the meta-driven contract, freshness is determined by artifact_fresh():
        meta must exist, meta must be newer than every input, and artifact must not
        be newer than meta. A genuine second run (no mtime manipulation of the
        artifact) satisfies all three conditions because finalize_artifact writes
        meta AFTER the artifact rename.

        Do NOT set only the artifact's mtime to the future — that would make
        artifact newer than meta, which artifact_fresh() treats as a crash signal
        and returns False (triggering a re-run, not a skip).
        """
        output_path = str(tmp_path / "idf_out.parquet")
        meta_path = output_path + ".meta.json"
        t0 = time.monotonic()

        # First run: create artifact + meta sidecar
        stage_idf("overture_place", small_overture_parquet, output_path, t0)
        assert os.path.exists(output_path), "First run must create artifact"
        assert os.path.exists(meta_path), "First run must create .meta.json"

        # Advance BOTH artifact and meta to the future so meta is still newer
        # than the input parquet and artifact is still not newer than meta.
        future_time = time.time() + 3600
        os.utime(output_path, (future_time, future_time))
        os.utime(meta_path, (future_time + 1, future_time + 1))

        # Second run: artifact_fresh() must return True → stage skips
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_idf("overture_place", small_overture_parquet, output_path, t0)

        assert any("skip" in record.message.lower() for record in caplog.records), (
            "Should log skip message when artifact+meta are fresh"
        )

    def test_runs_when_output_missing(self, small_overture_parquet, tmp_path, caplog):
        """Runs normally when output doesn't exist."""
        output_path = str(tmp_path / "idf_out.parquet")
        t0 = time.monotonic()

        assert not os.path.exists(output_path)

        with caplog.at_level(logging.INFO):
            stage_idf("overture_place", small_overture_parquet, output_path, t0)

        assert os.path.exists(output_path), "Should create output when missing"
        assert any("start" in record.message.lower() for record in caplog.records), (
            "Should log starting message when output is missing"
        )

    def test_runs_when_input_newer(self, small_overture_parquet, tmp_path, caplog):
        """Re-runs when an input parquet is newer than the meta sidecar.

        Under the meta-driven contract, artifact_fresh() checks:
          mtime(meta) > mtime(every input)
        Making the input newer than the meta (by moving BOTH artifact and meta
        to the past) correctly signals that inputs changed and a rebuild is needed.

        Setting only the artifact to the past is NOT enough — artifact_fresh() also
        checks that artifact is not newer than meta; moving only the artifact while
        leaving meta at its original (newer) time means artifact < meta < input is
        still possible but what matters is meta vs input comparison.
        Moving BOTH to the past makes meta < input (current time), which is the
        canonical "inputs changed" scenario.
        """
        output_path = str(tmp_path / "idf_out.parquet")
        meta_path = output_path + ".meta.json"
        t0 = time.monotonic()

        # First run: produces artifact + meta
        stage_idf("overture_place", small_overture_parquet, output_path, t0)
        assert os.path.exists(output_path)
        assert os.path.exists(meta_path)

        # Move both artifact and meta to the past so the input parquet
        # (which has its current filesystem mtime) is newer than meta.
        past_time = time.time() - 3600
        os.utime(output_path, (past_time, past_time))
        os.utime(meta_path, (past_time, past_time))

        # Second run: artifact_fresh() returns False (meta < input) → re-runs
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_idf("overture_place", small_overture_parquet, output_path, t0)

        assert any("start" in record.message.lower() for record in caplog.records), (
            "Should log starting message when input is newer than meta"
        )

    def test_force_overrides_fresh(self, small_overture_parquet, tmp_path, caplog):
        """force=True re-runs despite fresh output."""
        output_path = str(tmp_path / "idf_out.parquet")
        t0 = time.monotonic()

        # First run
        stage_idf("overture_place", small_overture_parquet, output_path, t0)
        assert os.path.exists(output_path)

        # Set output to the future
        future_time = time.time() + 3600
        os.utime(output_path, (future_time, future_time))

        # Second run with force=True
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_idf("overture_place", small_overture_parquet, output_path, t0, force=True)

        assert any("start" in record.message.lower() for record in caplog.records), (
            "Should log starting message when force=True"
        )

    def test_osm_mtime_checks_both_parquets(self, osm_idf_parquets, tmp_path, caplog):
        """OSM freshness check covers both node-glob and way-glob inputs.

        artifact_fresh() checks mtime(meta) > mtime(every input). For OSM,
        input_files = resolved(node_glob) + resolved(way_glob). If EITHER
        glob's files are newer than meta, artifact_fresh() returns False and
        the stage re-runs.

        This test verifies the two-sided coverage by running two sub-scenarios:
        (A) node newer than meta → re-run
        (B) way newer than meta → re-run

        In each sub-scenario both artifact and meta are moved to the past, then
        one glob's parquet is left at its current (newer) mtime while the other
        is also moved to the past. Only the "newer" glob should trigger the re-run.
        """
        node_path, way_path = osm_idf_parquets
        output_path = str(tmp_path / "osm_idf_out.parquet")
        meta_path = output_path + ".meta.json"
        t0 = time.monotonic()

        # --- Sub-scenario A: node parquet newer than meta triggers re-run ---
        stage_idf("osm", osm_idf_parquets, output_path, t0)
        assert os.path.exists(output_path)
        assert os.path.exists(meta_path)

        past_time = time.time() - 7200  # 2 hours ago
        os.utime(output_path, (past_time, past_time))
        os.utime(meta_path, (past_time, past_time))
        # Move way to past as well; keep node at current mtime (newer than meta)
        os.utime(way_path, (past_time - 1, past_time - 1))
        # node_path left at its current filesystem mtime (newer than meta)

        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_idf("osm", osm_idf_parquets, output_path, t0)

        assert any("start" in record.message.lower() for record in caplog.records), (
            "Sub-scenario A: should re-run when node parquet is newer than meta"
        )

        # --- Sub-scenario B: way parquet newer than meta triggers re-run ---
        # After the re-run above, artifact+meta are fresh again; move both to past.
        assert os.path.exists(meta_path), "Re-run must produce new meta"
        os.utime(output_path, (past_time, past_time))
        os.utime(meta_path, (past_time, past_time))
        # Move node to past as well; keep way at current mtime (newer than meta)
        os.utime(node_path, (past_time - 1, past_time - 1))
        # way_path left at its current filesystem mtime (renewed by the re-run's input read)
        # Ensure way is actually newer than meta by touching it explicitly.
        os.utime(way_path, (time.time(), time.time()))

        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_idf("osm", osm_idf_parquets, output_path, t0)

        assert any("start" in record.message.lower() for record in caplog.records), (
            "Sub-scenario B: should re-run when way parquet is newer than meta"
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


# ---------------------------------------------------------------------------
# §7.1.6 Sort pin — idf.parquet must be non-decreasing on category
# ---------------------------------------------------------------------------

class TestIdfSortPin:
    """§7.1.6: idf.parquet must be non-decreasing on 'category'.

    Phase 2 spec §3.2 makes ORDER BY category explicit.  If the current
    implementation produces an unsorted parquet, this test fails in Red phase.
    """

    def test_idf_sorted_by_category(self, overture_idf_parquet, tmp_path):
        """idf.parquet must be sorted non-decreasingly on the 'category' column."""
        output = str(tmp_path / "idf.parquet")
        stage_idf("overture_place", str(overture_idf_parquet), output, time.monotonic(), force=True)
        con = duckdb.connect()
        cats = [r[0] for r in con.execute(
            f"SELECT category FROM read_parquet('{output}')"
        ).fetchall()]
        con.close()
        assert cats == sorted(cats), (
            f"idf.parquet must be sorted by category; "
            f"out-of-order pairs: {[(a, b) for a, b in zip(cats, cats[1:]) if a > b][:3]}"
        )


# ---------------------------------------------------------------------------
# §RED: TestStageIdfMetaSidecar — crash-safety via finalize_artifact
# ---------------------------------------------------------------------------


class TestStageIdfMetaSidecar:
    """Prove that stage_idf must adopt finalize_artifact crash-safety discipline.

    All four tests fail RED because the current stage_idf:
      - writes output in-place via COPY … TO '{output_path}' (no .tmp step)
      - calls _is_output_fresh() (mtime-only) instead of artifact_fresh()
      - never calls finalize_artifact, so no .meta.json sidecar is produced

    The green implementer must:
      1. Remove the stale <output_path>.tmp at stage start.
      2. Write to <output_path>.tmp via COPY.
      3. Call finalize_artifact(tmp, output_path, params={}, inputs=resolved_inputs)
         which atomically renames .tmp → artifact and writes .meta.json LAST.
      4. Replace _is_output_fresh() gate with artifact_fresh() (meta-aware).
    """

    @pytest.fixture
    def small_overture_parquet(self, tmp_path):
        """Minimal Overture parquet for meta-sidecar tests."""
        parquet_path = tmp_path / "small_overture.parquet"

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
                ('x001', {'primary': 'cat1'}, {'primary': 'Place A'}),
                ('x002', {'primary': 'cat2'}, {'primary': 'Place B'})
        """)
        conn.execute(f"COPY tmp_ov TO '{parquet_path}' (FORMAT PARQUET)")
        conn.close()

        return str(parquet_path)

    def test_meta_json_written_after_stage(self, small_overture_parquet, tmp_path):
        """After stage_idf, <output_path>.meta.json must exist.

        meta.json must contain:
          - 'params': {} (idf stage has no named parameters)
          - 'inputs': a non-empty list of resolved source parquet paths

        Fails RED because current stage_idf uses direct COPY (no finalize_artifact).
        """
        output = tmp_path / "idf.parquet"
        meta_path = tmp_path / "idf.parquet.meta.json"

        stage_idf("overture_place", small_overture_parquet, str(output), time.monotonic())

        assert output.exists(), "idf.parquet must be written"
        assert meta_path.exists(), (
            "idf.parquet.meta.json must be written by finalize_artifact; "
            "fails RED because current stage_idf uses direct COPY with no meta sidecar"
        )
        meta = json.loads(meta_path.read_text())
        assert "params" in meta, (
            f"meta.json must have 'params' key; got keys: {list(meta)}"
        )
        assert "inputs" in meta, (
            f"meta.json must have 'inputs' key; got keys: {list(meta)}"
        )
        assert meta["params"] == {}, (
            f"idf stage params must be empty dict (no stage parameters); "
            f"got {meta['params']!r}"
        )
        assert isinstance(meta["inputs"], list) and len(meta["inputs"]) > 0, (
            f"meta.json 'inputs' must be a non-empty list of resolved parquet paths; "
            f"got {meta.get('inputs')!r}"
        )

    def test_stale_tmp_clobbered(self, small_overture_parquet, tmp_path):
        """A stale <output_path>.tmp is removed before building; meta.json is written after.

        Fails RED because current stage_idf never writes .meta.json, so the
        meta_path.exists() assertion fails even though .tmp removal may already work.
        """
        output = tmp_path / "idf.parquet"
        tmp_file = tmp_path / "idf.parquet.tmp"
        meta_path = tmp_path / "idf.parquet.meta.json"

        # Plant garbage .tmp that must be removed at stage start
        tmp_file.write_bytes(b"garbage data from a previous interrupted run")
        assert tmp_file.exists(), "Test setup: .tmp must exist before the call"

        stage_idf("overture_place", small_overture_parquet, str(output), time.monotonic(), force=True)

        assert output.exists(), "idf.parquet must be written"
        assert not tmp_file.exists(), (
            ".tmp file must be cleaned up by stage_idf before building"
        )
        assert meta_path.exists(), (
            "idf.parquet.meta.json must exist after stage; "
            "fails RED because current implementation does not call finalize_artifact"
        )

    def test_freshness_meta_driven(self, small_overture_parquet, tmp_path):
        """artifact_fresh(output, resolved_inputs, {}) must return True after a run.

        Fails RED because:
          - current code never writes .meta.json
          - artifact_fresh() returns False (missing meta)
          - the assertion that artifact_fresh returns True therefore fails
        """
        from garganorn.stages import artifact_fresh, _resolve_glob_paths

        output = tmp_path / "idf.parquet"

        stage_idf("overture_place", small_overture_parquet, str(output), time.monotonic())
        assert output.exists(), "idf.parquet must be written"

        resolved_inputs = _resolve_glob_paths(small_overture_parquet)
        assert resolved_inputs, "small_overture_parquet fixture must resolve to at least one file"

        # artifact_fresh must return True for the just-built fresh artifact.
        # Fails RED because current code never writes .meta.json (artifact_fresh returns False).
        assert artifact_fresh(str(output), resolved_inputs, {}), (
            "artifact_fresh(idf.parquet, resolved_inputs, {}) must return True "
            "after a successful stage_idf; "
            "fails RED because no .meta.json is written by the current implementation"
        )

    def test_partial_write_not_reused(self, small_overture_parquet, tmp_path, caplog):
        """Meta absent → artifact_fresh False → stage_idf re-runs (does not skip).

        Scenario: a crash occurred BEFORE the meta sidecar was written (e.g. kill -9
        immediately after the atomic rename but before finalize_artifact wrote the
        .meta.json).  The artifact exists on disk but has no companion .meta.json.

        Steps:
          1. Run stage_idf successfully (produces output + meta).
          2. Delete .meta.json to reproduce the post-crash filesystem state.
          3. Assert artifact_fresh returns False (meta-absent check fires).
          4. Run stage_idf again WITHOUT force and assert it RE-RUNS
             (logs "start", does NOT log "skip" only).

        Fails RED on step 3 because current code uses _is_output_fresh() which
        checks only mtime(output) vs mtime(inputs) — it does not check for a
        missing .meta.json — so the artifact appears fresh and is silently reused.
        """
        from garganorn.stages import artifact_fresh, _resolve_glob_paths

        output = tmp_path / "idf.parquet"
        meta_path = tmp_path / "idf.parquet.meta.json"

        # Step 1: successful first run
        stage_idf("overture_place", small_overture_parquet, str(output), time.monotonic())
        assert output.exists(), "first run must produce idf.parquet"

        # Step 2: simulate crash — delete the meta sidecar.
        # (In a real crash, meta would never have been written; we delete it
        # after the fact to reproduce the same filesystem state.)
        if meta_path.exists():
            meta_path.unlink()

        # Step 3: meta-absent state must be detected.
        resolved_inputs = _resolve_glob_paths(small_overture_parquet)
        assert not artifact_fresh(str(output), resolved_inputs, {}), (
            "artifact_fresh must return False when .meta.json is absent; "
            "fails RED because _is_output_fresh() does not check for meta and "
            "returns True (mtime(output) > mtime(inputs)), silently reusing the artifact"
        )

        # Step 4: a non-forced re-run must detect the missing meta and re-run.
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_idf("overture_place", small_overture_parquet, str(output), time.monotonic())

        logged_messages = [r.message.lower() for r in caplog.records]
        assert any("start" in msg for msg in logged_messages), (
            "stage_idf must re-run (log 'start') when .meta.json is absent; "
            f"fails RED because current code uses _is_output_fresh() which sees "
            f"output as fresh and logs 'skip' instead. Logged: {logged_messages}"
        )
        skip_only = (
            any("skip" in msg for msg in logged_messages)
            and not any("start" in msg for msg in logged_messages)
        )
        assert not skip_only, (
            "stage_idf must not skip when .meta.json is absent (partial write not safe to reuse)"
        )

    def test_stale_artifact_newer_than_meta_not_reused(
        self, small_overture_parquet, tmp_path, caplog
    ):
        """Crash between rename and meta-write: artifact newer than meta → not reused.

        Scenario: finalize_artifact completed the atomic rename (artifact exists)
        but was killed before it could write .meta.json.  In the GREEN implementation
        this never produces an artifact-newer-than-meta state because meta is written
        AFTER rename — but a subsequent run might restore the meta and leave the
        artifact mtime ahead of it (e.g. via manual repair or partial re-run).

        This test exercises artifact_fresh rule 6:
          if mtime(artifact) > mtime(meta) → return False

        Steps:
          1. Run stage_idf once so BOTH idf.parquet and idf.parquet.meta.json exist.
          2. Assert meta exists (RED failure if stage_idf never writes meta).
          3. Advance the ARTIFACT's mtime to be strictly newer than meta's mtime,
             leaving the meta file untouched (simulates the crash window).
          4. Assert artifact_fresh returns False (artifact-newer-than-meta guard).
          5. Run stage_idf again WITHOUT force; assert it RE-RUNS (logs "start").

        Fails RED because:
          - current stage_idf never writes .meta.json (step 2 fails immediately)
          - even if a meta somehow existed, _is_output_fresh() ignores it
        """
        from garganorn.stages import artifact_fresh, _resolve_glob_paths

        output = tmp_path / "idf.parquet"
        meta_path = tmp_path / "idf.parquet.meta.json"

        # Step 1: successful first run
        stage_idf("overture_place", small_overture_parquet, str(output), time.monotonic())
        assert output.exists(), "first run must produce idf.parquet"

        # Step 2: meta must exist — RED failure if finalize_artifact is not called.
        assert meta_path.exists(), (
            "idf.parquet.meta.json must exist after stage_idf; "
            "fails RED because current implementation uses direct COPY with no finalize_artifact"
        )

        # Step 3: advance artifact mtime to be strictly newer than meta.
        meta_mtime = os.stat(str(meta_path)).st_mtime
        future_art = meta_mtime + 7200  # 2 hours after meta write
        os.utime(str(output), (future_art, future_art))

        # Sanity check: artifact is now newer than meta.
        assert os.stat(str(output)).st_mtime > os.stat(str(meta_path)).st_mtime, (
            "Test setup error: artifact mtime should be strictly newer than meta mtime"
        )

        # Step 4: artifact-newer-than-meta must be detected as stale.
        resolved_inputs = _resolve_glob_paths(small_overture_parquet)
        assert not artifact_fresh(str(output), resolved_inputs, {}), (
            "artifact_fresh must return False when mtime(artifact) > mtime(meta); "
            "this guards against a crash between the atomic rename and the meta write"
        )

        # Step 5: a non-forced re-run must detect the stale state and re-run.
        caplog.clear()
        with caplog.at_level(logging.INFO):
            stage_idf("overture_place", small_overture_parquet, str(output), time.monotonic())

        logged_messages = [r.message.lower() for r in caplog.records]
        assert any("start" in msg for msg in logged_messages), (
            "stage_idf must re-run (log 'start') when artifact is newer than meta; "
            f"fails RED because current code uses _is_output_fresh() which does not "
            f"check artifact vs meta ordering. Logged: {logged_messages}"
        )
        skip_only = (
            any("skip" in msg for msg in logged_messages)
            and not any("start" in msg for msg in logged_messages)
        )
        assert not skip_only, (
            "stage_idf must not skip when artifact is newer than meta (crash-safety guard)"
        )
