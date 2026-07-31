"""Tests for density_extract.sql and stage_density_extract() function."""

import time

import duckdb
import pytest
from garganorn.stages import stage_density_extract, quadkey_to_bbox


# ---------------------------------------------------------------------------
# Tests: density_extract.sql (via stage_density_extract)
# ---------------------------------------------------------------------------

class TestDensityExtract:
    """Tests for garganorn/sql/density_extract.sql and the stage_density_extract() function.

    The stage_density_extract() function reads Overture parquet and writes a
    density parquet file with one row per zoom-15 quadkey containing the
    density score ln(1 + count(*)).
    """

    def test_produces_parquet(self, overture_parquet, tmp_path):
        """stage_density_extract must produce a parquet file."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        assert output.exists(), f"Density parquet not found at {output}"
        assert output.stat().st_size > 0, "Density parquet is empty"

    def test_schema_columns(self, overture_parquet, tmp_path):
        """Density parquet must have tile_qk15 and density_score columns."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")
        cols = {row[0] for row in conn.execute("DESCRIBE density").fetchall()}
        conn.close()

        assert "tile_qk15" in cols, f"tile_qk15 column missing; found: {cols}"
        assert "density_score" in cols, f"density_score column missing; found: {cols}"

    def test_density_score_positive(self, overture_parquet, tmp_path):
        """All density_score values must be positive (ln(1 + count) > 0)."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")
        bad = conn.execute("SELECT tile_qk15, density_score FROM density WHERE density_score <= 0").fetchall()
        conn.close()

        assert not bad, f"Rows with non-positive density_score: {bad}"

    def test_tile_qk15_length_15(self, overture_parquet, tmp_path):
        """All tile_qk15 values must be exactly 15 characters."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")
        bad = conn.execute("SELECT tile_qk15 FROM density WHERE length(tile_qk15) != 15").fetchall()
        conn.close()

        assert not bad, f"Rows with tile_qk15 length != 15: {bad}"

    def test_global_no_bbox_filter(self, overture_parquet, tmp_path):
        """stage_density_extract must not filter by bbox; all records contribute to density."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        conn.execute(f"CREATE TABLE density AS SELECT * FROM read_parquet('{output}')")
        count = conn.execute("SELECT COUNT(*) FROM density").fetchone()[0]
        conn.close()

        # The overture_parquet fixture has 7 rows total (including ov006 outside bbox).
        # All should contribute to density since there's no bbox filter.
        # We expect at least 1 density tile (probably 1-5 given the small fixture).
        assert count > 0, f"Expected at least 1 density tile, got {count}"


# ---------------------------------------------------------------------------
# Tile bounds computed in SQL (qk_env macro) must match quadkey_to_bbox()
# ---------------------------------------------------------------------------

class TestDensityTileBoundsSQL:
    """Guard test for the SQL-computed tile bounds columns.

    stage_density_extract computes tile_xmin/tile_ymin/tile_xmax/tile_ymax
    in SQL via the qk_env() macro (garganorn/sql/qk_env_macro.sql) rather
    than in a Python post-processing loop over quadkey_to_bbox(). This test
    asserts the SQL path still produces all 6 output columns, in the
    documented order, and that the SQL-computed bounds agree with
    quadkey_to_bbox() (the reference implementation) to within 1e-9 for
    every row in the output.
    """

    def test_sql_bounds_match_quadkey_to_bbox(self, overture_parquet, tmp_path):
        """All 6 columns present; SQL bounds agree with quadkey_to_bbox() to 1e-9."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())

        conn = duckdb.connect(":memory:")
        cols = [row[0] for row in conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{output}')"
        ).fetchall()]
        assert cols == [
            "tile_qk15", "density_score",
            "tile_xmin", "tile_ymin", "tile_xmax", "tile_ymax",
        ], f"unexpected column set/order: {cols}"

        rows = conn.execute(
            f"SELECT tile_qk15, tile_xmin, tile_ymin, tile_xmax, tile_ymax "
            f"FROM read_parquet('{output}')"
        ).fetchall()
        conn.close()

        assert rows, "expected at least one density tile from the overture_parquet fixture"

        for qk, xmin, ymin, xmax, ymax in rows:
            expected = quadkey_to_bbox(qk)
            assert abs(xmin - expected[0]) <= 1e-9, f"tile_xmin mismatch for {qk}: got {xmin}, expected {expected[0]}"
            assert abs(ymin - expected[1]) <= 1e-9, f"tile_ymin mismatch for {qk}: got {ymin}, expected {expected[1]}"
            assert abs(xmax - expected[2]) <= 1e-9, f"tile_xmax mismatch for {qk}: got {xmax}, expected {expected[2]}"
            assert abs(ymax - expected[3]) <= 1e-9, f"tile_ymax mismatch for {qk}: got {ymax}, expected {expected[3]}"


# ---------------------------------------------------------------------------
# §7.1.6 Sort pin — density parquet must be non-decreasing on tile_qk15
# ---------------------------------------------------------------------------

class TestDensitySortPin:
    """§7.1.6: density_tiles.parquet must be non-decreasing on tile_qk15.

    Phase 2 spec §3.1 makes the ORDER BY tile_qk15 explicit. The test
    pins this invariant as a regression guard.
    """

    def test_density_sorted_by_tile_qk15(self, overture_parquet, tmp_path):
        """density_tiles.parquet must be sorted non-decreasingly on tile_qk15."""
        output = tmp_path / "density.parquet"
        stage_density_extract(overture_parquet, str(output), time.monotonic())
        conn = duckdb.connect()
        qks = [r[0] for r in conn.execute(
            f"SELECT tile_qk15 FROM read_parquet('{output}')"
        ).fetchall()]
        conn.close()
        assert qks == sorted(qks), (
            f"density_tiles.parquet must be sorted by tile_qk15; "
            f"got out-of-order: {[(a, b) for a, b in zip(qks, qks[1:]) if a > b][:3]}"
        )


# ---------------------------------------------------------------------------
# §3.1/§2.2/§2.3 Gate #13 Finding #2 — density finalize via .meta.json sidecar
# ---------------------------------------------------------------------------

class TestDensityMetaSidecar:
    """§3.1/§2.2 tests for stage_density_extract producing a .meta.json sidecar.

    All tests fail RED because the current stage_density_extract writes output
    directly via `COPY density_export TO '{output_path}'` without calling
    finalize_artifact, so no .meta.json is ever written.

    The green implementer must:
      1. Write to <output_path>.tmp first (via COPY).
      2. Call finalize_artifact(tmp, output_path, params={}, inputs=resolved_paths)
         which atomically renames .tmp → output and writes .meta.json last.
      3. Replace _is_output_fresh() gate with artifact_fresh() (meta-aware).
    """

    def test_meta_json_written_after_stage(self, overture_parquet, tmp_path):
        """§2.2: after stage_density_extract, <output_path>.meta.json must exist.

        meta.json must contain:
          - 'params': {} (density stage has no named parameters)
          - 'inputs': [list of resolved source parquet paths from the glob]

        Fails RED because current stage_density_extract uses direct COPY (no finalize_artifact).
        """
        import json as _json
        output = tmp_path / "density.parquet"
        meta_path = tmp_path / "density.parquet.meta.json"

        stage_density_extract(overture_parquet, str(output), time.monotonic())

        assert output.exists(), "density.parquet must be written"
        assert meta_path.exists(), (
            f"density.parquet.meta.json must be written by finalize_artifact; "
            "fails RED because current stage uses direct COPY with no meta sidecar"
        )
        meta = _json.loads(meta_path.read_text())
        assert "params" in meta, (
            f"meta.json must have 'params' key; got keys: {list(meta)}"
        )
        assert "inputs" in meta, (
            f"meta.json must have 'inputs' key; got keys: {list(meta)}"
        )
        assert meta["params"] == {}, (
            f"density_extract params must be empty dict (no stage parameters); "
            f"got {meta['params']!r}"
        )
        assert isinstance(meta["inputs"], list) and len(meta["inputs"]) > 0, (
            f"meta.json 'inputs' must be a non-empty list of resolved parquet paths; "
            f"got {meta.get('inputs')!r}"
        )

    def test_stale_tmp_clobbered(self, overture_parquet, tmp_path):
        """§2.3 rule 1: a stale <output_path>.tmp is removed before building.

        After the call: output is correct AND meta.json exists.
        Current code already removes .tmp at stage start; this test adds the
        assertion that meta.json is also written.

        Fails RED because meta.json is not written by the current implementation.
        """
        output = tmp_path / "density.parquet"
        tmp_file = tmp_path / "density.parquet.tmp"
        meta_path = tmp_path / "density.parquet.meta.json"

        # Plant garbage .tmp that must be removed at stage start
        tmp_file.write_bytes(b"garbage data from a previous interrupted run")
        assert tmp_file.exists(), "Test setup: .tmp must exist before the call"

        stage_density_extract(overture_parquet, str(output), time.monotonic())

        assert output.exists(), "density.parquet must be written"
        assert not tmp_file.exists(), (
            ".tmp file must be cleaned up by stage_density_extract "
            "(current code already does this, but meta.json is still missing)"
        )
        assert meta_path.exists(), (
            "density.parquet.meta.json must exist after stage; "
            "fails RED because current implementation does not call finalize_artifact"
        )

    def test_freshness_meta_driven(self, overture_parquet, tmp_path):
        """§2.2: freshness must be driven by artifact_fresh(), not _is_output_fresh().

        artifact_fresh(output, resolved_inputs, {}) must return True after a successful
        stage_density_extract, enabling the second call to be a no-op.

        Fails RED because:
          - current code never writes .meta.json
          - artifact_fresh() returns False (missing meta)
          - the assertion that artifact_fresh returns True therefore fails
        """
        from garganorn.stages import artifact_fresh, _resolve_glob_paths

        output = tmp_path / "density.parquet"

        stage_density_extract(overture_parquet, str(output), time.monotonic())
        assert output.exists(), "density.parquet must be written"

        # Resolve the actual input file paths that finalize_artifact should record in meta.
        resolved_inputs = _resolve_glob_paths(overture_parquet)
        assert resolved_inputs, "overture_parquet fixture must resolve to at least one file"

        # artifact_fresh must return True for the just-built fresh artifact.
        # Fails RED because current code never writes .meta.json (artifact_fresh returns False).
        assert artifact_fresh(str(output), resolved_inputs, {}), (
            "artifact_fresh(density.parquet, resolved_inputs, {}) must return True "
            "after a successful stage_density_extract; "
            "fails RED because no .meta.json is written by the current implementation"
        )
