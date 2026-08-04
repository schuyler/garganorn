"""Red tests for §7.1 — artifact_fresh / finalize_artifact helpers (Phase 2).

Tests in TestArtifactFresh and TestFinalizeArtifact fail with
AttributeError until artifact_fresh and finalize_artifact are implemented
in garganorn/stages.py.

§7.1 item mapping:
  1. artifact_fresh truth table        → TestArtifactFresh
  2. finalize_artifact behavior        → TestFinalizeArtifact
  3. Stale-.tmp clobber                → TestStaleTmpClobber
  4. Directory-artifact recovery       → covered by existing covering tests; §3.7 adds containment
  5. DuckDB construct pinning          → TestDuckDBConstructPinning (passes immediately)
  6. Sort pins                         → TestDensitySortPin, TestIdfSortPin
"""
import json
import os
import pathlib
import time

import duckdb
import pytest

import garganorn.stages as _stages


# ---------------------------------------------------------------------------
# §7.1.1 artifact_fresh truth table
# ---------------------------------------------------------------------------

class TestArtifactFresh:
    """Truth table for artifact_fresh.

    Every test here fails with AttributeError: module 'garganorn.stages'
    has no attribute 'artifact_fresh' until production code implements it.
    """

    def _write_parquet(self, path):
        """Write a minimal parquet file (1 row)."""
        con = duckdb.connect()
        con.execute(f"COPY (SELECT 1 AS x) TO '{path}' (FORMAT PARQUET)")
        con.close()

    def _write_meta(self, meta_path, params, inputs, newer_than=None):
        """Write a .meta.json sidecar, optionally touching it to be newer than an mtime."""
        meta = {
            "stage": "test",
            "params": params,
            "inputs": inputs,
            "stats": None,
            "generated_at": "2026-01-01T00:00:00Z",
        }
        pathlib.Path(meta_path).write_text(json.dumps(meta))
        if newer_than is not None:
            t = newer_than + 0.01
            os.utime(meta_path, (t, t))

    # --- stale cases ---

    def test_missing_artifact_is_stale(self, tmp_path):
        """artifact_fresh returns False when the artifact does not exist."""
        artifact = str(tmp_path / "artifact.parquet")
        result = _stages.artifact_fresh(artifact, [], {})
        assert result is False

    def test_missing_meta_is_stale(self, tmp_path):
        """artifact_fresh returns False when .meta.json is absent."""
        artifact = str(tmp_path / "artifact.parquet")
        self._write_parquet(artifact)
        result = _stages.artifact_fresh(artifact, [], {})
        assert result is False

    def test_unparsable_meta_is_stale(self, tmp_path):
        """artifact_fresh returns False when .meta.json is not valid JSON."""
        artifact = str(tmp_path / "artifact.parquet")
        self._write_parquet(artifact)
        pathlib.Path(str(artifact) + ".meta.json").write_text("not-json{{")
        result = _stages.artifact_fresh(artifact, [], {})
        assert result is False

    def test_params_mismatch_is_stale(self, tmp_path):
        """artifact_fresh returns False when supplied params differ from meta['params']."""
        artifact = str(tmp_path / "artifact.parquet")
        self._write_parquet(artifact)
        art_mtime = os.path.getmtime(artifact)
        self._write_meta(str(artifact) + ".meta.json", {"bbox": [1, 2, 3, 4]}, [],
                         newer_than=art_mtime)
        result = _stages.artifact_fresh(artifact, [], {"bbox": [0, 0, 0, 0]})
        assert result is False

    def test_input_newer_than_meta_is_stale(self, tmp_path):
        """artifact_fresh returns False when an input is newer than meta."""
        # Write artifact, then meta, then input (newest)
        artifact = str(tmp_path / "artifact.parquet")
        self._write_parquet(artifact)
        time.sleep(0.02)
        meta_path = str(artifact) + ".meta.json"
        input_path = str(tmp_path / "input.parquet")
        self._write_meta(meta_path, {}, [input_path])
        time.sleep(0.02)
        self._write_parquet(input_path)
        result = _stages.artifact_fresh(artifact, [input_path], {})
        assert result is False

    def test_meta_older_than_artifact_is_stale(self, tmp_path):
        """artifact_fresh returns False when artifact is newer than meta (crash gap)."""
        artifact = str(tmp_path / "artifact.parquet")
        meta_path = str(artifact) + ".meta.json"
        # Write meta first
        self._write_meta(meta_path, {}, [])
        time.sleep(0.02)
        # Then write artifact (newer than meta — simulates crash between rename and meta write)
        self._write_parquet(artifact)
        result = _stages.artifact_fresh(artifact, [], {})
        assert result is False

    def test_equal_mtime_input_meta_is_stale(self, tmp_path):
        """Equal mtime of input vs meta counts as stale (mirrors _is_output_fresh)."""
        artifact = str(tmp_path / "artifact.parquet")
        input_path = str(tmp_path / "input.parquet")
        self._write_parquet(artifact)
        self._write_parquet(input_path)
        meta_path = str(artifact) + ".meta.json"
        self._write_meta(meta_path, {}, [input_path])
        # Force input and meta to share the exact same mtime
        now = time.time()
        os.utime(input_path, (now, now))
        os.utime(meta_path, (now, now))
        result = _stages.artifact_fresh(artifact, [input_path], {})
        assert result is False

    def test_resolved_inputs_mismatch_is_stale(self, tmp_path):
        """artifact_fresh returns False when resolved inputs differ from meta['inputs']."""
        input_a = str(tmp_path / "a.parquet")
        input_b = str(tmp_path / "b.parquet")
        self._write_parquet(input_a)
        time.sleep(0.02)
        artifact = str(tmp_path / "artifact.parquet")
        self._write_parquet(artifact)
        time.sleep(0.02)
        meta_path = str(artifact) + ".meta.json"
        # Meta records input_a, caller supplies input_b
        self._write_meta(meta_path, {}, [input_a])
        self._write_parquet(input_b)
        result = _stages.artifact_fresh(artifact, [input_b], {})
        assert result is False

    # --- happy path ---

    def test_happy_path_is_fresh(self, tmp_path):
        """artifact_fresh returns True when everything is in order."""
        input_path = str(tmp_path / "input.parquet")
        self._write_parquet(input_path)
        time.sleep(0.05)
        artifact = str(tmp_path / "artifact.parquet")
        self._write_parquet(artifact)
        time.sleep(0.05)
        meta_path = str(artifact) + ".meta.json"
        self._write_meta(meta_path, {"key": "val"}, [input_path])
        result = _stages.artifact_fresh(artifact, [input_path], {"key": "val"})
        assert result is True


# ---------------------------------------------------------------------------
# §7.1.2 finalize_artifact behavior
# ---------------------------------------------------------------------------

class TestFinalizeArtifact:
    """finalize_artifact: artifact and meta land atomically.

    Fails with AttributeError: module 'garganorn.stages' has no attribute
    'finalize_artifact' until production code implements it.
    """

    def _write_tmp(self, tmp_path, name="artifact.parquet.tmp"):
        """Write a minimal parquet .tmp file and return its path."""
        tmp_file = str(tmp_path / name)
        con = duckdb.connect()
        con.execute(f"COPY (SELECT 42 AS x) TO '{tmp_file}' (FORMAT PARQUET)")
        con.close()
        return tmp_file

    def test_artifact_and_meta_created(self, tmp_path):
        """finalize_artifact must create the artifact and its .meta.json sidecar."""
        tmp_file = self._write_tmp(tmp_path)
        artifact = str(tmp_path / "artifact.parquet")
        _stages.finalize_artifact(tmp_file, artifact, {"k": "v"})
        assert os.path.exists(artifact), "artifact must exist after finalize_artifact"
        assert os.path.exists(artifact + ".meta.json"), ".meta.json must exist after finalize_artifact"

    def test_meta_schema(self, tmp_path):
        """meta must contain stage, params, inputs, stats, generated_at."""
        tmp_file = self._write_tmp(tmp_path)
        artifact = str(tmp_path / "artifact.parquet")
        _stages.finalize_artifact(tmp_file, artifact, {"param1": 42})
        meta = json.loads(pathlib.Path(artifact + ".meta.json").read_text())
        for key in ("stage", "params", "inputs", "stats", "generated_at"):
            assert key in meta, f"meta missing key: {key!r}"
        assert meta["params"] == {"param1": 42}

    def test_meta_mtime_ge_artifact_mtime(self, tmp_path):
        """meta mtime must be >= artifact mtime (meta written after rename)."""
        tmp_file = self._write_tmp(tmp_path)
        artifact = str(tmp_path / "artifact.parquet")
        _stages.finalize_artifact(tmp_file, artifact, {})
        art_mtime = os.path.getmtime(artifact)
        meta_mtime = os.path.getmtime(artifact + ".meta.json")
        assert meta_mtime >= art_mtime, (
            f"meta mtime ({meta_mtime}) must be >= artifact mtime ({art_mtime})"
        )

    def test_tmp_gone_after_finalize(self, tmp_path):
        """The .tmp file must not exist after finalize_artifact completes."""
        tmp_file = self._write_tmp(tmp_path)
        artifact = str(tmp_path / "artifact.parquet")
        _stages.finalize_artifact(tmp_file, artifact, {})
        assert not os.path.exists(tmp_file), ".tmp must be gone after finalize_artifact"

    def test_artifact_is_readable_parquet(self, tmp_path):
        """The artifact produced by finalize_artifact must be readable parquet."""
        tmp_file = self._write_tmp(tmp_path)
        artifact = str(tmp_path / "artifact.parquet")
        _stages.finalize_artifact(tmp_file, artifact, {})
        con = duckdb.connect()
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{artifact}')").fetchone()[0]
        con.close()
        assert count == 1, "finalized artifact must contain the expected row"


# ---------------------------------------------------------------------------
# §7.1.3 Stale-.tmp clobber
# ---------------------------------------------------------------------------

class TestStaleTmpClobber:
    """stage_density_extract must delete a stale .tmp before building.

    In Red phase the stage does not delete stale .tmp files, so
    the assertion on the .tmp's non-existence fails.
    """

    def test_stale_tmp_deleted_before_build(self, tmp_path, overture_parquet):
        """A pre-existing garbage .tmp must be deleted before the stage writes its output."""
        output = str(tmp_path / "density.parquet")
        tmp_file = output + ".tmp"
        pathlib.Path(tmp_file).write_bytes(b"garbage-not-a-parquet")
        _stages.stage_density_extract(overture_parquet, output, time.monotonic(), force=True)
        # After a successful build, the .tmp must have been deleted by the stage
        assert not os.path.exists(tmp_file), (
            "stage_density_extract must delete stale .tmp before building; "
            f".tmp still present at {tmp_file}"
        )


# ---------------------------------------------------------------------------
# §7.1.5 DuckDB construct pinning (these tests pass immediately)
# ---------------------------------------------------------------------------

class TestDuckDBConstructPinning:
    """Smoke-test DuckDB constructs used in Phase 2 SQL on the running interpreter.

    These tests pass regardless of production code state — they document
    the DuckDB syntax that both 1.2.1 (pytest venv) and 1.5.1 (app .venv)
    must support.
    """

    def test_attach_read_only_form(self, tmp_path):
        """ATTACH 'path' AS x (READ_ONLY) must parse on current DuckDB."""
        db_path = str(tmp_path / "test.duckdb")
        con = duckdb.connect(db_path)
        con.execute("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1)")
        con.close()
        con2 = duckdb.connect()
        con2.execute(f"ATTACH '{db_path}' AS ro_test (READ_ONLY)")
        count = con2.execute("SELECT COUNT(*) FROM ro_test.t").fetchone()[0]
        con2.close()
        assert count == 1

    def test_copy_parquet_with_compression(self, tmp_path):
        """COPY (SELECT ...) TO path (FORMAT PARQUET, COMPRESSION ZSTD) must parse."""
        out_path = str(tmp_path / "out.parquet")
        con = duckdb.connect()
        con.execute(
            f"COPY (SELECT 1 AS x, 'hello' AS y) TO '{out_path}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        con.close()
        assert os.path.exists(out_path)

    def test_read_parquet_list_form(self, tmp_path):
        """read_parquet(['a', 'b']) list form must work on current DuckDB."""
        p1 = str(tmp_path / "a.parquet")
        p2 = str(tmp_path / "b.parquet")
        con = duckdb.connect()
        con.execute(f"COPY (SELECT 1 AS x) TO '{p1}' (FORMAT PARQUET)")
        con.execute(f"COPY (SELECT 2 AS x) TO '{p2}' (FORMAT PARQUET)")
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet(['{p1}', '{p2}'])").fetchone()[0]
        con.close()
        assert count == 2

    def test_copy_order_by_nulls_last(self, tmp_path):
        """ORDER BY ... NULLS LAST inside COPY must parse on current DuckDB."""
        out_path = str(tmp_path / "sorted.parquet")
        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT x FROM (VALUES (NULL::VARCHAR), ('zzz'), ('aaa')) t(x)
                ORDER BY x NULLS LAST
            ) TO '{out_path}' (FORMAT PARQUET)
        """)
        con.close()
        con2 = duckdb.connect()
        rows = [r[0] for r in con2.execute(f"SELECT x FROM read_parquet('{out_path}')").fetchall()]
        con2.close()
        assert rows == ['aaa', 'zzz', None], f"NULLS LAST not preserved: {rows}"
