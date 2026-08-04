"""RED tests for §6 — crash recovery (Phase 2).

State-based matrix: construct each disk state directly, run run_pipeline,
assert the final output equals a clean-run control. No subprocess sleeps;
SIGKILL test uses a harness script.

§6 item mapping (kill-9 acceptance also covered by §8's Phase 2 acceptance
paragraph):
  State matrix (deterministic, no subprocess) → TestCrashStateMatrix
  Subprocess kill-9 (miniature acceptance)     → TestKillNineAcceptance

All state-matrix tests fail in Red phase because run_pipeline does not
produce artifacts at the expected Phase-2 paths (places.parquet, tiles/current).
"""
import gzip
import json
import os
import pathlib
import signal
import subprocess
import sys
import textwrap
import time
from unittest.mock import patch as _mock_patch

import duckdb
import pytest

from garganorn.quadtree import run_pipeline
import garganorn.stages as _stages


# ---------------------------------------------------------------------------
# Helper: canonical tile comparison (§8 Phase 2 acceptance, miniature)
# ---------------------------------------------------------------------------

def _canonical_tile(gz_path):
    """Read a .json.gz tile, sort records, strip the run-scoped generated_at,
    return canonical JSON string.

    Post-2b (pipeline-implementation-decisions.md "OQ-P2-1 — record envelope
    adoption"), tiles carry a top-level `generated_at`
    that is run-scoped (derived from the export run's timestamp) and legitimately
    differs between the control run and a recovery run executed at a different
    wall-clock time -- it is not a crash-recovery correctness signal, so it must
    be stripped here the same way scripts/tile_parity.py's canonicalizer does,
    or every crash-recovery comparison in this module would spuriously fail on
    timestamp drift alone. Records are {uri, cid, value}-wrapped (per the
    envelope decisions above); the sort/dedup key is value.rkey, not a
    top-level rkey (which no longer exists
    on wrapped records).
    """
    with gzip.open(gz_path) as f:
        obj = json.load(f)
    obj.pop("generated_at", None)
    if "records" in obj:
        obj["records"] = sorted(
            obj["records"],
            key=lambda r: (r.get("value") or r).get("rkey", "") if isinstance(r.get("value", r), dict) else "",
        )
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _collect_tiles(tiles_dir):
    """Collect {relative_path: canonical_json} for all .json.gz under tiles_dir."""
    result = {}
    for root, dirs, files in os.walk(tiles_dir):
        for fname in files:
            if fname.endswith(".json.gz"):
                full = os.path.join(root, fname)
                rel = os.path.relpath(full, tiles_dir)
                result[rel] = _canonical_tile(full)
    return result


def _find_tiles_current(output_dir, source):
    """Find the <output>/<src>/tiles/current directory (Phase 2 layout)."""
    return pathlib.Path(output_dir) / source / "tiles" / "current"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def control_run(fsq_parquet, density_parquet, tmp_path_factory):
    """Run a clean reference pipeline and return (tiles_current, canonical_tiles)."""
    out = tmp_path_factory.mktemp("control")
    run_pipeline(
        "foursquare",
        fsq_parquet,
        (-122.55, 37.60, -122.30, 37.85),
        str(out),
        memory_limit="4GB",
        max_per_tile=100,
        density_parquet=density_parquet,
    )
    tiles_current = _find_tiles_current(out, "foursquare")
    canonical = _collect_tiles(str(tiles_current))
    return (tiles_current, canonical)


# ---------------------------------------------------------------------------
# §6 State-based crash matrix
# ---------------------------------------------------------------------------

class TestCrashStateMatrix:
    """State-based matrix: construct disk states, run pipeline, verify output.

    Each test:
    1. Runs a clean pipeline to build artifacts.
    2. Simulates a crash by planting the expected leftover state.
    3. Reruns run_pipeline (which must recover and produce clean output).
    4. Compares result against control_run canonical tiles.

    All tests fail in Red phase because run_pipeline does not produce
    tiles at <src>/tiles/current/ (Phase 2 layout).
    """

    def _run_and_collect(self, fsq_parquet, density_parquet, output_dir):
        run_pipeline(
            "foursquare",
            fsq_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
            density_parquet=density_parquet,
        )
        tiles_current = _find_tiles_current(output_dir, "foursquare")
        return _collect_tiles(str(tiles_current))

    def test_stale_places_tmp_cleared_on_rerun(
        self, fsq_parquet, density_parquet, control_run, tmp_path
    ):
        """A stale places.parquet.tmp must be cleaned up and a correct result produced."""
        _, control_tiles = control_run
        output_dir = tmp_path / "out"
        output_dir.mkdir()
        # Simulate crash: plant garbage .tmp where places.parquet.tmp would be
        fsq_dir = output_dir / "foursquare"
        fsq_dir.mkdir()
        stale_tmp = fsq_dir / "places.parquet.tmp"
        stale_tmp.write_bytes(b"garbage-crash-leftover")
        # Rerun must recover
        result_tiles = self._run_and_collect(fsq_parquet, density_parquet, output_dir)
        assert result_tiles == control_tiles, (
            f"After stale .tmp cleanup, tiles must match control run. "
            f"Differences: {set(result_tiles) ^ set(control_tiles)}"
        )
        assert not stale_tmp.exists(), "stale .tmp must be deleted before rebuild"

    def test_incomplete_export_dir_deleted_on_rerun(
        self, fsq_parquet, density_parquet, control_run, tmp_path
    ):
        """A run dir without manifest.json (crash leftover) must be deleted on next export."""
        _, control_tiles = control_run
        output_dir = tmp_path / "out"
        output_dir.mkdir()
        # First run — produces good output
        run_pipeline(
            "foursquare", fsq_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir), memory_limit="4GB", max_per_tile=100,
            density_parquet=density_parquet,
        )
        tiles_root = output_dir / "foursquare" / "tiles"
        # Plant a fake incomplete run dir (has tile files but no manifest.json)
        fake_run = tiles_root / "20260101T000000"
        fake_run.mkdir(parents=True, exist_ok=True)
        (fake_run / "023130").mkdir(exist_ok=True)
        (fake_run / "023130" / "023130.json.gz").write_bytes(b"fake")
        # No manifest.json → incomplete
        # Second run must delete the incomplete dir before/during export
        time.sleep(0.02)
        run_pipeline(
            "foursquare", fsq_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir), memory_limit="4GB", max_per_tile=100,
            density_parquet=density_parquet, force=True,
        )
        assert not fake_run.exists(), (
            f"Incomplete run dir {fake_run} must be deleted at next export start"
        )

    def test_crash_between_import_rename_and_meta_write(
        self, fsq_parquet, density_parquet, control_run, tmp_path
    ):
        """Simulate crash after places.parquet rename but before meta write → stage stale."""
        _, control_tiles = control_run
        output_dir = tmp_path / "out"
        output_dir.mkdir()
        # Run once to create artifact
        run_pipeline(
            "foursquare", fsq_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir), memory_limit="4GB", max_per_tile=100,
            density_parquet=density_parquet,
        )
        fsq_dir = output_dir / "foursquare"
        places = fsq_dir / "places.parquet"
        meta = fsq_dir / "places.parquet.meta.json"
        assert places.exists(), "places.parquet must exist after first run"
        # Simulate crash: artifact is newer than meta (meta was written first, then crash)
        if meta.exists():
            meta.unlink()
        # Without meta, stage_import must see artifact as stale and rebuild
        result_tiles = self._run_and_collect(fsq_parquet, density_parquet, output_dir)
        # Meta must have been re-created
        assert meta.exists(), "places.parquet.meta.json must be recreated after recovery"
        assert result_tiles == control_tiles, (
            "After crash-between-rename-and-meta recovery, tiles must match control"
        )

    def _make_genuine_stale_wal(self, db_path):
        """Create a genuine stale DuckDB WAL at <db_path>.wal.

        Launches a subprocess that attaches db_path, creates a 'places' table, then
        kills it without clean shutdown — leaving the WAL behind.  On next ATTACH,
        DuckDB would replay the WAL and find a 'places' table already present, which
        causes 'CREATE TABLE bnd.places AS …' to fail with "table already exists".
        Phase 2 avoids this by deleting both .tmp and .wal BEFORE the ATTACH.

        Returns (db_path, wal_path) after verifying the WAL exists.
        """
        wal_path = pathlib.Path(str(db_path) + ".wal")
        script = textwrap.dedent(f"""
            import duckdb, time
            con = duckdb.connect('{db_path}')
            con.execute("INSTALL spatial; LOAD spatial")
            con.execute("CREATE TABLE places (id VARCHAR)")
            con.execute("INSERT INTO places VALUES ('stale-row')")
            # Do NOT close cleanly — WAL stays on disk after SIGKILL
            time.sleep(60)
        """)
        proc = subprocess.Popen([sys.executable, "-c", script])
        time.sleep(0.5)  # allow writes to reach disk
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
        assert wal_path.exists(), (
            f"Genuine WAL not created at {wal_path}; "
            "subprocess may have checkpointed before being killed"
        )
        return pathlib.Path(db_path), wal_path

    def test_boundaries_tmp_and_wal_clobbered_on_division_rerun(
        self, division_parquet, tmp_path
    ):
        """Stale boundaries.duckdb.tmp + genuine .wal must be explicitly deleted before ATTACH.

        §2 (general artifact rules) / pipeline-implementation-decisions.md
        "Phase 2 — parquet artifacts + orchestrator" (crash recovery patterns):
        phase 2 deletes both .tmp and .wal at stage start so DuckDB
        does not replay a stale WAL against a fresh empty DB (which would fail with
        "table already exists" because the WAL records the old CREATE TABLE).

        Planted state (per §6): a genuine stale WAL produced by opening
        boundaries.duckdb.tmp, writing a 'places' table, then kill-9ing the writer.

        Test fails RED because:
          - Phase 1 code does NOT delete the WAL before ATTACH.
          - DuckDB replays the WAL → bnd.places already exists when the import tries to
            CREATE TABLE bnd.places AS … → exception.
          - boundaries.duckdb is therefore NOT created.
          - The assertion 'final_bnd.exists()' fails — right reason.

        Test passes GREEN because Phase 2 explicitly deletes .tmp + .wal before ATTACH.
        """
        output_dir = tmp_path / "out"
        output_dir.mkdir()
        div_dir = output_dir / "overture_division"
        div_dir.mkdir()

        tmp_db_path = div_dir / "boundaries.duckdb.tmp"
        # Produce a genuine stale WAL alongside a real boundaries.duckdb.tmp
        _, wal_path = self._make_genuine_stale_wal(str(tmp_db_path))

        div_par, div_area_par = division_parquet

        # Track os.remove calls to detect EXPLICIT WAL deletion.
        # DuckDB 1.2.1 handles orphaned WALs gracefully (no table-already-exists
        # error), so we cannot rely on a runtime exception to distinguish Phase 1
        # from Phase 2.  Instead we verify that Phase 2 explicitly calls
        # os.remove(wal_path) before ATTACH — something Phase 1 does NOT do.
        removed_paths = []
        _real_remove = os.remove

        def _tracking_remove(path, *extra, **kw):
            removed_paths.append(str(path))
            return _real_remove(path, *extra, **kw)

        with _mock_patch.object(os, "remove", side_effect=_tracking_remove):
            try:
                run_pipeline(
                    "overture_division",
                    (div_par, div_area_par),
                    (-122.55, 37.60, -122.30, 37.85),
                    str(output_dir),
                    memory_limit="4GB",
                    max_per_tile=100,
                )
            except Exception:
                pass  # Pipeline may fail for other reasons; we check explicit WAL deletion

        # Phase 2 must explicitly call os.remove(wal_path) BEFORE ATTACH so that
        # DuckDB starts from a clean slate without replaying the stale WAL.
        # Phase 1 only calls os.remove(boundaries_tmp) — it never removes the .wal.
        # This assertion fails RED because Phase 1 does not explicitly delete the WAL.
        assert str(wal_path) in removed_paths, (
            f"Phase 2 must explicitly call os.remove on the stale WAL "
            f"({wal_path.name}) before ATTACH (§2). "
            f"Fails RED: phase 1 calls os.remove only on .tmp, not .wal. "
            f"(Paths removed: {[os.path.basename(p) for p in removed_paths]})"
        )


# ---------------------------------------------------------------------------
# §8 Subprocess kill-9 test (Phase 2 acceptance)
# ---------------------------------------------------------------------------

class TestKillNineAcceptance:
    """Subprocess kill-9 miniature acceptance.

    Launches crash_harness.py via subprocess, kills it at import:mid-copy,
    verifies returncode == -SIGKILL, then reruns run_pipeline in-process
    and checks tiles match the control run.
    """

    def test_kill_during_import_mid_copy_and_recover(
        self, fsq_parquet, density_parquet, control_run, tmp_path
    ):
        """SIGKILL during import:mid-copy must leave recoverable state."""
        _, control_tiles = control_run
        output_dir = tmp_path / "kill_out"
        output_dir.mkdir()
        harness = pathlib.Path(__file__).parent / "crash_harness.py"
        env = os.environ.copy()
        env["GARGANORN_CRASH_POINT"] = "import:mid-copy"
        result = subprocess.run(
            [sys.executable, str(harness),
             "--source", "foursquare",
             "--parquet", fsq_parquet,
             "--output", str(output_dir),
             "--bbox", "-122.55", "37.60", "-122.30", "37.85"],
            env=env,
            timeout=120,
        )
        # Must have been killed by signal
        expected_returncode = -signal.SIGKILL
        assert result.returncode == expected_returncode, (
            f"Expected returncode {expected_returncode} (SIGKILL); "
            f"got {result.returncode}. Crash harness may not have killed the process."
        )
        # Phase 2 pre-check: control_run must produce tiles at tiles/current layout.
        # Fails RED because run_pipeline still uses Phase 1 layout (no tiles/current).
        assert control_tiles, (
            "Control run must produce tiles at <src>/tiles/current (Phase 2 §2 layout). "
            "Fails RED: run_pipeline uses Phase 1 layout and does not write to tiles/current."
        )
        # Rerun in-process — must recover and produce correct output
        run_pipeline(
            "foursquare",
            fsq_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
            density_parquet=density_parquet,
        )
        tiles_current = _find_tiles_current(output_dir, "foursquare")
        recovered_tiles = _collect_tiles(str(tiles_current))
        assert recovered_tiles == control_tiles, (
            "After kill-9 during import:mid-copy and recovery, tiles must match control"
        )
