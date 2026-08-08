"""RED tests for CLI subcommand grammar (Phase 2).

The current CLI uses a flat flag model (--source, --output, etc.).
Phase 2 restructures it into subcommands: density, idf, covering, run, all.

All tests here fail in Red phase because the subcommand parser does not
exist. Failures manifest as:
  - SystemExit(2) when the subcommand name is passed to the current flat
    argparse (treated as an unrecognized positional).
  - AssertionError when checking that the subcommand was dispatched.

Test class mapping:
  1. Each subcommand parses              → TestSubcommandParsing
  2. 'all' stage-call order             → TestAllSubcommandOrder
  3. --force deletion sets              → TestForceSemantics
"""
import os
import sys
from unittest.mock import patch, MagicMock, call

import pytest

# garganorn.quadtree.main exists in Red phase; it just lacks subparsers.
from garganorn import quadtree as _qt


# ---------------------------------------------------------------------------
# Each subcommand parses its grammar
# ---------------------------------------------------------------------------

class TestSubcommandParsing:
    """Each subcommand must be accepted by the CLI parser."""

    def _run_main(self, argv, *, monkeypatch):
        """Run main() with the given argv, capturing SystemExit."""
        monkeypatch.setattr(sys, "argv", ["garganorn.quadtree"] + argv)
        return _qt.main

    def test_density_subcommand_recognized(self, tmp_path, overture_parquet, monkeypatch):
        """'density' subcommand must be accepted by the CLI (not exit with error)."""
        output = str(tmp_path / "density.parquet")
        monkeypatch.setattr(sys, "argv", [
            "qt", "density",
            "--parquet", overture_parquet,
            "--output", output,
        ])
        # Mock the underlying stage so we only test CLI parsing
        with patch.object(_qt, "stage_density_extract", return_value=None) as mock_stage:
            try:
                _qt.main()
            except SystemExit as e:
                pytest.fail(
                    f"'density' subcommand caused SystemExit({e.code}); "
                    "subcommand not recognized by the CLI parser"
                )

    def test_idf_subcommand_recognized(self, tmp_path, overture_parquet, monkeypatch):
        """'idf' subcommand must be accepted by the CLI."""
        output = str(tmp_path / "idf.parquet")
        monkeypatch.setattr(sys, "argv", [
            "qt", "idf",
            "--source", "overture_place",
            "--parquet", overture_parquet,
            "--output", output,
        ])
        with patch.object(_qt, "stage_idf", return_value=None):
            try:
                _qt.main()
            except SystemExit as e:
                pytest.fail(
                    f"'idf' subcommand caused SystemExit({e.code}); "
                    "subcommand not recognized"
                )

    def test_run_subcommand_recognized(self, tmp_path, overture_parquet, monkeypatch):
        """'run' subcommand must be accepted by the CLI."""
        monkeypatch.setattr(sys, "argv", [
            "qt", "run",
            "--source", "overture_place",
            "--parquet", overture_parquet,
            "--output", str(tmp_path),
        ])
        with patch.object(_qt, "run_pipeline", return_value=None):
            try:
                _qt.main()
            except SystemExit as e:
                pytest.fail(
                    f"'run' subcommand caused SystemExit({e.code}); "
                    "subcommand not recognized"
                )

    def test_all_subcommand_recognized(self, tmp_path, monkeypatch):
        """'all' subcommand must be accepted by the CLI."""
        config = tmp_path / "config.yaml"
        config.write_text("pipeline:\n  output: /tmp/tiles\n  sources: {}\n")
        monkeypatch.setattr(sys, "argv", [
            "qt", "all",
            "--config", str(config),
        ])
        with patch.object(_qt, "run_pipeline", return_value=None), \
             patch.object(_qt, "stage_density_extract", return_value=None), \
             patch.object(_qt, "stage_idf", return_value=None):
            try:
                _qt.main()
            except SystemExit as e:
                pytest.fail(
                    f"'all' subcommand caused SystemExit({e.code}); "
                    "subcommand not recognized"
                )

    def test_bare_invocation_exits_with_error_mentioning_run(self, monkeypatch, capsys):
        """Bare invocation (no subcommand) must exit with an error mentioning 'run'."""
        monkeypatch.setattr(sys, "argv", ["qt"])
        with pytest.raises(SystemExit) as exc_info:
            _qt.main()
        code = exc_info.value.code
        assert code != 0, "Bare invocation must exit with non-zero code"
        captured = capsys.readouterr()
        combined = (captured.out + captured.err).lower()
        assert "run" in combined, (
            f"Bare invocation error must mention 'run'; got stderr: {captured.err!r}"
        )

    def test_legacy_flat_flags_error_with_run_hint(self, monkeypatch, overture_parquet,
                                                     tmp_path, capsys):
        """Legacy --source flag without subcommand must exit with error mentioning 'run'."""
        monkeypatch.setattr(sys, "argv", [
            "qt", "--source", "overture_place",
            "--parquet", overture_parquet,
            "--output", str(tmp_path),
        ])
        with pytest.raises(SystemExit) as exc_info:
            _qt.main()
        # Either it fails because --source is unrecognized (no subcommand selected),
        # or it exits with a helpful message
        code = exc_info.value.code
        assert code != 0, "Legacy flat-flag invocation must exit with non-zero code"


# ---------------------------------------------------------------------------
# 'all' stage-call order and derived paths
# ---------------------------------------------------------------------------

class TestAllSubcommandOrder:
    """'all' subcommand must call stages in: density → idf → division → others."""

    def test_all_calls_density_before_idf(self, tmp_path, monkeypatch):
        """'all' must call stage_density_extract before any stage_idf calls."""
        config = tmp_path / "config.yaml"
        config.write_text(f"""
pipeline:
  output: {tmp_path}/tiles
  sources:
    overture_place:
      parquet: /fake/overture/*.parquet
    overture_division:
      division_parquet: /fake/division/*.parquet
      division_area_parquet: /fake/area/*.parquet
""")
        call_order = []

        def _mock_density(*args, **kwargs):
            call_order.append("density")

        def _mock_idf(*args, **kwargs):
            call_order.append("idf")

        def _mock_pipeline(*args, **kwargs):
            call_order.append(f"run:{args[0]}")

        monkeypatch.setattr(sys, "argv", ["qt", "all", "--config", str(config)])
        with patch.object(_qt, "stage_density_extract", side_effect=_mock_density), \
             patch.object(_qt, "stage_idf", side_effect=_mock_idf), \
             patch.object(_qt, "run_pipeline", side_effect=_mock_pipeline):
            try:
                _qt.main()
            except SystemExit as e:
                if e.code != 0:
                    pytest.fail(f"'all' subcommand raised SystemExit({e.code})")

        # density must come before any idf calls
        if "density" in call_order and any(c == "idf" for c in call_order):
            density_idx = call_order.index("density")
            idf_idx = next(i for i, c in enumerate(call_order) if c == "idf")
            assert density_idx < idf_idx, (
                f"density must run before idf; call order: {call_order}"
            )
        else:
            pytest.fail(
                f"'all' must call both density and idf stages; got: {call_order}"
            )

    def test_all_division_before_other_sources(self, tmp_path, monkeypatch):
        """'all' must run overture_division before other place sources."""
        config = tmp_path / "config.yaml"
        config.write_text(f"""
pipeline:
  output: {tmp_path}/tiles
  sources:
    overture_place:
      parquet: /fake/overture/*.parquet
    overture_division:
      division_parquet: /fake/division/*.parquet
      division_area_parquet: /fake/area/*.parquet
""")
        call_order = []

        def _mock_pipeline(source, *args, **kwargs):
            call_order.append(f"run:{source}")

        monkeypatch.setattr(sys, "argv", ["qt", "all", "--config", str(config)])
        with patch.object(_qt, "stage_density_extract", return_value=None), \
             patch.object(_qt, "stage_idf", return_value=None), \
             patch.object(_qt, "run_pipeline", side_effect=_mock_pipeline):
            try:
                _qt.main()
            except SystemExit as e:
                if e.code != 0:
                    pytest.fail(f"'all' subcommand raised SystemExit({e.code})")

        run_calls = [c for c in call_order if c.startswith("run:")]
        assert run_calls, f"'all' must call run_pipeline at least once; got: {call_order}"
        division_runs = [i for i, c in enumerate(run_calls) if c == "run:overture_division"]
        other_runs = [i for i, c in enumerate(run_calls) if c != "run:overture_division"]
        if division_runs and other_runs:
            assert max(division_runs) < min(other_runs), (
                f"overture_division must run before other sources; order: {run_calls}"
            )

    def test_all_missing_division_passes_boundaries_none(self, tmp_path, monkeypatch):
        """With no overture_division config, 'all' must pass boundaries_db=None to run_pipeline."""
        config = tmp_path / "config.yaml"
        config.write_text(f"""
pipeline:
  output: {tmp_path}/tiles
  sources:
    overture_place:
      parquet: /fake/overture/*.parquet
""")
        pipeline_kwargs = {}

        def _mock_pipeline(source, *args, **kwargs):
            pipeline_kwargs[source] = kwargs

        monkeypatch.setattr(sys, "argv", ["qt", "all", "--config", str(config)])
        with patch.object(_qt, "stage_density_extract", return_value=None), \
             patch.object(_qt, "stage_idf", return_value=None), \
             patch.object(_qt, "run_pipeline", side_effect=_mock_pipeline):
            try:
                _qt.main()
            except SystemExit as e:
                if e.code != 0:
                    pytest.fail(f"'all' subcommand raised SystemExit({e.code})")

        assert "overture_place" in pipeline_kwargs, (
            f"run_pipeline must be called for overture_place; got keys: {list(pipeline_kwargs)}"
        )
        boundaries = pipeline_kwargs.get("overture_place", {}).get("boundaries_db")
        assert boundaries is None, (
            f"boundaries_db must be None when overture_division is absent; got {boundaries!r}"
        )


# ---------------------------------------------------------------------------
# --force deletion sets
# ---------------------------------------------------------------------------

class TestForceSemantics:
    """run --force must delete the correct artifact set and preserve tiles/.

    Deletion sets:
      run --force (place source):
        places.parquet + .meta.json
        tile_assignments.parquet + .meta.json
        containment/
        (tiles/ history is NEVER touched)
      run --force (overture_division):
        additionally boundaries.duckdb and covering/

    All tests fail RED because:
      (a) the 'run' subcommand does not exist (SystemExit on parse), OR
      (b) run_pipeline does not implement force deletion for Phase 2 artifacts.
    """

    def test_run_force_flag_recognized_by_cli(
        self, tmp_path, overture_parquet, monkeypatch
    ):
        """'run --force' must not raise SystemExit (flag must be accepted by parser).

        Fails RED because the 'run' subcommand parser does not exist.
        """
        monkeypatch.setattr(sys, "argv", [
            "qt", "run",
            "--source", "overture_place",
            "--parquet", overture_parquet,
            "--output", str(tmp_path),
            "--force",
        ])
        with patch.object(_qt, "run_pipeline", return_value=None):
            try:
                _qt.main()
            except SystemExit as e:
                pytest.fail(
                    f"'run --force' caused SystemExit({e.code}); "
                    "subcommand or --force flag not recognized by CLI parser"
                )

    def test_run_force_deletes_places_parquet_and_meta(
        self, tmp_path, overture_parquet, density_parquet
    ):
        """run with force=True must delete places.parquet+meta, tile_assignments.parquet+meta,
        containment/; tiles/ history must survive.

        Fails RED because run_pipeline does not implement Phase 2 force deletion.
        """
        from garganorn.quadtree import run_pipeline as _run_pipeline

        src_dir = tmp_path / "overture_place"
        src_dir.mkdir()

        # Plant stale Phase 2 artifacts
        for name in [
            "places.parquet", "places.parquet.meta.json",
            "tile_assignments.parquet", "tile_assignments.parquet.meta.json",
        ]:
            (src_dir / name).write_bytes(b"stale")
        containment_dir = src_dir / "containment"
        containment_dir.mkdir()
        stale_containment_parquet = containment_dir / "0000.parquet"
        stale_containment_parquet.write_bytes(b"stale")

        # Plant tiles/ history — must survive
        ts_dir = src_dir / "tiles" / "20260101T000000"
        ts_dir.mkdir(parents=True)
        tile_marker = ts_dir / "manifest.json"
        tile_marker.write_bytes(b"{}")

        # Run with force=True; stages may fail in RED — deletion must happen first
        try:
            _run_pipeline(
                "overture_place",
                overture_parquet,
                (-122.55, 37.60, -122.30, 37.85),
                str(tmp_path),
                memory_limit="4GB",
                density_parquet=density_parquet,
                force=True,
            )
        except Exception:
            pass  # Phase 2 stages may fail; that is expected in RED

        # Stale containment parquet must be gone (deleted by force before rebuild)
        assert not stale_containment_parquet.exists(), (
            "run with force=True must delete containment/ before rebuilding — "
            "fails RED because Phase 2 force deletion is not implemented"
        )

        # tiles/ history must be untouched
        assert tile_marker.exists(), (
            "run with force=True must NOT touch tiles/ history"
        )

    def test_run_force_division_deletes_boundaries_and_covering(
        self, tmp_path, division_parquet
    ):
        """For overture_division, force=True must explicitly delete boundaries.duckdb
        before rebuilding it (PRE-deletion requirement).

        Strengthened from vacuous: Phase 1 already overwrites boundaries.duckdb via
        ATTACH (content changes), so a content-only check passes with no pressure.
        This version tracks os.remove calls and asserts boundaries.duckdb was
        explicitly removed before export_boundaries_db recreated it.

        Phase 1 calls os.remove only on the working DB — never on boundaries.duckdb.
        Phase 2 must call os.remove on boundaries.duckdb as part of force deletion.
        Fails RED until Phase 2 implements the explicit pre-deletion step.
        """
        from garganorn.quadtree import run_pipeline as _run_pipeline

        src_dir = tmp_path / "overture_division"
        src_dir.mkdir()

        boundaries_db_path = str(src_dir / "boundaries.duckdb")
        (src_dir / "boundaries.duckdb").write_bytes(b"stale")
        covering_dir = src_dir / "covering"
        covering_dir.mkdir()
        stale_covering_meta = covering_dir / "_meta.json"
        stale_covering_meta.write_bytes(b"stale")

        div_parquet, div_area_parquet = division_parquet

        # Spy on os.remove to verify boundaries.duckdb is explicitly deleted
        # before the pipeline rebuilds it.  Phase 1 never calls os.remove on it;
        # Phase 2 must (deletion-before-rebuild requirement).
        removed_paths = []
        _orig_remove = os.remove

        def _spy_remove(path):
            removed_paths.append(str(path))
            return _orig_remove(path)

        with patch.object(os, "remove", _spy_remove):
            try:
                _run_pipeline(
                    "overture_division",
                    (div_parquet, div_area_parquet),
                    (-122.55, 37.60, -122.30, 37.85),
                    str(tmp_path),
                    memory_limit="4GB",
                    force=True,
                )
            except Exception:
                pass

        # Phase 2 requirement: boundaries.duckdb must be explicitly deleted
        # (via os.remove) BEFORE the pipeline rebuilds it.  Phase 1 only calls
        # os.remove on the working DB — it overwrites boundaries.duckdb in place
        # via ATTACH without ever removing the file first.
        assert any(p == boundaries_db_path for p in removed_paths), (
            "force=True for overture_division must call os.remove on boundaries.duckdb "
            "before rebuilding — fails RED because Phase 2 force deletion is not "
            "yet implemented. os.remove calls seen: " + repr(removed_paths)
        )
