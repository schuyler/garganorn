"""Tests for run_pipeline(), write_manifest(), main() CLI, and atomic timestamped export."""

import gzip
import json
import os
import re
import sys
import textwrap
import time
from unittest.mock import patch

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Tests: run_pipeline() Python function
# ---------------------------------------------------------------------------

class TestRunPipeline:
    """Tests for garganorn.quadtree.run_pipeline().

    All tests fail at Red phase: garganorn.quadtree does not exist yet.
    """

    def test_import(self):
        """Importing run_pipeline must raise ImportError in Red phase."""
        from garganorn.quadtree import run_pipeline  # noqa: F401

    def test_fsq_pipeline_smoke(self, fsq_parquet, tmp_path):
        """End-to-end smoke test: at least one .json.gz and manifest.json produced."""
        try:
            from garganorn.quadtree import run_pipeline
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        output_dir = tmp_path / "pipeline_out"
        output_dir.mkdir()

        run_pipeline(
            "fsq",
            fsq_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
        )

        # At least one tile file under output_dir/fsq/current/
        fsq_dir = output_dir / "fsq"
        current_dir = fsq_dir / "current"
        gz_files = list(current_dir.rglob("*.json.gz")) if current_dir.exists() else []
        assert gz_files, (
            f"run_pipeline must write at least one .json.gz under {current_dir}"
        )

        # manifest.json must exist under current/
        manifest_path = current_dir / "manifest.json"
        assert manifest_path.exists(), (
            f"run_pipeline must write manifest.json at {manifest_path}"
        )
        with open(manifest_path) as fh:
            manifest = json.load(fh)
        assert "source" in manifest, "manifest.json missing 'source'"
        assert manifest["source"] == "fsq", (
            f"manifest source must be 'fsq'; got {manifest['source']!r}"
        )

        # No leftover .duckdb temp file (manifest.duckdb is expected)
        duckdb_files = [f for f in output_dir.rglob("*.duckdb") if f.name != "manifest.duckdb"]
        assert not duckdb_files, (
            f"run_pipeline must not leave .duckdb files behind: {duckdb_files}"
        )

    def test_fsq_manifest_db(self, fsq_parquet, tmp_path):
        """run_pipeline must write manifest.duckdb with record_tiles and metadata tables."""
        import duckdb as _duckdb
        from datetime import datetime
        try:
            from garganorn.quadtree import run_pipeline
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        output_dir = tmp_path / "manifest_db_out"
        output_dir.mkdir()

        run_pipeline(
            "fsq",
            fsq_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
        )

        fsq_dir = output_dir / "fsq"
        gz_files = list((fsq_dir / "current").rglob("*.json.gz")) if (fsq_dir / "current").exists() else []
        assert gz_files, f"run_pipeline must write at least one .json.gz under {fsq_dir / 'current'}"

        manifest_path = output_dir / "fsq" / "current" / "manifest.duckdb"
        assert manifest_path.exists(), f"manifest.duckdb must exist at {manifest_path}"

        con = _duckdb.connect(str(manifest_path), read_only=True)
        try:
            count = con.execute("SELECT COUNT(*) FROM record_tiles").fetchone()[0]
            assert count > 0, f"record_tiles must have rows; got {count}"

            rkeys = [r[0] for r in con.execute("SELECT rkey FROM record_tiles").fetchall()]
            assert all(rkeys), "all rkeys must be non-empty strings"

            tile_qks = [r[0] for r in con.execute("SELECT tile_qk FROM record_tiles").fetchall()]
            assert all(tile_qks), "all tile_qk values must be non-empty strings"
            assert all(qk.isdigit() for qk in tile_qks), (
                f"tile_qk values must be numeric quadkey strings; got {tile_qks[:5]!r}"
            )

            meta = con.execute("SELECT source, generated_at FROM metadata").fetchall()
            assert len(meta) == 1, f"metadata must have exactly one row; got {len(meta)}"
            source, generated_at = meta[0]
            assert source == "fsq", f"metadata source must be 'fsq'; got {source!r}"
            datetime.fromisoformat(generated_at)  # raises ValueError if not ISO 8601
        finally:
            con.close()

        leftover_dbs = [f for f in output_dir.rglob("*.duckdb") if f.name != "manifest.duckdb"]
        assert not leftover_dbs, (
            f"run_pipeline must not leave temp .duckdb files behind: {leftover_dbs}"
        )

    def test_overture_manifest_db(self, overture_parquet, tmp_path):
        """run_pipeline must write manifest.duckdb with record_tiles and metadata tables (Overture)."""
        import duckdb as _duckdb
        from datetime import datetime
        try:
            from garganorn.quadtree import run_pipeline
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        output_dir = tmp_path / "overture_manifest_db_out"
        output_dir.mkdir()

        run_pipeline(
            "overture",
            overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
        )

        ov_dir = output_dir / "overture"
        gz_files = list((ov_dir / "current").rglob("*.json.gz")) if (ov_dir / "current").exists() else []
        assert gz_files, f"run_pipeline must write at least one .json.gz under {ov_dir / 'current'}"

        manifest_path = ov_dir / "current" / "manifest.duckdb"
        assert manifest_path.exists(), f"manifest.duckdb must exist at {manifest_path}"

        con = _duckdb.connect(str(manifest_path), read_only=True)
        try:
            count = con.execute("SELECT COUNT(*) FROM record_tiles").fetchone()[0]
            assert count > 0, f"record_tiles must have rows; got {count}"

            rkeys = [r[0] for r in con.execute("SELECT rkey FROM record_tiles").fetchall()]
            assert all(rkeys), "all rkeys must be non-empty strings"

            tile_qks = [r[0] for r in con.execute("SELECT tile_qk FROM record_tiles").fetchall()]
            assert all(tile_qks), "all tile_qk values must be non-empty strings"
            assert all(qk.isdigit() for qk in tile_qks), (
                f"tile_qk values must be numeric quadkey strings; got {tile_qks[:5]!r}"
            )

            meta = con.execute("SELECT source, generated_at FROM metadata").fetchall()
            assert len(meta) == 1, f"metadata must have exactly one row; got {len(meta)}"
            source, generated_at = meta[0]
            assert source == "overture", f"metadata source must be 'overture'; got {source!r}"
            datetime.fromisoformat(generated_at)  # raises ValueError if not ISO 8601
        finally:
            con.close()

        leftover_dbs = [f for f in output_dir.rglob("*.duckdb") if f.name != "manifest.duckdb"]
        assert not leftover_dbs, (
            f"run_pipeline must not leave temp .duckdb files behind: {leftover_dbs}"
        )

    def test_osm_manifest_db(self, osm_parquet, tmp_path):
        """run_pipeline must write manifest.duckdb with record_tiles and metadata tables (OSM)."""
        import duckdb as _duckdb
        from datetime import datetime
        try:
            from garganorn.quadtree import run_pipeline
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        output_dir = tmp_path / "osm_manifest_db_out"
        output_dir.mkdir()

        run_pipeline(
            "osm",
            (osm_parquet["node"], osm_parquet["way"]),
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
        )

        osm_dir = output_dir / "osm"
        gz_files = list((osm_dir / "current").rglob("*.json.gz")) if (osm_dir / "current").exists() else []
        assert gz_files, f"run_pipeline must write at least one .json.gz under {osm_dir / 'current'}"

        manifest_path = osm_dir / "current" / "manifest.duckdb"
        assert manifest_path.exists(), f"manifest.duckdb must exist at {manifest_path}"

        con = _duckdb.connect(str(manifest_path), read_only=True)
        try:
            count = con.execute("SELECT COUNT(*) FROM record_tiles").fetchone()[0]
            assert count > 0, f"record_tiles must have rows; got {count}"

            rkeys = [r[0] for r in con.execute("SELECT rkey FROM record_tiles").fetchall()]
            assert all(rkeys), "all rkeys must be non-empty strings"

            tile_qks = [r[0] for r in con.execute("SELECT tile_qk FROM record_tiles").fetchall()]
            assert all(tile_qks), "all tile_qk values must be non-empty strings"
            assert all(qk.isdigit() for qk in tile_qks), (
                f"tile_qk values must be numeric quadkey strings; got {tile_qks[:5]!r}"
            )

            meta = con.execute("SELECT source, generated_at FROM metadata").fetchall()
            assert len(meta) == 1, f"metadata must have exactly one row; got {len(meta)}"
            source, generated_at = meta[0]
            assert source == "osm", f"metadata source must be 'osm'; got {source!r}"
            datetime.fromisoformat(generated_at)  # raises ValueError if not ISO 8601
        finally:
            con.close()

        leftover_dbs = [f for f in output_dir.rglob("*.duckdb") if f.name != "manifest.duckdb"]
        assert not leftover_dbs, (
            f"run_pipeline must not leave temp .duckdb files behind: {leftover_dbs}"
        )

    @pytest.mark.xfail(
        raises=(duckdb.IOException, duckdb.CatalogException),
        reason="OSM pipeline with nonexistent parquet raises DuckDB IO/Catalog error; "
               "test confirms tuple parquet_glob is unpacked without TypeError",
    )
    def test_osm_pipeline_parquet_is_tuple(self, tmp_path):
        """run_pipeline accepts a 2-tuple for parquet_glob (OSM node+way paths)."""
        try:
            from garganorn.quadtree import run_pipeline
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        output_dir = tmp_path / "osm_tuple_out"
        output_dir.mkdir()

        # Nonexistent paths — DuckDB will raise IOException or CatalogException,
        # but the function must not raise TypeError from failing to unpack the tuple.
        run_pipeline(
            "osm",
            ("/nonexistent/nodes/*.parquet", "/nonexistent/ways/*.parquet"),
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
        )


# ---------------------------------------------------------------------------
# Tests: write_manifest() Python function
# ---------------------------------------------------------------------------

class TestWriteManifest:
    """Tests for garganorn.quadtree.write_manifest().

    All tests fail at Red phase: garganorn.quadtree does not exist yet.
    """

    def test_import(self):
        """Importing write_manifest must raise ImportError in Red phase."""
        from garganorn.quadtree import write_manifest  # noqa: F401

    def test_creates_manifest_json(self, tmp_path):
        """write_manifest must create a manifest.json file."""
        try:
            from garganorn.quadtree import write_manifest
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        write_manifest({"023130": 42, "023131": 7}, str(tmp_path), "fsq")
        assert (tmp_path / "manifest.json").exists(), "manifest.json not found"

    def test_manifest_structure(self, tmp_path):
        """manifest.json must have 'source', 'generated_at', and 'quadkeys' fields."""
        try:
            from garganorn.quadtree import write_manifest
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        out_dir = tmp_path / "manifest_struct"
        out_dir.mkdir()
        write_manifest({"023130": 42}, str(out_dir), "fsq")
        with open(out_dir / "manifest.json") as fh:
            manifest = json.load(fh)
        assert "source" in manifest, f"manifest missing 'source'; keys={list(manifest)}"
        assert "generated_at" in manifest, (
            f"manifest missing 'generated_at'; keys={list(manifest)}"
        )
        assert "quadkeys" in manifest, (
            f"manifest missing 'quadkeys'; keys={list(manifest)}"
        )
        assert isinstance(manifest["quadkeys"], list), "quadkeys must be a list"
        assert manifest["source"] == "fsq", (
            f"source must be 'fsq'; got {manifest['source']!r}"
        )

    def test_quadkeys_sorted(self, tmp_path):
        """write_manifest must write quadkeys in sorted order regardless of input order."""
        try:
            from garganorn.quadtree import write_manifest
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        out_dir = tmp_path / "manifest_sorted"
        out_dir.mkdir()
        # Pass unsorted dict
        write_manifest(
            {"023133": 5, "023130": 42, "023132": 17, "023131": 7},
            str(out_dir),
            "fsq",
        )
        with open(out_dir / "manifest.json") as fh:
            manifest = json.load(fh)
        qkeys = manifest["quadkeys"]
        assert qkeys == sorted(qkeys), (
            f"quadkeys must be sorted; got {qkeys}"
        )


# ---------------------------------------------------------------------------
# Tests: main() CLI entry point
# ---------------------------------------------------------------------------

class TestQuadtreeMainCLI:
    """Tests for the main() CLI entry point in garganorn/quadtree.py."""

    # ------------------------------------------------------------------
    # Test 1: Parse all required arguments
    # ------------------------------------------------------------------

    def test_required_args_parsed(self, tmp_path):
        """main() must parse --source, --parquet, and --output correctly; bbox defaults to None."""
        from garganorn.quadtree import main

        output_dir = str(tmp_path / "tiles")

        argv = [
            "garganorn.quadtree",
            "--source", "fsq",
            "--parquet", "db/cache/fsq/*.parquet",
            "--output", output_dir,
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with patch("sys.argv", argv):
                main()

        mock_pipeline.assert_called_once()
        ca = mock_pipeline.call_args

        source_arg = ca.kwargs.get("source") if "source" in ca.kwargs else (ca.args[0] if len(ca.args) > 0 else None)
        parquet_arg = ca.kwargs.get("parquet_glob") if "parquet_glob" in ca.kwargs else (ca.args[1] if len(ca.args) > 1 else None)
        assert source_arg == "fsq", f"source must be 'fsq'; got {source_arg!r}. Full call: {ca}"
        assert parquet_arg == "db/cache/fsq/*.parquet", (
            f"parquet_glob must be 'db/cache/fsq/*.parquet'; got {parquet_arg!r}. Full call: {ca}"
        )

        bbox_arg = ca.kwargs.get("bbox") if "bbox" in ca.kwargs else (ca.args[2] if len(ca.args) > 2 else "NOT_PRESENT")
        assert bbox_arg is None, (
            f"bbox must be None when --bbox is omitted; got {bbox_arg!r}. Full call: {ca}"
        )

        output_dir_arg = ca.kwargs.get("output_dir") if "output_dir" in ca.kwargs else (ca.args[3] if len(ca.args) > 3 else None)
        assert str(output_dir_arg) == str(tmp_path / "tiles"), (
            f"output_dir must be {str(tmp_path / 'tiles')!r}; got {output_dir_arg!r}. Full call: {ca}"
        )

    # ------------------------------------------------------------------
    # Test 2: --memory-limit and --max-per-tile CLI values are used
    # ------------------------------------------------------------------

    def test_cli_memory_and_max_per_tile_used(self, tmp_path):
        """CLI --memory-limit and --max-per-tile must be forwarded to run_pipeline."""
        from garganorn.quadtree import main

        output_dir = str(tmp_path / "tiles")

        argv = [
            "garganorn.quadtree",
            "--source", "overture",
            "--parquet", "db/cache/overture/*.parquet",
            "--bbox", "-122.55", "37.60", "-122.30", "37.85",
            "--output", output_dir,
            "--memory-limit", "32GB",
            "--max-per-tile", "500",
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with patch("sys.argv", argv):
                main()

        mock_pipeline.assert_called_once()
        ca = mock_pipeline.call_args

        memory_limit = ca.kwargs.get("memory_limit") if "memory_limit" in ca.kwargs else (ca.args[4] if len(ca.args) > 4 else None)
        max_per_tile = ca.kwargs.get("max_per_tile") if "max_per_tile" in ca.kwargs else (ca.args[5] if len(ca.args) > 5 else None)

        assert memory_limit == "32GB", (
            f"memory_limit must be '32GB'; got {memory_limit!r}. Full call: {ca}"
        )
        assert max_per_tile == 500, (
            f"max_per_tile must be 500 (int); got {max_per_tile!r}. Full call: {ca}"
        )

    # ------------------------------------------------------------------
    # Test 3: --config loads defaults; CLI flags override config values
    # ------------------------------------------------------------------

    def test_config_defaults_and_cli_override(self, tmp_path):
        """--config must set tiles.memory_limit/max_per_tile as defaults; CLI overrides."""
        from garganorn.quadtree import main

        config_path = tmp_path / "config.yaml"
        config_path.write_text(textwrap.dedent("""\
            repo: places.atgeo.org
            tiles:
              memory_limit: "16GB"
              max_per_tile: 250
        """))

        output_dir = str(tmp_path / "tiles_override")

        argv = [
            "garganorn.quadtree",
            "--source", "fsq",
            "--parquet", "db/cache/fsq/*.parquet",
            "--bbox", "-74.1", "40.6", "-73.8", "40.9",
            "--output", output_dir,
            "--config", str(config_path),
            "--memory-limit", "64GB",
            "--max-per-tile", "2000",
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with patch("sys.argv", argv):
                main()

        mock_pipeline.assert_called_once()
        ca = mock_pipeline.call_args

        memory_limit = ca.kwargs.get("memory_limit") if "memory_limit" in ca.kwargs else (ca.args[4] if len(ca.args) > 4 else None)
        max_per_tile = ca.kwargs.get("max_per_tile") if "max_per_tile" in ca.kwargs else (ca.args[5] if len(ca.args) > 5 else None)

        assert memory_limit == "64GB", (
            f"CLI --memory-limit '64GB' must override config '16GB'; got {memory_limit!r}"
        )
        assert max_per_tile == 2000, (
            f"CLI --max-per-tile 2000 must override config 250; got {max_per_tile!r}"
        )

    def test_config_defaults_used_when_no_cli_flags(self, tmp_path):
        """When --config is set but CLI flags are absent, config values are used."""
        from garganorn.quadtree import main

        config_path = tmp_path / "config.yaml"
        config_path.write_text(textwrap.dedent("""\
            repo: places.atgeo.org
            tiles:
              memory_limit: "16GB"
              max_per_tile: 250
        """))

        output_dir = str(tmp_path / "tiles_config_only")

        argv = [
            "garganorn.quadtree",
            "--source", "fsq",
            "--parquet", "db/cache/fsq/*.parquet",
            "--bbox", "-74.1", "40.6", "-73.8", "40.9",
            "--output", output_dir,
            "--config", str(config_path),
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with patch("sys.argv", argv):
                main()

        mock_pipeline.assert_called_once()
        ca = mock_pipeline.call_args

        memory_limit = ca.kwargs.get("memory_limit") if "memory_limit" in ca.kwargs else (ca.args[4] if len(ca.args) > 4 else None)
        max_per_tile = ca.kwargs.get("max_per_tile") if "max_per_tile" in ca.kwargs else (ca.args[5] if len(ca.args) > 5 else None)

        assert memory_limit == "16GB", (
            f"Config tiles.memory_limit '16GB' must be used when CLI flag absent; got {memory_limit!r}"
        )
        assert max_per_tile == 250, (
            f"Config tiles.max_per_tile 250 must be used when CLI flag absent; got {max_per_tile!r}"
        )

    # ------------------------------------------------------------------
    # Test 4: Falls back to "48GB" / 1000 when neither CLI nor config
    # ------------------------------------------------------------------

    def test_hardcoded_defaults_when_no_config_or_cli(self, tmp_path):
        """With no --config and no --memory-limit/--max-per-tile, must use '48GB'/1000."""
        from garganorn.quadtree import main

        output_dir = str(tmp_path / "tiles_defaults")

        argv = [
            "garganorn.quadtree",
            "--source", "fsq",
            "--parquet", "db/cache/fsq/*.parquet",
            "--bbox", "-74.1", "40.6", "-73.8", "40.9",
            "--output", output_dir,
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with patch("sys.argv", argv):
                main()

        mock_pipeline.assert_called_once()
        ca = mock_pipeline.call_args

        memory_limit = ca.kwargs.get("memory_limit") if "memory_limit" in ca.kwargs else (ca.args[4] if len(ca.args) > 4 else None)
        max_per_tile = ca.kwargs.get("max_per_tile") if "max_per_tile" in ca.kwargs else (ca.args[5] if len(ca.args) > 5 else None)

        assert memory_limit == "48GB", f"Default memory_limit must be '48GB'; got {memory_limit!r}"
        assert max_per_tile == 1000, f"Default max_per_tile must be 1000; got {max_per_tile!r}"

    # ------------------------------------------------------------------
    # Test 5: Missing required args cause SystemExit
    # ------------------------------------------------------------------

    def test_missing_source_causes_systemexit(self, tmp_path):
        """Omitting required --source must cause argparse to call sys.exit."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "--parquet", "db/cache/fsq/*.parquet",
            "--bbox", "-74.1", "40.6", "-73.8", "40.9",
            "--output", str(tmp_path / "tiles"),
        ]

        with pytest.raises(SystemExit):
            with patch("sys.argv", argv):
                main()

    def test_missing_parquet_non_osm_causes_systemexit(self, tmp_path):
        """Omitting --parquet for non-OSM source must cause sys.exit."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "--source", "fsq",
            "--output", str(tmp_path / "tiles"),
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with pytest.raises(SystemExit):
                with patch("sys.argv", argv):
                    main()
        mock_pipeline.assert_not_called()

    def test_missing_output_causes_systemexit(self, tmp_path):
        """Omitting required --output must cause argparse to call sys.exit."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "--source", "fsq",
            "--parquet", "db/cache/fsq/*.parquet",
            "--bbox", "-74.1", "40.6", "-73.8", "40.9",
        ]

        with pytest.raises(SystemExit):
            with patch("sys.argv", argv):
                main()

    # ------------------------------------------------------------------
    # Test 6: OSM source uses --parquet-dir to derive node/way globs
    # ------------------------------------------------------------------

    def test_osm_parquet_dir_derives_node_way_paths(self, tmp_path):
        """--source osm --parquet-dir /some/dir must forward type=node/type=way globs as tuple."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "--source", "osm",
            "--parquet-dir", "/some/dir",
            "--output", str(tmp_path / "tiles_osm"),
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with patch("sys.argv", argv):
                main()

        mock_pipeline.assert_called_once()
        ca = mock_pipeline.call_args
        parquet_arg = ca.kwargs.get("parquet_glob") if "parquet_glob" in ca.kwargs else (ca.args[1] if len(ca.args) > 1 else None)

        assert isinstance(parquet_arg, tuple) and len(parquet_arg) == 2, (
            f"For OSM, parquet_glob must be a 2-element tuple; got {parquet_arg!r}"
        )
        node_glob, way_glob = parquet_arg
        assert node_glob == "/some/dir/type=node/*.parquet", f"node glob wrong: {node_glob!r}"
        assert way_glob == "/some/dir/type=way/*.parquet", f"way glob wrong: {way_glob!r}"

        bbox_arg = ca.kwargs.get("bbox") if "bbox" in ca.kwargs else (ca.args[2] if len(ca.args) > 2 else "NOT_PRESENT")
        assert bbox_arg is None, f"bbox must be None when --bbox is omitted; got {bbox_arg!r}"

    def test_osm_missing_parquet_dir_causes_systemexit(self, tmp_path):
        """--source osm without --parquet-dir must cause SystemExit."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "--source", "osm",
            "--output", str(tmp_path / "tiles_osm_bad"),
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with pytest.raises(SystemExit):
                with patch("sys.argv", argv):
                    main()
        mock_pipeline.assert_not_called()

    def test_osm_parquet_arg_rejected(self, tmp_path):
        """--source osm with --parquet (not --parquet-dir) must cause SystemExit."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "--source", "osm",
            "--parquet", "db/cache/osm/*.parquet",
            "--output", str(tmp_path / "tiles_osm_bad"),
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with pytest.raises(SystemExit):
                with patch("sys.argv", argv):
                    main()
        mock_pipeline.assert_not_called()

    def test_non_osm_parquet_dir_rejected(self, tmp_path):
        """--source fsq with --parquet-dir must cause SystemExit."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "--source", "fsq",
            "--parquet-dir", "/some/dir",
            "--output", str(tmp_path / "tiles_fsq_bad"),
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with pytest.raises(SystemExit):
                with patch("sys.argv", argv):
                    main()
        mock_pipeline.assert_not_called()

    def test_bbox_optional_defaults_to_none(self, tmp_path):
        """Omitting --bbox must result in run_pipeline being called with bbox=None."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "--source", "fsq",
            "--parquet", "db/cache/fsq/*.parquet",
            "--output", str(tmp_path / "tiles_no_bbox"),
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with patch("sys.argv", argv):
                main()

        mock_pipeline.assert_called_once()
        ca = mock_pipeline.call_args
        bbox_arg = ca.kwargs.get("bbox") if "bbox" in ca.kwargs else (ca.args[2] if len(ca.args) > 2 else "NOT_PRESENT")
        assert bbox_arg is None, f"bbox must be None when --bbox is omitted; got {bbox_arg!r}"

    def test_bbox_provided_passed_as_tuple(self, tmp_path):
        """--bbox values must be forwarded to run_pipeline as a 4-float tuple."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "--source", "fsq",
            "--parquet", "db/cache/fsq/*.parquet",
            "--bbox", "-74.1", "40.6", "-73.8", "40.9",
            "--output", str(tmp_path / "tiles_with_bbox"),
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with patch("sys.argv", argv):
                main()

        mock_pipeline.assert_called_once()
        ca = mock_pipeline.call_args
        bbox_arg = ca.kwargs.get("bbox") if "bbox" in ca.kwargs else (ca.args[2] if len(ca.args) > 2 else None)
        assert isinstance(bbox_arg, tuple), f"bbox must be a tuple; got {type(bbox_arg)!r}"
        assert bbox_arg is not None and len(bbox_arg) == 4, (
            f"bbox must be a 4-element sequence; got {bbox_arg!r}"
        )
        xmin, ymin, xmax, ymax = bbox_arg
        assert abs(xmin - (-74.1)) < 1e-9
        assert abs(ymin - 40.6) < 1e-9
        assert abs(xmax - (-73.8)) < 1e-9
        assert abs(ymax - 40.9) < 1e-9

    def test_osm_parquet_dir_with_bbox(self, tmp_path):
        """--source osm --parquet-dir with --bbox must forward both parquet globs and bbox tuple."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "--source", "osm",
            "--parquet-dir", "/some/dir",
            "--bbox", "-74.1", "40.6", "-73.8", "40.9",
            "--output", str(tmp_path / "tiles_osm_bbox"),
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with patch("sys.argv", argv):
                main()

        mock_pipeline.assert_called_once()
        ca = mock_pipeline.call_args

        parquet_arg = ca.kwargs.get("parquet_glob") if "parquet_glob" in ca.kwargs else (ca.args[1] if len(ca.args) > 1 else None)
        assert isinstance(parquet_arg, tuple) and len(parquet_arg) == 2, (
            f"For OSM, parquet_glob must be a 2-element tuple; got {parquet_arg!r}"
        )
        node_glob, way_glob = parquet_arg
        assert node_glob == "/some/dir/type=node/*.parquet"
        assert way_glob == "/some/dir/type=way/*.parquet"

        bbox_arg = ca.kwargs.get("bbox") if "bbox" in ca.kwargs else (ca.args[2] if len(ca.args) > 2 else None)
        assert isinstance(bbox_arg, tuple) and len(bbox_arg) == 4, (
            f"bbox must be a 4-element tuple; got {bbox_arg!r}"
        )
        xmin, ymin, xmax, ymax = bbox_arg
        assert abs(xmin - (-74.1)) < 1e-9
        assert abs(ymin - 40.6) < 1e-9
        assert abs(xmax - (-73.8)) < 1e-9
        assert abs(ymax - 40.9) < 1e-9


# ---------------------------------------------------------------------------
# Tests: run_pipeline resilience — pre-existing `places` table
# ---------------------------------------------------------------------------

class TestRunPipelineStaleDb:
    """run_pipeline must succeed even when a `places` table already exists in the
    work db (simulating a crashed prior run that left the table behind).

    Each test:
      1. Creates the work db directory and pre-populates the work db file with a
         `places` table — the same situation a failed previous run would leave.
      2. Calls run_pipeline with the real parquet fixtures and a small bbox.
      3. Asserts the call completes without raising an exception.

    These tests FAIL in the Red phase because the import SQL files do not yet
    contain `DROP TABLE IF EXISTS places;`, so DuckDB raises CatalogException
    when it tries to CREATE TABLE places on the second run.
    """

    def test_fsq_pipeline_succeeds_with_stale_places_table(self, fsq_parquet, tmp_path):
        """FSQ pipeline must not raise when a stale `places` table exists in the work db."""
        from garganorn.quadtree import run_pipeline

        output_dir = tmp_path / "fsq_restart_out"
        output_dir.mkdir()

        # Pre-create the work db directory and insert a stale `places` table,
        # simulating a prior crashed run.
        work_db_dir = output_dir / "fsq"
        work_db_dir.mkdir()
        work_db_path = work_db_dir / ".fsq_work.duckdb"
        stale_con = duckdb.connect(str(work_db_path))
        stale_con.execute("CREATE TABLE places (id VARCHAR, name VARCHAR)")
        stale_con.close()

        # Second run must succeed without raising CatalogException.
        run_pipeline(
            "fsq",
            fsq_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
        )

    def test_overture_pipeline_succeeds_with_stale_places_table(self, overture_parquet, tmp_path):
        """Overture pipeline must not raise when a stale `places` table exists in the work db."""
        from garganorn.quadtree import run_pipeline

        output_dir = tmp_path / "overture_restart_out"
        output_dir.mkdir()

        # Pre-create the work db directory and insert a stale `places` table.
        work_db_dir = output_dir / "overture"
        work_db_dir.mkdir()
        work_db_path = work_db_dir / ".overture_work.duckdb"
        stale_con = duckdb.connect(str(work_db_path))
        stale_con.execute("CREATE TABLE places (id VARCHAR, name VARCHAR)")
        stale_con.close()

        # Second run must succeed without raising CatalogException.
        run_pipeline(
            "overture",
            overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
        )

    def test_osm_pipeline_succeeds_with_stale_places_table(self, osm_parquet, tmp_path):
        """OSM pipeline must not raise when a stale `places` table exists in the work db."""
        from garganorn.quadtree import run_pipeline

        output_dir = tmp_path / "osm_restart_out"
        output_dir.mkdir()

        # Pre-create the work db directory and insert a stale `places` table.
        work_db_dir = output_dir / "osm"
        work_db_dir.mkdir()
        work_db_path = work_db_dir / ".osm_work.duckdb"
        stale_con = duckdb.connect(str(work_db_path))
        stale_con.execute("CREATE TABLE places (id VARCHAR, name VARCHAR)")
        stale_con.close()

        # Second run must succeed without raising CatalogException.
        run_pipeline(
            "osm",
            (osm_parquet["node"], osm_parquet["way"]),
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
        )


# ---------------------------------------------------------------------------
# Tests: atomic tile export with timestamped directories (Red phase)
# ---------------------------------------------------------------------------

class TestTimestampedExport:
    """run_pipeline must write tiles into a timestamped subdirectory and maintain
    a `current` symlink pointing to the latest run.

    All tests FAIL in the Red phase because run_pipeline writes directly into
    output_dir/source/ without creating a timestamped subdirectory or symlink.
    """

    _TIMESTAMP_RE = re.compile(r"^\d{8}T\d{6}$")

    def _run(self, fsq_parquet, output_dir):
        try:
            from garganorn.quadtree import run_pipeline
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")
        run_pipeline(
            "fsq",
            fsq_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
        )

    def test_creates_timestamped_subdir(self, fsq_parquet, tmp_path):
        """run_pipeline must create a timestamped subdirectory under output_dir/fsq/."""
        output_dir = tmp_path / "ts_subdir_out"
        output_dir.mkdir()
        self._run(fsq_parquet, output_dir)

        fsq_dir = output_dir / "fsq"
        assert fsq_dir.exists(), f"output_dir/fsq/ must exist; got {list(output_dir.iterdir())}"

        ts_dirs = [
            d for d in fsq_dir.iterdir()
            if d.is_dir() and not d.is_symlink() and self._TIMESTAMP_RE.match(d.name)
        ]
        assert ts_dirs, (
            f"run_pipeline must create a timestamped subdir matching {self._TIMESTAMP_RE.pattern!r} "
            f"under {fsq_dir}; found: {[d.name for d in fsq_dir.iterdir()]}"
        )

        gz_files = list(ts_dirs[0].rglob("*.json.gz"))
        assert gz_files, (
            f"Timestamped dir {ts_dirs[0]} must contain at least one .json.gz file"
        )

    def test_creates_current_symlink(self, fsq_parquet, tmp_path):
        """run_pipeline must create a `current` symlink under output_dir/fsq/."""
        output_dir = tmp_path / "ts_symlink_out"
        output_dir.mkdir()
        self._run(fsq_parquet, output_dir)

        fsq_dir = output_dir / "fsq"
        current = fsq_dir / "current"
        assert os.path.islink(str(current)), (
            f"output_dir/fsq/current must be a symlink; got {list(fsq_dir.iterdir())}"
        )

        target = os.readlink(str(current))
        assert self._TIMESTAMP_RE.match(target), (
            f"current symlink target must match {self._TIMESTAMP_RE.pattern!r}; got {target!r}"
        )

    def test_second_run_swaps_symlink(self, fsq_parquet, tmp_path):
        """A second run must update `current` to point to the new timestamped dir."""
        output_dir = tmp_path / "ts_swap_out"
        output_dir.mkdir()
        self._run(fsq_parquet, output_dir)

        fsq_dir = output_dir / "fsq"
        first_target = os.readlink(str(fsq_dir / "current"))

        time.sleep(1)
        self._run(fsq_parquet, output_dir)

        second_target = os.readlink(str(fsq_dir / "current"))
        assert second_target != first_target, (
            f"After second run, current symlink must point to a different dir; "
            f"both runs produced {second_target!r}"
        )

        # First run's dir must still exist (kept as previous)
        first_dir = fsq_dir / first_target
        assert first_dir.exists(), (
            f"First run's dir {first_dir} must still exist after second run"
        )

        # Tiles accessible through current
        gz_files = list((fsq_dir / "current").rglob("*.json.gz"))
        assert gz_files, "Tiles must be accessible through the current symlink after second run"

    def test_third_run_cleans_oldest(self, fsq_parquet, tmp_path):
        """A third run must delete the oldest timestamped dir, keeping only 2."""
        output_dir = tmp_path / "ts_clean_out"
        output_dir.mkdir()
        self._run(fsq_parquet, output_dir)

        fsq_dir = output_dir / "fsq"
        first_target = os.readlink(str(fsq_dir / "current"))

        time.sleep(1)
        self._run(fsq_parquet, output_dir)

        time.sleep(1)
        self._run(fsq_parquet, output_dir)

        ts_dirs = [
            d for d in fsq_dir.iterdir()
            if d.is_dir() and not d.is_symlink() and self._TIMESTAMP_RE.match(d.name)
        ]
        assert len(ts_dirs) == 2, (
            f"After three runs, exactly 2 timestamped dirs must remain; "
            f"found {len(ts_dirs)}: {[d.name for d in ts_dirs]}"
        )

        first_dir = fsq_dir / first_target
        assert not first_dir.exists(), (
            f"First run's dir {first_dir} must have been deleted after third run"
        )

    def test_tiles_accessible_through_current(self, fsq_parquet, tmp_path):
        """Tiles must be readable via the current symlink."""
        output_dir = tmp_path / "ts_readable_out"
        output_dir.mkdir()
        self._run(fsq_parquet, output_dir)

        current_dir = output_dir / "fsq" / "current"
        gz_files = list(current_dir.rglob("*.json.gz"))
        assert gz_files, f"No .json.gz files found under {current_dir}"

        for gz_file in gz_files:
            try:
                with gzip.open(gz_file, "rb") as fh:
                    fh.read(1)
            except Exception as exc:
                pytest.fail(f"Could not read {gz_file} via gzip.open: {exc}")

    def test_manifest_accessible_through_current(self, fsq_parquet, tmp_path):
        """manifest.json and manifest.duckdb must be accessible via current symlink."""
        output_dir = tmp_path / "ts_manifest_out"
        output_dir.mkdir()
        self._run(fsq_parquet, output_dir)

        current_dir = output_dir / "fsq" / "current"

        manifest_json = current_dir / "manifest.json"
        assert manifest_json.exists(), f"manifest.json must exist at {manifest_json}"
        with open(manifest_json) as fh:
            data = json.load(fh)
        assert isinstance(data, dict), f"manifest.json must be valid JSON dict; got {type(data)}"

        manifest_db = current_dir / "manifest.duckdb"
        assert manifest_db.exists(), f"manifest.duckdb must exist at {manifest_db}"

    def test_failed_run_leaves_partial_dir(self, fsq_parquet, tmp_path):
        """A failed run must leave partial timestamped dir for debugging, and not swap the symlink."""
        try:
            from garganorn.quadtree import run_pipeline
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        output_dir = tmp_path / "ts_cleanup_out"
        output_dir.mkdir()

        with pytest.raises(RuntimeError, match="boom"):
            with patch("garganorn.quadtree.export_tiles", side_effect=RuntimeError("boom")):
                run_pipeline(
                    "fsq",
                    fsq_parquet,
                    (-122.55, 37.60, -122.30, 37.85),
                    str(output_dir),
                    memory_limit="4GB",
                    max_per_tile=100,
                )

        fsq_dir = output_dir / "fsq"
        assert fsq_dir.exists(), "source dir must exist even after failed run"

        # Partial timestamped dir should be left for debugging
        ts_dirs = [
            d for d in fsq_dir.iterdir()
            if d.is_dir() and not d.is_symlink() and self._TIMESTAMP_RE.match(d.name)
        ]
        assert len(ts_dirs) == 1, (
            f"Failed run must leave exactly one partial timestamped dir for debugging; "
            f"found: {[d.name for d in ts_dirs]}"
        )

        # current symlink must NOT exist — swap didn't happen
        current_link = fsq_dir / "current"
        assert not current_link.is_symlink(), (
            "current symlink must not exist after a failed run"
        )

        # Work .duckdb must be present in the partial dir for debugging
        partial_dir = ts_dirs[0]
        work_dbs = list(partial_dir.glob(".*_work.duckdb"))
        assert work_dbs, (
            f"Failed run must leave the work .duckdb in the partial dir for debugging; "
            f"found nothing under {partial_dir}"
        )

    def test_work_db_in_timestamped_dir(self, fsq_parquet, tmp_path):
        """No .duckdb files should remain under output_dir/fsq/ except manifest.duckdb."""
        output_dir = tmp_path / "ts_workdb_out"
        output_dir.mkdir()
        self._run(fsq_parquet, output_dir)

        fsq_dir = output_dir / "fsq"
        leftover_dbs = [
            f for f in fsq_dir.rglob("*.duckdb")
            if f.name != "manifest.duckdb"
        ]
        assert not leftover_dbs, (
            f"run_pipeline must not leave non-manifest .duckdb files under {fsq_dir}: "
            f"{leftover_dbs}"
        )
