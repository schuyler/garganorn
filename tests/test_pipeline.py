"""Tests for run_pipeline(), write_manifest(), main() CLI, and atomic timestamped export."""

import gzip
import json
import logging
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
    """Tests for garganorn.quadtree.run_pipeline()."""

    def test_import(self):
        """run_pipeline is importable from garganorn.quadtree."""
        from garganorn.quadtree import run_pipeline  # noqa: F401

    def test_overture_pipeline_smoke(self, overture_parquet, density_parquet, tmp_path):
        """End-to-end smoke test: at least one .json.gz and manifest.json produced."""
        try:
            from garganorn.quadtree import run_pipeline
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        output_dir = tmp_path / "pipeline_out"
        output_dir.mkdir()

        run_pipeline(
            "overture_place",
            overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
            density_parquet=density_parquet,
        )

        # At least one tile file under output_dir/overture_place/tiles/current/
        ov_dir = output_dir / "overture_place"
        current_dir = ov_dir / "tiles" / "current"
        gz_files = list(current_dir.rglob("*.json.gz")) if current_dir.exists() else []
        assert gz_files, (
            f"run_pipeline must write at least one .json.gz under {current_dir}"
        )

        # manifest.json must exist under tiles/current/
        manifest_path = current_dir / "manifest.json"
        assert manifest_path.exists(), (
            f"run_pipeline must write manifest.json at {manifest_path}"
        )
        with open(manifest_path) as fh:
            manifest = json.load(fh)
        assert "generated_at" in manifest, "manifest.json missing 'generated_at'"

        # No leftover .duckdb temp file (manifest.duckdb is expected)
        duckdb_files = [f for f in output_dir.rglob("*.duckdb") if f.name != "manifest.duckdb"]
        assert not duckdb_files, (
            f"run_pipeline must not leave .duckdb files behind: {duckdb_files}"
        )

    def test_overture_manifest_db(self, overture_parquet, density_parquet, tmp_path):
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
            "overture_place",
            overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
            density_parquet=density_parquet,
        )

        ov_dir = output_dir / "overture_place"
        gz_files = list((ov_dir / "tiles" / "current").rglob("*.json.gz")) if (ov_dir / "tiles" / "current").exists() else []
        assert gz_files, f"run_pipeline must write at least one .json.gz under {ov_dir / 'tiles' / 'current'}"

        manifest_path = ov_dir / "tiles" / "current" / "manifest.duckdb"
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
            assert source == "overture_place", f"metadata source must be 'overture'; got {source!r}"
            datetime.fromisoformat(generated_at)  # raises ValueError if not ISO 8601
        finally:
            con.close()

        leftover_dbs = [f for f in output_dir.rglob("*.duckdb") if f.name != "manifest.duckdb"]
        assert not leftover_dbs, (
            f"run_pipeline must not leave temp .duckdb files behind: {leftover_dbs}"
        )

    def test_osm_manifest_db(self, osm_parquet, density_parquet, tmp_path):
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
            (osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"]),
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
            density_parquet=density_parquet,
        )

        osm_dir = output_dir / "osm"
        gz_files = list((osm_dir / "tiles" / "current").rglob("*.json.gz")) if (osm_dir / "tiles" / "current").exists() else []
        assert gz_files, f"run_pipeline must write at least one .json.gz under {osm_dir / 'tiles' / 'current'}"

        manifest_path = osm_dir / "tiles" / "current" / "manifest.duckdb"
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

    def test_osm_pipeline_parquet_is_tuple(self, tmp_path):
        """run_pipeline accepts a 3-tuple for parquet_glob (OSM node+way+relation paths)."""
        try:
            from garganorn.quadtree import run_pipeline
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        output_dir = tmp_path / "osm_tuple_out"
        output_dir.mkdir()

        # Nonexistent paths — the configured-glob preflight check raises
        # RuntimeError naming the unmatched pattern, but the function must
        # not raise TypeError from failing to unpack the tuple.
        with pytest.raises(RuntimeError, match=re.escape("/nonexistent/nodes/*.parquet")):
            run_pipeline(
                "osm",
                ("/nonexistent/nodes/*.parquet", "/nonexistent/ways/*.parquet",
                 "/nonexistent/relations/*.parquet"),
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

    write_manifest(output_dir, *, generated_at) -- generated_at is a required
    keyword-only arg (no default) -- and manifest.json's field set is exactly
    {generated_at}. Fixed generated_at value matches the convention used
    elsewhere in this suite.
    """

    _GENERATED_AT = "2026-07-09T18:00:00Z"

    def test_import(self):
        """write_manifest is importable from garganorn.quadtree."""
        from garganorn.quadtree import write_manifest  # noqa: F401

    def test_creates_manifest_json(self, tmp_path):
        """write_manifest must create a manifest.json file."""
        try:
            from garganorn.quadtree import write_manifest
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        write_manifest(str(tmp_path), generated_at=self._GENERATED_AT)
        assert (tmp_path / "manifest.json").exists(), "manifest.json not found"

    def test_manifest_structure(self, tmp_path):
        """manifest.json must match the manifest field set exactly."""
        try:
            from garganorn.quadtree import write_manifest
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        out_dir = tmp_path / "manifest_struct"
        out_dir.mkdir()
        write_manifest(str(out_dir), generated_at=self._GENERATED_AT)
        with open(out_dir / "manifest.json") as fh:
            manifest = json.load(fh)
        expected_keys = {"generated_at"}
        assert set(manifest.keys()) == expected_keys, (
            f"manifest.json must match the manifest field set exactly; "
            f"got {sorted(manifest.keys())}, expected {sorted(expected_keys)}"
        )
        assert manifest["generated_at"] == self._GENERATED_AT, (
            f"generated_at must be the passed-in run timestamp; "
            f"got {manifest['generated_at']!r}"
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
            "run",
            "--source", "overture_place",
            "--parquet", "db/cache/overture_place/*.parquet",
            "--output", output_dir,
        ]

        with patch("garganorn.quadtree.run_pipeline") as mock_pipeline:
            with patch("sys.argv", argv):
                main()

        mock_pipeline.assert_called_once()
        ca = mock_pipeline.call_args

        source_arg = ca.kwargs.get("source") if "source" in ca.kwargs else (ca.args[0] if len(ca.args) > 0 else None)
        parquet_arg = ca.kwargs.get("parquet_glob") if "parquet_glob" in ca.kwargs else (ca.args[1] if len(ca.args) > 1 else None)
        assert source_arg == "overture_place", f"source must be 'overture_place'; got {source_arg!r}. Full call: {ca}"
        assert parquet_arg == "db/cache/overture_place/*.parquet", (
            f"parquet_glob must be 'db/cache/overture_place/*.parquet'; got {parquet_arg!r}. Full call: {ca}"
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
            "run",
            "--source", "overture_place",
            "--parquet", "db/cache/overture_place/*.parquet",
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
        """--config must set pipeline.memory_limit/max_per_tile as defaults; CLI overrides."""
        from garganorn.quadtree import main

        config_path = tmp_path / "config.yaml"
        config_path.write_text(textwrap.dedent("""\
            repo: places.atgeo.org
            pipeline:
              memory_limit: "16GB"
              max_per_tile: 250
        """))

        output_dir = str(tmp_path / "tiles_override")

        argv = [
            "garganorn.quadtree",
            "run",
            "--source", "overture_place",
            "--parquet", "db/cache/overture_place/*.parquet",
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
        """When --config is set but CLI flags are absent, pipeline: config values are used."""
        from garganorn.quadtree import main

        config_path = tmp_path / "config.yaml"
        config_path.write_text(textwrap.dedent("""\
            repo: places.atgeo.org
            pipeline:
              memory_limit: "16GB"
              max_per_tile: 250
        """))

        output_dir = str(tmp_path / "tiles_config_only")

        argv = [
            "garganorn.quadtree",
            "run",
            "--source", "overture_place",
            "--parquet", "db/cache/overture_place/*.parquet",
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
            f"Config pipeline.memory_limit '16GB' must be used when CLI flag absent; got {memory_limit!r}"
        )
        assert max_per_tile == 250, (
            f"Config pipeline.max_per_tile 250 must be used when CLI flag absent; got {max_per_tile!r}"
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
            "run",
            "--source", "overture_place",
            "--parquet", "db/cache/overture_place/*.parquet",
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
            "--parquet", "db/cache/overture_place/*.parquet",
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
            "--source", "overture_place",
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
            "--source", "overture_place",
            "--parquet", "db/cache/overture_place/*.parquet",
            "--bbox", "-74.1", "40.6", "-73.8", "40.9",
        ]

        with pytest.raises(SystemExit):
            with patch("sys.argv", argv):
                main()

    # ------------------------------------------------------------------
    # Test 6: OSM source uses --parquet-dir to derive node/way globs
    # ------------------------------------------------------------------

    def test_osm_parquet_dir_derives_node_way_paths(self, tmp_path):
        """--source osm --parquet-dir /some/dir must forward type=node/type=way/type=relation globs as tuple."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "run",
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

        assert isinstance(parquet_arg, tuple) and len(parquet_arg) == 3, (
            f"For OSM, parquet_glob must be a 3-element tuple; got {parquet_arg!r}"
        )
        node_glob, way_glob, relation_glob = parquet_arg
        assert node_glob == "/some/dir/type=node/*.parquet", f"node glob wrong: {node_glob!r}"
        assert way_glob == "/some/dir/type=way/*.parquet", f"way glob wrong: {way_glob!r}"
        assert relation_glob == "/some/dir/type=relation/*.parquet", f"relation glob wrong: {relation_glob!r}"

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
        """--source overture_place with --parquet-dir must cause SystemExit."""
        from garganorn.quadtree import main

        argv = [
            "garganorn.quadtree",
            "--source", "overture_place",
            "--parquet-dir", "/some/dir",
            "--output", str(tmp_path / "tiles_overture_bad"),
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
            "run",
            "--source", "overture_place",
            "--parquet", "db/cache/overture_place/*.parquet",
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
            "run",
            "--source", "overture_place",
            "--parquet", "db/cache/overture_place/*.parquet",
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
            "run",
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
        assert isinstance(parquet_arg, tuple) and len(parquet_arg) == 3, (
            f"For OSM, parquet_glob must be a 3-element tuple; got {parquet_arg!r}"
        )
        node_glob, way_glob, relation_glob = parquet_arg
        assert node_glob == "/some/dir/type=node/*.parquet"
        assert way_glob == "/some/dir/type=way/*.parquet"
        assert relation_glob == "/some/dir/type=relation/*.parquet"

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

    Both import SQL files open with `DROP TABLE IF EXISTS places;`, so a
    stale table left by a crashed run does not raise CatalogException on
    the next CREATE TABLE places.
    """

    def test_overture_pipeline_succeeds_with_stale_places_table(self, overture_parquet, density_parquet, tmp_path):
        """Overture pipeline must not raise when a stale `places` table exists in the work db."""
        from garganorn.quadtree import run_pipeline

        output_dir = tmp_path / "overture_restart_out"
        output_dir.mkdir()

        # Pre-create the work db directory and insert a stale `places` table.
        work_db_dir = output_dir / "overture_place"
        work_db_dir.mkdir()
        work_db_path = work_db_dir / ".overture_work.duckdb"
        stale_con = duckdb.connect(str(work_db_path))
        stale_con.execute("CREATE TABLE places (id VARCHAR, name VARCHAR)")
        stale_con.close()

        # Second run must succeed without raising CatalogException.
        run_pipeline(
            "overture_place",
            overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
            density_parquet=density_parquet,
        )

    def test_osm_pipeline_succeeds_with_stale_places_table(self, osm_parquet, density_parquet, tmp_path):
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
            (osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"]),
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
            density_parquet=density_parquet,
        )


# ---------------------------------------------------------------------------
# Tests: atomic tile export with timestamped directories
# ---------------------------------------------------------------------------

class TestTimestampedExport:
    """run_pipeline must write tiles into a timestamped subdirectory and maintain
    a `current` symlink pointing to the latest run.
    """

    _TIMESTAMP_RE = re.compile(r"^\d{8}T\d{6}$")

    def _run(self, overture_parquet, density_parquet, output_dir):
        try:
            from garganorn.quadtree import run_pipeline
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")
        run_pipeline(
            "overture_place",
            overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
            density_parquet=density_parquet,
            force=True,
        )

    def test_creates_timestamped_subdir(self, overture_parquet, density_parquet, tmp_path):
        """run_pipeline must create a timestamped subdirectory under output_dir/overture_place/tiles/."""
        output_dir = tmp_path / "ts_subdir_out"
        output_dir.mkdir()
        self._run(overture_parquet, density_parquet, output_dir)

        ov_dir = output_dir / "overture_place"
        assert ov_dir.exists(), f"output_dir/overture_place/ must exist; got {list(output_dir.iterdir())}"

        # timestamped dirs live under overture_place/tiles/
        tiles_dir = ov_dir / "tiles"
        assert tiles_dir.exists(), f"overture_place/tiles/ must exist; got {list(ov_dir.iterdir())}"
        ts_dirs = [
            d for d in tiles_dir.iterdir()
            if d.is_dir() and not d.is_symlink() and self._TIMESTAMP_RE.match(d.name)
        ]
        assert ts_dirs, (
            f"run_pipeline must create a timestamped subdir matching {self._TIMESTAMP_RE.pattern!r} "
            f"under {tiles_dir}; found: {[d.name for d in tiles_dir.iterdir()]}"
        )

        gz_files = list(ts_dirs[0].rglob("*.json.gz"))
        assert gz_files, (
            f"Timestamped dir {ts_dirs[0]} must contain at least one .json.gz file"
        )

    def test_creates_current_symlink(self, overture_parquet, density_parquet, tmp_path):
        """run_pipeline must create a `current` symlink under output_dir/overture_place/tiles/."""
        output_dir = tmp_path / "ts_symlink_out"
        output_dir.mkdir()
        self._run(overture_parquet, density_parquet, output_dir)

        ov_dir = output_dir / "overture_place"
        # canonical symlink is overture_place/tiles/current → <timestamp>
        current = ov_dir / "tiles" / "current"
        assert os.path.islink(str(current)), (
            f"output_dir/overture_place/tiles/current must be a symlink; "
            f"got {list((ov_dir / 'tiles').iterdir())}"
        )

        target = os.readlink(str(current))
        assert self._TIMESTAMP_RE.match(target), (
            f"tiles/current symlink target must match {self._TIMESTAMP_RE.pattern!r}; got {target!r}"
        )

    def test_second_run_swaps_symlink(self, overture_parquet, density_parquet, tmp_path):
        """A second run must update `current` to point to the new timestamped dir."""
        output_dir = tmp_path / "ts_swap_out"
        output_dir.mkdir()
        self._run(overture_parquet, density_parquet, output_dir)

        ov_dir = output_dir / "overture_place"
        first_target = os.readlink(str(ov_dir / "tiles" / "current"))

        time.sleep(1)
        self._run(overture_parquet, density_parquet, output_dir)

        second_target = os.readlink(str(ov_dir / "tiles" / "current"))
        assert second_target != first_target, (
            f"After second run, current symlink must point to a different dir; "
            f"both runs produced {second_target!r}"
        )

        # First run's dir must still exist (kept as previous)
        first_dir = ov_dir / "tiles" / first_target
        assert first_dir.exists(), (
            f"First run's dir {first_dir} must still exist after second run"
        )

        # Tiles accessible through current
        gz_files = list((ov_dir / "tiles" / "current").rglob("*.json.gz"))
        assert gz_files, "Tiles must be accessible through the current symlink after second run"

    def test_third_run_cleans_oldest(self, overture_parquet, density_parquet, tmp_path):
        """A third run must delete the oldest timestamped dir, keeping only 2."""
        output_dir = tmp_path / "ts_clean_out"
        output_dir.mkdir()
        self._run(overture_parquet, density_parquet, output_dir)

        ov_dir = output_dir / "overture_place"
        # use tiles/current symlink (target is a bare timestamp)
        tiles_dir = ov_dir / "tiles"
        first_target = os.readlink(str(tiles_dir / "current"))

        time.sleep(1)
        self._run(overture_parquet, density_parquet, output_dir)

        time.sleep(1)
        self._run(overture_parquet, density_parquet, output_dir)

        ts_dirs = [
            d for d in tiles_dir.iterdir()
            if d.is_dir() and not d.is_symlink() and self._TIMESTAMP_RE.match(d.name)
        ]
        assert len(ts_dirs) == 2, (
            f"After three runs, exactly 2 timestamped dirs must remain; "
            f"found {len(ts_dirs)}: {[d.name for d in ts_dirs]}"
        )

        first_dir = tiles_dir / first_target
        assert not first_dir.exists(), (
            f"First run's dir {first_dir} must have been deleted after third run"
        )

    def test_tiles_accessible_through_current(self, overture_parquet, density_parquet, tmp_path):
        """Tiles must be readable via the current symlink."""
        output_dir = tmp_path / "ts_readable_out"
        output_dir.mkdir()
        self._run(overture_parquet, density_parquet, output_dir)

        current_dir = output_dir / "overture_place" / "tiles" / "current"
        gz_files = list(current_dir.rglob("*.json.gz"))
        assert gz_files, f"No .json.gz files found under {current_dir}"

        for gz_file in gz_files:
            try:
                with gzip.open(gz_file, "rb") as fh:
                    fh.read(1)
            except Exception as exc:
                pytest.fail(f"Could not read {gz_file} via gzip.open: {exc}")

    def test_manifest_accessible_through_current(self, overture_parquet, density_parquet, tmp_path):
        """manifest.json and manifest.duckdb must be accessible via current symlink."""
        output_dir = tmp_path / "ts_manifest_out"
        output_dir.mkdir()
        self._run(overture_parquet, density_parquet, output_dir)

        current_dir = output_dir / "overture_place" / "tiles" / "current"

        manifest_json = current_dir / "manifest.json"
        assert manifest_json.exists(), f"manifest.json must exist at {manifest_json}"
        with open(manifest_json) as fh:
            data = json.load(fh)
        assert isinstance(data, dict), f"manifest.json must be valid JSON dict; got {type(data)}"

        manifest_db = current_dir / "manifest.duckdb"
        assert manifest_db.exists(), f"manifest.duckdb must exist at {manifest_db}"

    def test_failed_run_leaves_partial_dir(self, overture_parquet, density_parquet, tmp_path):
        """A failed run must leave partial timestamped dir for debugging, and not swap the symlink."""
        try:
            from garganorn.quadtree import run_pipeline
        except (ImportError, ModuleNotFoundError):
            pytest.skip("garganorn.quadtree not available")

        output_dir = tmp_path / "ts_cleanup_out"
        output_dir.mkdir()

        with pytest.raises(RuntimeError, match="boom"):
            with patch("garganorn.stages.write_manifest_db", side_effect=RuntimeError("boom")):
                run_pipeline(
                    "overture_place",
                    overture_parquet,
                    (-122.55, 37.60, -122.30, 37.85),
                    str(output_dir),
                    memory_limit="4GB",
                    max_per_tile=100,
                    density_parquet=density_parquet,
                )

        ov_dir = output_dir / "overture_place"
        assert ov_dir.exists(), "source dir must exist even after failed run"

        # partial timestamped dir is under overture_place/tiles/
        tiles_dir = ov_dir / "tiles"
        assert tiles_dir.exists(), "overture_place/tiles/ must exist even after failed run"
        ts_dirs = [
            d for d in tiles_dir.iterdir()
            if d.is_dir() and not d.is_symlink() and self._TIMESTAMP_RE.match(d.name)
        ]
        assert len(ts_dirs) == 1, (
            f"Failed run must leave exactly one partial timestamped dir for debugging; "
            f"found: {[d.name for d in ts_dirs]}"
        )

        # canonical symlink must NOT exist — swap didn't happen
        current_link = tiles_dir / "current"
        assert not current_link.is_symlink(), (
            "tiles/current symlink must not exist after a failed run"
        )

    def test_work_db_in_timestamped_dir(self, overture_parquet, density_parquet, tmp_path):
        """No .duckdb files should remain under output_dir/overture_place/ except manifest.duckdb."""
        output_dir = tmp_path / "ts_workdb_out"
        output_dir.mkdir()
        self._run(overture_parquet, density_parquet, output_dir)

        ov_dir = output_dir / "overture_place"
        leftover_dbs = [
            f for f in ov_dir.rglob("*.duckdb")
            if f.name != "manifest.duckdb"
        ]
        assert not leftover_dbs, (
            f"run_pipeline must not leave non-manifest .duckdb files under {ov_dir}: "
            f"{leftover_dbs}"
        )


# ---------------------------------------------------------------------------
# TestRunPipelineMtime
# ---------------------------------------------------------------------------


class TestRunPipelineMtime:
    """Tests for mtime-based caching in run_pipeline()."""

    @pytest.fixture
    def small_overture_parquet(self, tmp_path):
        """Create a minimal Overture-schema parquet file for mtime testing."""
        parquet_path = tmp_path / "overture_places.parquet"

        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Schema mirrors conftest.py's overture_parquet fixture (needed so
        # overture_place_export_tiles.sql's p.addresses/websites/socials/
        # emails/phones/brand/confidence/version/sources references bind).
        conn.execute("""
            CREATE TABLE tmp_ov (
                id          VARCHAR,
                bbox        STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
                geometry    VARCHAR,
                names       STRUCT(
                                "primary" VARCHAR,
                                common MAP(VARCHAR, VARCHAR),
                                rules  STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]
                            ),
                categories  STRUCT("primary" VARCHAR),
                addresses   STRUCT(country VARCHAR, postcode VARCHAR, locality VARCHAR, freeform VARCHAR, region VARCHAR)[],
                websites    VARCHAR[],
                socials     VARCHAR[],
                emails      VARCHAR[],
                phones      VARCHAR[],
                brand       VARCHAR,
                confidence  DOUBLE,
                version     INTEGER,
                sources     VARCHAR[]
            )
        """)

        # Insert 2 rows of realistic test data.
        conn.execute("""
            INSERT INTO tmp_ov VALUES (
                'ov201',
                {'xmin': -122.4204, 'ymin': 37.7739, 'xmax': -122.4184, 'ymax': 37.7759},
                'POINT(-122.4194 37.7749)',
                {'primary': 'Blue Bottle Coffee',
                 'common': map([]::VARCHAR[], []::VARCHAR[]),
                 'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
                {'primary': 'coffee_shop'},
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            )
        """)

        conn.execute("""
            INSERT INTO tmp_ov VALUES (
                'ov202',
                {'xmin': -122.4872, 'ymin': 37.7684, 'xmax': -122.4852, 'ymax': 37.7704},
                'POINT(-122.4862 37.7694)',
                {'primary': 'Golden Gate Park',
                 'common': map([]::VARCHAR[], []::VARCHAR[]),
                 'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
                {'primary': 'park'},
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            )
        """)

        conn.execute(f"COPY tmp_ov TO '{parquet_path}' (FORMAT PARQUET)")
        conn.close()

        return str(parquet_path)

    @pytest.fixture
    def small_density_parquet(self, tmp_path, small_overture_parquet):
        """Create a minimal density parquet for testing."""
        parquet_path = tmp_path / "density.parquet"

        conn = duckdb.connect(":memory:")

        conn.execute("""
            CREATE TABLE tmp_density (
                tile_qk15     VARCHAR,
                density_score DOUBLE,
                tile_xmin     DOUBLE,
                tile_ymin     DOUBLE,
                tile_xmax     DOUBLE,
                tile_ymax     DOUBLE
            )
        """)

        # Tile bounds computed from quadkeys using quadkey_to_bbox
        # '023022222222222' -> (-122.490234375, 37.74809924204635, -122.4609375, 37.76595435305952)
        # '023022222222223' -> (-122.490234375, 37.76595435305952, -122.4609375, 37.78380942582586)
        conn.execute("""
            INSERT INTO tmp_density VALUES
                ('023022222222222', 1.5, -122.490234375, 37.74809924204635, -122.4609375, 37.76595435305952),
                ('023022222222223', 2.0, -122.490234375, 37.76595435305952, -122.4609375, 37.78380942582586)
        """)

        conn.execute(f"COPY tmp_density TO '{parquet_path}' (FORMAT PARQUET)")
        conn.close()

        return str(parquet_path)

    def test_skips_when_manifest_fresh(self, small_overture_parquet, small_density_parquet, tmp_path, caplog):
        """run_pipeline skips when manifest.json mtime is newer than input."""
        import logging
        from garganorn.quadtree import run_pipeline

        output_dir = tmp_path / "mtime_skip_out"
        output_dir.mkdir()

        # First run
        with caplog.at_level(logging.INFO):
            run_pipeline(
                "overture_place",
                small_overture_parquet,
                (-122.55, 37.60, -122.30, 37.85),
                str(output_dir),
                memory_limit="4GB",
                density_parquet=small_density_parquet,
            )

        # Verify first run created output
        ov_dir = output_dir / "overture_place"
        assert ov_dir.exists()
        current_link = ov_dir / "tiles" / "current"
        assert current_link.is_symlink()
        first_target = os.readlink(str(current_link))

        # Set manifest.json mtime to the future
        manifest_path = ov_dir / "tiles" / "current" / "manifest.json"
        future_time = time.time() + 3600
        os.utime(manifest_path, (future_time, future_time))

        # Second run: should skip
        caplog.clear()
        with caplog.at_level(logging.INFO):
            run_pipeline(
                "overture_place",
                small_overture_parquet,
                (-122.55, 37.60, -122.30, 37.85),
                str(output_dir),
                memory_limit="4GB",
                density_parquet=small_density_parquet,
            )

        # Verify no new timestamped dir was created
        second_target = os.readlink(str(current_link))
        assert second_target == first_target, (
            "Should not create new timestamped dir when manifest is fresh"
        )

        # Verify skip message was logged
        assert any("skip" in record.message.lower() for record in caplog.records), (
            "Should log skip message when manifest is fresh"
        )

    def test_runs_when_manifest_missing(self, small_overture_parquet, small_density_parquet, tmp_path, caplog):
        """run_pipeline runs normally when manifest.json doesn't exist."""
        import logging
        from garganorn.quadtree import run_pipeline

        output_dir = tmp_path / "mtime_missing_out"
        output_dir.mkdir()

        # Verify no manifest exists
        ov_dir = output_dir / "overture_place"
        assert not ov_dir.exists()

        # Run pipeline
        with caplog.at_level(logging.INFO):
            run_pipeline(
                "overture_place",
                small_overture_parquet,
                (-122.55, 37.60, -122.30, 37.85),
                str(output_dir),
                memory_limit="4GB",
                density_parquet=small_density_parquet,
            )

        # Verify output was created
        assert ov_dir.exists()
        current_link = ov_dir / "tiles" / "current"
        assert current_link.is_symlink()
        manifest_path = current_link / "manifest.json"
        assert manifest_path.exists()

    def test_runs_when_input_newer(self, small_overture_parquet, small_density_parquet, tmp_path, caplog):
        """run_pipeline re-runs when input parquet is newer than manifest.json."""
        import logging
        from garganorn.quadtree import run_pipeline

        output_dir = tmp_path / "mtime_newer_out"
        output_dir.mkdir()

        # First run
        run_pipeline(
            "overture_place",
            small_overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            density_parquet=small_density_parquet,
        )

        ov_dir = output_dir / "overture_place"
        current_link = ov_dir / "tiles" / "current"
        first_target = os.readlink(str(current_link))

        # Set manifest.json mtime to the past
        manifest_path = current_link / "manifest.json"
        past_time = time.time() - 3600
        os.utime(manifest_path, (past_time, past_time))

        # Touch input to make it newer
        time.sleep(0.01)
        # Re-write the input to update its mtime using DuckDB
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute(f"""
            CREATE TABLE tmp_ov AS SELECT * FROM read_parquet('{small_overture_parquet}')
        """)
        conn.execute(f"COPY tmp_ov TO '{small_overture_parquet}' (FORMAT PARQUET)")
        conn.close()

        # Second run: should re-run
        time.sleep(1)  # timestamped dirs use %Y%m%dT%H%M%S
        caplog.clear()
        with caplog.at_level(logging.INFO):
            run_pipeline(
                "overture_place",
                small_overture_parquet,
                (-122.55, 37.60, -122.30, 37.85),
                str(output_dir),
                memory_limit="4GB",
                density_parquet=small_density_parquet,
            )

        # Verify new timestamped dir was created
        second_target = os.readlink(str(current_link))
        assert second_target != first_target, (
            "Should create new timestamped dir when input is newer"
        )

    def test_force_overrides_fresh(self, small_overture_parquet, small_density_parquet, tmp_path, caplog):
        """run_pipeline with force=True re-runs even when manifest is fresh."""
        import logging
        from garganorn.quadtree import run_pipeline

        output_dir = tmp_path / "mtime_force_out"
        output_dir.mkdir()

        # First run
        run_pipeline(
            "overture_place",
            small_overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            density_parquet=small_density_parquet,
        )

        ov_dir = output_dir / "overture_place"
        current_link = ov_dir / "tiles" / "current"
        first_target = os.readlink(str(current_link))

        # Set manifest.json mtime to the future
        manifest_path = current_link / "manifest.json"
        future_time = time.time() + 3600
        os.utime(manifest_path, (future_time, future_time))

        # Second run with force=True: should re-run despite fresh manifest
        time.sleep(1)  # timestamped dirs use %Y%m%dT%H%M%S
        caplog.clear()
        with caplog.at_level(logging.INFO):
            run_pipeline(
                "overture_place",
                small_overture_parquet,
                (-122.55, 37.60, -122.30, 37.85),
                str(output_dir),
                memory_limit="4GB",
                density_parquet=small_density_parquet,
                force=True,
            )

        # Verify new timestamped dir was created
        second_target = os.readlink(str(current_link))
        assert second_target != first_target, (
            "Should create new timestamped dir when force=True"
        )


# ---------------------------------------------------------------------------
# Tests: export layout — tiles under <src>/tiles/current/, manifests written last
# ---------------------------------------------------------------------------

class TestExportPhase2:
    """Export layout — tiles under <src>/tiles/current/, manifests written last.

    Run-dir lifecycle: tiles relocate to <src>/tiles/, manifest.json written last,
    symlink swap, keep-2.
    """

    def test_tiles_under_tiles_subdir(self, overture_parquet, density_parquet, tmp_path):
        """run_pipeline must write tiles under <src>/tiles/current/, not <src>/current/."""
        from garganorn.quadtree import run_pipeline
        output_dir = tmp_path / "pipeline_out"
        output_dir.mkdir()
        run_pipeline(
            "overture_place",
            overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
            density_parquet=density_parquet,
        )
        # tiles live under <src>/tiles/current/
        tiles_current = output_dir / "overture_place" / "tiles" / "current"
        assert tiles_current.exists(), (
            f"tiles must be at <src>/tiles/current/, not found at {tiles_current}"
        )
        gz_files = list(tiles_current.rglob("*.json.gz"))
        assert gz_files, (
            f"run_pipeline must write at least one .json.gz under {tiles_current}"
        )

    def test_manifest_json_written_last(self, overture_parquet, density_parquet, tmp_path):
        """manifest.json must have a later mtime than manifest.duckdb (written last)."""
        from garganorn.quadtree import run_pipeline
        output_dir = tmp_path / "pipeline_out2"
        output_dir.mkdir()
        run_pipeline(
            "overture_place",
            overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
            density_parquet=density_parquet,
        )
        # Find the timestamped run dir via the current symlink
        tiles_current = output_dir / "overture_place" / "tiles" / "current"
        manifest_json = tiles_current / "manifest.json"
        manifest_duckdb = tiles_current / "manifest.duckdb"
        assert manifest_json.exists(), f"manifest.json not found at {manifest_json}"
        assert manifest_duckdb.exists(), f"manifest.duckdb not found at {manifest_duckdb}"
        assert os.path.getmtime(manifest_json) >= os.path.getmtime(manifest_duckdb), (
            "manifest.json must be written after manifest.duckdb (it is the completeness marker)"
        )

    def test_stage_export_new_signature(self):
        """stage_export must not take 'con' as its first parameter."""
        import inspect
        from garganorn.stages import stage_export
        params = list(inspect.signature(stage_export).parameters.keys())
        assert params[0] != "con", (
            f"stage_export must not have 'con' as first param; got {params[0]!r}."
        )

    def test_no_working_duckdb_after_pipeline(self, overture_parquet, density_parquet, tmp_path):
        """run_pipeline must not leave a working .duckdb in the output tree."""
        from garganorn.quadtree import run_pipeline
        output_dir = tmp_path / "pipeline_out3"
        output_dir.mkdir()
        run_pipeline(
            "overture_place",
            overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
            density_parquet=density_parquet,
        )
        # Only manifest.duckdb and boundaries.duckdb are permitted .duckdb files
        duckdb_files = [
            f for f in output_dir.rglob("*.duckdb")
            if f.name not in ("manifest.duckdb", "boundaries.duckdb")
        ]
        assert not duckdb_files, (
            f"run_pipeline must not leave working .duckdb files; found: {duckdb_files}"
        )
