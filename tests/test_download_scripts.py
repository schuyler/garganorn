"""Tests for download scripts and import cache enforcement.

These tests are written in red (failing) state. They will pass once:
- download-fsq.sh, download-overture.sh, download-osm.sh are created
- import-fsq-extract.sh and import-overture-extract.sh enforce cache presence
  (replacing their S3 download loops with cache presence checks)
"""

import os
import pathlib
import shutil
import subprocess
import tempfile

import pytest

SCRIPTS_DIR = pathlib.Path(__file__).parent.parent / "scripts"
SAMPLE_BBOX = ["-122.5", "37.7", "-122.4", "37.8"]


def _run(script_name: str, args: list[str], env: dict | None = None) -> subprocess.CompletedProcess:
    """Run a script from the scripts directory with subprocess.run."""
    script_path = SCRIPTS_DIR / script_name
    cmd = [str(script_path)] + args
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=merged_env,
    )


# ---------------------------------------------------------------------------
# Tests 1-3: import script cache enforcement
#
# These fail because import-fsq-extract.sh and import-overture-extract.sh do
# not yet enforce cache presence. They currently download from S3 rather than
# checking for a local cache and failing with the expected messages.
# ---------------------------------------------------------------------------


class TestFsqImportCacheEnforcement:
    """import-fsq-extract.sh must fail when the cache is absent or incomplete."""

    def test_fsq_import_fails_without_cache(self, tmp_path):
        """import-fsq-extract.sh exits 1 and mentions download-fsq.sh when cache is empty.

        Red: --cache-dir flag does not exist yet in the script. Once the green
        implementation adds --cache-dir, this test will pass.
        """
        # Pass --cache-dir pointing at a nonexistent directory so the script
        # can skip S3 discovery and check the cache directly.
        result = _run(
            "import-fsq-extract.sh",
            ["--cache-dir", str(tmp_path / "nonexistent")] + SAMPLE_BBOX,
        )

        assert result.returncode == 1, (
            f"Expected exit code 1, got {result.returncode}.\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )
        combined = result.stdout + result.stderr
        assert "Cache missing" in combined, (
            "Expected 'Cache missing' in output, but got:\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )
        assert "download-fsq.sh" in combined, (
            "Expected 'download-fsq.sh' in output, but got:\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )

    def test_fsq_import_fails_incomplete_cache(self, tmp_path):
        """import-fsq-extract.sh exits 1 with an 'Incomplete FSQ cache' message when fewer than 100 parquet files are present.

        Red: --cache-dir flag does not exist yet in the script. Once the green
        implementation adds --cache-dir, this test will pass.
        """
        # Create 50 fake parquet files directly in tmp_path (incomplete cache).
        for i in range(50):
            (tmp_path / f"places-{i:05d}.zstd.parquet").touch()

        result = _run(
            "import-fsq-extract.sh",
            ["--cache-dir", str(tmp_path)] + SAMPLE_BBOX,
        )

        assert result.returncode == 1, (
            f"Expected exit code 1, got {result.returncode}.\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )
        combined = result.stdout + result.stderr
        assert "Incomplete FSQ cache" in combined, (
            "Expected 'Incomplete FSQ cache' in output, but got:\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )
        assert "50" in combined, (
            "Expected file count '50' mentioned in output, but got:\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )


class TestOvertureImportCacheEnforcement:
    """import-overture-extract.sh must fail when the cache is absent."""

    def test_overture_import_fails_without_cache(self, tmp_path):
        """import-overture-extract.sh exits 1 and mentions download-overture.sh when cache is empty.

        Red: --cache-dir flag does not exist yet in the script. Once the green
        implementation adds --cache-dir, this test will pass.
        """
        # Pass --cache-dir pointing at a nonexistent directory so the script
        # can skip S3 discovery and check the cache directly.
        result = _run(
            "import-overture-extract.sh",
            ["--cache-dir", str(tmp_path / "nonexistent")] + SAMPLE_BBOX,
        )

        assert result.returncode == 1, (
            f"Expected exit code 1, got {result.returncode}.\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )
        combined = result.stdout + result.stderr
        assert "Cache missing" in combined, (
            "Expected 'Cache missing' in output, but got:\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )
        assert "download-overture.sh" in combined, (
            "Expected 'download-overture.sh' in output, but got:\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )


# ---------------------------------------------------------------------------
# Tests 4-7: download scripts (don't exist yet)
#
# These fail with FileNotFoundError or non-zero exit because the scripts have
# not been created.
# ---------------------------------------------------------------------------


class TestDownloadFsqUsage:
    """download-fsq.sh --help exits 0 with usage information."""

    def test_download_fsq_usage(self):
        """Running download-fsq.sh --help exits 0 and prints usage.

        Red: scripts/download-fsq.sh does not exist yet.
        """
        result = _run("download-fsq.sh", ["--help"])

        assert result.returncode == 0, (
            f"Expected exit code 0, got {result.returncode}.\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )
        combined = result.stdout + result.stderr
        assert "Usage" in combined or "usage" in combined or "--cache-dir" in combined, (
            "Expected usage information in output, but got:\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )


class TestDownloadOvertureUsage:
    """download-overture.sh --help exits 0 with usage information."""

    def test_download_overture_usage(self):
        """Running download-overture.sh --help exits 0 and prints usage.

        Red: scripts/download-overture.sh does not exist yet.
        """
        result = _run("download-overture.sh", ["--help"])

        assert result.returncode == 0, (
            f"Expected exit code 0, got {result.returncode}.\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )
        combined = result.stdout + result.stderr
        assert "Usage" in combined or "usage" in combined or "--cache-dir" in combined, (
            "Expected usage information in output, but got:\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )


class TestDownloadOsmUsage:
    """download-osm.sh --help exits 0 with usage information."""

    def test_download_osm_usage(self):
        """Running download-osm.sh --help exits 0 and prints usage.

        Red: scripts/download-osm.sh does not exist yet.
        """
        result = _run("download-osm.sh", ["--help"])

        assert result.returncode == 0, (
            f"Expected exit code 0, got {result.returncode}.\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )
        combined = result.stdout + result.stderr
        assert "Usage" in combined or "usage" in combined or "--cache-dir" in combined, (
            "Expected usage information in output, but got:\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )

    def test_download_osm_unknown_option(self):
        """Running download-osm.sh --bogus exits 1 and mentions 'Unknown option'.

        Red: scripts/download-osm.sh does not exist yet.
        """
        result = _run("download-osm.sh", ["--bogus"])

        assert result.returncode == 1, (
            f"Expected exit code 1, got {result.returncode}.\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )
        combined = result.stdout + result.stderr
        assert "Unknown option" in combined or "unknown option" in combined or "unrecognized" in combined.lower(), (
            "Expected 'Unknown option' or similar in output, but got:\n"
            f"stdout: {result.stdout[:500]}\nstderr: {result.stderr[:500]}"
        )


# ---------------------------------------------------------------------------
# Tests 8-11: Overture cache directory layout restructure
#
# These fail because the current code stores parquet files at the cache root
# and uses S3-style partitioning (type=division, etc.) locally. After the
# restructure, places should be in a places/ subdir and divisions should use
# flat naming (division/, division_area/, division_boundary/) without type=
# prefixes.
# ---------------------------------------------------------------------------


class TestOvertureCacheLayout:
    """Overture cache should use places/ subdirectory and flat division naming."""

    def test_import_finds_parquets_in_places_subdir(self, tmp_path):
        """import-overture-extract.sh finds parquet files in cache_dir/places/.

        Red: Current script looks at cache root (${cache_dir}/*.parquet).
        After restructure, it should look in ${cache_dir}/places/*.parquet.
        """
        # Create fake parquet files in places/ subdirectory
        places_dir = tmp_path / "places"
        places_dir.mkdir()
        for i in range(5):
            (places_dir / f"part-{i:05d}-00000.parquet").touch()

        # Run import with --cache-dir pointing to tmp_path
        result = _run(
            "import-overture-extract.sh",
            ["--cache-dir", str(tmp_path)] + SAMPLE_BBOX,
        )

        combined = result.stdout + result.stderr
        # Should NOT say "Cache missing" because files exist in places/
        assert "Cache missing" not in combined, (
            f"Script should find parquet files in places/ subdirectory, but it reported 'Cache missing'.\n"
            f"stdout: {result.stdout[:1000]}\nstderr: {result.stderr[:1000]}"
        )

    def test_download_script_no_equals_in_local_paths(self):
        """download-overture.sh should not use S3 type= naming in local directory paths.

        Red: Current script uses S3 partitioning locally (divisions/type=division/).
        The script extracts type=division, type=division_area, etc. from S3 and
        constructs local paths like ${cache_dir}/divisions/${type}, which expands
        to cache_dir/divisions/type=division/. After restructure, local paths
        should use flat naming (division/, division_area/) without the type= prefix.
        """
        script_path = SCRIPTS_DIR / "download-overture.sh"
        script_content = script_path.read_text()

        # The problematic pattern: extracting type=foo from S3 and using it directly
        # in local path construction via ${type} variable
        has_s3_type_extraction = False
        has_local_type_usage = False

        for line_num, line in enumerate(script_content.splitlines(), 1):
            # Check for S3 type= extraction pattern
            if "sed" in line and "type=" in line and "divisions" in line:
                has_s3_type_extraction = True
            # Check for using extracted ${type} in local paths
            if "${type}" in line and any(keyword in line for keyword in ["cache_dir", "type_dir", "mkdir"]):
                has_local_type_usage = True

        # The issue is: script extracts "type=division" from S3 and uses it in local paths
        assert not (has_s3_type_extraction and has_local_type_usage), (
            "Script extracts S3 type= names (type=division, type=division_area, etc.) "
            "and uses them directly in local path construction. "
            "After restructure, local paths should strip the type= prefix and use flat naming.\n"
            "Expected: cache_dir/division/, cache_dir/division_area/, cache_dir/division_boundary/\n"
            "Current pattern: cache_dir/divisions/type=division/ (via ${cache_dir}/divisions/${type})"
        )

    def test_download_script_no_divisions_nesting(self):
        """download-overture.sh should not nest divisions under a divisions/ directory.

        Red: Current script creates divisions/type=division/, etc.
        After restructure, division types should be at cache root: division/,
        division_area/, division_boundary/. The word 'divisions' should only
        appear in S3 URL contexts.
        """
        script_path = SCRIPTS_DIR / "download-overture.sh"
        script_content = script_path.read_text()

        # Check each line for divisions/ in local path contexts
        problematic_lines = []
        for line_num, line in enumerate(script_content.splitlines(), 1):
            # Skip lines that are purely S3 URLs (contain source_base, s3://, or theme=divisions)
            if "source_base" in line or "s3://" in line or "theme=divisions" in line:
                continue
            # Check if line has divisions/ AND local path indicators
            if "divisions/" in line and any(keyword in line for keyword in ["cache_dir", "type_dir", "mkdir"]):
                problematic_lines.append((line_num, line.strip()))

        assert not problematic_lines, (
            "Found divisions/ in local path construction. "
            "Division types should be at cache root (division/, division_area/), not nested under divisions/.\n"
            f"Problematic lines:\n" + "\n".join(f"  Line {n}: {line}" for n, line in problematic_lines)
        )

    def test_import_cache_check_uses_places_subdir(self):
        """import-overture-extract.sh cache glob should use places/ subdirectory.

        Red: Current script checks ${cache_dir}/*.parquet at root level.
        After restructure, it should check ${cache_dir}/places/*.parquet.
        """
        script_path = SCRIPTS_DIR / "import-overture-extract.sh"
        script_content = script_path.read_text()

        # Find lines with *.parquet glob patterns
        parquet_lines = []
        for line_num, line in enumerate(script_content.splitlines(), 1):
            if "*.parquet" in line:
                parquet_lines.append((line_num, line.strip()))

        # At least one *.parquet line should reference places/
        places_references = [line for n, line in parquet_lines if "places/" in line]

        assert places_references, (
            "No *.parquet glob pattern references places/ subdirectory.\n"
            f"Found {len(parquet_lines)} lines with *.parquet:\n" +
            "\n".join(f"  Line {n}: {line}" for n, line in parquet_lines) +
            "\n\nAt least one should include places/ path."
        )
