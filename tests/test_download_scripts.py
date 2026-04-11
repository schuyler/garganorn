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
