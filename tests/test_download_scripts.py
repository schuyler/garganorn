"""Tests for download-overture.sh and download-osm.sh."""

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


class TestDownloadOvertureUsage:
    """download-overture.sh --help exits 0 with usage information."""

    def test_download_overture_usage(self):
        """Running download-overture.sh --help exits 0 and prints usage."""
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
        """Running download-osm.sh --help exits 0 and prints usage."""
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
        """Running download-osm.sh --bogus exits 1 and mentions 'Unknown option'."""
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


class TestOvertureCacheLayout:
    """Overture cache should use a flat layout for places (matching the
    quadtree pipeline's config glob) and flat division naming."""

    def test_download_script_no_equals_in_local_paths(self):
        """download-overture.sh should not use S3 type= naming in local directory paths."""
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
            "Local paths should use flat naming (division/, division_area/, "
            "division_boundary/), not S3 type= names (type=division, "
            "type=division_area, etc.) extracted into local path construction.\n"
            "Expected: cache_dir/division/, cache_dir/division_area/, cache_dir/division_boundary/\n"
            "Found: cache_dir/divisions/type=division/ (via ${cache_dir}/divisions/${type})"
        )

    def test_download_script_no_divisions_nesting(self):
        """download-overture.sh keeps division types at cache root (division/,
        division_area/, division_boundary/); 'divisions' appears only in S3
        URL contexts.
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

    def test_download_script_no_places_nesting(self):
        """download-overture.sh should not nest places under a places/ directory.

        Places parquet files should land at cache_dir root, matching
        division/division_area and the quadtree pipeline's config glob
        (db/cache/overture/<release>/*.parquet).
        """
        script_path = SCRIPTS_DIR / "download-overture.sh"
        script_content = script_path.read_text()

        problematic_lines = []
        for line_num, line in enumerate(script_content.splitlines(), 1):
            if "cache_dir}/places" in line or 'cache_dir"/places' in line:
                problematic_lines.append((line_num, line.strip()))

        assert not problematic_lines, (
            "Found places/ subdirectory nesting in local path construction. "
            "Places files should be written to cache_dir root, not cache_dir/places/.\n"
            "Problematic lines:\n" + "\n".join(f"  Line {n}: {line}" for n, line in problematic_lines)
        )
