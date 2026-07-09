"""Crash harness for §7.7 kill-9 acceptance tests.

This script is invoked via subprocess by test_crash_recovery.py.
It reads GARGANORN_CRASH_POINT from the environment and either:
  (a) Plants the expected crash-state artifact and immediately SIGKILLs itself, OR
  (b) Monkeypatches os.replace so the kill fires at a deterministic call-site
      (used for crash points that require production code to be partially executed).

Strategy (a) is used for import:mid-copy because Phase 1 production code does not
call os.replace with "places.parquet" as the destination.  Planting the stale .tmp
directly and killing guarantees returncode -9 so the test can proceed to the
RECOVERY assertion (which fails RED because Phase 2 recovery is not implemented).

GARGANORN_CRASH_POINT values (format: <stage>:<moment>):
  import:mid-copy       — plant stale places.parquet.tmp then SIGKILL
  import:pre-meta       — kill after artifact rename, before meta write (os.replace hook)
  export:pre-manifest-json — kill after manifest.duckdb, before manifest.json (hook)

Usage (called by test_crash_recovery.py via subprocess.run):
  python tests/crash_harness.py \\
      --source foursquare \\
      --parquet <glob> \\
      --output <dir> \\
      [--bbox XMIN YMIN XMAX YMAX]

Exit codes:
  Killed by SIGKILL → returncode == -9 (on POSIX)
  Normal completion → 0
  Error → nonzero
"""
import argparse
import os
import pathlib
import signal
import sys

# Add repo root to path so garganorn imports work when invoked directly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _make_crash_hook(crash_point, original_fn):
    """Return a monkeypatched version of original_fn that kills the process
    when the crash_point matches the target path in the arguments.

    Keys on args[1] (the DESTINATION) not args[0] (the source temp path).
    C1/C2 fix: the old code keyed on args[0] which never matched the final
    artifact name (it was always the .tmp source), so the kill never fired.
    """

    def _hooked(*args, **kwargs):
        if crash_point == "import:pre-meta":
            # Kill after artifact rename but before meta write.
            # os.replace(meta_tmp, meta_dest): args[1] = destination ending in .meta.json.
            if len(args) >= 2 and str(args[1]).endswith(".meta.json"):
                os.kill(os.getpid(), signal.SIGKILL)
        elif crash_point == "export:pre-manifest-json":
            # Kill before manifest.json is written.
            # os.replace(tmp, dest): args[1] ends in manifest.json.
            if len(args) >= 2 and str(args[1]).endswith("manifest.json"):
                os.kill(os.getpid(), signal.SIGKILL)
        return original_fn(*args, **kwargs)

    return _hooked


def main():
    parser = argparse.ArgumentParser(description="Crash harness for garganorn pipeline")
    parser.add_argument("--source", required=True)
    parser.add_argument("--parquet", default=None)
    parser.add_argument("--output", required=True)
    parser.add_argument("--bbox", nargs=4, type=float,
                        metavar=("XMIN", "YMIN", "XMAX", "YMAX"), default=None)
    parser.add_argument("--memory-limit", default="4GB", dest="memory_limit")
    args = parser.parse_args()

    crash_point = os.environ.get("GARGANORN_CRASH_POINT", "")

    bbox = tuple(args.bbox) if args.bbox else (-122.55, 37.60, -122.30, 37.85)
    parquet = args.parquet or ""

    if crash_point == "import:mid-copy":
        # Phase 1 production code does NOT call os.replace/os.rename with
        # "places.parquet" as the destination (it writes to DuckDB in-memory and
        # does not yet atomically rename a .tmp artifact).  A hook on os.replace
        # would never fire.
        #
        # Strategy: plant the stale crash-state file directly, then SIGKILL.
        # This gives returncode -9 (the kill assertion passes) and leaves a
        # places.parquet.tmp on disk (the recovery assertion then fails because
        # Phase 2 recovery logic is not yet implemented).
        source_dir = pathlib.Path(args.output) / args.source
        source_dir.mkdir(parents=True, exist_ok=True)
        (source_dir / "places.parquet.tmp").write_bytes(b"GARGANORN_CRASH_SENTINEL")
        # os.kill(SIGKILL) does not return on POSIX — this is the last statement.
        os.kill(os.getpid(), signal.SIGKILL)

    from garganorn.quadtree import run_pipeline

    if crash_point:
        # Monkeypatch os.replace to crash at the right moment for other crash points.
        original_replace = os.replace
        os.replace = _make_crash_hook(crash_point, original_replace)

    run_pipeline(
        args.source,
        parquet,
        bbox,
        args.output,
        memory_limit=args.memory_limit,
        max_per_tile=100,
    )


if __name__ == "__main__":
    main()
