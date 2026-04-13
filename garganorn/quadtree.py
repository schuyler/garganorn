import argparse
import glob
import logging
import os
import re
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml
from .database import FoursquareOSP, OverturePlaces, OpenStreetMap, OvertureDivisions
from .stages import (
    quadkey_to_bbox,
    bboxes_intersect,
    _coord_exprs,
    compute_containment,
    export_tiles,
    write_manifest,
    write_manifest_db,
    stage_import,
    stage_density_extract,
    stage_importance,
    stage_variants,
    stage_tile_assignment,
    stage_containment,
    stage_export,
    stage_manifest,
    stage_division_importance_backfill,
    stage_boundary_export,
    _is_output_fresh,
)

log = logging.getLogger(__name__)

SOURCES = {cls.source_key: cls for cls in [FoursquareOSP, OverturePlaces, OpenStreetMap, OvertureDivisions]}

_TIMESTAMP_RE = re.compile(r"^\d{8}T\d{6}$")

STAGE_ORDER = {
    'default': ['import', 'importance', 'variants', 'tile_assignment', 'containment', 'export', 'manifest'],
    'overture_division': ['import', 'boundary_export', 'division_importance_backfill', 'tile_assignment', 'containment', 'export', 'manifest'],
}


def _ensure_sentinel_table(con):
    """Create sentinel table if it doesn't exist. Call once after connect."""
    try:
        con.execute("SELECT 1 FROM _pipeline_progress LIMIT 1")
    except duckdb.CatalogException:
        con.execute("""
            CREATE TABLE _pipeline_progress (
                stage VARCHAR PRIMARY KEY,
                completed_at VARCHAR
            )
        """)


def _read_sentinel(con):
    """Return set of completed stage names."""
    _ensure_sentinel_table(con)
    rows = con.execute("SELECT stage FROM _pipeline_progress").fetchall()
    return {row[0] for row in rows}


def _mark_complete(con, stage):
    """Mark a stage as complete and checkpoint."""
    con.execute("DELETE FROM _pipeline_progress WHERE stage = ?", [stage])
    con.execute("INSERT INTO _pipeline_progress VALUES (?, ?)",
                [stage, datetime.now(timezone.utc).isoformat()])
    con.execute("CHECKPOINT")


def _find_incomplete_run(source_dir, source):
    """Find the most recent timestamped dir with a work DB but no manifest."""
    # First, collect all directories that are symlink targets
    symlink_targets = set()
    for d in os.listdir(source_dir):
        ts_dir = os.path.join(source_dir, d)
        if os.path.islink(ts_dir):
            target = os.readlink(ts_dir)
            # Resolve relative symlinks
            if not os.path.isabs(target):
                target = os.path.join(source_dir, d, os.path.pardir, target)
                target = os.path.normpath(target)
            # Add the target directory (not the symlink itself)
            symlink_targets.add(os.path.realpath(ts_dir))

    candidates = []
    for d in sorted(os.listdir(source_dir), reverse=True):
        if not _TIMESTAMP_RE.match(d):
            continue
        ts_dir = os.path.join(source_dir, d)
        # Skip symlinks AND directories that are symlink targets
        if not os.path.isdir(ts_dir) or os.path.islink(ts_dir) or os.path.realpath(ts_dir) in symlink_targets:
            continue
        db_path = os.path.join(ts_dir, f".{source}_work.duckdb")
        manifest = os.path.join(ts_dir, "manifest.json")
        if os.path.exists(db_path) and not os.path.exists(manifest):
            # Verify the DB is readable (not corrupted)
            try:
                test_con = duckdb.connect(db_path)
                test_con.execute("SELECT 1")
                test_con.close()
                candidates.append(ts_dir)
            except duckdb.Error:
                # Corrupted DB, skip this candidate
                log.warning("Corrupted working DB at %s, skipping", db_path)
    return candidates[0] if candidates else None


def _collect_input_files(source, parquet_glob, density_parquet, boundaries_db):
    """Collect all input file paths for mtime comparison."""
    files = []

    if source == "osm":
        node_glob, way_glob = parquet_glob
        files.extend(glob.glob(node_glob))
        files.extend(glob.glob(way_glob))
    elif source == "overture_division":
        division_parquet, division_area_parquet = parquet_glob
        if os.path.exists(division_parquet):
            files.append(division_parquet)
        if os.path.exists(division_area_parquet):
            files.append(division_area_parquet)
    else:
        files.extend(glob.glob(parquet_glob))

    if density_parquet and os.path.exists(density_parquet):
        files.append(density_parquet)
    if boundaries_db and os.path.exists(boundaries_db):
        files.append(boundaries_db)

    return sorted(files)


def run_pipeline(source, parquet_glob, bbox, output_dir, memory_limit="48GB", max_per_tile=1000, boundaries_db=None, export_workers=None, density_parquet=None, force=False):
    """Run the full import-assign-containment-export pipeline for a data source.

    Stage logic is delegated to individual functions in garganorn.stages.
    This orchestrator handles connection lifecycle, directory setup, and cleanup.

    Stages:
      1. Import: load parquet into a `places` DuckDB table via source-specific SQL.
      2. Importance + variants: compute search ranking and name variants (skipped for
         overture_division, which inlines importance=0 and variants=[] in the import SQL).
      3. Tile assignment: assign each place to one or more quadtree tiles.
      4. Containment: populate place_containment with admin boundary relations
         (no-op if boundaries_db is None).
      5. Export tiles: write gzipped JSON tile files to a timestamped subdirectory.
      6. Manifest: write manifest.json and manifest.duckdb for tile serving.
      7. DuckDB boundary export (overture_division only): write boundaries.duckdb with
         Hilbert-sorted geometries and an R-tree index for use by other sources'
         containment stage.

    Output layout:
      <output_dir>/<source>/<timestamp>/   -- tile files, manifests
      <output_dir>/<source>/current        -- symlink to latest timestamp dir
      <output_dir>/overture_division/boundaries.duckdb  -- (overture_division only)

    The working DuckDB file is written to the tile directory and deleted on success.
    Old timestamped directories beyond the two most recent are removed.

    Args:
        source: Pipeline source key (foursquare, overture_place, osm, overture_division).
        parquet_glob: Parquet path(s). String glob for single-parquet sources;
            (division_parquet, division_area_parquet) tuple for overture_division;
            (node_parquet, way_parquet) tuple for osm.
        bbox: (xmin, ymin, xmax, ymax) bounding box filter, or None for all records.
        output_dir: Base directory for all pipeline outputs.
        memory_limit: DuckDB memory limit string (e.g. "48GB").
        max_per_tile: Maximum records assigned to a single tile.
        boundaries_db: Path to boundaries.duckdb for containment enrichment, or None.
        export_workers: Thread count for tile gzip compression. Defaults to CPU count.
        density_parquet: Path to density_tiles.parquet from stage_density_extract.
        force: If True, re-run even if output is fresh. Default False.
    """
    source_dir = os.path.join(output_dir, source)
    manifest_path = os.path.join(source_dir, "current", "manifest.json")

    if not force:
        input_files = _collect_input_files(source, parquet_glob,
                                            density_parquet, boundaries_db)
        if _is_output_fresh(manifest_path, input_files):
            log.info("[%s] pipeline: skipping (manifest is fresh)", source)
            return

    # Check for incomplete run before creating new timestamp
    incomplete_dir = None
    if os.path.exists(source_dir):
        incomplete_dir = _find_incomplete_run(source_dir, source)
        if incomplete_dir and force:
            # Delete the incomplete run's working DB
            work_db = os.path.join(incomplete_dir, f".{source}_work.duckdb")
            try:
                os.remove(work_db)
                log.info("[%s] Deleted incomplete run's working DB: %s", source, work_db)
            except OSError:
                pass
            incomplete_dir = None

    # Determine stage order for this source
    stage_order_key = 'overture_division' if source == 'overture_division' else 'default'
    stage_order = STAGE_ORDER[stage_order_key]

    # Set up directory and working DB
    if incomplete_dir:
        # Resume from incomplete run
        tile_dir = incomplete_dir
        db_path = os.path.join(tile_dir, f".{source}_work.duckdb")
        log.info("[%s] Resuming from incomplete run: %s", source, tile_dir)
    else:
        # Create new timestamped directory
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        tile_dir = os.path.join(source_dir, timestamp)
        os.makedirs(tile_dir, exist_ok=True)
        db_path = os.path.join(tile_dir, f".{source}_work.duckdb")

    con = duckdb.connect(db_path)
    _ensure_sentinel_table(con)  # Always create sentinel table on first open
    t0 = time.monotonic()

    # Read sentinel to get completed stages
    completed = _read_sentinel(con) if incomplete_dir else set()
    if incomplete_dir:
        log.info("[%s] Resuming from %s, completed stages: %s",
                 source, incomplete_dir, sorted(completed))

    try:
        if 'import' not in completed:
            stage_import(con, source, parquet_glob, bbox, memory_limit, t0)
            _mark_complete(con, 'import')
        else:
            log.info("[%s] Skipping 'import' stage (already complete)", source)

        if source == "overture_division":
            if 'boundary_export' not in completed:
                stage_boundary_export(con, source, source_dir, t0)
                _mark_complete(con, 'boundary_export')
            else:
                log.info("[%s] Skipping 'boundary_export' stage (already complete)", source)

            if 'division_importance_backfill' not in completed:
                stage_division_importance_backfill(con, density_parquet, t0)
                _mark_complete(con, 'division_importance_backfill')
            else:
                log.info("[%s] Skipping 'division_importance_backfill' stage (already complete)", source)
        else:
            if 'importance' not in completed:
                stage_importance(con, source, t0, density_parquet)
                _mark_complete(con, 'importance')
            else:
                log.info("[%s] Skipping 'importance' stage (already complete)", source)

            if 'variants' not in completed:
                stage_variants(con, source, t0)
                _mark_complete(con, 'variants')
            else:
                log.info("[%s] Skipping 'variants' stage (already complete)", source)

        pk_expr = SOURCES[source].source_pk

        if 'tile_assignment' not in completed:
            stage_tile_assignment(con, source, pk_expr, max_per_tile, t0)
            _mark_complete(con, 'tile_assignment')
        else:
            log.info("[%s] Skipping 'tile_assignment' stage (already complete)", source)

        lon_expr, lat_expr = _coord_exprs(source, alias="p")

        if 'containment' not in completed:
            stage_containment(con, source, f"p.{pk_expr}", lon_expr, lat_expr, boundaries_db, t0)
            _mark_complete(con, 'containment')
        else:
            log.info("[%s] Skipping 'containment' stage (already complete)", source)

        if 'export' not in completed:
            manifest = stage_export(con, source, tile_dir, t0, export_workers)
            _mark_complete(con, 'export')
        else:
            log.info("[%s] Skipping 'export' stage (already complete)", source)
            # Read manifest from file for resumed runs
            import json
            with open(os.path.join(tile_dir, "manifest.json")) as f:
                manifest = json.load(f)

        if 'manifest' not in completed:
            stage_manifest(con, manifest, source, tile_dir, t0)
            _mark_complete(con, 'manifest')
        else:
            log.info("[%s] Skipping 'manifest' stage (already complete)", source)
    except Exception:
        con.close()
        raise
    con.close()

    # Only delete working DB and update symlink for fresh runs, not resumed runs
    if not incomplete_dir:
        try:
            os.remove(db_path)
        except OSError:
            pass

        # Atomically swap the `current` symlink to the new timestamped directory
        link_path = os.path.join(source_dir, "current")
        tmp_link = link_path + ".tmp"
        try:
            os.remove(tmp_link)
        except OSError:
            pass
        os.symlink(timestamp, tmp_link)
        os.rename(tmp_link, link_path)

    # Clean up old timestamped dirs: keep current + previous, delete older
    ts_dirs = sorted(
        d for d in os.listdir(source_dir)
        if _TIMESTAMP_RE.match(d)
        and os.path.isdir(os.path.join(source_dir, d))
        and not os.path.islink(os.path.join(source_dir, d))
    )
    for old_dir in ts_dirs[:-2]:
        shutil.rmtree(os.path.join(source_dir, old_dir), ignore_errors=True)

    log.info("[%s] pipeline complete (%.1fs total)", source, time.monotonic() - t0)


class BboxTooLarge(Exception):
    pass


class TileManifest:
    def __init__(self, manifest_path: str, base_url: str):
        con = duckdb.connect(manifest_path, read_only=True)
        try:
            rows = con.execute("SELECT DISTINCT tile_qk FROM record_tiles").fetchall()
            self.quadkeys = set(row[0] for row in rows)
        finally:
            con.close()
        self.base_url = base_url.rstrip("/")

    def get_tiles_for_bbox(self, xmin, ymin, xmax, ymax, max_tiles=50):
        urls = []
        for qk in self.quadkeys:
            tile_bbox = quadkey_to_bbox(qk)
            if bboxes_intersect(tile_bbox, (xmin, ymin, xmax, ymax)):
                urls.append(f"{self.base_url}/{qk[:6]}/{qk}.json.gz")
                if len(urls) > max_tiles:
                    raise BboxTooLarge(f"Bounding box covers more than {max_tiles} tiles")
        return urls


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Build quadtree tile exports from place parquet data."
    )
    parser.add_argument("--source", required=True, choices=["foursquare", "overture_place", "osm", "overture_division"],
                        help="Data source: foursquare, overture_place, osm, or overture_division")
    parser.add_argument("--parquet", default=None,
                        help="Parquet glob pattern (fsq, overture)")
    parser.add_argument("--parquet-dir", default=None, dest="parquet_dir",
                        help="osm-pbf-parquet output directory (osm only)")
    parser.add_argument("--bbox", default=None, nargs=4, type=float,
                        metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
                        help="Bounding box filter (optional; default: all records)")
    parser.add_argument("--output", required=True,
                        help="Base output directory")
    parser.add_argument("--config", default=None,
                        help="Path to YAML config file")
    parser.add_argument("--memory-limit", default=None, dest="memory_limit",
                        help="DuckDB memory limit (e.g. 48GB)")
    parser.add_argument("--max-per-tile", default=None, type=int, dest="max_per_tile",
                        help="Maximum records per tile")
    parser.add_argument("--boundaries", default=None,
                        help="Path to division boundaries DuckDB for containment enrichment")
    parser.add_argument("--export-workers", default=None, type=int, dest="export_workers",
                        help="Number of threads for tile gzip compression (default: CPU count)")
    parser.add_argument("--division-parquet", default=None, dest="division_parquet",
                        help="Path to division parquet (overture_division only)")
    parser.add_argument("--division-area-parquet", default=None, dest="division_area_parquet",
                        help="Path to division_area parquet (overture_division only)")
    parser.add_argument("--density-parquet", default=None, dest="density_parquet",
                        help="Path to density_tiles.parquet (from density_extract stage)")
    parser.add_argument("--force", action="store_true", default=False,
                        help="Force re-run even if output is up-to-date")

    args = parser.parse_args()

    if args.source == "osm":
        if args.parquet_dir is None:
            parser.error("--source osm requires --parquet-dir")
        if args.parquet is not None:
            parser.error("--source osm uses --parquet-dir, not --parquet")
    elif args.source == "overture_division":
        if args.division_parquet is None or args.division_area_parquet is None:
            parser.error("--source overture_division requires --division-parquet and --division-area-parquet")
        if args.parquet is not None:
            parser.error("--source overture_division uses --division-parquet/--division-area-parquet, not --parquet")
    else:
        if args.parquet is None:
            parser.error(f"--source {args.source} requires --parquet")
        if args.parquet_dir is not None:
            parser.error(f"--source {args.source} uses --parquet, not --parquet-dir")

    # Load config defaults
    config_memory_limit = None
    config_max_per_tile = None
    config_boundaries = None
    if args.config is not None:
        with open(args.config) as f:
            cfg = yaml.safe_load(f)
        tiles_cfg = cfg.get("tiles", {}) if cfg else {}
        config_memory_limit = tiles_cfg.get("memory_limit")
        config_max_per_tile = tiles_cfg.get("max_per_tile")
        config_boundaries = tiles_cfg.get("boundaries")

    # Resolve memory_limit: CLI > config > hardcoded default
    memory_limit = args.memory_limit if args.memory_limit is not None else (
        config_memory_limit if config_memory_limit is not None else "48GB"
    )

    # Resolve max_per_tile: CLI > config > hardcoded default
    max_per_tile = args.max_per_tile if args.max_per_tile is not None else (
        config_max_per_tile if config_max_per_tile is not None else 1000
    )

    # Resolve boundaries_db: CLI > config > None
    boundaries_db = args.boundaries if args.boundaries is not None else config_boundaries

    # Build bbox: None means no filter
    bbox = tuple(args.bbox) if args.bbox is not None else None

    # Build parquet_glob: derive paths for sources with multiple parquet inputs
    if args.source == "osm":
        parquet_glob = (
            f"{args.parquet_dir}/type=node/*.parquet",
            f"{args.parquet_dir}/type=way/*.parquet",
        )
    elif args.source == "overture_division":
        parquet_glob = (args.division_parquet, args.division_area_parquet)
    else:
        parquet_glob = args.parquet

    run_pipeline(
        args.source,
        parquet_glob,
        bbox,
        args.output,
        memory_limit=memory_limit,
        max_per_tile=max_per_tile,
        boundaries_db=boundaries_db,
        export_workers=args.export_workers,
        density_parquet=args.density_parquet,
        force=args.force,
    )


if __name__ == "__main__":
    main()
