import argparse
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
)

log = logging.getLogger(__name__)

SOURCES = {cls.source_key: cls for cls in [FoursquareOSP, OverturePlaces, OpenStreetMap, OvertureDivisions]}

_TIMESTAMP_RE = re.compile(r"^\d{8}T\d{6}$")


def run_pipeline(source, parquet_glob, bbox, output_dir, memory_limit="48GB", max_per_tile=1000, boundaries_db=None, export_workers=None, density_parquet=None):
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
    """
    source_dir = os.path.join(output_dir, source)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    tile_dir = os.path.join(source_dir, timestamp)
    os.makedirs(tile_dir, exist_ok=True)
    db_path = os.path.join(tile_dir, f".{source}_work.duckdb")
    con = duckdb.connect(db_path)
    t0 = time.monotonic()

    try:
        stage_import(con, source, parquet_glob, bbox, memory_limit, t0)

        if density_parquet is None:
            raise ValueError("density_parquet is required for importance computation")

        if source == "overture_division":
            stage_boundary_export(con, source, source_dir, t0)
            stage_division_importance_backfill(con, density_parquet, t0)
        else:
            stage_importance(con, source, t0, density_parquet)
            stage_variants(con, source, t0)

        pk_expr = SOURCES[source].source_pk
        stage_tile_assignment(con, source, pk_expr, max_per_tile, t0)
        lon_expr, lat_expr = _coord_exprs(source, alias="p")
        stage_containment(con, source, f"p.{pk_expr}", lon_expr, lat_expr, boundaries_db, t0)
        manifest = stage_export(con, source, tile_dir, t0, export_workers)
        stage_manifest(con, manifest, source, tile_dir, t0)
    except Exception:
        con.close()
        raise
    con.close()
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
    )


if __name__ == "__main__":
    main()
