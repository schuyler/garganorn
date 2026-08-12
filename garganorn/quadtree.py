import argparse
import logging
import os
import shutil
import time

import duckdb
import yaml
from .database import OverturePlaces, OpenStreetMap, OvertureDivisions
from .stages import (
    quadkey_to_bbox,
    bboxes_intersect,
    _coord_exprs,
    _is_output_fresh,
    compute_containment,
    write_manifest,
    write_manifest_db,
    stage_import,
    stage_density_extract,
    stage_idf,
    stage_tile_assignment,
    stage_export,
)
from .covering import stage_covering

log = logging.getLogger(__name__)

SOURCES = {cls.source_key: cls for cls in [OverturePlaces, OpenStreetMap, OvertureDivisions]}


def run_pipeline(source, parquet_glob, bbox, output_dir, memory_limit="48GB", max_per_tile=1000, boundaries_db=None, export_workers=None, density_parquet=None, idf_parquet=None, force=False, temp_directory=None, max_temp_directory_size="250GB"):
    """Phase 2 orchestrator: import → covering → tile-assign → containment → export."""
    source_dir = os.path.join(output_dir, source)
    tiles_root = os.path.join(source_dir, "tiles")
    places_parquet = os.path.join(source_dir, "places.parquet")
    ta_parquet = os.path.join(source_dir, "tile_assignments.parquet")
    containment_dir = os.path.join(source_dir, "containment")
    t0 = time.monotonic()

    os.makedirs(source_dir, exist_ok=True)

    # Force deletion: delete Phase 2 artifacts before stages rebuild them.
    # Never touches tiles/ history.
    if force:
        for fname in [
            "places.parquet", "places.parquet.meta.json",
            "tile_assignments.parquet", "tile_assignments.parquet.meta.json",
        ]:
            path = os.path.join(source_dir, fname)
            if os.path.exists(path):
                os.remove(path)
        if os.path.exists(containment_dir):
            shutil.rmtree(containment_dir)
        if source == "overture_division":
            bnd_path = os.path.join(source_dir, "boundaries.duckdb")
            if os.path.exists(bnd_path):
                os.remove(bnd_path)
            cov_path = os.path.join(source_dir, "covering")
            if os.path.exists(cov_path):
                shutil.rmtree(cov_path)

    # Import (self-gating; dispatches to stage_division_import for overture_division)
    stage_import(source, parquet_glob, bbox, places_parquet,
                 memory_limit=memory_limit, temp_directory=temp_directory,
                 max_temp_directory_size=max_temp_directory_size,
                 density_parquet=density_parquet, idf_parquet=idf_parquet,
                 force=force)

    # Division-only: build covering from the just-written boundaries.duckdb
    if source == "overture_division":
        bnd_path = os.path.join(source_dir, "boundaries.duckdb")
        stage_covering(bnd_path, os.path.join(source_dir, "covering"),
                       memory_limit=memory_limit, temp_directory=temp_directory,
                       max_temp_directory_size=max_temp_directory_size,
                       force=force)

    covering_dir = None
    if boundaries_db is not None:
        covering_dir = os.path.join(os.path.dirname(boundaries_db), "covering")
        meta = os.path.join(covering_dir, "_meta.json")
        if not _is_output_fresh(meta, [boundaries_db]):
            raise RuntimeError(
                f"{covering_dir} is missing or older than {boundaries_db}; "
                f"run the overture_division pipeline, or `quadtree covering`, first"
            )

    # Tile assignment (self-gating)
    stage_tile_assignment(places_parquet, ta_parquet, source,
                          max_per_tile=max_per_tile, memory_limit=memory_limit,
                          temp_directory=temp_directory,
                          max_temp_directory_size=max_temp_directory_size,
                          force=force)

    # Containment (self-gating, parquet-based)
    pk_expr = SOURCES[source].source_pk
    lon_expr, lat_expr = _coord_exprs(source, alias="p")
    compute_containment(places_parquet, ta_parquet, boundaries_db,
                        pk_expr, lon_expr, lat_expr, containment_dir,
                        covering_dir=covering_dir, memory_limit=memory_limit,
                        temp_directory=temp_directory,
                        max_temp_directory_size=max_temp_directory_size,
                        force=force)

    # Export (self-gating, manages manifests + symlink + keep-2)
    stage_export(source, places_parquet, ta_parquet, containment_dir, tiles_root,
                 t0, export_workers=export_workers, memory_limit=memory_limit,
                 temp_directory=temp_directory,
                 max_temp_directory_size=max_temp_directory_size,
                 force=force)

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


# ---------------------------------------------------------------------------
# CLI subcommand helpers
# ---------------------------------------------------------------------------

def _load_pipeline_config(config_path):
    """Load the pipeline: section from a YAML config file."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    return cfg.get("pipeline", {}) if cfg else {}


def _cmd_density(args):
    t0 = time.monotonic()
    kwargs = dict(force=args.force)
    if args.memory_limit is not None:
        kwargs["memory_limit"] = args.memory_limit
    if args.temp_directory is not None:
        kwargs["temp_directory"] = args.temp_directory
    if args.max_temp_directory_size is not None:
        kwargs["max_temp_directory_size"] = args.max_temp_directory_size
    stage_density_extract(args.parquet, args.output, t0, **kwargs)


def _cmd_idf(args, idf_parser):
    if args.source == "osm":
        if args.parquet_dir is None:
            idf_parser.error("--source osm requires --parquet-dir")
        if args.parquet is not None:
            idf_parser.error("--source osm uses --parquet-dir, not --parquet")
        parquet_glob = (
            f"{args.parquet_dir}/type=node/*.parquet",
            f"{args.parquet_dir}/type=way/*.parquet",
        )
    else:
        if args.parquet is None:
            idf_parser.error(f"--source {args.source} requires --parquet")
        parquet_glob = args.parquet

    t0 = time.monotonic()
    idf_kwargs = dict(force=args.force,
                       memory_limit=args.memory_limit, temp_directory=args.temp_directory)
    if args.max_temp_directory_size is not None:
        idf_kwargs["max_temp_directory_size"] = args.max_temp_directory_size
    stage_idf(args.source, parquet_glob, args.output, t0, **idf_kwargs)


def _cmd_covering(args):
    output = args.output
    if output is None:
        output = os.path.join(os.path.dirname(args.boundaries), "covering")
    kwargs = dict(force=args.force)
    if args.memory_limit is not None:
        kwargs["memory_limit"] = args.memory_limit
    if args.temp_directory is not None:
        kwargs["temp_directory"] = args.temp_directory
    if args.max_temp_directory_size is not None:
        kwargs["max_temp_directory_size"] = args.max_temp_directory_size
    if args.min_zoom is not None:
        kwargs["cover_min_zoom"] = args.min_zoom
    if args.max_zoom is not None:
        kwargs["cover_max_zoom"] = args.max_zoom
    if args.min_leaf_zoom is not None:
        kwargs["cover_min_leaf_zoom"] = args.min_leaf_zoom
    if args.vertex_capacity is not None:
        kwargs["cover_vertex_capacity"] = args.vertex_capacity
    stage_covering(args.boundaries, output, **kwargs)


def _cmd_run(args, run_parser):
    """Validate per-source flags and dispatch to run_pipeline."""
    if args.source == "osm":
        if args.parquet_dir is None:
            run_parser.error("--source osm requires --parquet-dir")
        if args.parquet is not None:
            run_parser.error("--source osm uses --parquet-dir, not --parquet")
    elif args.source == "overture_division":
        if args.division_parquet is None or args.division_area_parquet is None:
            run_parser.error(
                "--source overture_division requires --division-parquet and --division-area-parquet"
            )
        if args.parquet is not None:
            run_parser.error(
                "--source overture_division uses --division-parquet/--division-area-parquet, not --parquet"
            )
    else:
        if args.parquet is None:
            run_parser.error(f"--source {args.source} requires --parquet")
        if args.parquet_dir is not None:
            run_parser.error(f"--source {args.source} uses --parquet, not --parquet-dir")

    # Load config defaults from pipeline: section
    config = {}
    if args.config is not None:
        config = _load_pipeline_config(args.config)

    memory_limit = args.memory_limit if args.memory_limit is not None else (
        config.get("memory_limit") if config.get("memory_limit") is not None else "48GB"
    )
    max_per_tile = args.max_per_tile if args.max_per_tile is not None else (
        config.get("max_per_tile") if config.get("max_per_tile") is not None else 1000
    )
    temp_directory = args.temp_directory if args.temp_directory is not None else (
        config.get("temp_directory")
    )
    max_temp_directory_size = args.max_temp_directory_size if args.max_temp_directory_size is not None else (
        config.get("max_temp_directory_size") if config.get("max_temp_directory_size") is not None else "250GB"
    )
    boundaries_db = args.boundaries

    bbox = tuple(args.bbox) if args.bbox is not None else None

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
        idf_parquet=args.idf_parquet,
        temp_directory=temp_directory,
        max_temp_directory_size=max_temp_directory_size,
        force=args.force,
    )


def _cmd_all(args):
    """Orchestrate all configured sources: density → idf → division → others."""
    config = _load_pipeline_config(args.config)
    output_dir = config["output"]
    memory_limit = config.get("memory_limit", "48GB")
    temp_directory = config.get("temp_directory")
    max_temp_directory_size = config.get("max_temp_directory_size", "250GB")
    max_per_tile = config.get("max_per_tile", 1000)
    bbox_list = config.get("bbox")
    bbox = tuple(bbox_list) if bbox_list else None
    sources = config.get("sources") or {}

    overture_cfg = sources.get("overture_place")
    division_cfg = sources.get("overture_division")

    # Derived paths
    shared_dir = os.path.join(output_dir, "shared")
    density_parquet_path = os.path.join(shared_dir, "density_tiles.parquet")

    boundaries_db_path = None
    if division_cfg:
        div_src_dir = os.path.join(output_dir, "overture_division")
        boundaries_db_path = os.path.join(div_src_dir, "boundaries.duckdb")

    # Step 1: density (from overture_place parquet)
    if overture_cfg:
        os.makedirs(shared_dir, exist_ok=True)
        stage_density_extract(
            overture_cfg.get("parquet"),
            density_parquet_path,
            time.monotonic(),
            force=args.force,
            memory_limit=memory_limit,
            temp_directory=temp_directory,
            max_temp_directory_size=max_temp_directory_size,
        )

    # Step 2: idf per configured place source (fixed order)
    PLACE_SOURCES_ORDER = ["overture_place", "osm"]
    for src_name in PLACE_SOURCES_ORDER:
        src_cfg = sources.get(src_name)
        if src_cfg is None:
            continue
        src_dir = os.path.join(output_dir, src_name)
        os.makedirs(src_dir, exist_ok=True)
        idf_out = os.path.join(src_dir, "idf.parquet")
        if src_name == "osm":
            parquet_dir = src_cfg.get("parquet_dir")
            parquet_glob = (
                f"{parquet_dir}/type=node/*.parquet",
                f"{parquet_dir}/type=way/*.parquet",
            )
        else:
            parquet_glob = src_cfg.get("parquet")
        stage_idf(src_name, parquet_glob, idf_out, time.monotonic(), force=args.force,
                  memory_limit=memory_limit, temp_directory=temp_directory,
                  max_temp_directory_size=max_temp_directory_size)

    # Step 3: run overture_division first (produces boundaries.duckdb for other sources)
    if division_cfg:
        run_pipeline(
            "overture_division",
            (division_cfg.get("division_parquet"), division_cfg.get("division_area_parquet")),
            bbox,
            output_dir,
            memory_limit=memory_limit,
            max_per_tile=max_per_tile,
            density_parquet=density_parquet_path if overture_cfg else None,
            temp_directory=temp_directory,
            max_temp_directory_size=max_temp_directory_size,
            force=args.force,
        )

    # Step 4: run each remaining configured place source
    for src_name in PLACE_SOURCES_ORDER:
        src_cfg = sources.get(src_name)
        if src_cfg is None:
            continue
        src_dir = os.path.join(output_dir, src_name)
        idf_parquet_path = os.path.join(src_dir, "idf.parquet")
        if src_name == "osm":
            parquet_dir = src_cfg.get("parquet_dir")
            parquet_glob = (
                f"{parquet_dir}/type=node/*.parquet",
                f"{parquet_dir}/type=way/*.parquet",
            )
        else:
            parquet_glob = src_cfg.get("parquet")
        run_pipeline(
            src_name,
            parquet_glob,
            bbox,
            output_dir,
            memory_limit=memory_limit,
            max_per_tile=max_per_tile,
            boundaries_db=boundaries_db_path,
            density_parquet=density_parquet_path if overture_cfg else None,
            idf_parquet=idf_parquet_path,
            temp_directory=temp_directory,
            max_temp_directory_size=max_temp_directory_size,
            force=args.force,
        )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Build quadtree tile exports from place parquet data."
    )
    subparsers = parser.add_subparsers(dest="subcommand")

    # --- density subcommand ---
    density_p = subparsers.add_parser(
        "density", help="Extract density tiles from overture_place parquet"
    )
    density_p.add_argument("--parquet", required=True,
                           help="Parquet glob pattern for overture_place source")
    density_p.add_argument("--output", required=True,
                           help="Output path for density_tiles.parquet")
    density_p.add_argument("--memory-limit", default=None, dest="memory_limit",
                           help="DuckDB memory limit (e.g. 48GB)")
    density_p.add_argument("--temp-directory", default=None, dest="temp_directory",
                           help="DuckDB temp directory for spill")
    density_p.add_argument("--max-temp-directory-size", default=None,
                           dest="max_temp_directory_size",
                           help="DuckDB max_temp_directory_size (e.g. 250GB); "
                                "bounds spill under --temp-directory")
    density_p.add_argument("--force", action="store_true", default=False,
                           help="Force rebuild even if output is fresh")

    # --- idf subcommand ---
    idf_p = subparsers.add_parser(
        "idf", help="Compute IDF scores from place parquet"
    )
    idf_p.add_argument("--source", required=True,
                       choices=["overture_place", "osm"])
    idf_p.add_argument("--parquet", default=None,
                       help="Parquet glob pattern (overture_place)")
    idf_p.add_argument("--parquet-dir", default=None, dest="parquet_dir",
                       help="osm-pbf-parquet output directory (osm only)")
    idf_p.add_argument("--output", required=True,
                       help="Output path for idf.parquet")
    idf_p.add_argument("--memory-limit", default=None, dest="memory_limit",
                       help="DuckDB memory limit")
    idf_p.add_argument("--temp-directory", default=None, dest="temp_directory",
                       help="DuckDB temp directory")
    idf_p.add_argument("--max-temp-directory-size", default=None,
                       dest="max_temp_directory_size",
                       help="DuckDB max_temp_directory_size (e.g. 250GB); "
                            "bounds spill under --temp-directory")
    idf_p.add_argument("--force", action="store_true", default=False,
                       help="Force rebuild even if output is fresh")

    # --- covering subcommand ---
    covering_p = subparsers.add_parser(
        "covering", help="Build quadkey covering from division boundaries"
    )
    covering_p.add_argument("--boundaries", required=True,
                            help="Path to boundaries.duckdb")
    covering_p.add_argument("--output", default=None,
                            help="Output directory (default: sibling covering/ next to boundaries)")
    covering_p.add_argument("--min-zoom", default=None, type=int, dest="min_zoom",
                            help="Minimum zoom level for covering")
    covering_p.add_argument("--max-zoom", default=None, type=int, dest="max_zoom",
                            help="Maximum zoom level for covering")
    covering_p.add_argument("--min-leaf-zoom", default=None, type=int, dest="min_leaf_zoom",
                            help="Shallowest zoom an edge leaf may be emitted at")
    covering_p.add_argument("--vertex-capacity", default=None, type=int, dest="vertex_capacity",
                            help="Max vertex count (V) for an edge leaf before it splits further")
    covering_p.add_argument("--memory-limit", default=None, dest="memory_limit",
                            help="DuckDB memory limit")
    covering_p.add_argument("--temp-directory", default=None, dest="temp_directory",
                            help="DuckDB temp directory")
    covering_p.add_argument("--max-temp-directory-size", default=None,
                            dest="max_temp_directory_size",
                            help="DuckDB max_temp_directory_size (e.g. 250GB); "
                                 "bounds spill under --temp-directory")
    covering_p.add_argument("--force", action="store_true", default=False,
                            help="Force rebuild even if output is fresh")

    # --- run subcommand ---
    run_p = subparsers.add_parser(
        "run", help="Run the full pipeline for a data source"
    )
    run_p.add_argument("--source", required=True,
                       choices=["overture_place", "osm", "overture_division"],
                       help="Data source")
    run_p.add_argument("--parquet", default=None,
                       help="Parquet glob pattern (overture_place)")
    run_p.add_argument("--parquet-dir", default=None, dest="parquet_dir",
                       help="osm-pbf-parquet output directory (osm only)")
    run_p.add_argument("--division-parquet", default=None, dest="division_parquet",
                       help="Path to division parquet (overture_division only)")
    run_p.add_argument("--division-area-parquet", default=None, dest="division_area_parquet",
                       help="Path to division_area parquet (overture_division only)")
    run_p.add_argument("--output", required=True,
                       help="Base output directory")
    run_p.add_argument("--bbox", default=None, nargs=4, type=float,
                       metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
                       help="Bounding box filter (optional; default: all records)")
    run_p.add_argument("--config", default=None,
                       help="Path to YAML config file (pipeline: section)")
    run_p.add_argument("--memory-limit", default=None, dest="memory_limit",
                       help="DuckDB memory limit (e.g. 48GB)")
    run_p.add_argument("--max-per-tile", default=None, type=int, dest="max_per_tile",
                       help="Maximum records per tile")
    run_p.add_argument("--boundaries", default=None,
                       help="Path to division boundaries DuckDB for containment enrichment")
    run_p.add_argument("--export-workers", default=None, type=int, dest="export_workers",
                       help="Number of threads for tile gzip compression")
    run_p.add_argument("--density-parquet", default=None, dest="density_parquet",
                       help="Path to density_tiles.parquet (input)")
    run_p.add_argument("--idf-parquet", default=None, dest="idf_parquet",
                       help="Path to idf.parquet (input)")
    run_p.add_argument("--temp-directory", default=None, dest="temp_directory",
                       help="DuckDB temp directory for spill")
    run_p.add_argument("--max-temp-directory-size", default=None,
                       dest="max_temp_directory_size",
                       help="DuckDB max_temp_directory_size (e.g. 250GB); "
                            "bounds spill under --temp-directory")
    run_p.add_argument("--force", action="store_true", default=False,
                       help="Delete and rebuild all stage outputs before running")

    # --- all subcommand ---
    all_p = subparsers.add_parser(
        "all", help="Run all sources configured in a pipeline: config file"
    )
    all_p.add_argument("--config", required=True,
                       help="Path to YAML config file with pipeline: section")
    all_p.add_argument("--force", action="store_true", default=False,
                       help="Delete and rebuild all stage outputs before running")

    args = parser.parse_args()

    if args.subcommand is None:
        parser.error(
            "No subcommand specified. Use 'run' to process a single source, "
            "'all' to run all configured sources, or 'density'/'idf'/'covering' "
            "for individual stages."
        )

    if args.subcommand == "density":
        _cmd_density(args)
    elif args.subcommand == "idf":
        _cmd_idf(args, idf_p)
    elif args.subcommand == "covering":
        _cmd_covering(args)
    elif args.subcommand == "run":
        _cmd_run(args, run_p)
    elif args.subcommand == "all":
        _cmd_all(args)


if __name__ == "__main__":
    main()
