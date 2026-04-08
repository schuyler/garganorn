import argparse
import gzip
import json
import logging
import math
import os
import string
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml

log = logging.getLogger(__name__)

SOURCE_PK = {
    "fsq": "fsq_place_id",
    "overture": "id",
    "osm": "rkey",
}

ATTRIBUTION = {
    "fsq": "https://docs.foursquare.com/data-products/docs/access-fsq-os-places",
    "overture": "https://docs.overturemaps.org/attribution/",
    "osm": "https://www.openstreetmap.org/copyright",
}

REPO = "places.atgeo.org"


def export_tiles(con, output_dir: str, source: str) -> dict:
    """Query DuckDB for per-tile JSON and write gzipped files; streams results via fetchmany(100). Returns {qk: record_count}."""
    sql_dir = Path(__file__).parent / "sql"
    raw = (sql_dir / f"{source}_export_tiles.sql").read_text()
    sql = string.Template(raw).safe_substitute(
        attribution=ATTRIBUTION[source], repo=REPO
    )
    con.execute(sql)  # creates tile_export view
    cursor = con.execute("SELECT tile_qk, tile_json FROM tile_export")
    manifest = {}
    tile_count = 0
    while True:
        batch = cursor.fetchmany(100)
        if not batch:
            break
        for tile_qk, tile_json in batch:
            subdir = os.path.join(output_dir, tile_qk[:6])
            os.makedirs(subdir, exist_ok=True)
            with gzip.open(os.path.join(subdir, f"{tile_qk}.json.gz"), "wb") as f:
                f.write(tile_json.encode("utf-8"))
            manifest[tile_qk] = len(json.loads(tile_json)["records"])
            tile_count += 1
            if tile_count % 1000 == 0:
                log.info("export: wrote %d tiles", tile_count)
    log.info("export: wrote %d tiles total", len(manifest))
    return manifest


def run_pipeline(source, parquet_glob, bbox, output_dir, memory_limit="48GB", max_per_tile=1000):
    output_dir = os.path.join(output_dir, source)
    os.makedirs(output_dir, exist_ok=True)
    db_path = os.path.join(output_dir, f".{source}_work.duckdb")
    con = duckdb.connect(db_path)
    sql_dir = Path(__file__).parent / "sql"
    t0 = time.monotonic()

    def run_sql(stage, filename, **params):
        log.info("[%s] %s: starting", source, stage)
        sql = (sql_dir / filename).read_text()
        for k, v in params.items():
            sql = sql.replace(f"${{{k}}}", str(v))
        con.execute(sql)
        count = con.execute("SELECT count(*) FROM places").fetchone()[0]
        log.info("[%s] %s: done (%.1fs, %d places)",
                 source, stage, time.monotonic() - t0, count)

    xmin, ymin, xmax, ymax = bbox if bbox is not None else (-180, -90, 180, 90)

    # Import stage — OSM needs two separate parquet paths
    if source == "osm":
        node_parquet, way_parquet = parquet_glob
        run_sql("import", "osm_import.sql",
                memory_limit=memory_limit,
                node_parquet=node_parquet,
                way_parquet=way_parquet,
                xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)
    else:
        run_sql("import", f"{source}_import.sql",
                memory_limit=memory_limit,
                parquet_glob=parquet_glob,
                xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)

    run_sql("importance", f"{source}_importance.sql",
            density_norm=10.0, idf_norm=18.0)
    run_sql("variants", f"{source}_variants.sql")

    pk_expr = SOURCE_PK[source]
    run_sql("tile assignment", "compute_tile_assignments.sql",
            pk_expr=pk_expr, min_zoom=6, max_zoom=17, max_per_tile=max_per_tile)

    log.info("[%s] export: starting", source)
    manifest = export_tiles(con, output_dir, source)
    log.info("[%s] export: %d tiles, %d records (%.1fs)",
             source, len(manifest),
             sum(manifest.values()), time.monotonic() - t0)

    write_manifest(manifest, output_dir, source)
    write_manifest_db(con, output_dir, source)
    con.close()
    try:
        os.remove(db_path)
    except OSError:
        pass
    log.info("[%s] pipeline complete (%.1fs total)", source, time.monotonic() - t0)


def write_manifest_db(con, output_dir: str, source: str):
    """Write manifest.duckdb with record_tiles and metadata tables.

    Reads tile_assignments from the open working DuckDB connection and exports
    rkey→tile_qk mappings plus source metadata to a separate manifest.duckdb file.
    Writes atomically: builds in a .tmp file then renames into place.
    Must be called before con.close() so tile_assignments is still accessible.
    """
    manifest_path = os.path.join(output_dir, "manifest.duckdb")
    tmp_path = manifest_path + ".tmp"
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    con.execute(f"ATTACH '{tmp_path}' AS manifest")
    con.execute("""
        CREATE TABLE manifest.record_tiles AS
        SELECT place_id AS rkey, tile_qk
        FROM tile_assignments
        ORDER BY place_id
    """)
    con.execute("""
        CREATE TABLE manifest.metadata AS
        SELECT ? AS source, ? AS generated_at
    """, [source, datetime.now(timezone.utc).isoformat()])
    con.execute("DETACH manifest")
    os.rename(tmp_path, manifest_path)


def write_manifest(manifest, output_dir, source):
    data = {
        "source": source,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "quadkeys": sorted(manifest.keys()),
    }
    with open(os.path.join(output_dir, "manifest.json"), "w") as f:
        json.dump(data, f, indent=2)


class BboxTooLarge(Exception):
    pass


def quadkey_to_bbox(quadkey: str) -> tuple[float, float, float, float]:
    x, y, level = 0, 0, len(quadkey)
    for i, ch in enumerate(quadkey):
        bit = level - i - 1
        mask = 1 << bit
        digit = int(ch)
        if digit & 1:
            x |= mask
        if digit & 2:
            y |= mask
    n = 2 ** level if level > 0 else 1
    lon_min = x / n * 360 - 180
    lon_max = (x + 1) / n * 360 - 180
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n)))) if n > 0 else 85.05112877980659
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))) if n > 0 else -85.05112877980659
    return (lon_min, lat_min, lon_max, lat_max)


def bboxes_intersect(a, b):
    return a[0] <= b[2] and a[2] >= b[0] and a[1] <= b[3] and a[3] >= b[1]


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
    parser.add_argument("--source", required=True, choices=["fsq", "overture", "osm"],
                        help="Data source: fsq, overture, or osm")
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

    args = parser.parse_args()

    if args.source == "osm":
        if args.parquet_dir is None:
            parser.error("--source osm requires --parquet-dir")
        if args.parquet is not None:
            parser.error("--source osm uses --parquet-dir, not --parquet")
    else:
        if args.parquet is None:
            parser.error(f"--source {args.source} requires --parquet")
        if args.parquet_dir is not None:
            parser.error(f"--source {args.source} uses --parquet, not --parquet-dir")

    # Load config defaults
    config_memory_limit = None
    config_max_per_tile = None
    if args.config is not None:
        with open(args.config) as f:
            cfg = yaml.safe_load(f)
        tiles_cfg = cfg.get("tiles", {}) if cfg else {}
        config_memory_limit = tiles_cfg.get("memory_limit")
        config_max_per_tile = tiles_cfg.get("max_per_tile")

    # Resolve memory_limit: CLI > config > hardcoded default
    memory_limit = args.memory_limit if args.memory_limit is not None else (
        config_memory_limit if config_memory_limit is not None else "48GB"
    )

    # Resolve max_per_tile: CLI > config > hardcoded default
    max_per_tile = args.max_per_tile if args.max_per_tile is not None else (
        config_max_per_tile if config_max_per_tile is not None else 1000
    )

    # Build bbox: None means no filter
    bbox = tuple(args.bbox) if args.bbox is not None else None

    # Build parquet_glob: derive node/way paths for OSM, single string otherwise
    if args.source == "osm":
        parquet_glob = (
            f"{args.parquet_dir}/type=node/*.parquet",
            f"{args.parquet_dir}/type=way/*.parquet",
        )
    else:
        parquet_glob = args.parquet

    run_pipeline(
        args.source,
        parquet_glob,
        bbox,
        args.output,
        memory_limit=memory_limit,
        max_per_tile=max_per_tile,
    )


if __name__ == "__main__":
    main()
