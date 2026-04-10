import argparse
import gzip
import json
import logging
import math
import os
import re
import shutil
import string
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml

log = logging.getLogger(__name__)

SOURCE_PK = {
    "fsq": "fsq_place_id",
    "overture": "id",
    "osm": "rkey",
    "overture_division": "id",
}

ATTRIBUTION = {
    "fsq": "https://docs.foursquare.com/data-products/docs/access-fsq-os-places",
    "overture": "https://docs.overturemaps.org/attribution/",
    "osm": "https://www.openstreetmap.org/copyright",
    "overture_division": "https://docs.overturemaps.org/attribution/",
}

REPO = "places.atgeo.org"

_TIMESTAMP_RE = re.compile(r"^\d{8}T\d{6}$")


def _coord_exprs(source, alias=""):
    """Return (lon_expr, lat_expr) SQL expressions for the given source.

    When alias is provided, column and struct field references are qualified
    with that table alias (e.g. "t.longitude" instead of "longitude").
    """
    prefix = f"{alias}." if alias else ""
    if source in ("overture", "overture_division"):
        return (f"({prefix}bbox.xmin + {prefix}bbox.xmax) / 2.0",
                f"({prefix}bbox.ymin + {prefix}bbox.ymax) / 2.0")
    return f"{prefix}longitude", f"{prefix}latitude"


def compute_containment(con, boundaries_db, pk_expr, lon_expr, lat_expr,
                        collection_prefix="org.atgeo.places.overture.division"):
    """Populate place_containment with boundary relations for each place.

    Creates place_containment(place_id, relations_json). Returns an empty
    table if boundaries_db is None.

    Args:
        con: Open DuckDB connection with a `places` table (must have qk17 column).
        boundaries_db: Path to boundaries.duckdb, or None to skip containment.
        pk_expr: SQL expression for the place primary key column (e.g. "p.id").
        lon_expr: SQL expression for place longitude.
        lat_expr: SQL expression for place latitude.
        collection_prefix: NSID prefix prepended to boundary IDs in rkey values.
            Parameterized so the same function can be reused if the boundary
            source or collection changes without altering callers.
            Defaults to "org.atgeo.places.overture.division".

    The boundaries database is attached under the alias `bnd` (generic, not
    source-specific) and detached when processing completes. The `bnd.places`
    table must have columns `id`, `geometry`, `admin_level`, `min_latitude`,
    `max_latitude`, `min_longitude`, `max_longitude`.

    Places are grouped by z6 quadkey tile so each spatial join operates on
    a small spatial partition. Within each tile, a three-step approach
    reduces both boundary count and vertex complexity before running
    per-point containment:

      Step 0 (pre-filter and clip): ST_Intersects with R-tree narrows
      boundaries to those overlapping the tile envelope. ST_Intersection
      clips surviving geometries to the tile bbox, reducing vertex counts
      for boundaries that extend beyond the tile (e.g. country-spanning
      polygons clipped from hundreds of thousands of vertices to hundreds).
      Results are materialized to a temp table.

      Step 1 (phase 1 -- full containment): ST_Contains identifies clipped
      boundaries whose geometry fully contains the tile bbox. Every place
      in the tile is assigned to these boundaries via CROSS JOIN (no
      per-point geometry test).

      Step 2 (phase 2 -- per-point containment): ST_Contains runs per-point
      only for "edge" boundaries -- those that overlap the tile but were
      not matched in phase 1. Bbox pre-filter on lat/lon columns reduces
      the number of ST_Contains calls.

    Output relations contain only {rkey: ...} per boundary. Name, level, and
    other division metadata are not inlined here; clients resolve them from
    the division tile for each rkey.

    Correctness depends on each place belonging to exactly one z6 tile
    (determined by its qk17 prefix). The CROSS JOIN in phase 1 assigns
    all phase-1 boundaries to every place in that tile; if a place appeared
    in multiple tiles, it would receive duplicate boundary assignments.
    """
    con.execute("LOAD spatial")
    con.execute("""
        CREATE TABLE place_containment (
            place_id       VARCHAR,
            relations_json VARCHAR
        )
    """)

    if boundaries_db is None:
        return

    con.execute(f"ATTACH '{boundaries_db}' AS bnd (READ_ONLY)")
    try:
        z6_tiles = [
            row[0]
            for row in con.execute("SELECT DISTINCT LEFT(qk17, 6) FROM places").fetchall()
        ]
        total = len(z6_tiles)
        log.info("compute_containment: processing %d z6 tiles", total)
        for i, z6 in enumerate(z6_tiles, 1):
            bbox = quadkey_to_bbox(z6)
            t_tile = time.monotonic()

            # Step 0: pre-filter and clip boundaries to tile envelope
            con.execute("""
                CREATE OR REPLACE TEMP TABLE tile_boundaries AS
                SELECT id, admin_level,
                       ST_Intersection(geometry, ST_MakeEnvelope(?, ?, ?, ?)) AS geometry,
                       greatest(min_latitude, ?) AS min_latitude,
                       least(max_latitude, ?)    AS max_latitude,
                       greatest(min_longitude, ?) AS min_longitude,
                       least(max_longitude, ?)    AS max_longitude
                FROM bnd.places
                WHERE ST_Intersects(geometry, ST_MakeEnvelope(?, ?, ?, ?))
            """, [bbox[0], bbox[1], bbox[2], bbox[3],   # ST_Intersection envelope
                  bbox[1], bbox[3], bbox[0], bbox[2],   # bbox clamping (lat_min, lat_max, lon_min, lon_max)
                  bbox[0], bbox[1], bbox[2], bbox[3]])  # ST_Intersects WHERE

            tile_boundary_count = con.execute(
                "SELECT count(*) FROM tile_boundaries"
            ).fetchone()[0]

            # Phase 1: materialize full-tile containment matches as a temp table
            # so Phase 2 can use NOT EXISTS anti-join to skip them
            con.execute("""
                CREATE OR REPLACE TEMP TABLE phase1 AS
                SELECT id, admin_level FROM tile_boundaries
                WHERE ST_Contains(geometry, ST_MakeEnvelope(?, ?, ?, ?))
            """, [bbox[0], bbox[1], bbox[2], bbox[3]])

            phase1_count = con.execute("SELECT count(*) FROM phase1").fetchone()[0]

            # Phase 1 bulk assignment: CROSS JOIN all tile places with phase1 boundaries
            # Phase 2: per-point ST_Contains only for boundaries NOT in phase1
            # Combine and insert into place_containment
            con.execute(f"""
                INSERT INTO place_containment
                WITH bulk_assign AS (
                    SELECT {pk_expr} AS pk,
                           '{collection_prefix}:' || ph.id AS rkey,
                           ph.admin_level
                    FROM places p
                    CROSS JOIN phase1 ph
                    WHERE LEFT(p.qk17, 6) = ?
                ),
                edge_matches AS (
                    SELECT {pk_expr} AS pk,
                           '{collection_prefix}:' || b.id AS rkey,
                           b.admin_level
                    FROM places p
                    JOIN tile_boundaries b
                        ON {lat_expr} BETWEEN b.min_latitude AND b.max_latitude
                       AND {lon_expr} BETWEEN b.min_longitude AND b.max_longitude
                       AND ST_Contains(b.geometry, ST_Point({lon_expr}, {lat_expr}))
                    WHERE LEFT(p.qk17, 6) = ?
                      AND NOT EXISTS (
                          SELECT 1 FROM phase1 ph WHERE ph.id = b.id
                      )
                ),
                all_matches AS (
                    SELECT * FROM bulk_assign
                    UNION ALL
                    SELECT * FROM edge_matches
                )
                SELECT pk, to_json({{within: list(
                    {{rkey: rkey}}
                    ORDER BY admin_level ASC
                )}})::VARCHAR
                FROM all_matches
                GROUP BY pk
            """, [z6, z6])

            elapsed = time.monotonic() - t_tile
            log.info("compute_containment: tile %d/%d z6=%s boundaries=%d phase1=%d (%.1fs)",
                     i, total, z6, tile_boundary_count, phase1_count, elapsed)
    finally:
        con.execute("DROP TABLE IF EXISTS tile_boundaries")
        con.execute("DROP TABLE IF EXISTS phase1")
        con.execute("DETACH bnd")


def export_tiles(con, output_dir: str, source: str, max_workers: int = None) -> dict:
    """Query DuckDB for per-record JSON, group by tile_qk, write gzipped files.

    Streams results via fetchmany(1000) to keep memory bounded. One tile's
    records are accumulated in-memory at a time; on tile boundary, submits a
    flush job to a ThreadPoolExecutor. Backpressure limits inflight futures to
    2 * max_workers. Returns {qk: record_count}.
    """
    sql_dir = Path(__file__).parent / "sql"
    raw = (sql_dir / f"{source}_export_tiles.sql").read_text()
    sql = string.Template(raw).safe_substitute(repo=REPO)
    total_tiles = con.execute("SELECT COUNT(DISTINCT tile_qk) FROM tile_assignments").fetchone()[0]
    log.info("export: %d tiles to write", total_tiles)
    con.execute(sql)
    con.execute("SET enable_progress_bar = false")
    cursor = con.execute("SELECT tile_qk, record_json FROM tile_export ORDER BY tile_qk")

    def flush_tile(qk, records):
        # records are DuckDB to_json()::VARCHAR strings — already valid JSON.
        # String concatenation avoids json.loads/json.dumps overhead.
        # ATTRIBUTION values must be JSON-safe (no quotes, backslashes, or control chars).
        joined = ",".join(records)
        payload = f'{{"attribution":"{ATTRIBUTION[source]}","records":[{joined}]}}'
        subdir = os.path.join(output_dir, qk[:6])
        os.makedirs(subdir, exist_ok=True)
        with gzip.open(os.path.join(subdir, f"{qk}.json.gz"), "wb") as f:
            f.write(payload.encode("utf-8"))
        return (qk, len(records))

    manifest = {}
    current_qk = None
    accumulated = []
    futures = deque()
    max_inflight = 2 * (max_workers or os.cpu_count() or 4)

    def _drain_oldest():
        """Wait on oldest future, collect result into manifest, log progress."""
        qk, count = futures.popleft().result()
        manifest[qk] = count
        if len(manifest) % 1000 == 0:
            log.info("export: wrote %d tiles", len(manifest))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            batch = cursor.fetchmany(1000)
            if not batch:
                break
            for tile_qk, record_json in batch:
                if tile_qk != current_qk:
                    if current_qk is not None:
                        if len(futures) >= max_inflight:
                            _drain_oldest()
                        futures.append(executor.submit(flush_tile, current_qk, accumulated))
                    current_qk = tile_qk
                    accumulated = []  # rebind, not .clear() — workers hold a ref to the old list
                accumulated.append(record_json)

        if current_qk is not None:
            futures.append(executor.submit(flush_tile, current_qk, accumulated))

        # Drain remaining futures
        while futures:
            _drain_oldest()

    log.info("export: wrote %d tiles total", len(manifest))
    return manifest


def run_pipeline(source, parquet_glob, bbox, output_dir, memory_limit="48GB", max_per_tile=1000, boundaries_db=None, export_workers=None):
    """Run the full import-assign-containment-export pipeline for a data source.

    Stages:
      1. Import: load parquet into a `places` DuckDB table via source-specific SQL.
      2. Importance + variants: compute search ranking and name variants (skipped for
         overture_division, which inlines importance=0 and variants=[] in the import SQL).
      3. Tile assignment: assign each place to one or more quadtree tiles.
      4. Containment: populate place_containment with admin boundary relations
         (no-op if boundaries_db is None).
      5. Export tiles: write gzipped JSON tile files to a timestamped subdirectory.
      6. Manifest: write manifest.json and manifest.duckdb for tile serving.
      7. Boundary export (overture_division only): write boundaries.duckdb with
         Hilbert-sorted geometries and an R-tree index for use by other sources'
         containment stage.

    Output layout:
      <output_dir>/<source>/<timestamp>/   -- tile files, manifests
      <output_dir>/<source>/current        -- symlink to latest timestamp dir
      <output_dir>/overture_division/boundaries.duckdb  -- (overture_division only)

    The working DuckDB file is written to the tile directory and deleted on success.
    Old timestamped directories beyond the two most recent are removed.

    Args:
        source: Pipeline source key (fsq, overture, osm, overture_division).
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
    sql_dir = Path(__file__).parent / "sql"
    t0 = time.monotonic()

    try:
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

        # Import stage — OSM and overture_division need two separate parquet paths
        if source == "osm":
            node_parquet, way_parquet = parquet_glob
            run_sql("import", "osm_import.sql",
                    memory_limit=memory_limit,
                    node_parquet=node_parquet,
                    way_parquet=way_parquet,
                    xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)
        elif source == "overture_division":
            division_parquet, division_area_parquet = parquet_glob
            run_sql("import", "overture_division_import.sql",
                    memory_limit=memory_limit,
                    division_parquet=division_parquet,
                    division_area_parquet=division_area_parquet,
                    xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)
        else:
            run_sql("import", f"{source}_import.sql",
                    memory_limit=memory_limit,
                    parquet_glob=parquet_glob,
                    xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)

        if source not in ("overture_division",):
            run_sql("importance", f"{source}_importance.sql",
                    density_norm=10.0, idf_norm=18.0)
            run_sql("variants", f"{source}_variants.sql")

        pk_expr = SOURCE_PK[source]
        run_sql("tile assignment", "compute_tile_assignments.sql",
                pk_expr=pk_expr, min_zoom=6, max_zoom=17, max_per_tile=max_per_tile)

        lon_expr, lat_expr = _coord_exprs(source, alias="p")
        compute_containment(con, boundaries_db, f"p.{pk_expr}", lon_expr, lat_expr)

        log.info("[%s] export: starting", source)
        manifest = export_tiles(con, tile_dir, source, max_workers=export_workers)
        log.info("[%s] export: %d tiles, %d records (%.1fs)",
                 source, len(manifest),
                 sum(manifest.values()), time.monotonic() - t0)

        write_manifest(manifest, tile_dir, source)
        write_manifest_db(con, tile_dir, source)

        if source == "overture_division":
            # Write boundaries.duckdb for use by other sources' containment stage.
            # Rows are sorted by ST_Hilbert before insertion so the R-tree index
            # gets spatially coherent pages, improving range-query performance.
            # Written to a .tmp file first, then renamed atomically so a concurrent
            # reader never sees a partially-written file.
            boundaries_path = os.path.join(source_dir, "boundaries.duckdb")
            boundaries_tmp = boundaries_path + ".tmp"
            if os.path.exists(boundaries_tmp):
                os.remove(boundaries_tmp)
            log.info("[%s] boundary export: starting", source)
            con.execute(f"ATTACH '{boundaries_tmp}' AS bnd")
            con.execute("""
                CREATE TABLE bnd.places AS
                SELECT id, geometry, admin_level,
                       min_latitude, max_latitude,
                       min_longitude, max_longitude
                FROM places
                ORDER BY ST_Hilbert(geometry,
                    {'min_x': -180.0, 'min_y': -90.0,
                     'max_x': 180.0, 'max_y': 90.0}::BOX_2D)
            """)
            con.execute("CREATE INDEX bnd_places_rtree ON bnd.places USING RTREE(geometry)")
            con.execute("DETACH bnd")
            os.rename(boundaries_tmp, boundaries_path)
            log.info("[%s] boundary export: done (%.1fs)", source, time.monotonic() - t0)
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
    parser.add_argument("--source", required=True, choices=["fsq", "overture", "osm", "overture_division"],
                        help="Data source: fsq, overture, osm, or overture_division")
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
    )


if __name__ == "__main__":
    main()
