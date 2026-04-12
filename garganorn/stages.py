"""Pipeline stage functions extracted from quadtree.py for testability.

This module contains individual stage functions that were previously part of
the monolithic run_pipeline() function. Each stage corresponds to a logical
step in the import-assign-containment-export pipeline.

TODO: The _SOURCES dict here duplicates SOURCES in quadtree.py. This duplication
exists because stages.py must not import from quadtree.py (circular import risk),
and quadtree.py needs to import from stages.py for backward compatibility.
Consider consolidating in a future refactor.
"""
import gzip
import json
import logging
import math
import os
import re
import string
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from garganorn.database import FoursquareOSP, OverturePlaces, OpenStreetMap, OvertureDivisions

log = logging.getLogger(__name__)

_SOURCES = {
    cls.source_key: cls
    for cls in [FoursquareOSP, OverturePlaces, OpenStreetMap, OvertureDivisions]
}

REPO = "places.atgeo.org"
_SQL_DIR = Path(__file__).parent / "sql"


def _run_sql(con, source, stage, filename, t0, **params):
    """Read SQL from file, substitute ${var} params, execute, and log."""
    log.info("[%s] %s: starting", source, stage)
    sql = (_SQL_DIR / filename).read_text()
    for k, v in params.items():
        sql = sql.replace(f"${{{k}}}", str(v))
    con.execute(sql)
    count = con.execute("SELECT count(*) FROM places").fetchone()[0]
    log.info("[%s] %s: done (%.1fs, %d places)", source, stage, time.monotonic() - t0, count)


def _coord_exprs(source, alias=""):
    """Return (lon_expr, lat_expr) SQL expressions for the given source.

    When alias is provided, column and struct field references are qualified
    with that table alias (e.g. "t.longitude" instead of "longitude").
    """
    prefix = f"{alias}." if alias else ""
    if source in ("overture_place", "overture_division"):
        return (f"({prefix}bbox.xmin + {prefix}bbox.xmax) / 2.0",
                f"({prefix}bbox.ymin + {prefix}bbox.ymax) / 2.0")
    return f"{prefix}longitude", f"{prefix}latitude"


def _run_containment(con, qk_prefix, bbox, zoom, pk_expr, lon_expr, lat_expr,
                     collection_prefix, stats):
    """Process a single tile: clip boundaries, run phase-1/phase-2 containment.

    Inserts matching rows into place_containment. Updates stats in-place.

    Args:
        con: Open DuckDB connection with `places` and attached `bnd` database.
        qk_prefix: Quadkey prefix string for this tile (length == zoom).
        bbox: (lon_min, lat_min, lon_max, lat_max) bounding box for qk_prefix.
        zoom: Tile zoom level (== len(qk_prefix)).
        pk_expr: SQL expression for place primary key.
        lon_expr: SQL expression for place longitude.
        lat_expr: SQL expression for place latitude.
        collection_prefix: NSID prefix for boundary rkey values.
        stats: Mutable dict; this function increments leaf_tiles and updates
            max_depth. The subdivisions key is not read or written here.
    """
    stats["leaf_tiles"] += 1
    stats["max_depth"] = max(stats["max_depth"], zoom)
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
            WHERE LEFT(p.qk17, ?) = ?
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
            WHERE LEFT(p.qk17, ?) = ?
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
    """, [zoom, qk_prefix, zoom, qk_prefix])

    elapsed = time.monotonic() - t_tile
    log.info("compute_containment: z%d qk=%s boundaries=%d phase1=%d (%.1fs)",
             zoom, qk_prefix, tile_boundary_count, phase1_count, elapsed)


def _process_tile(con, qk_prefix, pk_expr, lon_expr, lat_expr,
                  collection_prefix, max_boundaries, max_zoom, stats):
    """Recursively process a tile, subdividing if boundary count exceeds max_boundaries.

    Subdivides into 4 children (quadkey digits 0-3) when the tile has more
    than max_boundaries boundaries and zoom < max_zoom. Otherwise delegates
    to _run_containment for the actual spatial join.

    Args:
        con: Open DuckDB connection.
        qk_prefix: Quadkey prefix for this tile.
        pk_expr: SQL expression for place primary key.
        lon_expr: SQL expression for place longitude.
        lat_expr: SQL expression for place latitude.
        collection_prefix: NSID prefix for boundary rkey values.
        max_boundaries: Subdivision threshold.
        max_zoom: Maximum zoom level; no further subdivision beyond this.
        stats: Mutable dict with keys subdivisions, leaf_tiles, max_depth.
    """
    zoom = len(qk_prefix)
    bbox = quadkey_to_bbox(qk_prefix)

    # Count boundaries intersecting this tile
    count = con.execute("""
        SELECT count(*) FROM bnd.places
        WHERE ST_Intersects(geometry, ST_MakeEnvelope(?, ?, ?, ?))
    """, [bbox[0], bbox[1], bbox[2], bbox[3]]).fetchone()[0]

    if count > max_boundaries and zoom < max_zoom:
        stats["subdivisions"] += 1
        for child in '0123':
            _process_tile(con, qk_prefix + child, pk_expr, lon_expr, lat_expr,
                          collection_prefix, max_boundaries, max_zoom, stats)
        return

    if count > max_boundaries:
        log.warning("compute_containment: z%d qk=%s has %d boundaries "
                    "(exceeds %d at max zoom)", zoom, qk_prefix, count,
                    max_boundaries)

    _run_containment(con, qk_prefix, bbox, zoom, pk_expr, lon_expr, lat_expr,
                     collection_prefix, stats)


def compute_containment(con, boundaries_db, pk_expr, lon_expr, lat_expr,
                        collection_prefix="org.atgeo.places.overture.division",
                        max_boundaries=200, max_zoom=14):
    """Populate place_containment with boundary relations for each place.

    Creates place_containment(place_id, relations_json). Returns an empty
    table if boundaries_db is None.

    Uses an adaptive quadtree strategy: starting from z6 seed tiles, each
    tile is subdivided into 4 children (up to max_zoom) if the number of
    intersecting boundaries exceeds max_boundaries. This reduces per-tile
    boundary counts in dense areas while keeping coarse tiles for sparse
    regions.

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
        max_boundaries: Maximum number of boundaries allowed in a tile before
            it is subdivided. Defaults to 200.
        max_zoom: Maximum zoom level for subdivision. Tiles at this zoom are
            processed even if they exceed max_boundaries. Defaults to 14.

    The boundaries database is attached under the alias `bnd` (generic, not
    source-specific) and detached when processing completes. The `bnd.places`
    table must have columns `id`, `geometry`, `admin_level`, `min_latitude`,
    `max_latitude`, `min_longitude`, `max_longitude`.

    Within each leaf tile, a three-step approach reduces both boundary count
    and vertex complexity before running per-point containment:

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

    Correctness depends on each place belonging to exactly one leaf tile
    (determined by the appropriate prefix of its qk17). The CROSS JOIN in
    phase 1 assigns all phase-1 boundaries to every place in that tile; if a
    place appeared in multiple tiles, it would receive duplicate boundary
    assignments.
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
        stats = {"subdivisions": 0, "leaf_tiles": 0, "max_depth": 0}
        log.info("compute_containment: processing %d z6 seed tiles", len(z6_tiles))
        for z6 in z6_tiles:
            _process_tile(con, z6, pk_expr, lon_expr, lat_expr,
                          collection_prefix, max_boundaries, max_zoom, stats)
        log.info("compute_containment: %d leaf tiles, %d subdivisions, max depth z%d",
                 stats["leaf_tiles"], stats["subdivisions"], stats["max_depth"])
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
    raw = (_SQL_DIR / f"{source}_export_tiles.sql").read_text()
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
        source_cls = _SOURCES[source]
        payload = f'{{"collection":"{source_cls.collection}","attribution":"{source_cls.attribution}","records":[{joined}]}}'
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


def stage_import(con, source, parquet_glob, bbox, memory_limit, t0):
    """Import parquet data into places table. Handles bbox unpacking and source dispatch."""
    xmin, ymin, xmax, ymax = bbox if bbox is not None else (-180, -90, 180, 90)
    if source == "osm":
        node_parquet, way_parquet = parquet_glob
        _run_sql(con, source, "import", "osm_import.sql", t0,
                 memory_limit=memory_limit, node_parquet=node_parquet,
                 way_parquet=way_parquet, xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)
    elif source == "overture_division":
        division_parquet, division_area_parquet = parquet_glob
        _run_sql(con, source, "import", "overture_division_import.sql", t0,
                 memory_limit=memory_limit, division_parquet=division_parquet,
                 division_area_parquet=division_area_parquet,
                 xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)
    else:
        _run_sql(con, source, "import", f"{source}_import.sql", t0,
                 memory_limit=memory_limit, parquet_glob=parquet_glob,
                 xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)


def stage_density_extract(parquet_glob: str, output_path: str, t0: float) -> None:
    """Extract z15 density tiles from Overture place parquet.

    Runs a global density extract (no bbox filter) against the source
    parquet file. Groups places by their z15 quadtile and computes
    ln(1 + count) as density_score. Output is written to output_path
    and reused in importance computation across all place sources.

    Args:
        parquet_glob: Glob pattern for Overture place parquet files.
        output_path: Destination path for density parquet output.
        t0: Start time for logging (monotonic time).
    """
    log.info("density_extract: starting (ephemeral connection)")
    sql = (_SQL_DIR / "density_extract.sql").read_text()
    sql = sql.replace("${parquet_glob}", str(parquet_glob))
    con = duckdb.connect()
    try:
        con.execute(sql)
        con.execute(f"COPY density_tiles TO '{output_path}' (FORMAT PARQUET)")
        count = con.execute("SELECT count(*) FROM density_tiles").fetchone()[0]
        log.info("density_extract: done (%.1fs, %d z15 tiles)",
                 time.monotonic() - t0, count)
    finally:
        con.close()


def stage_importance(con, source, t0, density_parquet, density_norm=10.0, idf_norm=18.0):
    """Compute importance scores using density and IDF.

    Runs source-specific importance SQL which LEFT JOINs density_parquet
    (z15 tile density from stage_density_extract) and computes IDF scores
    per category. Final importance = 60% density + 40% IDF, both normalized
    and clamped to [0, 100].

    Args:
        con: Open DuckDB connection with places table.
        source: Source key for SQL filename lookup.
        t0: Start time for logging.
        density_parquet: Path to z15 density parquet from stage_density_extract.
        density_norm: Density score normalization factor (default 10.0).
        idf_norm: IDF score normalization factor (default 18.0).

    Note: Caller must guard for overture_division (no importance computation).
    """
    _run_sql(con, source, "importance", f"{source}_importance.sql", t0,
             density_parquet=density_parquet, density_norm=density_norm, idf_norm=idf_norm)


def stage_variants(con, source, t0):
    """Compute name variants. Guard for overture_division is in the caller."""
    _run_sql(con, source, "variants", f"{source}_variants.sql", t0)


def stage_tile_assignment(con, source, pk_expr, max_per_tile, t0):
    """Assign each place to quadtree tiles."""
    _run_sql(con, source, "tile assignment", "compute_tile_assignments.sql", t0,
             pk_expr=pk_expr, min_zoom=6, max_zoom=17, max_per_tile=max_per_tile)


def stage_containment(con, source, pk_expr, lon_expr, lat_expr, boundaries_db, t0):
    """Populate place_containment with boundary relations. No-op if boundaries_db is None."""
    compute_containment(con, boundaries_db, pk_expr, lon_expr, lat_expr)


def stage_export(con, source, tile_dir, t0, export_workers=None):
    """Export tiles as gzipped JSON files. Returns manifest dict {qk: count}."""
    log.info("[%s] export: starting", source)
    manifest = export_tiles(con, tile_dir, source, max_workers=export_workers)
    log.info("[%s] export: %d tiles, %d records (%.1fs)",
             source, len(manifest), sum(manifest.values()), time.monotonic() - t0)
    return manifest


def stage_manifest(con, manifest, source, tile_dir, t0):
    """Write manifest.json and manifest.duckdb."""
    write_manifest(manifest, tile_dir, source)
    write_manifest_db(con, tile_dir, source)


def stage_division_importance_backfill(con, density_parquet, t0,
                                       density_norm=10.0, pop_norm=20.0):
    """Backfill division importance from density + population.

    Localities get 60% density + 40% population. Non-localities get
    population only. Density is the average density_score of z15 tiles
    whose centroids fall within the division's bbox.

    Args:
        con: Open DuckDB connection with places table (overture_division schema).
        density_parquet: Path to density_tiles.parquet (must have centroid_lon/centroid_lat).
        t0: Start time for logging (monotonic time).
        density_norm: Density score normalization factor (default 10.0).
        pop_norm: Population normalization factor (default 20.0).
    """
    _run_sql(con, "overture_division", "importance backfill",
             "division_importance_backfill.sql", t0,
             density_parquet=density_parquet, density_norm=density_norm,
             pop_norm=pop_norm)


def stage_boundary_export(con, source, source_dir, t0):
    """Export boundaries.duckdb for overture_division. No-op for other sources."""
    if source != "overture_division":
        return
    boundaries_path = os.path.join(source_dir, "boundaries.duckdb")
    boundaries_tmp = boundaries_path + ".tmp"
    if os.path.exists(boundaries_tmp):
        os.remove(boundaries_tmp)
    log.info("[%s] DuckDB boundary export: starting", source)
    con.execute(f"ATTACH '{boundaries_tmp}' AS bnd")
    con.execute("LOAD spatial")
    con.execute("""
        CREATE TABLE bnd.places AS
        SELECT id, geometry, admin_level,
               names, subtype, country, region, wikidata, population,
               min_latitude, max_latitude,
               min_longitude, max_longitude,
               importance, variants
        FROM places
        WHERE admin_level BETWEEN 0 AND 2
           OR subtype = 'locality'
        ORDER BY ST_Hilbert(geometry,
            {'min_x': -180.0, 'min_y': -90.0,
             'max_x': 180.0, 'max_y': 90.0}::BOX_2D)
    """)
    con.execute("CREATE INDEX bnd_places_rtree ON bnd.places USING RTREE(geometry)")
    con.execute("DETACH bnd")
    os.rename(boundaries_tmp, boundaries_path)
    log.info("[%s] DuckDB boundary export: done (%.1fs)", source, time.monotonic() - t0)
