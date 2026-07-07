"""Pipeline stage functions extracted from quadtree.py for testability.

This module contains individual stage functions that were previously part of
the monolithic run_pipeline() function. Each stage corresponds to a logical
step in the import-assign-containment-export pipeline.

TODO: The _SOURCES dict here duplicates SOURCES in quadtree.py. This duplication
exists because stages.py must not import from quadtree.py (circular import risk),
and quadtree.py needs to import from stages.py for backward compatibility.
Consider consolidating in a future refactor.
"""
import glob as glob_module
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


def _is_output_fresh(output_path: str, input_paths: list[str]) -> bool:
    """True if output exists and is strictly newer than all inputs.

    Equal timestamps count as stale (triggering a rebuild).
    Returns False if input_paths is empty or output doesn't exist.
    """
    if not input_paths:
        return False
    if not os.path.exists(output_path):
        return False
    out_mtime = os.path.getmtime(output_path)
    for inp in input_paths:
        if not os.path.exists(inp):
            return False
        if os.path.getmtime(inp) >= out_mtime:
            return False
    return True


def _resolve_glob_paths(pattern: str) -> list[str]:
    """Expand a glob pattern to a sorted list of file paths."""
    return sorted(glob_module.glob(pattern))


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


def compute_containment(
    con,
    boundaries_db,
    pk_expr,
    lon_expr,
    lat_expr,
    collection_prefix="org.atgeo.places.overture.division",
    covering_dir=None,
    containment_dir=None,
):
    """Write containment/<qk4>.parquet and create the place_containment VIEW.

    Empty place_containment table when boundaries_db is None,
    covering_dir is absent/empty, or no prefix produced rows (Q3).

    Implements §3.2 of docs/covering-containment-design.md (Phase 1).

    Args:
        con: Open DuckDB connection with a `places` table (qk17 column required)
             and a `tile_assignments` table (place_id, tile_qk).
        boundaries_db: Path to boundaries.duckdb, or None to skip containment.
        pk_expr: SQL expression for the place primary key (e.g. "pk", "p.id").
        lon_expr: SQL expression for place longitude.
        lat_expr: SQL expression for place latitude.
        collection_prefix: NSID prefix prepended to boundary IDs in rkey values.
            Defaults to "org.atgeo.places.overture.division".
        covering_dir: Directory containing covering/<qk4>.parquet files.
            When None or pointing to a nonexistent/empty directory, an empty
            place_containment table is created (Q3 graceful degradation, §3.1).
        containment_dir: Directory to write containment/<qk4>.parquet files.
            Created on first use.  When None, a temporary sibling directory
            is used (callers from run_pipeline always pass an explicit path).
    """
    import os
    from pathlib import Path

    con.execute("LOAD spatial")

    # Robust drop: DuckDB 1.2.1 raises CatalogException when DROP VIEW IF EXISTS
    # is called on a TABLE (and vice versa), so we try each type in turn.
    for _drop_type in ("VIEW", "TABLE"):
        try:
            con.execute(f"DROP {_drop_type} IF EXISTS place_containment")
        except duckdb.CatalogException:
            pass

    def _make_empty():
        con.execute("""
            CREATE TABLE place_containment (
                place_id       VARCHAR,
                relations_json VARCHAR
            )
        """)

    # Q3 short-circuit: no boundaries at all
    if boundaries_db is None:
        _make_empty()
        return

    # Q3 short-circuit: covering absent or empty
    if not covering_dir or not os.path.isdir(covering_dir):
        _make_empty()
        return

    covering_parquets = {
        f[:-8]: os.path.join(covering_dir, f)
        for f in os.listdir(covering_dir)
        if f.endswith(".parquet")
    }
    if not covering_parquets:
        _make_empty()
        return

    # Resolve covering zoom range from _meta.json (fallback to defaults)
    cover_min_zoom = 4
    cover_max_zoom = 12
    meta_path = os.path.join(covering_dir, "_meta.json")
    if os.path.exists(meta_path):
        try:
            import json
            with open(meta_path) as _f:
                _meta = json.load(_f)
            cover_min_zoom = _meta.get("cover_min_zoom", cover_min_zoom)
            cover_max_zoom = _meta.get("cover_max_zoom", cover_max_zoom)
        except Exception:
            pass

    # Resolve containment output directory
    if containment_dir is None:
        import tempfile
        _tmp_base = tempfile.mkdtemp(prefix="garganorn_containment_")
        containment_dir = _tmp_base
    os.makedirs(containment_dir, exist_ok=True)

    # Read the query template
    _sql_dir = Path(__file__).parent / "sql"
    template_sql = (_sql_dir / "compute_containment.sql").read_text()

    # Generate interior arms (one per zoom level L in [cover_min_zoom, cover_max_zoom])
    interior_arms = "\nUNION ALL\n".join(
        f"    SELECT p.place_id, c.boundary_id, c.level\n"
        f"    FROM p JOIN cov c\n"
        f"      ON c.kind = 'interior' AND len(c.tile_qk) = {L}\n"
        f"     AND left(p.qk17, {L}) = c.tile_qk"
        for L in range(cover_min_zoom, cover_max_zoom + 1)
    )

    con.execute(f"ATTACH '{boundaries_db}' AS bnd (READ_ONLY)")
    try:
        # Materialize places_slim once, sorted by qk17 (D2 zone-map optimization).
        # Places with NULL or invalid qk17 are excluded (§3.2): they are never
        # assigned a tile and never reach export (inner join on tile_assignments).
        con.execute("DROP TABLE IF EXISTS places_slim")
        con.execute(f"""
            CREATE TEMP TABLE places_slim AS
            SELECT {pk_expr} AS place_id,
                   p.qk17,
                   CAST(({lon_expr}) AS DOUBLE) AS lon,
                   CAST(({lat_expr}) AS DOUBLE) AS lat
            FROM places p
            WHERE p.qk17 IS NOT NULL AND length(p.qk17) = 17 AND p.qk17 ~ '^[0-3]{{17}}$'
            ORDER BY p.qk17
        """)

        # Prefix loop over qk4 prefixes present in places_slim
        place_prefixes = [
            row[0]
            for row in con.execute(
                "SELECT DISTINCT left(qk17, 4) FROM places_slim ORDER BY 1"
            ).fetchall()
        ]

        written_files = []

        for prefix in place_prefixes:
            covering_file = covering_parquets.get(prefix)
            if covering_file is None:
                # No boundary overlaps this z4 cell — skip
                continue

            tmp_out = os.path.join(containment_dir, f"{prefix}.parquet.tmp")
            final_out = os.path.join(containment_dir, f"{prefix}.parquet")

            # Substitute template parameters
            sql = template_sql
            sql = sql.replace("${prefix}", prefix)
            sql = sql.replace("${covering_file}", covering_file)
            sql = sql.replace("${interior_arms}", interior_arms)
            sql = sql.replace("${max_zoom}", str(cover_max_zoom))
            sql = sql.replace("${collection_prefix}", collection_prefix)

            con.execute(
                f"COPY ({sql}) TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            os.rename(tmp_out, final_out)
            written_files.append(final_out)

        if written_files:
            file_list = ", ".join(f"'{p}'" for p in written_files)
            con.execute(f"""
                CREATE OR REPLACE VIEW place_containment AS
                SELECT place_id, relations_json
                FROM read_parquet([{file_list}])
            """)
        else:
            _make_empty()

    finally:
        con.execute("DROP TABLE IF EXISTS places_slim")
        try:
            con.execute("DETACH bnd")
        except Exception:
            pass


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
        # EXPORT-14: Use json.dumps() for proper escaping of attribution string.
        joined = ",".join(records)
        source_cls = _SOURCES[source]
        # Build payload dict and serialize with json.dumps() to handle special chars
        payload_dict = {
            "collection": source_cls.collection,
            "attribution": source_cls.attribution,
            "records": json.loads(f"[{joined}]")  # Parse joined JSON strings back to list
        }
        payload = json.dumps(payload_dict)
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
    # EXPORT-3/EXPORT-8: Transform OSM rkeys to match export format (n12345 → node:12345)
    if source == "osm":
        con.execute("""
            CREATE TABLE manifest.record_tiles AS
            SELECT CASE left(place_id, 1)
                       WHEN 'n' THEN 'node:' || substr(place_id, 2)
                       WHEN 'w' THEN 'way:' || substr(place_id, 2)
                       WHEN 'r' THEN 'relation:' || substr(place_id, 2)
                       ELSE place_id
                   END AS rkey, tile_qk
            FROM tile_assignments
            ORDER BY rkey
        """)
    else:
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
    """Check if two bboxes intersect, handling antimeridian crossing.

    For antimeridian-crossing bboxes (xmin > xmax), the bbox wraps around the
    ±180° meridian. This function detects crossing and computes intersection
    correctly for all combinations of normal and crossing bboxes.
    """
    a_crosses = a[0] > a[2]  # xmin > xmax indicates antimeridian crossing
    b_crosses = b[0] > b[2]

    # Latitude check is always the same (no wrapping in latitude)
    if a[1] > b[3] or a[3] < b[1]:
        return False

    if not a_crosses and not b_crosses:
        # Normal case: neither box crosses the antimeridian
        return a[0] <= b[2] and a[2] >= b[0]

    if a_crosses and not b_crosses:
        # a wraps around: check if b overlaps the western or eastern part of a
        return a[2] >= b[0] or a[0] <= b[2]

    if not a_crosses and b_crosses:
        # b wraps around: check if a overlaps the western or eastern part of b
        return b[2] >= a[0] or b[0] <= a[2]

    # Both wrap: they must overlap (both span the antimeridian)
    return True


def stage_import(con, source, parquet_glob, bbox, memory_limit, t0,
                  density_parquet=None, idf_parquet=None,
                  density_norm=10.0, idf_norm=18.0, pop_norm=20.0):
    """Import parquet data into places table. Handles bbox unpacking and source dispatch.

    Phase 2: importance and variants computation are now absorbed into the import stage.
    This function passes density_parquet, idf_parquet, and normalization constants
    to the import SQL for inline computation.

    Phase 4: density_parquet includes tile bounds (tile_xmin, tile_ymin, tile_xmax,
    tile_ymax) computed by stage_density_extract via Python post-processing. This
    enables bbox-overlap joins in overture_division_import.sql for accurate density
    scoring of small localities.

    Args:
        con: Open DuckDB connection.
        source: Source key (foursquare, overture_place, osm, overture_division).
        parquet_glob: Parquet path(s).
        bbox: (xmin, ymin, xmax, ymax) or None for global.
        memory_limit: DuckDB memory limit string.
        t0: Start time for logging.
        density_parquet: Path to density_tiles.parquet (optional, defaults to empty).
        idf_parquet: Path to idf_scores.parquet (optional, defaults to empty for fsq/overture/osm).
        density_norm: Density normalization constant (default 10.0).
        idf_norm: IDF normalization constant (default 18.0).
        pop_norm: Population normalization constant for divisions (default 20.0).
    """
    # Validate norm constants (SCORE-1/2/3/4)
    invalid_norms = []
    for name, val in [("density_norm", density_norm), ("idf_norm", idf_norm), ("pop_norm", pop_norm)]:
        if val is not None and val <= 0:
            invalid_norms.append(name)

    if invalid_norms:
        raise ValueError(f"{', '.join(invalid_norms)} must be positive")

    xmin, ymin, xmax, ymax = bbox if bbox is not None else (-180, -90, 180, 90)

    # For OSM, read the category snippet and pass it as ${osm_category_case}
    osm_category_case = None
    if source == "osm":
        osm_category_case = (_SQL_DIR / "_osm_category_case.sql").read_text().strip()

    # Build density and IDF CTEs based on whether paths are provided
    # When None, create empty temp tables (LEFT JOINs produce NULL, importance defaults to 0)
    if source == "overture_division":
        # Divisions only use density, no IDF
        if density_parquet:
            density_cte = f"CREATE TEMP TABLE density_tiles AS SELECT * FROM read_parquet('{density_parquet}');"
        else:
            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"

        division_parquet, division_area_parquet = parquet_glob
        _run_sql(con, source, "import", "overture_division_import.sql", t0,
                 memory_limit=memory_limit, division_parquet=division_parquet,
                 division_area_parquet=division_area_parquet,
                 xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax,
                 density_cte=density_cte, density_norm=density_norm, pop_norm=pop_norm)
    else:
        # FSQ, Overture, OSM use both density and IDF
        if density_parquet:
            density_cte = f"CREATE TEMP TABLE density_tiles AS SELECT * FROM read_parquet('{density_parquet}');"
        else:
            density_cte = "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"

        if idf_parquet:
            idf_cte = f"CREATE TEMP TABLE idf_scores AS SELECT * FROM read_parquet('{idf_parquet}');"
        else:
            idf_cte = "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"

        if source == "osm":
            node_parquet, way_parquet = parquet_glob
            _run_sql(con, source, "import", "osm_import.sql", t0,
                     memory_limit=memory_limit, node_parquet=node_parquet,
                     way_parquet=way_parquet, xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax,
                     osm_category_case=osm_category_case,
                     density_cte=density_cte, idf_cte=idf_cte,
                     density_norm=density_norm, idf_norm=idf_norm)
        else:
            _run_sql(con, source, "import", f"{source}_import.sql", t0,
                     memory_limit=memory_limit, parquet_glob=parquet_glob,
                     xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax,
                     density_cte=density_cte, idf_cte=idf_cte,
                     density_norm=density_norm, idf_norm=idf_norm)


def stage_density_extract(parquet_glob: str, output_path: str, t0: float,
                          force: bool = False) -> None:
    """Extract z15 density tiles from Overture place parquet.

    Runs a global density extract (no bbox filter) against the source
    parquet file. Groups places by their z15 quadtile and computes
    ln(1 + count) as density_score. Output is written to output_path
    and reused in importance computation across all place sources.

    Phase 4: Tile bounds (tile_xmin, tile_ymin, tile_xmax, tile_ymax) are
    computed via Python post-processing using quadkey_to_bbox(). This ensures
    bbox-overlap joins work correctly for small localities.

    Args:
        parquet_glob: Glob pattern for Overture place parquet files.
        output_path: Destination path for density parquet output.
        t0: Start time for logging (monotonic time).
        force: If True, re-run even if output is fresh. Default False.
    """
    if not force:
        input_files = _resolve_glob_paths(parquet_glob)
        if _is_output_fresh(output_path, input_files):
            log.info("density_extract: skipping (output is fresh)")
            return
    log.info("density_extract: starting (ephemeral connection)")
    sql = (_SQL_DIR / "density_extract.sql").read_text()
    sql = sql.replace("${parquet_glob}", str(parquet_glob))
    con = duckdb.connect()
    try:
        con.execute(sql)
        # Read results and add tile bounds via Python post-processing
        rows = con.execute("SELECT tile_qk15, density_score FROM density_tiles ORDER BY tile_qk15").fetchall()

        # Build output rows with tile bounds computed from quadkey
        output_rows = []
        for tile_qk15, density_score in rows:
            tile_xmin, tile_ymin, tile_xmax, tile_ymax = quadkey_to_bbox(tile_qk15)
            output_rows.append((
                tile_qk15,
                density_score,
                tile_xmin,
                tile_ymin,
                tile_xmax,
                tile_ymax
            ))

        # Create table with expanded schema using proper SQL
        con.execute("""
            CREATE TABLE density_export (
                tile_qk15 VARCHAR,
                density_score DOUBLE,
                tile_xmin DOUBLE,
                tile_ymin DOUBLE,
                tile_xmax DOUBLE,
                tile_ymax DOUBLE
            )
        """)

        # Insert rows using executemany for batch insert
        con.executemany("INSERT INTO density_export VALUES (?, ?, ?, ?, ?, ?)", output_rows)

        con.execute(f"COPY density_export TO '{output_path}' (FORMAT PARQUET)")
        count = len(output_rows)
        log.info("density_extract: done (%.1fs, %d z15 tiles)",
                 time.monotonic() - t0, count)
    finally:
        con.close()


def stage_idf(source, parquet_glob, output_path, t0, force=False):
    """Compute IDF scores per category from raw parquet.

    Reads source parquet directly (ephemeral DuckDB connection), computes
    ln(N_total / n_places) per category, and writes results to output_path
    as a parquet file with columns (category, n_places, idf_score).

    Args:
        source: Source key (foursquare, overture_place, osm).
        parquet_glob: Parquet path(s). String for FSQ/Overture;
            (node_glob, way_glob) tuple for OSM.
        output_path: Destination path for IDF parquet output.
        t0: Start time for logging (monotonic time).
        force: If True, re-run even if output is fresh. Default False.
    """
    if source not in ("foursquare", "overture_place", "osm"):
        raise ValueError(f"unsupported source for IDF: {source}")

    if not force:
        if source == "osm":
            node_glob, way_glob = parquet_glob
            input_files = _resolve_glob_paths(node_glob) + _resolve_glob_paths(way_glob)
        else:
            input_files = _resolve_glob_paths(parquet_glob)
        if _is_output_fresh(output_path, input_files):
            log.info("idf: skipping (output is fresh)")
            return

    log.info("idf: starting (ephemeral connection)")
    sql = (_SQL_DIR / f"{source}_idf.sql").read_text()

    # For OSM, read the category snippet and pass it as ${osm_category_case}
    if source == "osm":
        node_glob, way_glob = parquet_glob
        osm_category_case = (_SQL_DIR / "_osm_category_case.sql").read_text().strip()
        sql = sql.replace("${osm_category_case}", osm_category_case)
        sql = sql.replace("${node_parquet}", str(node_glob))
        sql = sql.replace("${way_parquet}", str(way_glob))
    else:
        sql = sql.replace("${parquet_glob}", str(parquet_glob))

    con = duckdb.connect()
    try:
        con.execute(sql)
        con.execute(f"COPY idf_scores TO '{output_path}' (FORMAT PARQUET)")
        count = con.execute("SELECT count(*) FROM idf_scores").fetchone()[0]
        log.info("idf: done (%.1fs, %d categories)", time.monotonic() - t0, count)
    finally:
        con.close()


def stage_tile_assignment(con, source, pk_expr, max_per_tile, t0):
    """Assign each place to quadtree tiles."""
    _run_sql(con, source, "tile assignment", "compute_tile_assignments.sql", t0,
             pk_expr=pk_expr, min_zoom=6, max_zoom=17, max_per_tile=max_per_tile)

    # EXPORT-6: Log warning if places were dropped (NULL or malformed qk17)
    total = con.execute("SELECT count(*) FROM places").fetchone()[0]
    assigned = con.execute("SELECT count(*) FROM tile_assignments").fetchone()[0]
    dropped = total - assigned
    if dropped > 0:
        log.warning("tile assignment: %d places dropped (NULL qk17 or invalid)", dropped)

    # EXPORT-7: Check for duplicate place_ids
    dupes = con.execute("""
        SELECT place_id, count(*) AS cnt
        FROM tile_assignments
        GROUP BY place_id
        HAVING cnt > 1
    """).fetchall()
    if dupes:
        log.error("tile assignment: %d places assigned to multiple tiles", len(dupes))


def stage_containment(con, source, pk_expr, lon_expr, lat_expr, boundaries_db, t0,
                      covering_dir=None, containment_dir=None):
    """Populate place_containment with boundary relations.

    Creates an empty place_containment table when boundaries_db is None,
    covering_dir is absent/empty, or no prefix produced rows (Q3 degradation).
    Delegates to compute_containment(); see its docstring for full details.
    """
    compute_containment(con, boundaries_db, pk_expr, lon_expr, lat_expr,
                        covering_dir=covering_dir, containment_dir=containment_dir)


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


def export_boundaries_db(work_db_path: str, source_dir: str, t0: float) -> None:
    """Export boundaries.duckdb from a working DuckDB file.

    Creates boundaries.duckdb in source_dir by reading division places from
    work_db_path. Uses atomic write-to-temp-then-rename pattern. Creates
    an R-tree index on the geometry column for fast spatial queries.

    Args:
        work_db_path: Path to working DuckDB file containing populated places table.
        source_dir: Directory where boundaries.duckdb will be written.
        t0: Start time for logging (monotonic time).
    """
    boundaries_path = os.path.join(source_dir, "boundaries.duckdb")
    boundaries_tmp = boundaries_path + ".tmp"
    if os.path.exists(boundaries_tmp):
        os.remove(boundaries_tmp)

    log.info("DuckDB boundary export: starting (work_db=%s)", work_db_path)

    # Open ephemeral connection to the working database
    con = duckdb.connect(work_db_path)
    try:
        con.execute("LOAD spatial")
        con.execute(f"ATTACH '{boundaries_tmp}' AS bnd")
        try:
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
        finally:
            con.execute("DETACH bnd")
        os.rename(boundaries_tmp, boundaries_path)
        count = con.execute("SELECT count(*) FROM places WHERE admin_level BETWEEN 0 AND 2 OR subtype = 'locality'").fetchone()[0]
        log.info("DuckDB boundary export: done (%.1fs, %d boundaries)",
                 time.monotonic() - t0, count)
    except Exception:
        if os.path.exists(boundaries_tmp):
            try:
                os.remove(boundaries_tmp)
            except OSError:
                pass
        raise
    finally:
        con.close()


def stage_boundary_export(con, source, source_dir, t0):
    """Export boundaries.duckdb for overture_division. No-op for other sources.

    Phase 3: This is a thin wrapper around export_boundaries_db() for backward
    compatibility. The boundary export logic has been extracted to export_boundaries_db()
    which can be called standalone. This wrapper extracts the database path from the
    connection and delegates to export_boundaries_db().

    Note: This wrapper tries to determine the database file path from the connection.
    For test scenarios with relative paths, it searches common locations. For production
    use with quadtree.run_pipeline(), the database path is passed explicitly to
    export_boundaries_db().
    """
    if source != "overture_division":
        return

    # Extract the database path from the connection
    # For in-memory databases, current_database() returns 'memory'
    # For file-based databases, current_database() returns the database name (without path or extension)
    db_name = con.execute("SELECT current_database()").fetchone()[0]

    if db_name == 'memory':
        raise ValueError("Cannot export boundaries from in-memory database")

    # Try different methods to get the full database path
    work_db_path = None

    # Method 1: Try pragmas that might have the path (different DuckDB versions)
    try:
        # Try various pragmas that might contain path information
        for pragma_func in ['pragma_database_list', 'pragma_databases', 'pragma_database_size']:
            try:
                if pragma_func == 'pragma_database_list':
                    result = con.execute(f"SELECT path FROM {pragma_func}() WHERE name = current_database()").fetchone()
                elif pragma_func == 'pragma_databases':
                    result = con.execute(f"SELECT path FROM {pragma_func}() WHERE name = current_database()").fetchone()
                else:
                    result = con.execute(f"SELECT * FROM {pragma_func}()").fetchone()

                # Check if the result looks like a valid file path
                # It should contain path separators or start with /
                if result and result[0] and result[0] != '' and result[0] != ':memory:':
                    potential_path = result[0]
                    # Only accept if it looks like a path (contains separators or is absolute)
                    if os.path.sep in potential_path or (os.path.altsep and os.path.altsep in potential_path) or potential_path.startswith('/'):
                        work_db_path = potential_path
                        break
            except Exception:
                continue
    except Exception:
        pass

    # Method 2: Check if db_name contains path separators (full path case)
    if not work_db_path:
        if os.path.sep in db_name or (os.path.altsep and os.path.altsep in db_name):
            work_db_path = db_name
        else:
            # Method 3: db_name is just a filename, try to find it
            db_with_ext = db_name + ".duckdb"

            # First, check if source_dir has a parent directory with the database
            # (test case: database is in tmp_path, source_dir is tmp_path/output)
            source_dir_parent = os.path.dirname(source_dir) if source_dir else None
            if source_dir_parent:
                potential_path = os.path.join(source_dir_parent, db_with_ext)
                if os.path.exists(potential_path):
                    work_db_path = potential_path

            # If not found, try source_dir itself
            if not work_db_path:
                potential_path = os.path.join(source_dir, db_with_ext)
                if os.path.exists(potential_path):
                    work_db_path = potential_path

            # If still not found, try current working directory
            if not work_db_path:
                if os.path.exists(db_with_ext):
                    work_db_path = db_with_ext
                else:
                    # Method 4: Last resort - raise error with helpful message
                    raise ValueError(
                        f"Cannot determine full path for database '{db_name}'. "
                        f"Searched in: source_dir='{source_dir}', "
                        f"parent_dir='{source_dir_parent}', cwd='{os.getcwd()}'. "
                        f"For test scenarios, ensure the database file exists with .duckdb extension."
                    )

    export_boundaries_db(work_db_path, source_dir, t0)
