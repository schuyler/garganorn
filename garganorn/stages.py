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
import itertools
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

from garganorn import envelope
from garganorn.database import OverturePlaces, OpenStreetMap, OvertureDivisions
from garganorn.levels import LEVEL_VOCAB, level_case_sql

log = logging.getLogger(__name__)

_SOURCES = {
    cls.source_key: cls
    for cls in [OverturePlaces, OpenStreetMap, OvertureDivisions]
}

REPO = "places.atgeo.org"
_SQL_DIR = Path(__file__).parent / "sql"

# Compiled pattern for timestamp directory names produced by run_pipeline / stage_export.
_TIMESTAMP_RE = re.compile(r"^\d{8}T\d{6}$")


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


def artifact_fresh(artifact: str, inputs: list, params: dict) -> bool:
    """True iff artifact is up-to-date and safe to reuse.

    Checks (all must hold):
    1. artifact exists.
    2. <artifact>.meta.json exists and is valid JSON.
    3. meta['params'] == params.
    4. meta['inputs'] == inputs (caller-supplied list matches recorded list).
    5. mtime(meta) > mtime(every input) — strictly newer (equal = stale).
    6. mtime(artifact) <= mtime(meta) — meta written after artifact (atomicity
       guarantee; if artifact is newer, a crash interrupted finalize_artifact).
    """
    meta_path = artifact + ".meta.json"

    if not os.path.exists(artifact):
        return False
    if not os.path.exists(meta_path):
        return False

    try:
        meta = json.loads(Path(meta_path).read_text())
    except Exception:
        return False

    if meta.get("params") != params:
        return False

    if meta.get("inputs") != inputs:
        return False

    art_mtime = os.path.getmtime(artifact)
    meta_mtime = os.path.getmtime(meta_path)

    # Artifact must not be newer than meta (guards against crash between rename and meta write).
    if art_mtime > meta_mtime:
        return False

    # Meta must be strictly newer than every listed input.
    for inp in inputs:
        if not os.path.exists(inp):
            return False
        if os.path.getmtime(inp) >= meta_mtime:
            return False

    return True


def finalize_artifact(tmp_path: str, artifact: str, params: dict,
                      stats=None, inputs=None) -> None:
    """Atomically promote tmp_path → artifact and write a .meta.json sidecar.

    Sequence (order matters for atomicity):
    1. fsync tmp_path so data is durable before rename.
    2. os.replace(tmp_path, artifact) — atomic on POSIX.
    3. Write <artifact>.meta.json (LAST — meta presence signals completion).

    Args:
        tmp_path: Temporary file path (will be consumed/renamed).
        artifact: Destination path for the promoted artifact.
        params: Stage parameters to record in meta.
        stats: Optional statistics dict to record in meta.
        inputs: Optional list of input paths recorded in meta (default []).
    """
    if inputs is None:
        inputs = []

    # 1. fsync before rename so the content is durable.
    with open(tmp_path, "rb") as fh:
        os.fsync(fh.fileno())

    # 2. Atomic rename.
    os.replace(tmp_path, artifact)

    # 3. Write meta LAST — its presence is the completion signal.
    stage_name = Path(artifact).stem.split(".")[0]
    meta = {
        "stage": stage_name,
        "params": params,
        "inputs": inputs,
        "stats": stats,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    Path(artifact + ".meta.json").write_text(json.dumps(meta))


def _resolve_glob_paths(pattern: str, *, required: bool = False) -> list[str]:
    """Expand a glob pattern to a sorted list of file paths.

    Raises RuntimeError if `required` is True and the pattern matches nothing
    (a misconfigured or empty source cache would otherwise build silently on
    zero input files).
    """
    paths = sorted(glob_module.glob(pattern))
    if required and not paths:
        raise RuntimeError(
            f"configured source glob matched no files: {pattern!r}"
        )
    return paths


def _run_sql(con, source, stage, filename, t0, **params):
    """Read SQL from file, substitute ${var} params, execute, and log."""
    log.info("[%s] %s: starting", source, stage)
    sql = (_SQL_DIR / filename).read_text()
    for k, v in params.items():
        sql = sql.replace(f"${{{k}}}", str(v))
    con.execute(sql)
    count = con.execute("SELECT count(*) FROM places").fetchone()[0]
    log.info("[%s] %s: done (%.1fs, %d places)", source, stage, time.monotonic() - t0, count)


def _assert_unique_key(con, table, key_col, source_path):
    """Raise if `table`.`key_col` has a duplicate non-NULL value.

    stage_import LEFT JOINs density_tiles on tile_qk15 and idf_scores on
    category; a duplicate key multiplies the matching place's row through
    the join at every join site (overture_place/osm import SQL).
    Production data is clean today (uniqueness is guaranteed by the
    producing stage's GROUP BY -- e.g. density_extract.sql), but
    --density-parquet/--idf-parquet accept arbitrary files, so the guarantee
    is convention, not contract. Fail fast, before the import SQL runs any
    join: a silent wrong answer (one arbitrary score picked per duplicate
    key) is far worse than an early loud error.

    NULL keys are ignored: NULL = NULL is never true in SQL, so a NULL key
    can never fan out a LEFT JOIN, and count(key_col)/count(DISTINCT
    key_col) both exclude NULLs automatically. A single NULL-key row (e.g.
    density_extract.sql grouping a NULL-geometry/NULL-bbox row into a NULL
    tile_qk15 group) must not trip this guard.
    """
    non_null_total, non_null_distinct = con.execute(
        f"SELECT count({key_col}), count(DISTINCT {key_col}) FROM {table}"
    ).fetchone()
    if non_null_total != non_null_distinct:
        raise ValueError(
            f"{source_path!r} has a non-unique {key_col} "
            f"key: {non_null_total} rows but only {non_null_distinct} distinct "
            f"{key_col} value(s) (NULL {key_col} values are ignored). A "
            f"duplicate join key would silently multiply place rows through "
            f"the import SQL's LEFT JOIN; refusing to import."
        )


def _assert_interior_points(con, source_expr, description):
    """Fail loudly if any row's (interior_lon, interior_lat) is not
    ST_Within its own geometry. Standalone for direct unit-testability --
    ST_PointOnSurface should make this unreachable via real import data.
    Catches: NULL coordinates, NaN coordinates (e.g. from POINT EMPTY when
    ST_PointOnSurface has nothing to work with), and any point that ST_Within
    reports as outside the geometry (including via off-surface points from
    degenerate GEOMETRYCOLLECTION-shaped input)."""
    non_interior_count = con.execute(f"""
        SELECT count(*) FROM {source_expr}
        WHERE interior_lon IS NULL OR interior_lat IS NULL
           OR isnan(interior_lon) OR isnan(interior_lat)
           OR NOT ST_Within(ST_Point(interior_lon, interior_lat), geometry)
    """).fetchone()[0]
    if non_interior_count != 0:
        raise RuntimeError(
            f"{description}: {non_interior_count} rows have an interior "
            f"point outside their own geometry; ST_PointOnSurface should "
            f"guarantee this cannot happen. Never default or guess."
        )


def _assert_density_parquet_unique(density_parquet, *, temp_directory=None,
                                   max_temp_directory_size="250GB",
                                   memory_limit=None):
    """Check density_parquet's tile_qk15 key is unique, once, ahead of ANY
    stage_import dispatch.

    stage_import has three destinations for density_parquet: overture_place
    and osm build their own density_tiles TEMP TABLE directly (and used to
    assert uniqueness there); overture_division dispatches to
    stage_division_import and RETURNS before that point is ever reached, so
    it was invisible to the guard even though it is on the global run path
    and does receive density_parquet (quadtree.py _cmd_all). A duplicate
    tile_qk15 there does not fan out any join row count (division_density's
    avg(density_score) just double-counts the duplicate), so it produces a
    silently wrong importance value rather than an error -- exactly the
    failure mode _assert_unique_key exists to turn loud.

    Calling this once, here, ahead of the dispatch branch, is what keeps
    all three consumers on one guard instead of two independently
    maintained copies of it.

    The uniqueness count runs directly against read_parquet(density_parquet)
    rather than materializing it into a TEMP TABLE first, so DuckDB streams
    just the tile_qk15 column via projection pushdown instead of loading the
    whole file into memory.

    Args:
        temp_directory: DuckDB temp_directory for spill (optional). Threaded
            from stage_import's own temp_directory so this connection spills
            the same place every other connection in the import pipeline
            does, rather than to DuckDB's unbounded default.
        max_temp_directory_size: DuckDB max_temp_directory_size string,
            bounding spill (default "250GB"), independently of whether
            temp_directory is also supplied.
        memory_limit: DuckDB memory_limit string (optional). Threaded from
            stage_import's own memory_limit and set before any query runs on
            this connection, so the uniqueness count is bounded the same
            way the surrounding import connection is rather than running at
            DuckDB's default (~80% of RAM).
    """
    if not density_parquet:
        return
    con = duckdb.connect(":memory:")
    try:
        if memory_limit:
            con.execute(f"SET memory_limit = '{memory_limit}'")
        if temp_directory:
            con.execute(f"SET temp_directory = '{temp_directory}'")
        if max_temp_directory_size:
            con.execute(f"SET max_temp_directory_size = '{max_temp_directory_size}'")
        source_sql = density_parquet.replace("'", "''")
        _assert_unique_key(con, f"read_parquet('{source_sql}')", "tile_qk15", density_parquet)
    finally:
        con.close()


def _coord_exprs(source, alias=""):
    """Return (lon_expr, lat_expr) SQL expressions for the given source.

    When alias is provided, column and struct field references are qualified
    with that table alias (e.g. "t.longitude" instead of "longitude").
    """
    prefix = f"{alias}." if alias else ""
    if source == "overture_place":
        return (f"({prefix}bbox.xmin + {prefix}bbox.xmax) / 2.0",
                f"({prefix}bbox.ymin + {prefix}bbox.ymax) / 2.0")
    if source == "overture_division":
        return f"{prefix}interior_lon", f"{prefix}interior_lat"
    return f"{prefix}longitude", f"{prefix}latitude"


def compute_containment(
    places_parquet,
    tile_assignments_parquet,
    boundaries_db,
    pk_expr,
    lon_expr,
    lat_expr,
    containment_dir,
    *,
    collection_prefix="org.atgeo.places.overture.division",
    covering_dir=None,
    memory_limit="48GB",
    temp_directory=None,
    max_temp_directory_size="250GB",
    force=False,
    partition_zoom: int = 6,
) -> None:
    """Write <src>/containment/ with per-prefix parquet files and _meta.json (Phase 2).

    Sequence:
    - Reads places and tile_assignments from parquet artifacts.
    - Builds per-qk4-prefix containment parquet files under containment_dir.
    - Uses dir-swap atomicity (covering.py pattern): builds under .tmp/, writes
      _meta.json last inside .tmp/, renames .tmp/ → containment_dir.
    - Q3 degradation: boundaries_db=None, missing/empty covering_dir → writes
      containment_dir/_meta.json with "empty": true (no parquet files).
    - Freshness: skips rebuild if containment_dir/_meta.json is newer than all
      inputs and params+inputs match the recorded meta.

    Args:
        places_parquet: Path to places.parquet artifact (qk17-sorted).
        tile_assignments_parquet: Path to tile_assignments.parquet artifact.
        boundaries_db: Path to boundaries.duckdb, or None for Q3 degradation.
        pk_expr: SQL expression for the place primary key column in places parquet.
        lon_expr: SQL expression for the longitude column in places parquet.
        lat_expr: SQL expression for the latitude column in places parquet.
        containment_dir: Directory to write containment parquet files and _meta.json.
        collection_prefix: NSID prefix for rkey values.
        covering_dir: Directory containing covering parquet files (from stage_covering).
        memory_limit: DuckDB memory_limit string.
        temp_directory: DuckDB temp_directory for spill (optional).
        max_temp_directory_size: DuckDB max_temp_directory_size string,
            bounding spill (default "250GB"), independently of whether
            temp_directory is also supplied.
        force: Re-build even when the artifact is fresh.
        partition_zoom: Quadkey prefix depth used to batch the containment query
            (default 6, was hardcoded 4). Finer batches bound peak memory per
            batch at the cost of more COPY calls.
    """
    # Resolve covering zoom range from covering _meta.json (fallback to defaults)
    cover_min_zoom = 4
    cover_max_zoom = 12

    # Build input list for freshness tracking
    inputs = [places_parquet, tile_assignments_parquet]
    if covering_dir:
        covering_meta_path = os.path.join(covering_dir, "_meta.json")
        if os.path.exists(covering_meta_path):
            inputs.append(covering_meta_path)
            try:
                with open(covering_meta_path) as f:
                    _m = json.load(f)
                cover_min_zoom = _m.get("cover_min_zoom", cover_min_zoom)
                cover_max_zoom = _m.get("cover_max_zoom", cover_max_zoom)
            except Exception:
                pass
    if boundaries_db is not None:
        inputs.append(str(boundaries_db))

    params = {
        "collection_prefix": collection_prefix,
        "cover_min_zoom": cover_min_zoom,
        "cover_max_zoom": cover_max_zoom,
        "partition_zoom": partition_zoom,
    }

    # Freshness gate: containment_dir/_meta.json is the completion sentinel
    meta_path = os.path.join(containment_dir, "_meta.json")
    if not force:
        if os.path.exists(meta_path):
            try:
                with open(meta_path) as f:
                    recorded = json.load(f)
                if (recorded.get("params") == params
                        and recorded.get("inputs") == inputs
                        and _is_output_fresh(meta_path, inputs)):
                    log.info("compute_containment: skipping (artifact fresh)")
                    return
            except (OSError, json.JSONDecodeError):
                pass

    # Determine Q3 short-circuit conditions
    empty = False
    covering_parquets = {}
    if boundaries_db is None:
        empty = True
    elif not covering_dir or not os.path.isdir(covering_dir):
        empty = True
    else:
        covering_parquets = {
            f[:-8]: os.path.join(covering_dir, f)
            for f in os.listdir(covering_dir)
            if f.endswith(".parquet")
        }
        if not covering_parquets:
            empty = True

    # Dir-swap atomicity setup (covering.py pattern)
    tmp_dir = containment_dir + ".tmp"
    old_dir = containment_dir + ".old"

    # Build-start cleanup: clobber stale crash leftovers
    for d in [tmp_dir, old_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)
    os.makedirs(tmp_dir)

    if not empty:
        # Generate interior arms SQL (one per zoom level L in [cover_min_zoom, cover_max_zoom])
        interior_arms = "\nUNION ALL\n".join(
            f"    SELECT p.place_id, c.boundary_id, c.level\n"
            f"    FROM p JOIN cov c\n"
            f"      ON c.kind = 'interior' AND len(c.tile_qk) = {L}\n"
            f"     AND left(p.qk17, {L}) = c.tile_qk"
            for L in range(cover_min_zoom, cover_max_zoom + 1)
        )

        template_sql = (_SQL_DIR / "compute_containment.sql").read_text()

        # Escape single quotes in paths for SQL embedding
        pq_sql = places_parquet.replace("'", "''")
        ta_sql = tile_assignments_parquet.replace("'", "''")
        bnd_sql = str(boundaries_db).replace("'", "''")

        con = duckdb.connect(":memory:")
        try:
            if temp_directory:
                con.execute(f"SET temp_directory = '{temp_directory}'")
            if max_temp_directory_size:
                con.execute(f"SET max_temp_directory_size = '{max_temp_directory_size}'")
            if memory_limit:
                con.execute(f"SET memory_limit = '{memory_limit}'")
            con.execute("SET preserve_insertion_order = false")
            con.execute("SET enable_progress_bar = false")
            con.execute("LOAD spatial")

            # Build places_slim from parquet (sorted by qk17 for zone-map optimization)
            con.execute(f"""
                CREATE TEMP TABLE places_slim AS
                SELECT {pk_expr} AS place_id,
                       p.qk17,
                       CAST(({lon_expr}) AS DOUBLE) AS lon,
                       CAST(({lat_expr}) AS DOUBLE) AS lat
                FROM read_parquet('{pq_sql}') p
                WHERE p.qk17 IS NOT NULL
                  AND length(p.qk17) = 17
                  AND p.qk17 ~ '^[0-3]{{17}}$'
                ORDER BY p.qk17
            """)

            # Build tile_assignments from parquet
            con.execute(f"""
                CREATE TEMP TABLE tile_assignments AS
                SELECT place_id, tile_qk
                FROM read_parquet('{ta_sql}')
            """)

            # Get prefix list (at partition_zoom depth) with per-prefix counts,
            # used both to batch the containment query and to log the largest
            # batches. Sorted ascending (SQL ORDER BY 1) so identical z4
            # prefixes are contiguous for the itertools.groupby below.
            prefix_counts = con.execute(
                f"SELECT left(qk17, {partition_zoom}) AS prefix, count(*) AS n "
                f"FROM places_slim GROUP BY 1 ORDER BY 1"
            ).fetchall()
            if prefix_counts:
                top = sorted(prefix_counts, key=lambda r: -r[1])[:10]
                log.info(
                    "compute_containment: largest batches (partition_zoom=%d): %s",
                    partition_zoom,
                    ", ".join(f"{p}={n}" for p, n in top),
                )

            total_batches = len(prefix_counts)
            batch_num = 0
            con.execute(f"ATTACH '{bnd_sql}' AS bnd (READ_ONLY)")
            try:
                for z4_prefix, group in itertools.groupby(prefix_counts, key=lambda r: r[0][:4]):
                    covering_file = covering_parquets.get(z4_prefix)
                    if covering_file is None:
                        continue

                    covering_file_sql = covering_file.replace("'", "''")
                    con.execute(f"""
                        CREATE TEMP TABLE cov AS
                        SELECT * FROM read_parquet('{covering_file_sql}')
                    """)
                    try:
                        for prefix, n in group:
                            batch_num += 1
                            t0 = time.monotonic()
                            log.info(
                                "compute_containment: batch %d/%d prefix=%s n=%d",
                                batch_num, total_batches, prefix, n,
                            )
                            # p is pre-materialized (not a CTE over places_slim) so the
                            # join plans against this batch's true small size, not the
                            # ~75M-row backing table -- see compute_containment.sql header.
                            prefix_upper = prefix + "3" * (17 - len(prefix))
                            con.execute(f"""
                                CREATE TEMP TABLE p AS
                                SELECT place_id, qk17, lon, lat FROM places_slim
                                WHERE qk17 >= '{prefix}' AND qk17 <= '{prefix_upper}'
                            """)
                            try:
                                out_path = os.path.join(tmp_dir, f"{prefix}.parquet")
                                out_path_sql = out_path.replace("'", "''")

                                sql = template_sql
                                sql = sql.replace("${interior_arms}", interior_arms)
                                sql = sql.replace("${max_zoom}", str(cover_max_zoom))
                                sql = sql.replace("${collection_prefix}", collection_prefix)

                                con.execute(
                                    f"COPY ({sql}) TO '{out_path_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                                )
                            finally:
                                con.execute("DROP TABLE p")
                            log.info(
                                "compute_containment: batch %d/%d prefix=%s done (%.1fs)",
                                batch_num, total_batches, prefix, time.monotonic() - t0,
                            )
                    finally:
                        con.execute("DROP TABLE cov")
            finally:
                try:
                    con.execute("DETACH bnd")
                except Exception:
                    pass
        finally:
            con.close()

    meta = {
        "params": params,
        "inputs": inputs,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    if empty:
        meta["empty"] = True

    # Write _meta.json LAST inside tmp_dir (completion sentinel)
    with open(os.path.join(tmp_dir, "_meta.json"), "w") as f:
        json.dump(meta, f)

    # Dir-swap: promote tmp_dir → containment_dir
    if os.path.exists(containment_dir):
        os.rename(containment_dir, old_dir)
    os.rename(tmp_dir, containment_dir)
    if os.path.exists(old_dir):
        shutil.rmtree(old_dir, ignore_errors=True)


def write_manifest_db(tile_assignments_parquet: str, output_dir: str, source: str,
                      *, generated_at: str = None,
                      temp_directory: str | None = None,
                      max_temp_directory_size: str | None = "250GB") -> None:
    """Phase 2: write manifest.duckdb from a tile_assignments parquet file.

    Reads tile_assignments from parquet (no open working connection required).
    Atomic: builds in .tmp, renames into place. The CREATE TABLE ... ORDER BY
    below runs over the full tile_assignments parquet (millions of rows for
    Overture), so it can spill like any other stage; temp_directory/
    max_temp_directory_size bound that the same way stage_import does.
    Must be called BEFORE write_manifest() so manifest.json lands last.

    Args:
        tile_assignments_parquet: Path to tile_assignments.parquet artifact.
        output_dir: Directory where manifest.duckdb will be written.
        source: Source key (overture_place, osm, overture_division).
        generated_at: RFC 3339 Z run-scoped timestamp shared with the tiles and
            manifest.json. Defaults to the current time if omitted
            (legacy/test callers that don't thread a run timestamp).
        temp_directory: DuckDB temp_directory for spill (optional). Callers
            reached via stage_export should pass its own temp_directory so
            the manifest step spills where the export did.
        max_temp_directory_size: DuckDB max_temp_directory_size string,
            bounding spill (default "250GB"), independently of whether
            temp_directory is also supplied.
    """
    if generated_at is None:
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    source_cls = _SOURCES[source]
    manifest_path = os.path.join(output_dir, "manifest.duckdb")
    tmp_path = manifest_path + ".tmp"
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    con = duckdb.connect()
    try:
        con.execute("SET enable_progress_bar = false")
        if temp_directory:
            con.execute(f"SET temp_directory = '{temp_directory}'")
        if max_temp_directory_size:
            con.execute(f"SET max_temp_directory_size = '{max_temp_directory_size}'")
        con.execute(f"ATTACH '{tmp_path}' AS manifest")
        if source == "osm":
            con.execute(f"""
                CREATE TABLE manifest.record_tiles AS
                SELECT CASE left(place_id, 1)
                           WHEN 'n' THEN 'node:' || substr(place_id, 2)
                           WHEN 'w' THEN 'way:' || substr(place_id, 2)
                           WHEN 'r' THEN 'relation:' || substr(place_id, 2)
                           ELSE place_id
                       END AS rkey, tile_qk
                FROM read_parquet('{tile_assignments_parquet}')
                ORDER BY rkey
            """)
        else:
            con.execute(f"""
                CREATE TABLE manifest.record_tiles AS
                SELECT place_id AS rkey, tile_qk
                FROM read_parquet('{tile_assignments_parquet}')
                ORDER BY place_id
            """)
        con.execute("""
            CREATE TABLE manifest.metadata AS
            SELECT ? AS source, ? AS collection, ? AS generated_at
        """, [source, source_cls.collection, generated_at])
        con.execute("DETACH manifest")
    finally:
        con.close()
    os.rename(tmp_path, manifest_path)


def write_manifest(output_dir, *, generated_at):
    """Write manifest.json as the run's completeness marker.

    generated_at is required-keyword: callers must supply the single
    run-scoped timestamp shared with every tile and manifest.duckdb.
    """
    data = {"generated_at": generated_at}
    manifest_path = os.path.join(output_dir, "manifest.json")
    tmp_path = manifest_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, manifest_path)


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


# Geometry column name per source, excluded from Phase 2 Parquet output.
# overture_place is None: overture_place_import.sql drops geometry inside
# ov_base (before any join/materialization), so `places` never carries it —
# unlike osm, there is no column left to EXCLUDE at COPY time.
_GEOM_COL = {
    "overture_place": None,
    "osm": "geom",
}


def stage_division_import(parquet_glob, bbox, output_path, *,
                          memory_limit="48GB", temp_directory=None,
                          max_temp_directory_size="250GB",
                          density_parquet=None,
                          density_norm=10.0, pop_norm=20.0,
                          force=False) -> None:
    """Write places.parquet + boundaries.duckdb for overture_division (Phase 2).

    One ephemeral in-memory DuckDB connection. Sequence:
      1. Run overture_division_import.sql transformed to CREATE TEMP TABLE division_all.
      2. COPY (SELECT * EXCLUDE (geometry) FROM division_all ORDER BY qk17 NULLS LAST)
         to places.parquet.tmp (FORMAT PARQUET, COMPRESSION ZSTD).
      3. Delete stale boundaries.duckdb.tmp + .wal (crash recovery).
         ATTACH boundaries.duckdb.tmp AS bnd; CREATE bnd.places + RTREE index; DETACH.
      4. fsync + os.replace boundaries.duckdb.tmp → boundaries.duckdb.
      5. finalize_artifact(places.parquet.tmp → places.parquet) — meta written LAST,
         gates the whole stage (presence of boundaries.duckdb also required).

    Freshness: artifact_fresh(places.parquet, inputs, params) AND boundaries.duckdb
    must exist.  Crash between step 4 and 5 leaves new boundaries.duckdb but stale
    meta → both rebuilt on next run (idempotent).

    Args:
        parquet_glob: (division_parquet_path, division_area_parquet_path) tuple.
        bbox: (xmin, ymin, xmax, ymax) tuple, dict with those keys, or None for global.
        output_path: Destination path for places.parquet.
        memory_limit: DuckDB memory_limit string.
        temp_directory: DuckDB temp_directory for spill (optional).
        max_temp_directory_size: DuckDB max_temp_directory_size string,
            bounding spill (default "250GB"), independently of whether
            temp_directory is also supplied.
        density_parquet: Path to density_tiles.parquet (optional).
        density_norm: Density normalization constant (must be > 0).
        pop_norm: Population normalization constant (must be > 0).
        force: Re-build even when artifacts are fresh.
    """
    t0 = time.monotonic()

    # Normalize bbox
    if isinstance(bbox, dict):
        xmin = bbox["xmin"]; ymin = bbox["ymin"]
        xmax = bbox["xmax"]; ymax = bbox["ymax"]
    elif bbox is not None:
        xmin, ymin, xmax, ymax = bbox
    else:
        xmin, ymin, xmax, ymax = -180, -90, 180, 90

    # Validate norm constants
    invalid_norms = []
    for name, val in [("density_norm", density_norm), ("pop_norm", pop_norm)]:
        if val is not None and val <= 0:
            invalid_norms.append(name)
    if invalid_norms:
        raise ValueError(f"{', '.join(invalid_norms)} must be positive")

    division_parquet_path, division_area_parquet_path = parquet_glob

    # Compute input files for freshness tracking
    input_files = (
        _resolve_glob_paths(division_parquet_path, required=True) +
        _resolve_glob_paths(division_area_parquet_path, required=True)
    )
    if density_parquet:
        input_files += _resolve_glob_paths(str(density_parquet))

    params = {
        "source": "overture_division",
        "bbox": [xmin, ymin, xmax, ymax],
        "density_norm": density_norm,
        "pop_norm": pop_norm,
    }

    # boundaries.duckdb lives alongside places.parquet
    boundaries_path = str(Path(output_path).parent / "boundaries.duckdb")

    # Freshness: standard artifact_fresh + boundaries.duckdb must exist
    # (the places meta gates both artifacts — see step 5 above)
    if not force:
        if artifact_fresh(output_path, input_files, params) and os.path.exists(boundaries_path):
            log.info("[overture_division] import: skipping (artifact fresh)")
            return

    # Build density CTE (empty if no density_parquet provided)
    if density_parquet:
        density_cte = (
            f"CREATE TEMP TABLE density_tiles AS "
            f"SELECT * FROM read_parquet('{density_parquet}');"
        )
    else:
        density_cte = (
            "CREATE TEMP TABLE density_tiles AS "
            "SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, "
            "NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, "
            "NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
        )

    # Render the subtype -> level CASE from garganorn.levels.LEVEL_VOCAB (the
    # single source of truth) so SQL and Python can't drift. NO ELSE branch:
    # an unmapped subtype must yield NULL (belt-and-braces), caught by the
    # fail-loud validator below.
    level_case = level_case_sql()

    # Read and transform import SQL for Phase 2:
    #   - Remove "DROP TABLE IF EXISTS places;" (no places table on fresh connection)
    #   - Rename "CREATE TABLE places AS" → "CREATE TEMP TABLE division_all AS"
    #   - Trailing "DROP TABLE density_tiles;" is left in place (harmless cleanup)
    sql = (_SQL_DIR / "overture_division_import.sql").read_text()
    sql = sql.replace("${memory_limit}", memory_limit)
    sql = sql.replace("${density_cte}", density_cte)
    sql = sql.replace("${division_parquet}", division_parquet_path)
    sql = sql.replace("${division_area_parquet}", division_area_parquet_path)
    sql = sql.replace("${xmin}", str(xmin))
    sql = sql.replace("${ymin}", str(ymin))
    sql = sql.replace("${xmax}", str(xmax))
    sql = sql.replace("${ymax}", str(ymax))
    sql = sql.replace("${density_norm}", str(density_norm))
    sql = sql.replace("${pop_norm}", str(pop_norm))
    sql = sql.replace("${level_case}", level_case)
    sql = sql.replace("DROP TABLE IF EXISTS places;\n", "")
    sql = sql.replace("CREATE TABLE places AS\n", "CREATE TEMP TABLE division_all AS\n")

    # Tmp paths
    tmp_output = output_path + ".tmp"
    boundaries_tmp = boundaries_path + ".tmp"
    boundaries_wal = boundaries_tmp + ".wal"

    # Stage-start cleanup: remove stale .tmp/.wal from prior crashes
    for stale in [tmp_output, boundaries_tmp, boundaries_wal]:
        if os.path.exists(stale):
            os.remove(stale)

    con = duckdb.connect()
    try:
        if temp_directory:
            con.execute(f"SET temp_directory = '{temp_directory}'")
        if max_temp_directory_size:
            con.execute(f"SET max_temp_directory_size = '{max_temp_directory_size}'")
        con.execute("SET preserve_insertion_order = false")
        con.execute("SET enable_progress_bar = false")

        # Step 1: Create division_all TEMP TABLE (geometry included for boundaries export)
        log.info("[overture_division] import: starting")
        con.execute(sql)
        count = con.execute("SELECT count(*) FROM division_all").fetchone()[0]
        log.info("[overture_division] import: %d rows loaded (%.1fs)",
                 count, time.monotonic() - t0)

        # Fail-loud enforcement: every division subtype must be a known
        # LEVEL_VOCAB key. Runs after the CTAS and before any artifact write
        # (before the COPY and the boundaries.duckdb ATTACH) so a bad release
        # never produces partial output. Never default or guess.
        placeholders = ",".join("?" * len(LEVEL_VOCAB))
        unmapped = con.execute(f"""
            SELECT DISTINCT subtype FROM division_all
            WHERE subtype IS NULL OR subtype NOT IN ({placeholders})
        """, list(LEVEL_VOCAB)).fetchall()
        if unmapped:
            raise RuntimeError(
                f"overture_division import: unmapped division subtype(s) "
                f"{sorted(s for (s,) in unmapped)}; garganorn.levels.LEVEL_VOCAB "
                f"must be amended before import. "
                f"Never default or guess."
            )

        # Belt-and-braces: the ${level_case} CASE has no ELSE branch, so
        # an unmapped subtype would already have raised above; this assertion
        # confirms level is total regardless.
        null_level_count = con.execute(
            "SELECT count(*) FROM division_all WHERE level IS NULL"
        ).fetchone()[0]
        if null_level_count != 0:
            raise RuntimeError(
                f"overture_division import: {null_level_count} division_all rows "
                f"have a NULL level after the level_case CASE; this should be "
                f"unreachable once the fail-loud subtype check above passes."
            )

        _assert_interior_points(con, "division_all", "overture_division import")

        # Step 2: COPY places artifact (geometry excluded, qk17-sorted)
        con.execute(
            f"COPY ("
            f"  SELECT * EXCLUDE (geometry)"
            f"  FROM division_all"
            f"  ORDER BY qk17 NULLS LAST"
            f") TO '{tmp_output}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

        # Step 3: Build boundaries.duckdb
        # WAL cleanup before ATTACH (stale .wal from prior kill -9)
        for stale in [boundaries_tmp, boundaries_wal]:
            if os.path.exists(stale):
                os.remove(stale)

        con.execute(f"ATTACH '{boundaries_tmp}' AS bnd")
        try:
            con.execute(f"""
                CREATE TABLE bnd.places AS
                SELECT id, geometry, level,
                       names, subtype, country, region, wikidata, population,
                       min_latitude, max_latitude,
                       min_longitude, max_longitude,
                       importance, variants
                FROM division_all
                ORDER BY ST_Hilbert(geometry,
                    {{'min_x': -180.0, 'min_y': -90.0,
                      'max_x': 180.0, 'max_y': 90.0}}::BOX_2D)
            """)
            con.execute(
                "CREATE INDEX bnd_places_rtree ON bnd.places USING RTREE(geometry)"
            )
        finally:
            con.execute("DETACH bnd")
    finally:
        con.close()

    # Step 4: Atomically promote boundaries.duckdb.tmp → boundaries.duckdb
    with open(boundaries_tmp, "rb") as fh:
        os.fsync(fh.fileno())
    os.replace(boundaries_tmp, boundaries_path)

    # Step 5: Finalize places artifact — meta written LAST, gates the whole stage
    finalize_artifact(tmp_output, output_path, params, inputs=input_files)
    log.info("[overture_division] import artifact: done (%.1fs)", time.monotonic() - t0)


def stage_import(source, parquet_glob, bbox, output_path, *,
                 memory_limit="48GB", temp_directory=None,
                 max_temp_directory_size="250GB",
                 density_parquet=None, idf_parquet=None,
                 density_norm=10.0, idf_norm=18.0, pop_norm=20.0,
                 force=False) -> None:
    """Write a places.parquet artifact for the given source (Phase 2).

    Writes places.parquet (geometry column excluded, ORDER BY qk17 NULLS LAST)
    plus a .meta.json sidecar to output_path. Uses an ephemeral in-memory
    DuckDB connection; no .duckdb file is created under the output tree.
    Skips rebuild when artifact_fresh() returns True (unless force=True).

    Args:
        source: Source key — one of overture_place, osm.
                (overture_division is handled by stage_division_import.)
        parquet_glob: Glob/path for source parquet file(s). For osm, a
                      (node_parquet, way_parquet) tuple.
        bbox: (xmin, ymin, xmax, ymax) tuple, dict with those keys, or None
              for global.
        output_path: Destination path for places.parquet.
        memory_limit: DuckDB memory_limit setting.
        temp_directory: DuckDB temp_directory for spill (optional).
        max_temp_directory_size: DuckDB max_temp_directory_size string,
            bounding spill (default "250GB"), independently of whether
            temp_directory is also supplied.
        density_parquet: Path to density_tiles.parquet (optional).
        idf_parquet: Path to idf_scores.parquet (optional).
        density_norm: Density normalization constant (must be > 0).
        idf_norm: IDF normalization constant (must be > 0).
        pop_norm: Population normalization constant (must be > 0).
        force: Re-build even when the artifact is fresh.
    """
    t0 = time.monotonic()

    # Check density_parquet's join-key uniqueness once, ahead of ANY
    # dispatch below -- see _assert_density_parquet_unique for why this
    # must happen here rather than inside each destination separately.
    _assert_density_parquet_unique(density_parquet, temp_directory=temp_directory,
                                   max_temp_directory_size=max_temp_directory_size,
                                   memory_limit=memory_limit)

    # overture_division has its own two-artifact stage; dispatch immediately.
    if source == "overture_division":
        return stage_division_import(
            parquet_glob, bbox, output_path,
            memory_limit=memory_limit, temp_directory=temp_directory,
            max_temp_directory_size=max_temp_directory_size,
            density_parquet=density_parquet,
            density_norm=density_norm, pop_norm=pop_norm,
            force=force,
        )

    # Normalize bbox: accept tuple (xmin, ymin, xmax, ymax) or dict
    if isinstance(bbox, dict):
        xmin = bbox["xmin"]; ymin = bbox["ymin"]
        xmax = bbox["xmax"]; ymax = bbox["ymax"]
    elif bbox is not None:
        xmin, ymin, xmax, ymax = bbox
    else:
        xmin, ymin, xmax, ymax = -180, -90, 180, 90

    # Validate norm constants before any DuckDB work
    invalid_norms = []
    for name, val in [("density_norm", density_norm), ("idf_norm", idf_norm), ("pop_norm", pop_norm)]:
        if val is not None and val <= 0:
            invalid_norms.append(name)
    if invalid_norms:
        raise ValueError(f"{', '.join(invalid_norms)} must be positive")

    # Determine geometry column to exclude from Parquet output (None means
    # the source's import SQL never carries a geometry column into `places`)
    if source not in _GEOM_COL:
        raise ValueError(
            f"stage_import: unsupported source '{source}'. "
            f"Supported: {list(_GEOM_COL)} plus overture_division (dispatched internally)."
        )
    exclude_col = _GEOM_COL[source]

    # Compute input file list for freshness tracking
    if source == "osm":
        node_parquet_path, way_parquet_path = parquet_glob
        input_files = (_resolve_glob_paths(node_parquet_path, required=True)
                       + _resolve_glob_paths(way_parquet_path, required=True))
    else:
        input_files = _resolve_glob_paths(str(parquet_glob), required=True)

    if density_parquet:
        input_files += _resolve_glob_paths(str(density_parquet))
    if idf_parquet:
        input_files += _resolve_glob_paths(str(idf_parquet))

    params = {
        "source": source,
        "bbox": [xmin, ymin, xmax, ymax],
        "density_norm": density_norm,
        "idf_norm": idf_norm,
    }

    if not force and artifact_fresh(output_path, input_files, params):
        log.info("[%s] import: skipping (artifact fresh)", source)
        return

    # Build density / IDF CTEs (empty when paths not provided)
    if density_parquet:
        density_cte = (
            f"CREATE TEMP TABLE density_tiles AS "
            f"SELECT * FROM read_parquet('{density_parquet}');"
        )
    else:
        density_cte = (
            "CREATE TEMP TABLE density_tiles AS "
            "SELECT NULL::VARCHAR AS tile_qk15, NULL::DOUBLE AS density_score, "
            "NULL::DOUBLE AS tile_xmin, NULL::DOUBLE AS tile_ymin, "
            "NULL::DOUBLE AS tile_xmax, NULL::DOUBLE AS tile_ymax WHERE 1=0;"
        )

    if idf_parquet:
        idf_cte = (
            f"CREATE TEMP TABLE idf_scores AS "
            f"SELECT * FROM read_parquet('{idf_parquet}');"
        )
    else:
        idf_cte = (
            "CREATE TEMP TABLE idf_scores AS "
            "SELECT NULL::VARCHAR AS category, NULL::DOUBLE AS idf_score WHERE 1=0;"
        )

    # Run import SQL on ephemeral in-memory connection (no .duckdb files)
    con = duckdb.connect()
    try:
        if memory_limit:
            con.execute(f"SET memory_limit = '{memory_limit}'")
        if temp_directory:
            con.execute(f"SET temp_directory = '{temp_directory}'")
        if max_temp_directory_size:
            con.execute(f"SET max_temp_directory_size = '{max_temp_directory_size}'")
        con.execute("SET enable_progress_bar = false")

        # Load the lookup tables as a standalone step, ahead of the import
        # SQL, so their join key uniqueness can be asserted before any join
        # runs (see _assert_unique_key). density_tiles' own uniqueness was
        # already checked once above (_assert_density_parquet_unique, ahead
        # of the overture_division dispatch); idf_scores has no
        # overture_division counterpart, so it is checked here as before.
        # The import SQL's ${density_cte}/${idf_cte} placeholders are then
        # substituted with no-ops below, since the tables already exist on
        # this connection.
        con.execute(density_cte)
        con.execute(idf_cte)
        _assert_unique_key(con, "idf_scores", "category", idf_parquet)
        no_op_density_cte = "-- density_tiles loaded above (uniqueness-checked ahead of dispatch)"
        no_op_idf_cte = "-- idf_scores loaded and uniqueness-checked above"

        if source == "osm":
            node_parquet_path, way_parquet_path = parquet_glob
            osm_category_case = (_SQL_DIR / "_osm_category_case.sql").read_text().strip()
            _run_sql(con, source, "import", "osm_import.sql", t0,
                     memory_limit=memory_limit,
                     node_parquet=node_parquet_path,
                     way_parquet=way_parquet_path,
                     xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax,
                     osm_category_case=osm_category_case,
                     density_cte=no_op_density_cte, idf_cte=no_op_idf_cte,
                     density_norm=density_norm, idf_norm=idf_norm)
        else:  # overture_place
            _run_sql(con, source, "import", "overture_place_import.sql", t0,
                     memory_limit=memory_limit, parquet_glob=parquet_glob,
                     xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax,
                     density_cte=no_op_density_cte, idf_cte=no_op_idf_cte,
                     density_norm=density_norm, idf_norm=idf_norm)

        # Write to a temp file first; finalize_artifact does the atomic rename
        tmp_output = output_path + ".tmp"
        select_clause = "SELECT *" if exclude_col is None else f"SELECT * EXCLUDE ({exclude_col})"
        con.execute(
            f"COPY ("
            f"  {select_clause}"
            f"  FROM places"
            f"  ORDER BY qk17 NULLS LAST"
            f") TO '{tmp_output}' (FORMAT PARQUET)"
        )
    finally:
        con.close()

    finalize_artifact(tmp_output, output_path, params, inputs=input_files)
    log.info("[%s] import artifact: done (%.1fs)", source, time.monotonic() - t0)


def _load_qk_env_macros(con):
    """Execute qk_env_macro.sql into an open DuckDB connection.

    Defines qk_tile_x/qk_tile_y/qk_env scalar macros used to compute z15
    tile envelopes in SQL. Mirrors garganorn.covering._load_qk_env_macros
    (duplicated here rather than imported, since covering.py imports from
    this module and importing back would create a cycle).
    """
    sql = (_SQL_DIR / "qk_env_macro.sql").read_text()
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


def stage_density_extract(parquet_glob: str, output_path: str, t0: float,
                          force: bool = False,
                          memory_limit: str = "48GB",
                          temp_directory: str = None,
                          max_temp_directory_size: str = "250GB") -> None:
    """Extract z15 density tiles from Overture place parquet.

    Runs a global density extract (no bbox filter) against the source
    parquet file. Groups places by their z15 quadtile and computes
    ln(1 + count) as density_score. Output is written to output_path
    and reused in importance computation across all place sources.

    Phase 2: Uses tmp+rename + finalize_artifact for atomicity; freshness
    gate uses artifact_fresh() (meta-aware). Output sorted by tile_qk15 so
    zone-map pruning works on tile-prefix filters downstream.

    Phase 4: Tile bounds (tile_xmin, tile_ymin, tile_xmax, tile_ymax) are
    computed in SQL using the qk_env() macro (garganorn/sql/qk_env_macro.sql),
    which matches quadkey_to_bbox() to within 1e-9. This ensures bbox-overlap
    joins work correctly for small localities.

    Args:
        parquet_glob: Glob pattern for Overture place parquet files.
        output_path: Destination path for density parquet output.
        t0: Start time for logging (monotonic time).
        force: If True, re-run even if output is fresh. Default False.
        memory_limit: DuckDB memory_limit string. Default "48GB".
        temp_directory: DuckDB temp_directory for spill (optional).
        max_temp_directory_size: DuckDB max_temp_directory_size string,
            bounding spill (default "250GB"), independently of whether
            temp_directory is also supplied.
    """
    # Remove any stale .tmp from a previous interrupted run before building.
    _tmp = output_path + ".tmp"
    if os.path.exists(_tmp):
        os.remove(_tmp)

    input_files = _resolve_glob_paths(parquet_glob, required=True)

    if not force:
        if artifact_fresh(output_path, input_files, {}):
            log.info("density_extract: skipping (output is fresh)")
            return

    log.info("density_extract: starting (ephemeral connection)")
    sql = (_SQL_DIR / "density_extract.sql").read_text()
    sql = sql.replace("${parquet_glob}", str(parquet_glob))
    con = duckdb.connect()
    count = 0
    try:
        if temp_directory:
            con.execute(f"SET temp_directory = '{temp_directory}'")
        if max_temp_directory_size:
            con.execute(f"SET max_temp_directory_size = '{max_temp_directory_size}'")
        if memory_limit:
            con.execute(f"SET memory_limit = '{memory_limit}'")
        con.execute("SET preserve_insertion_order = false")
        con.execute("SET enable_progress_bar = false")
        con.execute("INSTALL spatial; LOAD spatial")
        _load_qk_env_macros(con)
        con.execute(sql)

        # Write to .tmp first; finalize_artifact will atomically rename it
        con.execute(
            f"COPY ("
            f"  SELECT tile_qk15, density_score, tile_xmin, tile_ymin, tile_xmax, tile_ymax"
            f"  FROM density_tiles"
            f"  ORDER BY tile_qk15"
            f") TO '{_tmp}' (FORMAT PARQUET)"
        )
        count = con.execute("SELECT count(*) FROM density_tiles").fetchone()[0]
        log.info("density_extract: done (%.1fs, %d z15 tiles)",
                 time.monotonic() - t0, count)
    finally:
        con.close()

    # Atomic promotion: rename .tmp → artifact and write .meta.json sidecar
    finalize_artifact(_tmp, output_path, params={}, stats={"tiles": count}, inputs=input_files)


def stage_idf(source, parquet_glob, output_path, t0, force=False,
              memory_limit="48GB", temp_directory=None,
              max_temp_directory_size="250GB"):
    """Compute IDF scores per category from raw parquet.

    Reads source parquet directly (ephemeral DuckDB connection), computes
    ln(N_total / n_places) per category, and writes results to output_path
    as a parquet file with columns (category, n_places, idf_score).

    Args:
        source: Source key (overture_place, osm).
        parquet_glob: Parquet path(s). String for Overture;
            (node_glob, way_glob) tuple for OSM.
        output_path: Destination path for IDF parquet output.
        t0: Start time for logging (monotonic time).
        force: If True, re-run even if output is fresh. Default False.
        memory_limit: DuckDB memory_limit string. Default "48GB".
        temp_directory: DuckDB temp_directory for spill (optional).
        max_temp_directory_size: DuckDB max_temp_directory_size string,
            bounding spill (default "250GB"), independently of whether
            temp_directory is also supplied.
    """
    if source not in ("overture_place", "osm"):
        raise ValueError(f"unsupported source for IDF: {source}")

    # Remove any stale .tmp from a previous interrupted run before building.
    _tmp = output_path + ".tmp"
    if os.path.exists(_tmp):
        os.remove(_tmp)

    # Resolve input_files unconditionally so finalize_artifact can record them.
    if source == "osm":
        node_glob, way_glob = parquet_glob
        input_files = (_resolve_glob_paths(node_glob, required=True)
                       + _resolve_glob_paths(way_glob, required=True))
    else:
        input_files = _resolve_glob_paths(parquet_glob, required=True)

    if not force and artifact_fresh(output_path, input_files, {}):
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
    count = 0
    try:
        if temp_directory:
            con.execute(f"SET temp_directory = '{temp_directory}'")
        if max_temp_directory_size:
            con.execute(f"SET max_temp_directory_size = '{max_temp_directory_size}'")
        if memory_limit:
            con.execute(f"SET memory_limit = '{memory_limit}'")
        con.execute("SET preserve_insertion_order = false")
        con.execute("SET enable_progress_bar = false")
        con.execute(sql)
        con.execute(
            f"COPY (SELECT * FROM idf_scores ORDER BY category) TO '{_tmp}' (FORMAT PARQUET)"
        )
        count = con.execute("SELECT count(*) FROM idf_scores").fetchone()[0]
        log.info("idf: done (%.1fs, %d categories)", time.monotonic() - t0, count)
    finally:
        con.close()

    # Atomic promotion: rename .tmp → artifact and write .meta.json sidecar
    finalize_artifact(_tmp, output_path, params={}, stats={"categories": count}, inputs=input_files)


def stage_tile_assignment(places_parquet, output_path, source, *,
                          max_per_tile=1000, min_zoom=6, max_zoom=17,
                          memory_limit="48GB", temp_directory=None,
                          max_temp_directory_size="250GB",
                          force=False) -> dict:
    """Assign places to quadtree tiles and write tile_assignments.parquet (Phase 2).

    Reads places_parquet, assigns each place to its coarsest quadtree tile where
    the tile contains no more than max_per_tile places, and writes the result to
    output_path as a parquet artifact (columns: place_id, tile_qk; sorted by
    tile_qk, place_id). Skips rebuild when artifact_fresh() returns True unless
    force=True.

    Diagnostics emitted:
      Warning when places are dropped due to NULL or malformed qk17.
      Error when any place_id appears in more than one tile.

    Args:
        places_parquet: Path to input places.parquet file.
        output_path: Destination path for tile_assignments.parquet artifact.
        source: Source key (overture_place, osm, overture_division).
        max_per_tile: Maximum places per tile (triggers tile splitting). Default 1000.
        min_zoom: Minimum zoom level for tile assignment. Default 6.
        max_zoom: Maximum zoom level for tile assignment. Default 17.
        memory_limit: DuckDB memory_limit string. Default "48GB".
        temp_directory: DuckDB temp_directory for spill (optional).
        max_temp_directory_size: DuckDB max_temp_directory_size string,
            bounding spill (default "250GB"), independently of whether
            temp_directory is also supplied.
        force: Re-build even when the artifact is fresh.

    Returns:
        dict: {"total": int, "assigned": int, "dropped": int}
    """
    t0 = time.monotonic()

    inputs = [places_parquet]
    params = {"max_per_tile": max_per_tile, "min_zoom": min_zoom, "max_zoom": max_zoom}

    if not force and artifact_fresh(output_path, inputs, params):
        log.info("[%s] tile_assignment: skipping (artifact fresh)", source)
        return {}

    # Stage-start cleanup: remove stale .tmp from prior crashes
    tmp_output = output_path + ".tmp"
    if os.path.exists(tmp_output):
        os.remove(tmp_output)

    # Escape single quotes in paths for SQL embedding
    pq_sql = places_parquet.replace("'", "''")
    tmp_sql = tmp_output.replace("'", "''")

    # Resolve the source-specific primary key column name
    pk_col = _SOURCES[source].source_pk

    con = duckdb.connect()
    try:
        if temp_directory:
            con.execute(f"SET temp_directory = '{temp_directory}'")
        if max_temp_directory_size:
            con.execute(f"SET max_temp_directory_size = '{max_temp_directory_size}'")
        con.execute("SET preserve_insertion_order = false")

        # Total places count for dropped-place diagnostic
        total = con.execute(
            f"SELECT count(*) FROM read_parquet('{pq_sql}')"
        ).fetchone()[0]

        # Build per-zoom tile counts (same algorithm as compute_tile_assignments.sql)
        con.execute(f"""
            CREATE TEMP TABLE tile_counts AS
            SELECT t.level, left(qk17, t.level) AS qk, count(*) AS cnt
            FROM read_parquet('{pq_sql}'),
                 generate_series({min_zoom}, {max_zoom}) AS t(level)
            WHERE qk17 IS NOT NULL
              AND length(qk17) = 17
              AND qk17 ~ '^[0-3]{{17}}$'
            GROUP BY t.level, left(qk17, t.level)
        """)

        # Assign each place to its coarsest valid tile; fall back to max_zoom
        con.execute(f"""
            COPY (
                WITH place_zoom AS (
                    SELECT p.{pk_col} AS place_id, t.level, left(p.qk17, t.level) AS qk
                    FROM read_parquet('{pq_sql}') p
                    CROSS JOIN generate_series({min_zoom}, {max_zoom}) AS t(level)
                    WHERE p.qk17 IS NOT NULL
                      AND length(p.qk17) = 17
                      AND p.qk17 ~ '^[0-3]{{17}}$'
                ),
                best_zoom AS (
                    SELECT pz.place_id, min(pz.level) AS level
                    FROM place_zoom pz
                    JOIN tile_counts tc ON tc.level = pz.level AND tc.qk = pz.qk
                    WHERE tc.cnt <= {max_per_tile}
                    GROUP BY pz.place_id
                )
                SELECT p.{pk_col} AS place_id,
                       left(p.qk17, coalesce(bz.level, {max_zoom})) AS tile_qk
                FROM read_parquet('{pq_sql}') p
                LEFT JOIN best_zoom bz ON bz.place_id = p.{pk_col}
                WHERE p.qk17 IS NOT NULL
                  AND length(p.qk17) = 17
                  AND p.qk17 ~ '^[0-3]{{17}}$'
                ORDER BY tile_qk, p.{pk_col}
            ) TO '{tmp_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        # Count assigned rows and check for duplicates in the written artifact
        assigned = con.execute(
            f"SELECT count(*) FROM read_parquet('{tmp_sql}')"
        ).fetchone()[0]

        # Duplicate place_id check
        dupes = con.execute(f"""
            SELECT place_id, count(*) AS cnt
            FROM read_parquet('{tmp_sql}')
            GROUP BY place_id
            HAVING count(*) > 1
        """).fetchall()

    finally:
        con.close()

    # Warn about dropped places (NULL or invalid qk17)
    dropped = total - assigned
    if dropped > 0:
        log.warning(
            "[%s] tile_assignment: %d places dropped (NULL qk17 or invalid)",
            source, dropped,
        )

    # Warn about duplicate assignments
    if dupes:
        log.error(
            "[%s] tile_assignment: %d places assigned to multiple tiles",
            source, len(dupes),
        )

    stats = {"total": total, "assigned": assigned, "dropped": dropped}
    finalize_artifact(tmp_output, output_path, params, stats=stats, inputs=inputs)
    log.info(
        "[%s] tile_assignment artifact: done (%.1fs, %d assigned, %d dropped)",
        source, time.monotonic() - t0, assigned, dropped,
    )
    return stats


def stage_export(source: str, places_parquet: str, tile_assignments_parquet: str,
                 containment_dir: str, tiles_root: str, t0: float,
                 export_workers: int = None,
                 export_partition_zoom: int = 6,
                 memory_limit: str = "48GB",
                 temp_directory: str | None = None,
                 max_temp_directory_size: str | None = "250GB",
                 force: bool = False,
                 now: datetime | None = None) -> str:
    """Phase 2: export tiles from parquet artifacts.

    Reads places and tile_assignments from parquet artifacts. Builds a
    timestamped run dir under tiles_root with per-tile .json.gz files,
    manifest.duckdb, and manifest.json. Updates tiles_root/current symlink.

    Args:
        source: Source key (overture_place, osm, overture_division).
        places_parquet: Path to places parquet artifact from import stage.
        tile_assignments_parquet: Path to tile_assignments parquet artifact.
        containment_dir: Directory containing containment *.parquet files and _meta.json.
        tiles_root: Directory under which <timestamp>/ tile dirs are written.
        t0: Start time for logging (monotonic time).
        export_workers: Thread count for tile gzip compression.
        export_partition_zoom: Hive partition-prefix depth for the two-pass
            export (pass 1 partitions tile_export unsorted by
            left(tile_qk, export_partition_zoom); pass 2 sorts and flushes
            one partition at a time). Default 6, matching the qk[:6] output
            subdirectory grain. Not exposed via run_pipeline, the CLI, or
            config.yaml.
        memory_limit: DuckDB memory_limit string. Default "48GB".
        temp_directory: optional caller-supplied spill directory, same contract
            as stage_covering's temp_directory (garganorn/covering.py):
            stage_export never rmtree's or otherwise owns this directory
            itself -- it only ever creates and destroys private subdirectories
            under it that it exclusively owns. When temp_directory is None
            (default), the owned spill/staging dirs are run_dir + '.spill' and
            run_dir + '.staging', siblings of the run dir on the tiles
            volume. When the caller supplies temp_directory, the
            owned dirs are os.path.join(temp_directory, 'export.spill') and
            os.path.join(temp_directory, 'export.staging') -- private
            subdirs on the caller's chosen volume that stage_export creates
            and destroys, leaving the rest of the caller's directory
            untouched.
        max_temp_directory_size: DuckDB max_temp_directory_size string,
            bounding spill under the owned spill dir (default "250GB").
            Applied unconditionally, since SET temp_directory is always
            issued for this stage.
        force: If True, bypass freshness gate. Default False.
        now: Optional aware UTC datetime for deterministic-timestamp injection.
            When provided, it names the run dir AND derives the
            shared generated_at RFC 3339 Z string stamped into every tile,
            manifest.json, and manifest.duckdb metadata. Defaults to
            datetime.now(timezone.utc) when omitted.

    Returns:
        str: Path to the run directory created (or existing if fresh and not forced).
    """
    # Step 1: Freshness gate — key on tiles_root/current/manifest.json
    current_link = os.path.join(tiles_root, "current")
    current_manifest = os.path.join(current_link, "manifest.json")
    containment_meta = os.path.join(containment_dir, "_meta.json")

    if not force and _is_output_fresh(
        current_manifest,
        [places_parquet, tile_assignments_parquet, containment_meta],
    ):
        log.info("[%s] export: skipping (fresh)", source)
        current_target = os.readlink(current_link)
        return os.path.join(tiles_root, current_target)

    # Step 2: Cleanup — delete incomplete run dirs (no manifest.json), except current target
    current_target = None
    if os.path.islink(current_link):
        current_target = os.readlink(current_link)

    if os.path.isdir(tiles_root):
        for entry in os.listdir(tiles_root):
            if _TIMESTAMP_RE.match(entry) and entry != current_target:
                entry_path = os.path.join(tiles_root, entry)
                if (
                    os.path.isdir(entry_path)
                    and not os.path.islink(entry_path)
                    and not os.path.exists(os.path.join(entry_path, "manifest.json"))
                ):
                    shutil.rmtree(entry_path, ignore_errors=True)
                    log.info("[%s] export: removed incomplete run dir %s", source, entry)

    # Step 3: Create new timestamped run dir
    # One timestamp, threaded to both the run-dir name and generated_at,
    # so the run dir and manifest generated_at can never straddle a clock tick.
    os.makedirs(tiles_root, exist_ok=True)
    run_now = now if now is not None else datetime.now(timezone.utc)
    timestamp = run_now.strftime("%Y%m%dT%H%M%S")
    generated_at = run_now.strftime("%Y-%m-%dT%H:%M:%SZ")
    run_dir = os.path.join(tiles_root, timestamp)
    os.makedirs(run_dir, exist_ok=True)

    # Build containment expression for SQL substitution
    # NEVER use a glob for containment — read_parquet on empty glob errors on DuckDB 1.2.1.
    containment_files = sorted(glob_module.glob(os.path.join(containment_dir, "*.parquet")))
    if containment_files:
        file_list = ", ".join(
            f"'{f.replace(chr(39), chr(39) * 2)}'" for f in containment_files
        )
        containment_expr = f"read_parquet([{file_list}])"
    else:
        # Empty subquery with matching column names so LEFT JOIN ON resolves correctly
        containment_expr = (
            "(SELECT NULL::VARCHAR AS place_id, NULL::VARCHAR AS relations_json WHERE 1=0)"
        )

    # Step 4: SQL substitution — replace bare table names with parquet read expressions
    places_pq = places_parquet.replace("'", "''")
    ta_pq = tile_assignments_parquet.replace("'", "''")

    raw = (_SQL_DIR / f"{source}_export_tiles.sql").read_text()
    # Apply ${repo} substitution first (safe_substitute leaves unknown ${...} intact)
    sql = string.Template(raw).safe_substitute(repo=REPO)
    # Replace bare table references with read_parquet expressions
    sql = sql.replace("FROM places p", f"FROM read_parquet('{places_pq}') p")
    sql = sql.replace("JOIN tile_assignments ta", f"JOIN read_parquet('{ta_pq}') ta")
    sql = sql.replace("LEFT JOIN place_containment pc", f"LEFT JOIN {containment_expr} pc")

    # Step 5: Open ephemeral in-memory connection and execute the substituted SQL
    if temp_directory is None:
        spill_dir = run_dir + ".spill"
        staging_dir = run_dir + ".staging"
    else:
        spill_dir = os.path.join(temp_directory, "export.spill")
        staging_dir = os.path.join(temp_directory, "export.staging")
    con = duckdb.connect()
    manifest = {}
    try:
        # spill_dir/staging_dir names are fixed (not per-run), so residue
        # from a SIGKILLed run would otherwise persist across invocations.
        if os.path.exists(spill_dir):
            shutil.rmtree(spill_dir, ignore_errors=True)
        os.makedirs(spill_dir, exist_ok=True)
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir, ignore_errors=True)
        os.makedirs(staging_dir, exist_ok=True)
        con.execute(f"SET temp_directory = '{spill_dir}'")
        if max_temp_directory_size:
            con.execute(f"SET max_temp_directory_size = '{max_temp_directory_size}'")
        con.execute(f"SET memory_limit = '{memory_limit}'")
        # Prerequisite for pass 1 to stream straight to its partition writers
        # without materialising to preserve row order.
        con.execute("SET preserve_insertion_order = false")
        # No LOAD spatial needed: none of the export SQLs use ST_* functions
        con.execute(sql)
        con.execute("SET enable_progress_bar = false")

        # Count tiles for logging (from parquet, not table)
        tile_count = con.execute(
            f"SELECT COUNT(DISTINCT tile_qk) FROM read_parquet('{ta_pq}')"
        ).fetchone()[0]
        log.info("[%s] export: %d tiles to write", source, tile_count)

        # Step 6: Pass 1 -- materialise tile_export unsorted, partitioned by
        # tile_qk prefix. No ORDER BY: the join streams straight to each
        # partition's writer, so peak spill is bounded by one partition, not
        # the whole dataset.
        con.execute(
            f"COPY (SELECT tile_qk, place_id, rkey, record_json, "
            f"left(tile_qk, {export_partition_zoom}) AS pfx FROM tile_export) "
            f"TO '{staging_dir}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (pfx))"
        )

        # Partitions are enumerated by listing staging_dir, not by
        # synthesising a 4^d prefix list: pass 1 only creates a directory
        # for prefixes that have rows, and read_parquet on an empty glob
        # errors (see the containment_expr comment above).
        partition_prefixes = sorted(
            entry[len("pfx="):] for entry in os.listdir(staging_dir)
            if entry.startswith("pfx=")
        )

        source_cls = _SOURCES[source]

        def flush_tile(qk, records):
            """Compress and write one tile's records to disk.

            records are (rkey, record_json) pairs; record_json is a DuckDB
            to_json()::VARCHAR string. wrap_record composes the {uri, cid,
            value} envelope via string concatenation — no per-record
            json.loads/json.dumps round trip.
            """
            wrapped = [
                envelope.wrap_record(
                    envelope.record_uri(REPO, source_cls.collection, rkey), record_json
                )
                for rkey, record_json in records
            ]
            payload = envelope.build_tile_payload(
                source_cls.collection, source_cls.source_url, source_cls.license_url,
                generated_at, wrapped
            )
            subdir = os.path.join(run_dir, qk[:6])
            os.makedirs(subdir, exist_ok=True)
            compressed = gzip.compress(payload, mtime=0)
            with open(os.path.join(subdir, f"{qk}.json.gz"), "wb") as f:
                f.write(compressed)
            return (qk, len(records))

        futures = deque()
        max_inflight = 2 * (export_workers or os.cpu_count() or 4)

        def _drain_oldest():
            qk, count = futures.popleft().result()
            manifest[qk] = count
            if len(manifest) % 1000 == 0:
                log.info("[%s] export: wrote %d tiles", source, len(manifest))

        with ThreadPoolExecutor(max_workers=export_workers) as executor:
            # Pass 2: sort and flush one partition at a time.
            for p in partition_prefixes:
                cursor = con.execute(
                    f"SELECT tile_qk, place_id, rkey, record_json "
                    f"FROM read_parquet('{staging_dir}/pfx={p}/*.parquet') "
                    f"ORDER BY tile_qk, place_id"
                )
                # Must reset per partition, or the previous partition's last tile gets flushed twice.
                current_qk = None
                accumulated = []
                while True:
                    batch = cursor.fetchmany(1000)
                    if not batch:
                        break
                    for tile_qk, place_id, rkey, record_json in batch:
                        if tile_qk != current_qk:
                            if current_qk is not None:
                                if len(futures) >= max_inflight:
                                    _drain_oldest()
                                futures.append(executor.submit(flush_tile, current_qk, accumulated))
                            current_qk = tile_qk
                            accumulated = []
                        accumulated.append((rkey, record_json))

                if current_qk is not None:
                    futures.append(executor.submit(flush_tile, current_qk, accumulated))

            while futures:
                _drain_oldest()

    finally:
        con.close()
        if os.path.exists(spill_dir):
            shutil.rmtree(spill_dir, ignore_errors=True)
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir, ignore_errors=True)

    log.info("[%s] export: wrote %d tiles total (%.1fs)", source, len(manifest),
             time.monotonic() - t0)

    # Step 7: Manifests in completion order — manifest.json lands LAST as completeness marker
    write_manifest_db(tile_assignments_parquet, run_dir, source, generated_at=generated_at,
                      temp_directory=temp_directory,
                      max_temp_directory_size=max_temp_directory_size)
    write_manifest(run_dir, generated_at=generated_at)

    # Step 8: Symlink swap — atomic on POSIX via tmp-symlink + rename
    link_path = os.path.join(tiles_root, "current")
    tmp_link = link_path + ".tmp"
    try:
        os.remove(tmp_link)
    except OSError:
        pass
    os.symlink(timestamp, tmp_link)
    os.rename(tmp_link, link_path)

    # Step 9: Keep-2 sweep — retain only the 2 newest COMPLETE run dirs
    complete_dirs = sorted(
        d for d in os.listdir(tiles_root)
        if _TIMESTAMP_RE.match(d)
        and os.path.isdir(os.path.join(tiles_root, d))
        and not os.path.islink(os.path.join(tiles_root, d))
        and os.path.exists(os.path.join(tiles_root, d, "manifest.json"))
    )
    for old in complete_dirs[:-2]:
        shutil.rmtree(os.path.join(tiles_root, old), ignore_errors=True)
        log.info("[%s] export: removed old run dir %s", source, old)

    log.info("[%s] export: done (%.1fs total)", source, time.monotonic() - t0)
    return run_dir


