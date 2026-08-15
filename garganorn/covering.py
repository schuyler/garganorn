"""Covering stage: build quadkey covering for boundaries.duckdb."""
import json
import logging
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from garganorn.stages import quadkey_to_bbox, _is_output_fresh

log = logging.getLogger(__name__)

COVER_MIN_ZOOM = 4
COVER_MIN_LEAF_ZOOM = 12
COVER_MAX_ZOOM = 16
COVER_VERTEX_CAPACITY = 5000

_SQL_DIR = Path(__file__).parent / "sql"


def _tile_to_quadkey(x: int, y: int, zoom: int) -> str:
    """Convert tile (x, y, zoom) to quadkey string.

    Digit encoding: 0=NW, 1=NE, 2=SW, 3=SE (Bing Maps convention).
    """
    qk = []
    for i in range(zoom):
        bit = zoom - 1 - i
        digit = 0
        if x & (1 << bit):
            digit |= 1
        if y & (1 << bit):
            digit |= 2
        qk.append(str(digit))
    return "".join(qk)


def _load_qk_env_macros(con):
    """Execute qk_env_macro.sql into an open DuckDB connection."""
    sql = (_SQL_DIR / "qk_env_macro.sql").read_text()
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


def stage_covering(
    boundaries_db: str,
    covering_dir: str,
    *,
    memory_limit: str = "48GB",
    temp_directory: str | None = None,
    max_temp_directory_size: str | None = "250GB",
    cover_min_zoom: int = COVER_MIN_ZOOM,
    cover_min_leaf_zoom: int = COVER_MIN_LEAF_ZOOM,
    cover_max_zoom: int = COVER_MAX_ZOOM,
    cover_vertex_capacity: int = COVER_VERTEX_CAPACITY,
    force: bool = False,
) -> dict:
    """Build covering/<qk4>.parquet from boundaries.duckdb.

    Returns stats dict: {total, per_level: {z: n}, over_capacity_leaves}.

    An edge leaf is emitted at zoom z when
    NOT is_interior AND (z = cover_max_zoom OR
    (z >= cover_min_leaf_zoom AND npoints <= cover_vertex_capacity));
    otherwise the cell expands. cover_min_leaf_zoom bounds the containment
    join's fan-out; cover_vertex_capacity bounds the point-in-polygon test cost.

    Freshness: skips when covering_dir/_meta.json is newer than boundaries_db
    AND the recorded zoom/capacity parameters match. force=True always rebuilds.

    Atomicity: builds under covering_dir+'.tmp', then swaps.  At build
    start, any pre-existing .tmp, .old, and .spill dirs are removed (crash
    leftovers).

    temp_directory: optional caller-supplied spill directory.  stage_covering
    never rmtree's or otherwise owns this directory itself -- it only ever
    creates and destroys a private subdirectory under it that it exclusively
    owns.  When temp_directory is None (default), the owned spill dir is
    covering_dir+'.spill', a sibling of the output dir on the same volume.
    When the caller supplies temp_directory, the owned spill dir is
    os.path.join(temp_directory, 'covering.spill') -- a private subdir on the
    caller's chosen volume that stage_covering creates and destroys, leaving
    the rest of the caller's directory untouched.  SET temp_directory is
    always issued (pointing at the owned spill dir) so the in-memory
    connection can spill.

    max_temp_directory_size: DuckDB max_temp_directory_size string, bounding
    spill under the owned spill dir (default "250GB"). Applied
    unconditionally, since SET temp_directory is always issued for this
    stage.
    """
    if temp_directory is None:
        owned_spill_dir = covering_dir + ".spill"
    else:
        owned_spill_dir = os.path.join(temp_directory, "covering.spill")

    meta_path = os.path.join(covering_dir, "_meta.json")

    # Freshness gate (skipped when force=True)
    if not force:
        if _is_output_fresh(meta_path, [boundaries_db]):
            try:
                with open(meta_path) as f:
                    recorded = json.load(f)
                if (
                    recorded.get("cover_min_zoom") == cover_min_zoom
                    and recorded.get("cover_max_zoom") == cover_max_zoom
                    and recorded.get("cover_min_leaf_zoom") == cover_min_leaf_zoom
                    and recorded.get("cover_vertex_capacity") == cover_vertex_capacity
                ):
                    log.info("stage_covering: skipping (output is fresh)")
                    return {}
            except (OSError, json.JSONDecodeError):
                pass  # fall through to rebuild

    tmp_dir = covering_dir + ".tmp"
    old_dir = covering_dir + ".old"

    # Build start: unconditionally remove crash leftovers
    for d in [tmp_dir, old_dir, owned_spill_dir]:
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)

    os.makedirs(tmp_dir)
    os.makedirs(owned_spill_dir, exist_ok=True)

    t0 = time.monotonic()
    log.info(
        "stage_covering: building covering (boundaries=%s, z%d..z%d)",
        boundaries_db, cover_min_zoom, cover_max_zoom,
    )

    con = duckdb.connect(":memory:")
    try:
        con.execute(f"SET temp_directory = '{owned_spill_dir}'")
        if max_temp_directory_size:
            con.execute(f"SET max_temp_directory_size = '{max_temp_directory_size}'")
        con.execute(f"SET memory_limit = '{memory_limit}'")
        con.execute("SET preserve_insertion_order = false")
        con.execute("SET enable_progress_bar = false")
        con.execute("INSTALL spatial; LOAD spatial")

        _load_qk_env_macros(con)

        con.execute(f"ATTACH '{boundaries_db}' AS bnd (READ_ONLY)")

        # Output accumulator
        con.execute("""
            CREATE TEMP TABLE covering_out (
                tile_qk     VARCHAR,
                boundary_id VARCHAR,
                level       INTEGER,
                geom        GEOMETRY
            )
        """)

        # Generate seed tiles at cover_min_zoom and insert into z4_tiles
        n_seed = 2 ** cover_min_zoom
        seed_rows = []
        for x in range(n_seed):
            for y in range(n_seed):
                qk = _tile_to_quadkey(x, y, cover_min_zoom)
                xmin, ymin, xmax, ymax = quadkey_to_bbox(qk)
                seed_rows.append((qk, xmin, ymin, xmax, ymax))

        con.execute("""
            CREATE TEMP TABLE z4_tiles (
                qk   VARCHAR,
                xmin DOUBLE,
                ymin DOUBLE,
                xmax DOUBLE,
                ymax DOUBLE
            )
        """)
        con.executemany("INSERT INTO z4_tiles VALUES (?, ?, ?, ?, ?)", seed_rows)

        # Create l_current (seed join)
        seed_sql = (_SQL_DIR / "covering_seed.sql").read_text()
        con.execute(seed_sql)

        # Level loop: emit a leaf when NOT is_interior AND (${leaf}), where
        # ${leaf} is FALSE below cover_min_leaf_zoom, `npoints <= V` between
        # the floor and the cap, and TRUE at cover_max_zoom -- expand
        # otherwise. Runs uniformly for every zoom in [cover_min_zoom,
        # cover_max_zoom]; at cover_max_zoom expansion always yields nothing
        # since NOT (TRUE) is FALSE.
        level_sql_template = (_SQL_DIR / "covering_level.sql").read_text()
        for z in range(cover_min_zoom, cover_max_zoom + 1):
            if z == cover_max_zoom:
                leaf_expr = "TRUE"
            elif z >= cover_min_leaf_zoom:
                leaf_expr = f"npoints <= {cover_vertex_capacity}"
            else:
                leaf_expr = "FALSE"
            level_sql = level_sql_template.replace("${leaf}", leaf_expr)
            con.execute(level_sql)

            row_count = con.execute("SELECT COUNT(*) FROM covering_out").fetchone()[0]
            log.info("stage_covering: z%d done, covering_out total=%d", z, row_count)

        con.execute("DROP TABLE l_current")

        # Write per-qk4 parquet files. Loops per prefix (up to 256 scans of
        # the spilled covering_out) rather than one PARTITION_BY copy,
        # because preserve_insertion_order is set false above, so a
        # partitioned write can't preserve the per-file ORDER BY.
        prefixes = [
            row[0]
            for row in con.execute(
                "SELECT DISTINCT left(tile_qk, 4) FROM covering_out ORDER BY 1"
            ).fetchall()
        ]

        for prefix in prefixes:
            out_path = os.path.join(tmp_dir, f"{prefix}.parquet")
            con.execute(
                f"""
                COPY (
                    SELECT tile_qk, boundary_id, level, geom
                    FROM covering_out
                    WHERE left(tile_qk, 4) = ?
                    ORDER BY tile_qk, boundary_id
                ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """,
                [prefix],
            )

        # Compute stats
        stats_row = con.execute(
            """
            SELECT COUNT(*),
                   SUM(CASE WHEN length(tile_qk) = ?
                            AND ST_NPoints(geom) > ? THEN 1 ELSE 0 END)
            FROM covering_out
            """,
            [cover_max_zoom, cover_vertex_capacity],
        ).fetchone()
        stats = {
            "total": stats_row[0],
            "per_level": {},
            "over_capacity_leaves": stats_row[1] or 0,
        }
        for z_val, n_val in con.execute("""
            SELECT length(tile_qk) AS z, COUNT(*) AS n
            FROM covering_out
            GROUP BY z ORDER BY z
        """).fetchall():
            stats["per_level"][z_val] = n_val

        log.info(
            "stage_covering: done (%.1fs, %d total, %d over capacity, %d prefixes)",
            time.monotonic() - t0, stats["total"], stats["over_capacity_leaves"],
            len(prefixes),
        )

        # Write _meta.json last (freshness sentinel)
        meta = {
            "cover_min_zoom": cover_min_zoom,
            "cover_max_zoom": cover_max_zoom,
            "cover_min_leaf_zoom": cover_min_leaf_zoom,
            "cover_vertex_capacity": cover_vertex_capacity,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "stats": stats,
        }
        with open(os.path.join(tmp_dir, "_meta.json"), "w") as f:
            json.dump(meta, f)

    finally:
        con.close()

    # Atomic swap
    if os.path.exists(covering_dir):
        os.rename(covering_dir, old_dir)
    os.rename(tmp_dir, covering_dir)
    if os.path.exists(old_dir):
        shutil.rmtree(old_dir, ignore_errors=True)
    if os.path.exists(owned_spill_dir):
        shutil.rmtree(owned_spill_dir, ignore_errors=True)

    return stats
