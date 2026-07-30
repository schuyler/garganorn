"""Covering stage: build quadkey covering for boundaries.duckdb.

Implements §3.5 of docs/pipeline-restructure-design.md (Phase 1).
"""
import json
import logging
import math
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from garganorn.stages import quadkey_to_bbox, _is_output_fresh

log = logging.getLogger(__name__)

COVER_MIN_ZOOM = 4
COVER_MAX_ZOOM = 12

_SQL_DIR = Path(__file__).parent / "sql"

_MERC_LAT_MAX = 85.05112877980659


def lonlat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    """Web-mercator forward: clamp lat to ±85.05112878, return (x, y).

    y increases southward (tile 0,0 is top-left / NW corner of the world).
    """
    lat = max(-_MERC_LAT_MAX, min(_MERC_LAT_MAX, lat))
    n = 2 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    x = max(0, min(x, n - 1))
    lat_rad = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    y = max(0, min(y, n - 1))
    return (x, y)


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


def bbox_to_quadkeys(
    min_lon: float, min_lat: float, max_lon: float, max_lat: float, zoom: int
) -> list[str]:
    """Quadkeys at `zoom` whose tiles intersect the bbox.

    D7: min_lon > max_lon means antimeridian crossing; returns the union
    of the two lobes [min_lon, 180] and [-180, max_lon] with no gap tiles.
    """
    if min_lon > max_lon:
        # Antimeridian crossing: two lobes
        east = bbox_to_quadkeys(min_lon, min_lat, 180.0, max_lat, zoom)
        west = bbox_to_quadkeys(-180.0, min_lat, max_lon, max_lat, zoom)
        seen: set[str] = set()
        result: list[str] = []
        for qk in east + west:
            if qk not in seen:
                seen.add(qk)
                result.append(qk)
        return result

    n = 2 ** zoom
    # NW corner of bbox (min_lon, max_lat) → smallest (x, y)
    x_min, y_min = lonlat_to_tile(min_lon, max_lat, zoom)
    # SE corner of bbox (max_lon, min_lat) → largest (x, y)
    x_max, y_max = lonlat_to_tile(max_lon, min_lat, zoom)

    # Clamp to valid range
    x_max = min(x_max, n - 1)
    y_max = min(y_max, n - 1)

    result = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            result.append(_tile_to_quadkey(x, y, zoom))
    return result


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
    cover_min_zoom: int = COVER_MIN_ZOOM,
    cover_max_zoom: int = COVER_MAX_ZOOM,
    force: bool = False,
) -> dict:
    """Build covering/<qk4>.parquet from boundaries.duckdb.

    Returns stats dict: {total, interior, edge, per_level: {z: n}}.

    Freshness: skips when covering_dir/_meta.json is newer than boundaries_db
    AND the recorded zoom parameters match. force=True always rebuilds.

    Atomicity (§2.5): builds under covering_dir+'.tmp', then swaps.  At build
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
                ):
                    log.info("stage_covering: skipping (output is fresh)")
                    return {}
            except (OSError, json.JSONDecodeError):
                pass  # fall through to rebuild

    tmp_dir = covering_dir + ".tmp"
    old_dir = covering_dir + ".old"

    # Build start: unconditionally remove crash leftovers (§2.5 step 1)
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
        con.execute(f"SET memory_limit = '{memory_limit}'")
        con.execute("SET preserve_insertion_order = false")
        con.execute("INSTALL spatial; LOAD spatial")

        _load_qk_env_macros(con)

        con.execute(f"ATTACH '{boundaries_db}' AS bnd (READ_ONLY)")

        # Output accumulator
        con.execute("""
            CREATE TEMP TABLE covering_out (
                tile_qk     VARCHAR,
                boundary_id VARCHAR,
                level       INTEGER,
                kind        VARCHAR
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

        # Level loop
        level_sql = (_SQL_DIR / "covering_level.sql").read_text()
        for z in range(cover_min_zoom, cover_max_zoom + 1):
            is_last = z == cover_max_zoom

            if is_last:
                # Terminal level: flag interior/edge and emit both kinds
                con.execute("""
                    CREATE TEMP TABLE l_flagged AS
                    SELECT *, ST_Contains(geom, qk_env(tile_qk)) AS is_interior
                    FROM l_current
                """)
                con.execute("""
                    INSERT INTO covering_out
                    SELECT tile_qk, boundary_id, level, 'interior'
                    FROM l_flagged WHERE is_interior
                """)
                con.execute("""
                    INSERT INTO covering_out
                    SELECT tile_qk, boundary_id, level, 'edge'
                    FROM l_flagged WHERE NOT is_interior
                """)
                con.execute("DROP TABLE l_flagged")
                con.execute("DROP TABLE l_current")
            else:
                # Non-terminal level: flag, emit interior, expand to children
                con.execute(level_sql)

            row_count = con.execute("SELECT COUNT(*) FROM covering_out").fetchone()[0]
            log.debug("stage_covering: z%d done, covering_out total=%d", z, row_count)

        # Write per-qk4 parquet files
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
                    SELECT tile_qk, boundary_id, level, kind
                    FROM covering_out
                    WHERE left(tile_qk, 4) = ?
                    ORDER BY tile_qk, boundary_id
                ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """,
                [prefix],
            )

        # Compute stats
        stats_row = con.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN kind = 'interior' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN kind = 'edge' THEN 1 ELSE 0 END)
            FROM covering_out
        """).fetchone()
        stats = {
            "total": stats_row[0],
            "interior": stats_row[1],
            "edge": stats_row[2],
            "per_level": {},
        }
        for z_val, n_val in con.execute("""
            SELECT length(tile_qk) AS z, COUNT(*) AS n
            FROM covering_out
            GROUP BY z ORDER BY z
        """).fetchall():
            stats["per_level"][z_val] = n_val

        log.info(
            "stage_covering: done (%.1fs, %d total, %d interior, %d edge, %d prefixes)",
            time.monotonic() - t0, stats["total"], stats["interior"],
            stats["edge"], len(prefixes),
        )

        # Write _meta.json last (freshness sentinel)
        meta = {
            "cover_min_zoom": cover_min_zoom,
            "cover_max_zoom": cover_max_zoom,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "stats": stats,
        }
        with open(os.path.join(tmp_dir, "_meta.json"), "w") as f:
            json.dump(meta, f)

    finally:
        con.close()

    # Atomic swap (§2.5 step 2)
    if os.path.exists(covering_dir):
        os.rename(covering_dir, old_dir)
    os.rename(tmp_dir, covering_dir)
    if os.path.exists(old_dir):
        shutil.rmtree(old_dir, ignore_errors=True)
    if os.path.exists(owned_spill_dir):
        shutil.rmtree(owned_spill_dir, ignore_errors=True)

    return stats


def ensure_covering(
    boundaries_db: str, covering_dir: str | None = None, **kwargs
) -> str:
    """Derive covering_dir (default: dirname(boundaries_db)/covering),
    call stage_covering (no-op when fresh), return the dir.
    """
    if covering_dir is None:
        covering_dir = os.path.join(os.path.dirname(boundaries_db), "covering")
    stage_covering(boundaries_db, covering_dir, **kwargs)
    return covering_dir
