import gzip
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

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
    """Query DuckDB for per-tile JSON and write gzipped files. Returns {qk: record_count}."""
    sql_dir = Path(__file__).parent / "sql"
    sql = (sql_dir / f"{source}_export_tiles.sql").read_text()
    sql = sql.replace("${attribution}", ATTRIBUTION[source])
    sql = sql.replace("${repo}", REPO)
    con.execute(sql)  # creates tile_export view
    result = con.execute("SELECT tile_qk, tile_json FROM tile_export").fetchall()
    log.info("export: queried %d tiles from DuckDB", len(result))
    manifest = {}
    for i, (tile_qk, tile_json) in enumerate(result):
        subdir = os.path.join(output_dir, tile_qk[:6])
        os.makedirs(subdir, exist_ok=True)
        with gzip.open(os.path.join(subdir, f"{tile_qk}.json.gz"), "wb") as f:
            f.write(tile_json.encode("utf-8"))
        manifest[tile_qk] = len(json.loads(tile_json)["records"])
        if (i + 1) % 1000 == 0:
            log.info("export: wrote %d / %d tiles", i + 1, len(result))
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

    # Import stage — OSM needs two separate parquet paths
    if source == "osm":
        node_parquet, way_parquet = parquet_glob
        run_sql("import", "osm_import.sql",
                memory_limit=memory_limit,
                node_parquet=node_parquet,
                way_parquet=way_parquet,
                xmin=bbox[0], ymin=bbox[1], xmax=bbox[2], ymax=bbox[3])
    else:
        run_sql("import", f"{source}_import.sql",
                memory_limit=memory_limit,
                parquet_glob=parquet_glob,
                xmin=bbox[0], ymin=bbox[1], xmax=bbox[2], ymax=bbox[3])

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
    con.close()
    try:
        os.remove(db_path)
    except OSError:
        pass
    log.info("[%s] pipeline complete (%.1fs total)", source, time.monotonic() - t0)


def write_manifest(manifest, output_dir, source):
    data = {
        "source": source,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "quadkeys": sorted(manifest.keys()),
    }
    with open(os.path.join(output_dir, "manifest.json"), "w") as f:
        json.dump(data, f, indent=2)
