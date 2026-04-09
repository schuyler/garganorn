"""Shared test helpers for quadtree-related tests.

This module is a plain Python module, not a pytest conftest. Import directly.
"""

import pathlib
import string

REPO_ROOT = pathlib.Path(__file__).parent.parent


def _load_sql(filename: str, substitutions: dict) -> str:
    sql_path = REPO_ROOT / "garganorn" / "sql" / filename
    raw = sql_path.read_text()
    return string.Template(raw).safe_substitute(substitutions)


def _strip_spatial_install(sql: str) -> str:
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("INSTALL spatial") or stripped.startswith("LOAD spatial"):
            continue
        lines.append(line)
    return "\n".join(lines)


def _strip_memory_limit(sql: str) -> str:
    lines = [
        line for line in sql.splitlines()
        if not line.strip().startswith("SET memory_limit")
    ]
    return "\n".join(lines)


SF_BBOX = dict(xmin=-122.55, xmax=-122.30, ymin=37.60, ymax=37.85)
OV_BBOX = dict(xmin=-122.55, xmax=-122.30, ymin=37.60, ymax=37.85)
OSM_SF_BBOX = dict(xmin=-122.55, xmax=-122.30, ymin=37.60, ymax=37.85)

# (fsq_place_id, name, latitude, longitude, date_refreshed, date_closed, geom_wkt,
#  fsq_category_ids, expected_in_result)
FSQ_ROWS = [
    # In-bbox, good quality — should survive
    ("fsq001", "Blue Bottle Coffee",  37.7749, -122.4194, "2022-01-01", None,
     "POINT(-122.4194 37.7749)", ["13065143"], True),
    ("fsq002", "Golden Gate Park",    37.7694, -122.4862, "2021-06-15", None,
     "POINT(-122.4862 37.7694)", ["16000178", "16000179"], True),
    # Out of bbox (longitude < xmin)
    ("fsq003", "Faraway Place",       37.7500, -123.0000, "2022-01-01", None,
     "POINT(-123.0000 37.7500)", ["13065143"], False),
    # date_closed is not null — should be excluded
    ("fsq004", "Closed Cafe",         37.7600, -122.4000, "2022-01-01", "2023-01-01",
     "POINT(-122.4000 37.7600)", ["13065143"], False),
    # longitude == 0 — should be excluded
    ("fsq005", "Zero Lon Place",      37.7600,   0.0000,  "2022-01-01", None,
     "POINT(0.0 37.7600)", ["13065143"], False),
    # geom IS NULL — should be excluded
    ("fsq006", "Null Geom Place",     37.7700, -122.4100, "2022-01-01", None,
     None, ["13065143"], False),
    # date_refreshed too old — should be excluded
    ("fsq007", "Stale Place",         37.7710, -122.4110, "2019-01-01", None,
     "POINT(-122.4110 37.7710)", ["13065143"], False),
    # A second good in-bbox place with multiple categories (higher diversity)
    ("fsq008", "Diverse Venue",       37.7800, -122.4300, "2023-03-01", None,
     "POINT(-122.4300 37.7800)",
     ["13065143", "16000178", "10000001", "10000002"], True),
]


def run_fsq_import(conn, parquet_glob, bbox=None):
    if bbox is None:
        bbox = SF_BBOX
    substitutions = {
        "memory_limit": "4GB",
        "parquet_glob": parquet_glob,
        "xmin": bbox["xmin"],
        "xmax": bbox["xmax"],
        "ymin": bbox["ymin"],
        "ymax": bbox["ymax"],
    }
    raw_sql = _load_sql("fsq_import.sql", substitutions)
    sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
    conn.execute(sql)


def run_overture_import(conn, parquet_glob, bbox=None):
    if bbox is None:
        bbox = OV_BBOX
    substitutions = {
        "memory_limit": "4GB",
        "parquet_glob": parquet_glob,
        "xmin": bbox["xmin"],
        "xmax": bbox["xmax"],
        "ymin": bbox["ymin"],
        "ymax": bbox["ymax"],
    }
    raw_sql = _load_sql("overture_import.sql", substitutions)
    sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
    conn.execute(sql)


def run_osm_import(conn, node_glob, way_glob=None, bbox=None):
    if bbox is None:
        bbox = OSM_SF_BBOX
    if way_glob is None:
        way_glob = node_glob
    substitutions = {
        "memory_limit": "4GB",
        "node_parquet": node_glob,
        "way_parquet": way_glob,
        "xmin": bbox["xmin"],
        "xmax": bbox["xmax"],
        "ymin": bbox["ymin"],
        "ymax": bbox["ymax"],
    }
    raw_sql = _load_sql("osm_import.sql", substitutions)
    sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
    conn.execute(sql)


def run_tile_assignments(conn, pk_expr="fsq_place_id", min_zoom=6, max_zoom=17, max_per_tile=1000):
    sql = _load_sql(
        "compute_tile_assignments.sql",
        {
            "pk_expr": pk_expr,
            "min_zoom": min_zoom,
            "max_zoom": max_zoom,
            "max_per_tile": max_per_tile,
        },
    )
    conn.execute(sql)


def make_tile_assignment_db(conn, places):
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE places (
            fsq_place_id VARCHAR,
            name         VARCHAR,
            latitude     DOUBLE,
            longitude    DOUBLE,
            qk17         VARCHAR
        )
    """)
    for fsq_id, lat, lon in places:
        conn.execute(
            "INSERT INTO places VALUES (?, ?, ?, ?, ST_QuadKey(?, ?, 17))",
            [fsq_id, f"Place {fsq_id}", lat, lon, lon, lat],
        )
