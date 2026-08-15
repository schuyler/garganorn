"""Shared test helpers for quadtree-related tests.

This module is a plain Python module, not a pytest conftest. Import directly.
"""

import pathlib
import string

import duckdb

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


def _load_qk_env_macro(conn):
    """Load qk_tile_x/qk_tile_y/qk_env/qk17 onto an open connection.

    Mirrors stages._load_qk_env_macros; the import SQL under test calls
    qk17() and needs it defined on the connection before executing.
    """
    for stmt in (REPO_ROOT / "garganorn" / "sql" / "qk_env_macro.sql").read_text().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)


SF_BBOX = dict(xmin=-122.55, xmax=-122.30, ymin=37.60, ymax=37.85)
OV_BBOX = dict(xmin=-122.55, xmax=-122.30, ymin=37.60, ymax=37.85)
OSM_SF_BBOX = dict(xmin=-122.55, xmax=-122.30, ymin=37.60, ymax=37.85)


def _density_cte_sql(density_rows):
    """Build the density_tiles temp-table SQL fragment.

    density_rows=None (the default used by every *_import helper below)
    creates an empty table — importance defaults to 0 and the density
    LEFT JOIN never matches.

    density_rows: optional list of (tile_qk15, density_score, tile_xmin,
        tile_ymin, tile_xmax, tile_ymax) tuples to populate density_tiles
        with real rows, e.g. to exercise the density join with actual
        matches (including deliberately duplicate tile_qk15 keys, to test
        for row fan-out through the join).

    This is the single shared implementation used by run_overture_import
    and run_osm_import — do not copy this literal per import helper.
    """
    if density_rows is None:
        return (
            "CREATE TEMP TABLE density_tiles AS SELECT NULL::VARCHAR AS tile_qk15, "
            "NULL::DOUBLE AS density_score, NULL::DOUBLE AS tile_xmin, "
            "NULL::DOUBLE AS tile_ymin, NULL::DOUBLE AS tile_xmax, "
            "NULL::DOUBLE AS tile_ymax WHERE 1=0;"
        )
    values_sql = ", ".join(
        f"('{qk15}', {score}, {xmin}, {ymin}, {xmax}, {ymax})"
        for qk15, score, xmin, ymin, xmax, ymax in density_rows
    )
    return (
        "CREATE TEMP TABLE density_tiles (tile_qk15 VARCHAR, density_score DOUBLE, "
        "tile_xmin DOUBLE, tile_ymin DOUBLE, tile_xmax DOUBLE, tile_ymax DOUBLE);\n"
        f"INSERT INTO density_tiles VALUES {values_sql};"
    )


def _idf_cte_sql(idf_rows):
    """Build the idf_scores temp-table SQL fragment.

    idf_rows=None (the default used by every *_import helper below) creates
    an empty table.

    idf_rows: optional list of (category, idf_score) tuples to populate
        idf_scores with real rows (including deliberately duplicate
        category keys, to test for row fan-out through the join).

    This is the single shared implementation used by run_overture_import
    and run_osm_import — do not copy this literal per import helper.
    """
    if idf_rows is None:
        return (
            "CREATE TEMP TABLE idf_scores AS SELECT NULL::VARCHAR AS category, "
            "NULL::DOUBLE AS idf_score WHERE 1=0;"
        )
    values_sql = ", ".join(f"('{category}', {score})" for category, score in idf_rows)
    return (
        "CREATE TEMP TABLE idf_scores (category VARCHAR, idf_score DOUBLE);\n"
        f"INSERT INTO idf_scores VALUES {values_sql};"
    )

def run_overture_import(conn, parquet_glob, bbox=None, density_rows=None, idf_rows=None,
                         density_norm=10.0, idf_norm=18.0):
    """Run overture_place_import.sql against `parquet_glob`.

    By default (density_rows=None, idf_rows=None) density_tiles/idf_scores are
    created empty.

    density_rows: optional list of (tile_qk15, density_score, tile_xmin,
        tile_ymin, tile_xmax, tile_ymax) tuples to populate density_tiles with
        real rows, e.g. to exercise the density join with actual matches
        (including deliberately duplicate tile_qk15 keys, to test for
        row fan-out through the join).
    idf_rows: optional list of (category, idf_score) tuples to populate
        idf_scores with real rows.
    """
    if bbox is None:
        bbox = OV_BBOX
    density_cte = _density_cte_sql(density_rows)
    idf_cte = _idf_cte_sql(idf_rows)
    substitutions = {
        "memory_limit": "4GB",
        "parquet_glob": parquet_glob,
        "xmin": bbox["xmin"],
        "xmax": bbox["xmax"],
        "ymin": bbox["ymin"],
        "ymax": bbox["ymax"],
        "density_cte": density_cte,
        "idf_cte": idf_cte,
        "density_norm": density_norm,
        "idf_norm": idf_norm,
    }
    raw_sql = _load_sql("overture_place_import.sql", substitutions)
    sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
    _load_qk_env_macro(conn)
    conn.execute(sql)


def run_osm_import(conn, node_glob, way_glob=None, bbox=None, density_rows=None, idf_rows=None,
                    density_norm=10.0, idf_norm=18.0):
    """Run osm_import.sql against node_glob/way_glob.

    By default (density_rows=None, idf_rows=None) density_tiles/idf_scores are
    created empty.

    density_rows/idf_rows: see _density_cte_sql/_idf_cte_sql docstrings.
    """
    if bbox is None:
        bbox = OSM_SF_BBOX
    if way_glob is None:
        way_glob = node_glob
    density_cte = _density_cte_sql(density_rows)
    idf_cte = _idf_cte_sql(idf_rows)
    # Load OSM category case SQL
    osm_category_case = (REPO_ROOT / "garganorn" / "sql" / "_osm_category_case.sql").read_text().strip()
    substitutions = {
        "memory_limit": "4GB",
        "node_parquet": node_glob,
        "way_parquet": way_glob,
        "xmin": bbox["xmin"],
        "xmax": bbox["xmax"],
        "ymin": bbox["ymin"],
        "ymax": bbox["ymax"],
        "density_cte": density_cte,
        "idf_cte": idf_cte,
        "density_norm": density_norm,
        "idf_norm": idf_norm,
        "osm_category_case": osm_category_case,
    }
    raw_sql = _load_sql("osm_import.sql", substitutions)
    sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
    _load_qk_env_macro(conn)
    conn.execute(sql)


def run_tile_assignments(conn, pk_expr="place_id", min_zoom=6, max_zoom=17, max_per_tile=1000):
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
            place_id     VARCHAR,
            name         VARCHAR,
            latitude     DOUBLE,
            longitude    DOUBLE,
            qk17         VARCHAR
        )
    """)
    for place_id, lat, lon in places:
        conn.execute(
            "INSERT INTO places VALUES (?, ?, ?, ?, ST_QuadKey(?, ?, 17))",
            [place_id, f"Place {place_id}", lat, lon, lon, lat],
        )


def write_minimal_overture_parquet(path, place_rows):
    """Write a minimal Overture-schema parquet with the given (id, lon, lat, category) rows.

    Used by tests that need full control over which places match which
    density/idf keys (e.g. row-fan-out or importance-arithmetic
    characterization), independent of the shared overture_parquet fixture.
    names.primary is set to a placeholder ("Place <id>", matching the
    convention in make_tile_assignment_db above) so rows survive the
    non-empty-name import filter; names.common and names.rules are both
    NULL for every row, since these rows are not intended to exercise
    variants derivation.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE tmp_ov (
            id          VARCHAR,
            bbox        STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            geometry    VARCHAR,
            names       STRUCT(
                            "primary" VARCHAR,
                            common MAP(VARCHAR, VARCHAR),
                            rules  STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]
                        ),
            categories  STRUCT("primary" VARCHAR),
            addresses   STRUCT(country VARCHAR, postcode VARCHAR, locality VARCHAR, freeform VARCHAR, region VARCHAR)[],
            websites    VARCHAR[],
            socials     VARCHAR[],
            emails      VARCHAR[],
            phones      VARCHAR[],
            brand       VARCHAR,
            confidence  DOUBLE,
            version     INTEGER,
            sources     VARCHAR[]
        )
    """)
    for place_id, lon, lat, category in place_rows:
        conn.execute(
            """
            INSERT INTO tmp_ov VALUES (
                ?,
                {'xmin': ?, 'ymin': ?, 'xmax': ?, 'ymax': ?},
                ?,
                {'primary': ?,
                 'common': NULL::MAP(VARCHAR, VARCHAR),
                 'rules':  NULL::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
                {'primary': ?},
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            )
            """,
            [
                place_id,
                lon - 0.001, lat - 0.001, lon + 0.001, lat + 0.001,
                f"POINT({lon} {lat})",
                f"Place {place_id}",
                category,
            ],
        )
    conn.execute(f"COPY tmp_ov TO '{path}' (FORMAT PARQUET)")
    conn.close()
