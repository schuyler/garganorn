"""Tests that SQL export views emit flat place records: no `uri`/`value`
wrapper, `rkey` and `$type` at the top level.
"""

import json
from pathlib import Path

import duckdb
import pytest

from tests.quadtree_helpers import _load_sql as _load_sql_with_substitutions

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _load_sql(filename: str) -> str:
    """Load SQL file from garganorn/sql/ (no template substitutions)."""
    return _load_sql_with_substitutions(filename, {})


def _strip_spatial_install(sql: str) -> str:
    """Remove INSTALL/LOAD spatial statements for in-memory DuckDB."""
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("INSTALL") or stripped.startswith("LOAD"):
            continue
        lines.append(line)
    return "\n".join(lines)


def _strip_memory_limit(sql: str) -> str:
    """Remove SET memory_limit directives."""
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("SET memory_limit"):
            continue
        lines.append(line)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Export test configuration and unified DB builder
# ---------------------------------------------------------------------------

_EXPORT_CONFIGS = {
    "osm": {
        "sql_file": "osm_export_tiles.sql",
        "places_rows": [
            ("n", 240109189, "n240109189", "Tartine Manufactory", 37.7612, -122.4195, 65,
             "amenity=cafe", {"cuisine": "coffee", "addr:city": "San Francisco"}),
            ("w", 50637691, "w50637691", "Dolores Park", 37.7596, -122.4269, 55,
             "leisure=park", {}),
        ],
        "pk_col": "rkey",
        "create_table": """
            CREATE TABLE places (
                osm_type         VARCHAR,
                osm_id           BIGINT,
                rkey             VARCHAR,
                name             VARCHAR,
                latitude         DOUBLE,
                longitude        DOUBLE,
                geom             GEOMETRY,
                primary_category VARCHAR,
                tags             MAP(VARCHAR, VARCHAR),
                bbox             STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
                importance       INTEGER,
                variants         STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
                qk17             VARCHAR
            )
        """,
        "insert_template": """
            INSERT INTO places VALUES (
                '{osm_type}', {osm_id}, '{rkey}', '{name}', {lat}, {lon},
                ST_Point({lon}, {lat}),
                '{primary_category}',
                {map_literal},
                {{'xmin': {lon}-0.001, 'ymin': {lat}-0.001,
                  'xmax': {lon}+0.001, 'ymax': {lat}+0.001}},
                {imp},
                []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
                ST_QuadKey({lon}, {lat}, 17)
            )
        """,
    },
    "overture_place": {
        "sql_file": "overture_place_export_tiles.sql",
        "places_rows": [
            ("ovr001", "Philz Coffee", 37.7749, -122.4194, 70),
            ("ovr002", "Dolores Park", 37.7596, -122.4269, 55),
        ],
        "pk_col": "id",
        "create_table": """
            CREATE TABLE places (
                id          VARCHAR,
                geometry    GEOMETRY,
                bbox        STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
                names       STRUCT("primary" VARCHAR),
                categories  STRUCT("primary" VARCHAR),
                addresses   STRUCT(country VARCHAR, postcode VARCHAR, locality VARCHAR, freeform VARCHAR, region VARCHAR)[],
                websites    VARCHAR[],
                socials     VARCHAR[],
                emails      VARCHAR[],
                phones      VARCHAR[],
                brand       STRUCT(names STRUCT("primary" VARCHAR)),
                confidence  DOUBLE,
                version     INTEGER,
                sources     STRUCT(property VARCHAR, dataset VARCHAR, record_id VARCHAR, confidence DOUBLE)[],
                importance  INTEGER,
                variants    STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
                qk17        VARCHAR
            )
        """,
        "insert_template": """
            INSERT INTO places VALUES (
                '{pk}',
                ST_Point({lon}, {lat}),
                {{'xmin': {lon}-0.001, 'ymin': {lat}-0.001,
                  'xmax': {lon}+0.001, 'ymax': {lat}+0.001}},
                {{'primary': '{name}'}},
                {{'primary': NULL}},
                [{{'country': 'US', 'postcode': '94103', 'locality': 'San Francisco',
                   'freeform': '123 Main St', 'region': 'US-CA'}}],
                NULL, NULL, NULL, NULL,
                NULL,
                0.9, 1, NULL,
                {imp},
                []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
                ST_QuadKey({lon}, {lat}, 17)
            )
        """,
    },
    "overture_division": {
        "sql_file": "overture_division_export_tiles.sql",
        # level values are the atgeo containment vocabulary (garganorn.levels.LEVEL_VOCAB),
        # re-derived from subtype: locality=50, region=25.
        "places_rows": [
            ("div001", "San Francisco", 37.7749, -122.4194, 50, "locality", "US", "US-CA"),
            ("div002", "California", 37.5, -119.5, 25, "region", "US", "US-CA"),
        ],
        "pk_col": "id",
        "create_table": """
            CREATE TABLE places (
                id             VARCHAR,
                geometry       GEOMETRY,
                level          INTEGER,
                names          STRUCT("primary" VARCHAR),
                subtype        VARCHAR,
                country        VARCHAR,
                region         VARCHAR,
                wikidata       VARCHAR,
                population     BIGINT,
                min_latitude   DOUBLE,
                max_latitude   DOUBLE,
                min_longitude  DOUBLE,
                max_longitude  DOUBLE,
                importance     INTEGER,
                variants       STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
                qk17           VARCHAR
            )
        """,
        "insert_template": """
            INSERT INTO places VALUES (
                '{pk}',
                ST_Point({lon}, {lat}),
                {level},
                {{'primary': '{name}'}},
                '{subtype}',
                '{country}',
                '{region}',
                NULL,
                NULL,
                {lat}-0.1,
                {lat}+0.1,
                {lon}-0.1,
                {lon}+0.1,
                0,
                []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
                ST_QuadKey({lon}, {lat}, 17)
            )
        """,
    },
}

_EXPORT_TILE_QK = "023130"


def _make_export_db(conn, source, places_rows=None):
    """Create minimal export tables for any source.

    Args:
        conn: DuckDB connection
        source: Source key (osm, overture_place, overture_division)
        places_rows: Optional custom rows (uses config default if None)
    """
    config = _EXPORT_CONFIGS[source]
    if places_rows is None:
        places_rows = config["places_rows"]

    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute(config["create_table"])

    # Insert rows based on source-specific logic
    if source == "osm":
        for osm_type, osm_id, rkey, name, lat, lon, imp, primary_category, tags in places_rows:
            if tags:
                map_entries = ", ".join(f"'{k}': '{v}'" for k, v in tags.items())
                map_literal = f"MAP {{{map_entries}}}"
            else:
                map_literal = "MAP()::MAP(VARCHAR, VARCHAR)"
            conn.execute(config["insert_template"].format(
                osm_type=osm_type, osm_id=osm_id, rkey=rkey, name=name,
                lat=lat, lon=lon, imp=imp, primary_category=primary_category,
                map_literal=map_literal
            ))
    elif source == "overture_place":
        for pk, name, lat, lon, imp in places_rows:
            conn.execute(config["insert_template"].format(
                pk=pk, name=name, lat=lat, lon=lon, imp=imp
            ))
    elif source == "overture_division":
        for pk, name, lat, lon, level, subtype, country, region in places_rows:
            conn.execute(config["insert_template"].format(
                pk=pk, name=name, lat=lat, lon=lon, level=level,
                subtype=subtype, country=country, region=region
            ))

    # Create tile_assignments table
    conn.execute("""
        CREATE TABLE tile_assignments (
            place_id VARCHAR,
            tile_qk  VARCHAR
        )
    """)

    # Extract primary key values for tile_assignments
    pk_col = config["pk_col"]
    for row in places_rows:
        if source == "osm":
            pk_val = row[2]  # rkey
        elif source in ("overture_place", "overture_division"):
            pk_val = row[0]
        conn.execute("INSERT INTO tile_assignments VALUES (?, ?)", [pk_val, _EXPORT_TILE_QK])

    conn.execute("""
        CREATE TABLE place_containment (
            place_id       VARCHAR,
            relations_json VARCHAR,
            tile_qk        VARCHAR
        )
    """)


# ---------------------------------------------------------------------------
# Tests: SQL export views emit flat records (no uri/value wrapper)
# ---------------------------------------------------------------------------

def _assert_flat_record(parsed):
    """Assert a record has flat structure (no uri/value wrapper)."""
    assert "uri" not in parsed, (
        f"Record should NOT have 'uri' key at top level. Got keys: {list(parsed)}"
    )
    assert "value" not in parsed, (
        f"Record should NOT have 'value' key at top level. Got keys: {list(parsed)}"
    )
    assert "$type" in parsed, (
        f"Flat record must have '$type' at top level. Got keys: {list(parsed)}"
    )
    assert parsed["$type"] == "org.atgeo.place", (
        f"$type must be 'org.atgeo.place', got {parsed['$type']!r}"
    )
    assert "rkey" in parsed, (
        f"Flat record must have 'rkey' at top level. Got keys: {list(parsed)}"
    )


@pytest.mark.parametrize("source", ["osm", "overture_place", "overture_division"])
def test_export_no_uri_value_wrapper(tmp_path, source):
    """Export SQL must produce flat records without uri/value wrapper.

    Parametrized to test all three sources: osm, overture_place, overture_division.
    """
    config = _EXPORT_CONFIGS[source]
    db_path = tmp_path / f"test_{source}_flat.duckdb"
    conn = duckdb.connect(str(db_path))
    _make_export_db(conn, source)

    raw_sql = _load_sql(config["sql_file"])
    sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
    sql = sql.replace("${repo}", "https://example.com")
    conn.execute(sql)

    rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
    conn.close()

    assert rows, "No rows returned from tile_export"
    for (record_json,) in rows:
        parsed = json.loads(record_json)
        _assert_flat_record(parsed)
