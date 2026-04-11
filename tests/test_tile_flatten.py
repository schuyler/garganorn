"""Failing tests for flattened tile record structure (RED phase).

These tests assert the new flattened structure where:
1. SQL export views emit flat records (no `uri`/`value` wrapper) — rkey becomes top-level
2. Tile envelope gets a `collection` field
3. `tile_reader.get_record()` uses rkey matching instead of URI suffix matching

All tests MUST fail against the current codebase because the current SQL
still produces uri/value wrappers and tile_reader still uses URI matching.
"""

import gzip
import json
import os
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _load_sql(filename: str) -> str:
    """Load SQL file from garganorn/sql/."""
    sql_path = REPO_ROOT / "garganorn" / "sql" / filename
    return sql_path.read_text()


def _make_manifest_db(tmp_path, entries):
    """Create a manifest.duckdb with the given (rkey, tile_qk) entries."""
    p = tmp_path / "manifest.duckdb"
    con = duckdb.connect(str(p))
    con.execute("CREATE TABLE record_tiles (rkey VARCHAR, tile_qk VARCHAR)")
    for rkey, tile_qk in entries:
        con.execute("INSERT INTO record_tiles VALUES (?, ?)", [rkey, tile_qk])
    con.execute("CREATE TABLE metadata (source VARCHAR, generated_at VARCHAR)")
    con.execute("INSERT INTO metadata VALUES ('test', '2026-01-01T00:00:00+00:00')")
    con.close()
    return p


def _write_tile(tiles_dir, tile_qk, records):
    """Write a gzipped JSON tile file at the expected path."""
    subdir = os.path.join(str(tiles_dir), tile_qk[:6])
    os.makedirs(subdir, exist_ok=True)
    path = os.path.join(subdir, f"{tile_qk}.json.gz")
    with gzip.open(path, "wt") as f:
        json.dump({"records": records}, f)
    return path


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
# FSQ export test helpers
# ---------------------------------------------------------------------------

_FSQ_EXPORT_PLACES = [
    # (fsq_place_id, name, lat, lon, importance, country)
    ("exp001", "Blue Bottle Coffee", 37.7749, -122.4194, 72, "US"),
    ("exp002", "Golden Gate Park", 37.7694, -122.4862, 85, "US"),
    ("exp003", "Tartine Bakery", 37.7617, -122.4243, 68, "US"),
    # place with null country — should produce no address location
    ("exp004", "Mystery Spot", 37.7800, -122.4300, 40, None),
]

_EXPORT_TILE_QK = "023130"


def _make_fsq_export_db(conn, places_rows=None):
    """Populate `conn` with minimal `places` and `tile_assignments` tables."""
    if places_rows is None:
        places_rows = _FSQ_EXPORT_PLACES

    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE places (
            fsq_place_id        VARCHAR,
            name                VARCHAR,
            latitude            DOUBLE,
            longitude           DOUBLE,
            importance          INTEGER,
            address             VARCHAR,
            locality            VARCHAR,
            region              VARCHAR,
            postcode            VARCHAR,
            country             VARCHAR,
            admin_region        VARCHAR,
            post_town           VARCHAR,
            po_box              VARCHAR,
            date_created        DATE,
            date_refreshed      DATE,
            tel                 VARCHAR,
            website             VARCHAR,
            email               VARCHAR,
            facebook_id         VARCHAR,
            instagram           VARCHAR,
            twitter             VARCHAR,
            fsq_category_ids    VARCHAR[],
            fsq_category_labels VARCHAR[],
            placemaker_url      VARCHAR,
            variants            STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
            qk17                VARCHAR
        )
    """)

    for fsq_id, name, lat, lon, imp, country in places_rows:
        country_val = f"'{country}'" if country is not None else "NULL"
        conn.execute(f"""
            INSERT INTO places
            SELECT
                '{fsq_id}', '{name}', {lat}, {lon}, {imp},
                NULL, NULL, NULL, NULL, {country_val},
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                ARRAY['13065143'], ARRAY['Food & Drink'],
                NULL,
                []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
                ST_QuadKey({lon}, {lat}, 17)
        """)

    conn.execute("""
        CREATE TABLE tile_assignments (
            place_id VARCHAR,
            tile_qk  VARCHAR
        )
    """)
    for fsq_id, _name, _lat, _lon, _imp, _country in places_rows:
        conn.execute(
            "INSERT INTO tile_assignments VALUES (?, ?)",
            [fsq_id, _EXPORT_TILE_QK],
        )

    conn.execute("""
        CREATE TABLE place_containment (
            place_id       VARCHAR,
            relations_json VARCHAR
        )
    """)


# ---------------------------------------------------------------------------
# OSM export test helpers
# ---------------------------------------------------------------------------

_OSM_EXPORT_PLACES = [
    # (osm_type, osm_id, rkey, name, lat, lon, importance, primary_category, tags)
    ("n", 240109189, "n240109189", "Tartine Manufactory", 37.7612, -122.4195, 65,
     "amenity=cafe", {"cuisine": "coffee", "addr:city": "San Francisco"}),
    ("w", 50637691, "w50637691", "Dolores Park", 37.7596, -122.4269, 55,
     "leisure=park", {}),
]


def _make_osm_export_db(conn, places_rows=None):
    """Populate `conn` with minimal OSM `places` and `tile_assignments` tables."""
    if places_rows is None:
        places_rows = _OSM_EXPORT_PLACES

    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
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
    """)

    for osm_type, osm_id, rkey, name, lat, lon, imp, primary_category, tags in places_rows:
        if tags:
            map_entries = ", ".join(f"'{k}': '{v}'" for k, v in tags.items())
            map_literal = f"MAP {{{map_entries}}}"
        else:
            map_literal = "MAP()::MAP(VARCHAR, VARCHAR)"
        conn.execute(f"""
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
        """)

    conn.execute("""
        CREATE TABLE tile_assignments (
            place_id VARCHAR,
            tile_qk  VARCHAR
        )
    """)
    for _osm_type, _osm_id, rkey, _name, _lat, _lon, _imp, _primary_category, _tags in places_rows:
        conn.execute("INSERT INTO tile_assignments VALUES (?, ?)", [rkey, _EXPORT_TILE_QK])

    conn.execute("""
        CREATE TABLE place_containment (
            place_id       VARCHAR,
            relations_json VARCHAR
        )
    """)


# ---------------------------------------------------------------------------
# Overture export test helpers
# ---------------------------------------------------------------------------

_OVERTURE_EXPORT_PLACES = [
    # (id, name, lat, lon, importance)
    ("ovr001", "Philz Coffee", 37.7749, -122.4194, 70),
    ("ovr002", "Dolores Park", 37.7596, -122.4269, 55),
]


def _make_overture_export_db(conn, places_rows=None):
    """Populate `conn` with minimal Overture `places` and `tile_assignments` tables."""
    if places_rows is None:
        places_rows = _OVERTURE_EXPORT_PLACES

    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
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
    """)

    for ovr_id, name, lat, lon, imp in places_rows:
        conn.execute(f"""
            INSERT INTO places VALUES (
                '{ovr_id}',
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
        """)

    conn.execute("""
        CREATE TABLE tile_assignments (
            place_id VARCHAR,
            tile_qk  VARCHAR
        )
    """)
    for ovr_id, _name, _lat, _lon, _imp in places_rows:
        conn.execute("INSERT INTO tile_assignments VALUES (?, ?)", [ovr_id, _EXPORT_TILE_QK])

    conn.execute("""
        CREATE TABLE place_containment (
            place_id       VARCHAR,
            relations_json VARCHAR
        )
    """)


# ---------------------------------------------------------------------------
# Overture division export test helpers
# ---------------------------------------------------------------------------

_DIVISION_EXPORT_PLACES = [
    # (id, name, lat, lon, admin_level, subtype, country, region)
    ("div001", "San Francisco", 37.7749, -122.4194, 3, "locality", "US", "US-CA"),
    ("div002", "California", 37.5, -119.5, 2, "region", "US", "US-CA"),
]


def _make_division_export_db(conn, places_rows=None):
    """Populate `conn` with minimal division `places` and `tile_assignments` tables."""
    if places_rows is None:
        places_rows = _DIVISION_EXPORT_PLACES

    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE places (
            id             VARCHAR,
            geometry       GEOMETRY,
            admin_level    INTEGER,
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
    """)

    for div_id, name, lat, lon, admin_level, subtype, country, region in places_rows:
        conn.execute(f"""
            INSERT INTO places VALUES (
                '{div_id}',
                ST_Point({lon}, {lat}),
                {admin_level},
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
        """)

    conn.execute("""
        CREATE TABLE tile_assignments (
            place_id VARCHAR,
            tile_qk  VARCHAR
        )
    """)
    for div_id, _name, _lat, _lon, _admin_level, _subtype, _country, _region in places_rows:
        conn.execute("INSERT INTO tile_assignments VALUES (?, ?)", [div_id, _EXPORT_TILE_QK])

    conn.execute("""
        CREATE TABLE place_containment (
            place_id       VARCHAR,
            relations_json VARCHAR
        )
    """)


# ---------------------------------------------------------------------------
# Tests: SQL export views emit flat records (no uri/value wrapper)
# ---------------------------------------------------------------------------

class TestFsqExportNoUriValueWrapper:
    """FSQ export SQL should emit flat records without uri/value wrapper."""

    def test_fsq_export_no_uri_value_wrapper(self, tmp_path):
        """FSQ export SQL must produce flat records without uri/value wrapper.

        This test FAILS against current code because fsq_export_tiles.sql
        still produces the nested uri/value structure.
        """
        db_path = tmp_path / "test_fsq_flat.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)

        raw_sql = _load_sql("foursquare_export_tiles.sql")
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        sql = sql.replace("${repo}", "https://example.com")
        conn.execute(sql)

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        assert rows, "No rows returned from tile_export"
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            # FAIL: current structure has uri/value wrapper
            assert "uri" not in parsed, (
                f"Record should NOT have 'uri' key at top level. "
                f"Current structure has uri/value wrapper. Got keys: {list(parsed)}"
            )
            assert "value" not in parsed, (
                f"Record should NOT have 'value' key at top level. "
                f"Current structure has uri/value wrapper. Got keys: {list(parsed)}"
            )
            # PASS: flat structure should have these at top level
            assert "$type" in parsed, (
                f"Flat record must have '$type' at top level. Got keys: {list(parsed)}"
            )
            assert parsed["$type"] == "org.atgeo.place", (
                f"$type must be 'org.atgeo.place', got {parsed['$type']!r}"
            )
            assert "rkey" in parsed, (
                f"Flat record must have 'rkey' at top level. Got keys: {list(parsed)}"
            )


class TestOsmExportNoUriValueWrapper:
    """OSM export SQL should emit flat records without uri/value wrapper."""

    def test_osm_export_no_uri_value_wrapper(self, tmp_path):
        """OSM export SQL must produce flat records without uri/value wrapper.

        This test FAILS against current code because osm_export_tiles.sql
        still produces the nested uri/value structure.
        """
        db_path = tmp_path / "test_osm_flat.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_osm_export_db(conn)

        raw_sql = _load_sql("osm_export_tiles.sql")
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        sql = sql.replace("${repo}", "https://example.com")
        conn.execute(sql)

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        assert rows, "No rows returned from tile_export"
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            # FAIL: current structure has uri/value wrapper
            assert "uri" not in parsed, (
                f"Record should NOT have 'uri' key at top level. "
                f"Current structure has uri/value wrapper. Got keys: {list(parsed)}"
            )
            assert "value" not in parsed, (
                f"Record should NOT have 'value' key at top level. "
                f"Current structure has uri/value wrapper. Got keys: {list(parsed)}"
            )
            # PASS: flat structure should have these at top level
            assert "$type" in parsed, (
                f"Flat record must have '$type' at top level. Got keys: {list(parsed)}"
            )
            assert parsed["$type"] == "org.atgeo.place", (
                f"$type must be 'org.atgeo.place', got {parsed['$type']!r}"
            )
            assert "rkey" in parsed, (
                f"Flat record must have 'rkey' at top level. Got keys: {list(parsed)}"
            )


class TestOvertureExportNoUriValueWrapper:
    """Overture export SQL should emit flat records without uri/value wrapper."""

    def test_overture_export_no_uri_value_wrapper(self, tmp_path):
        """Overture export SQL must produce flat records without uri/value wrapper.

        This test FAILS against current code because overture_export_tiles.sql
        still produces the nested uri/value structure.
        """
        db_path = tmp_path / "test_overture_flat.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_overture_export_db(conn)

        raw_sql = _load_sql("overture_place_export_tiles.sql")
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        sql = sql.replace("${repo}", "https://example.com")
        conn.execute(sql)

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        assert rows, "No rows returned from tile_export"
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            # FAIL: current structure has uri/value wrapper
            assert "uri" not in parsed, (
                f"Record should NOT have 'uri' key at top level. "
                f"Current structure has uri/value wrapper. Got keys: {list(parsed)}"
            )
            assert "value" not in parsed, (
                f"Record should NOT have 'value' key at top level. "
                f"Current structure has uri/value wrapper. Got keys: {list(parsed)}"
            )
            # PASS: flat structure should have these at top level
            assert "$type" in parsed, (
                f"Flat record must have '$type' at top level. Got keys: {list(parsed)}"
            )
            assert parsed["$type"] == "org.atgeo.place", (
                f"$type must be 'org.atgeo.place', got {parsed['$type']!r}"
            )
            assert "rkey" in parsed, (
                f"Flat record must have 'rkey' at top level. Got keys: {list(parsed)}"
            )


class TestOvertureDivisionExportNoUriValueWrapper:
    """Overture division export SQL should emit flat records without uri/value wrapper."""

    def test_overture_division_export_no_uri_value_wrapper(self, tmp_path):
        """Overture division export SQL must produce flat records without uri/value wrapper.

        This test FAILS against current code because overture_division_export_tiles.sql
        still produces the nested uri/value structure.
        """
        db_path = tmp_path / "test_division_flat.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_division_export_db(conn)

        raw_sql = _load_sql("overture_division_export_tiles.sql")
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        sql = sql.replace("${repo}", "https://example.com")
        conn.execute(sql)

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        assert rows, "No rows returned from tile_export"
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            # FAIL: current structure has uri/value wrapper
            assert "uri" not in parsed, (
                f"Record should NOT have 'uri' key at top level. "
                f"Current structure has uri/value wrapper. Got keys: {list(parsed)}"
            )
            assert "value" not in parsed, (
                f"Record should NOT have 'value' key at top level. "
                f"Current structure has uri/value wrapper. Got keys: {list(parsed)}"
            )
            # PASS: flat structure should have these at top level
            assert "$type" in parsed, (
                f"Flat record must have '$type' at top level. Got keys: {list(parsed)}"
            )
            assert parsed["$type"] == "org.atgeo.place", (
                f"$type must be 'org.atgeo.place', got {parsed['$type']!r}"
            )
            assert "rkey" in parsed, (
                f"Flat record must have 'rkey' at top level. Got keys: {list(parsed)}"
            )


# ---------------------------------------------------------------------------
# Tests: tile_reader uses rkey matching instead of URI suffix matching
# ---------------------------------------------------------------------------

class TestTileReaderUsesRkeyMatching:
    """tile_reader.get_record() should find records by rkey field directly."""

    def test_tile_reader_uses_rkey_matching(self, tmp_path):
        """get_record() should match rkey field directly, not URI suffix.

        This test FAILS against current code because tile_reader.get_record()
        still uses record["uri"].endswith(target_uri_suffix) matching.
        """
        from garganorn.tile_reader import TileBackedCollection

        tile_qk = "023010"
        rkey = "place001"
        collection = "org.atgeo.places.test"

        # Create a flat-structure tile (no uri/value wrapper)
        manifest_db = _make_manifest_db(tmp_path, [(rkey, tile_qk)])
        _write_tile(tmp_path, tile_qk, [
            {
                # Flat structure: rkey at top level, no uri/value wrapper
                "$type": "org.atgeo.place",
                "rkey": rkey,
                "name": "Test Place",
                "importance": 75,
                "locations": [
                    {
                        "$type": "community.lexicon.location.geo",
                        "latitude": "37.7749",
                        "longitude": "-122.4194"
                    }
                ],
                "variants": [],
                "attributes": {},
                "relations": {}
            }
        ])

        col = TileBackedCollection(
            collection=collection,
            manifest_db_path=str(manifest_db),
            tiles_dir=str(tmp_path),
            attribution="https://example.com/attribution",
        )

        # This FAILS because current implementation looks for record["uri"]
        # which doesn't exist in flat-structure tiles
        result = col.get_record("repo", collection, rkey)

        assert result is not None, (
            f"get_record() should find record with rkey={rkey} in flat-structure tile. "
            f"Current implementation looks for 'uri' key which doesn't exist."
        )
        assert result["rkey"] == rkey, (
            f"Returned record should have rkey={rkey}, got {result.get('rkey')}"
        )
        assert result["name"] == "Test Place", (
            f"Returned record should have name='Test Place', got {result.get('name')}"
        )


# ---------------------------------------------------------------------------
# Tests: tile envelope has collection field
# ---------------------------------------------------------------------------

class TestTileEnvelopeHasCollectionField:
    """flush_tile in quadtree.py should produce envelope with collection field."""

    def test_tile_envelope_has_collection_field(self, tmp_path):
        """Tile envelope JSON must contain a 'collection' field.

        This test FAILS against current code because flush_tile in quadtree.py
        only includes 'attribution' and 'records', not 'collection'.
        """
        from garganorn.quadtree import export_tiles

        # Create a minimal FSQ database
        db_path = tmp_path / "test_tile_env.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_fsq_export_db(conn)

        # Prepare tile_assignments for export
        conn.execute("SET enable_progress_bar = false")

        # Run export_tiles to generate tile files
        output_dir = str(tmp_path / "tiles")
        export_tiles(conn, output_dir, "foursquare", max_workers=1)
        conn.close()

        # Read the generated tile file
        tile_path = os.path.join(output_dir, _EXPORT_TILE_QK[:6], f"{_EXPORT_TILE_QK}.json.gz")
        assert os.path.exists(tile_path), f"Tile file not created at {tile_path}"

        with gzip.open(tile_path, "rt") as f:
            envelope = json.load(f)

        # FAIL: current envelope only has 'attribution' and 'records'
        assert "collection" in envelope, (
            f"Tile envelope must have 'collection' field. "
            f"Current envelope has keys: {list(envelope)}. "
            f"Expected 'collection' to be present with value like 'org.atgeo.places.foursquare'"
        )
        assert envelope["collection"] == "org.atgeo.places.foursquare", (
            f"collection field should be 'org.atgeo.places.foursquare', "
            f"got {envelope.get('collection')!r}"
        )
        assert "attribution" in envelope, "envelope must still have 'attribution' field"
        assert "records" in envelope, "envelope must still have 'records' field"
