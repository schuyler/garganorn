"""Tests for the *_export_tiles.sql views and stage_export()."""

import gzip
import inspect
import json
import logging
import re

import duckdb
import pytest
from tests.quadtree_helpers import (
    _load_sql, _strip_spatial_install, _strip_memory_limit,
    run_overture_import, run_tile_assignments, run_osm_import,
)
from tests.duckdb_spy import spy_on_duckdb_connect


# ---------------------------------------------------------------------------
# Module-local helpers: build a minimal Overture places + tile_assignments DB
# ---------------------------------------------------------------------------

_OVERTURE_EXPORT_PLACES = [
    # (id, name, lat, lon, importance, country)
    ("exp001", "Blue Bottle Coffee",  37.7749, -122.4194, 72, "US"),
    ("exp002", "Golden Gate Park",    37.7694, -122.4862, 85, "US"),
    ("exp003", "Tartine Bakery",      37.7617, -122.4243, 68, "US"),
    # place with null country — should produce no address location
    ("exp004", "Mystery Spot",        37.7800, -122.4300, 40, None),
]

# 6-char zoom-6 quadkey prefix — all fixture places are assigned to this single tile
_EXPORT_TILE_QK = "023130"


def _make_overture_export_db(conn, places_rows=None, assignments=None):
    """Populate `conn` with minimal `places` and `tile_assignments` tables
    matching the schema garganorn/sql/overture_place_export_tiles.sql expects.

    `places_rows` defaults to _OVERTURE_EXPORT_PLACES if None.
    Each entry is (id, name, lat, lon, importance, country).

    `assignments`, if given, is a list of (place_id, tile_qk) pairs that
    populates tile_assignments directly, overriding the default of every
    places_row's place_id assigned to _EXPORT_TILE_QK.
    """
    if places_rows is None:
        places_rows = _OVERTURE_EXPORT_PLACES

    conn.execute("""
        CREATE TABLE places (
            id          VARCHAR PRIMARY KEY,
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
            variants    STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT []
        )
    """)

    for place_id, name, lat, lon, imp, country in places_rows:
        addr_sql = (
            f"[{{'country': '{country}', 'postcode': NULL, 'locality': NULL, "
            "'freeform': NULL, 'region': NULL}]"
            if country is not None else "NULL"
        )
        conn.execute(f"""
            INSERT INTO places VALUES (
                '{place_id}',
                {{'xmin': {lon}-0.001, 'ymin': {lat}-0.001, 'xmax': {lon}+0.001, 'ymax': {lat}+0.001}},
                {{'primary': '{name}'}},
                {{'primary': 'coffee_shop'}},
                {addr_sql},
                NULL, NULL, NULL, NULL,
                NULL,
                0.9, 1, NULL,
                {imp},
                []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
            )
        """)

    conn.execute("""
        CREATE TABLE tile_assignments (
            place_id VARCHAR,
            tile_qk  VARCHAR
        )
    """)
    if assignments is None:
        assignments = [(place_id, _EXPORT_TILE_QK) for place_id, *_ in places_rows]
    for place_id, tile_qk in assignments:
        conn.execute(
            "INSERT INTO tile_assignments VALUES (?, ?)",
            [place_id, tile_qk],
        )

    conn.execute("""
        CREATE TABLE place_containment (
            place_id       VARCHAR,
            relations_json VARCHAR,
            tile_qk        VARCHAR
        )
    """)


# ---------------------------------------------------------------------------
# Tests: overture_place_export_tiles.sql
# ---------------------------------------------------------------------------

class TestOvertureExportTiles:
    """Tests for garganorn/sql/overture_place_export_tiles.sql.

    Each test runs the full Overture pipeline:
      overture_import → overture_importance → overture_variants →
      compute_tile_assignments → overture_export_tiles
    """

    _SUBS = {"repo": "places.atgeo.org"}

    def _run_full_pipeline(self, conn, parquet_glob, density_parquet):
        """Run all Overture pipeline SQL stages on conn."""
        # 1. Import (includes importance and variants computation)
        run_overture_import(conn, parquet_glob)

        # 2. Tile assignments (pk_expr='id' for Overture)
        run_tile_assignments(conn, pk_expr="id", min_zoom=6, max_zoom=17, max_per_tile=5000)

        # 4b. Empty place_containment (no boundaries in pipeline tests)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS place_containment (
                place_id       VARCHAR,
                relations_json VARCHAR,
                tile_qk        VARCHAR
            )
        """)

        # 5. Export tiles
        raw = _load_sql("overture_place_export_tiles.sql", self._SUBS)
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw)))

    def _get_record(self, conn, place_id):
        """Return parsed JSON record dict for a given place_id, or None if not found.

        Fetches all rows from tile_export, parses each record_json, and returns
        the first record whose rkey matches place_id.
        """
        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            if parsed.get("rkey") == place_id:
                return parsed
        return None

    def test_overture_export_addresses_inline(self, overture_parquet, density_parquet, tmp_path):
        """ov001 (one address entry with country='US', region='US-CA') must have an
        address location entry with country='US' and region='CA' (trimmed at '-').

        locations must contain: [geo_entry, address_entry].
        """
        db_path = tmp_path / "test_ov_export_addr.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        record = self._get_record(conn, "ov001")
        conn.close()
        assert record is not None, "ov001 must appear in tile_export"
        locations = record["locations"]
        addr_entries = [loc for loc in locations if loc.get("$type") == "community.lexicon.location.address"]
        assert len(addr_entries) == 1, (
            f"ov001 must have exactly 1 address location; got {len(addr_entries)}: {addr_entries}"
        )
        addr = addr_entries[0]
        assert addr["country"] == "US", f"Expected country='US'; got {addr['country']!r}"
        assert addr["region"] == "CA", (
            f"Expected region='CA' (trimmed from 'US-CA'); got {addr['region']!r}"
        )

    def test_overture_export_no_addresses_no_error(self, overture_parquet, density_parquet, tmp_path):
        """ov003 (addresses=NULL) must render without error with exactly 1 location (geo only)."""
        db_path = tmp_path / "test_ov_export_no_addr.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        record = self._get_record(conn, "ov003")
        conn.close()
        assert record is not None, "ov003 must appear in tile_export"
        locations = record["locations"]
        assert len(locations) == 1, (
            f"ov003 (null addresses) must have exactly 1 location (geo only); got {len(locations)}: {locations}"
        )
        assert locations[0]["$type"] == "community.lexicon.location.geo", (
            f"Only location must be geo type; got {locations[0]['$type']!r}"
        )

    def test_overture_export_all_null_country_addresses(self, overture_parquet, density_parquet, tmp_path):
        """ov008 (addresses=[{country:NULL,...}]) must render with exactly 1 location (geo only).

        list_filter must remove all entries with NULL country, yielding an empty address list.
        """
        db_path = tmp_path / "test_ov_export_null_country.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        record = self._get_record(conn, "ov008")
        conn.close()
        assert record is not None, "ov008 must appear in tile_export"
        locations = record["locations"]
        assert len(locations) == 1, (
            f"ov008 (all null-country addresses) must have exactly 1 location (geo only); "
            f"got {len(locations)}: {locations}"
        )
        assert locations[0]["$type"] == "community.lexicon.location.geo", (
            f"Only location must be geo type; got {locations[0]['$type']!r}"
        )

    def test_no_null_fields_in_locations(self, overture_parquet, density_parquet, tmp_path):
        """No location in any record may contain a key with a None/null value."""
        db_path = tmp_path / "test_ov_no_null_loc.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()
        assert rows, "No rows returned from tile_export"
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            locations = parsed["locations"]
            for loc in locations:
                null_keys = [k for k, v in loc.items() if v is None]
                assert not null_keys, (
                    f"Location {loc.get('$type')!r} in record {parsed.get('rkey')!r} "
                    f"has null values for keys: {null_keys}. "
                    "Locations must contain only fields belonging to their type."
                )

    def test_overture_export_mixed_null_country_addresses(self, overture_parquet, density_parquet, tmp_path):
        """ov009 (one null-country entry + one non-null-country entry) must render with
        exactly 1 address location — the non-null-country entry only.

        This validates list_filter drops null-country entries without dropping valid ones.
        """
        db_path = tmp_path / "test_ov_export_mixed.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        record = self._get_record(conn, "ov009")
        conn.close()
        assert record is not None, "ov009 must appear in tile_export"
        locations = record["locations"]
        addr_entries = [loc for loc in locations if loc.get("$type") == "community.lexicon.location.address"]
        assert len(addr_entries) == 1, (
            f"ov009 (one null, one non-null country) must have exactly 1 address location; "
            f"got {len(addr_entries)}: {addr_entries}"
        )
        assert addr_entries[0]["country"] == "US", (
            f"Surviving address entry must have country='US'; got {addr_entries[0]['country']!r}"
        )
        assert addr_entries[0]["region"] == "CA", (
            f"Expected region='CA' (trimmed); got {addr_entries[0]['region']!r}"
        )

    def test_overture_export_uses_bbox_mean_not_centroid(self):
        """overture_place_export_tiles.sql must compute lat/lon from bbox mean, not st_centroid."""
        import pathlib
        sql_path = pathlib.Path(__file__).parent.parent / "garganorn" / "sql" / "overture_place_export_tiles.sql"
        sql = sql_path.read_text()
        assert "st_centroid" not in sql.lower(), (
            "overture_place_export_tiles.sql must not use st_centroid; "
            "use bbox mean ((bbox.ymin + bbox.ymax) / 2) instead"
        )
        assert "p.bbox.ymin" in sql, (
            "overture_place_export_tiles.sql must use p.bbox.ymin for latitude computation"
        )
        assert "p.bbox.xmin" in sql, (
            "overture_place_export_tiles.sql must use p.bbox.xmin for longitude computation"
        )
        assert "p.bbox.ymax" in sql, (
            "overture_place_export_tiles.sql must use p.bbox.ymax for latitude computation"
        )
        assert "p.bbox.xmax" in sql, (
            "overture_place_export_tiles.sql must use p.bbox.xmax for longitude computation"
        )

    def test_overture_export_latlon_matches_bbox_mean(self, overture_parquet, density_parquet, tmp_path):
        """Exported latitude/longitude must equal bbox center coordinates."""
        db_path = tmp_path / "test_ov_export_latlon.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        self._run_full_pipeline(conn, overture_parquet, density_parquet)
        record = self._get_record(conn, "ov001")
        conn.close()

        assert record is not None, "ov001 must appear in tile_export"
        locations = record["locations"]
        geo_entries = [loc for loc in locations if loc.get("$type") == "community.lexicon.location.geo"]
        assert len(geo_entries) >= 1, "ov001 must have at least one geo location"
        geo = geo_entries[0]

        # ov001 bbox: xmin=-122.420, ymin=37.774, xmax=-122.418, ymax=37.776
        expected_lat = (37.774 + 37.776) / 2   # = 37.775
        expected_lon = (-122.420 + -122.418) / 2  # = -122.419

        actual_lat = float(geo["latitude"])
        actual_lon = float(geo["longitude"])

        assert abs(actual_lat - expected_lat) < 1e-6, (
            f"latitude must match bbox mean {expected_lat}; got {actual_lat}"
        )
        assert abs(actual_lon - expected_lon) < 1e-6, (
            f"longitude must match bbox mean {expected_lon}; got {actual_lon}"
        )


# ---------------------------------------------------------------------------
# variant.language and attribute-struct null-vs-absent export fields
#
# org.atgeo.place's lexicon marks variant.language optional but not
# nullable: exported JSON must omit the key when the source value is NULL,
# not emit `"language": null`, which lexrpc's validator rejects. See
# docs/design-null-vs-absent-export-fields.md.
# ---------------------------------------------------------------------------

class TestVariantAndAttributeNullVsAbsent:
    """variant.language (all three sources) and overture_place's attributes
    struct must omit null-valued keys from exported JSON rather than emit
    them as JSON null."""

    def _overture_place_record(self, conn, variants_sql, brand_sql="NULL"):
        """Build a single-row overture_place places/tile_assignments/
        place_containment db (schema matches _make_overture_export_db
        above) and return the parsed record_json for that row."""
        conn.execute("""
            CREATE TABLE places (
                id          VARCHAR PRIMARY KEY,
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
                variants    STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT []
            )
        """)
        conn.execute(f"""
            INSERT INTO places VALUES (
                'op001',
                {{'xmin': -122.420, 'ymin': 37.774, 'xmax': -122.418, 'ymax': 37.776}},
                {{'primary': 'Test Place'}},
                {{'primary': 'coffee_shop'}},
                NULL, NULL, NULL, NULL, NULL,
                {brand_sql},
                0.9, 1, NULL,
                50,
                {variants_sql}
            )
        """)
        conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        conn.execute("INSERT INTO tile_assignments VALUES ('op001', '023130')")
        conn.execute("""
            CREATE TABLE place_containment (
                place_id VARCHAR, relations_json VARCHAR, tile_qk VARCHAR
            )
        """)
        raw_sql = _load_sql("overture_place_export_tiles.sql", {"repo": "https://example.com"})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw_sql)))
        (record_json,) = conn.execute("SELECT record_json FROM tile_export").fetchone()
        return json.loads(record_json)

    def test_overture_place_variant_without_language_omits_key(self):
        """A variant whose source language is NULL must have no 'language'
        key in the exported JSON object -- not `"language": null`."""
        conn = duckdb.connect()
        record = self._overture_place_record(
            conn,
            "[{'name': 'Alt Name', 'type': 'alternate', 'language': NULL}]"
            "::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]",
        )
        conn.close()
        variant = record["variants"][0]
        assert "language" not in variant, (
            f"variant.language must be omitted when the source value is "
            f"NULL, not emitted as JSON null; got variants={record['variants']}"
        )
        assert variant["name"] == "Alt Name"

    def test_overture_place_variant_with_language_present(self):
        """A variant WITH a language must still carry it after
        null-stripping lands -- regression guard against over-stripping."""
        conn = duckdb.connect()
        record = self._overture_place_record(
            conn,
            "[{'name': 'Nom Alternatif', 'type': 'alternate', 'language': 'fr'}]"
            "::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]",
        )
        conn.close()
        variant = record["variants"][0]
        assert variant.get("language") == "fr", (
            f"a variant WITH a language must still carry it (present, with "
            f"value) after null-stripping lands; got {variant}"
        )

    def test_overture_place_attribute_with_null_value_omits_key(self):
        """A place with brand IS NULL must omit the 'brand' key from its
        exported attributes object (R2), matching overture_division's
        existing attributes null-stripping."""
        conn = duckdb.connect()
        record = self._overture_place_record(
            conn,
            "[]::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]",
            brand_sql="NULL",
        )
        conn.close()
        attrs = record["attributes"]
        assert "brand" not in attrs, (
            f"attributes.brand must be omitted when NULL, not emitted as "
            f"JSON null; got {attrs}"
        )

    def test_overture_division_variant_without_language_omits_key(self):
        """Same requirement as overture_place, for overture_division's
        export SQL."""
        conn = duckdb.connect()
        conn.execute("""
            CREATE TABLE places (
                id            VARCHAR PRIMARY KEY,
                names         STRUCT("primary" VARCHAR),
                importance    INTEGER,
                min_latitude  DOUBLE, max_latitude  DOUBLE,
                min_longitude DOUBLE, max_longitude DOUBLE,
                variants      STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT [],
                subtype       VARCHAR, country VARCHAR, region VARCHAR,
                level         VARCHAR, wikidata VARCHAR, population BIGINT
            )
        """)
        conn.execute("""
            INSERT INTO places VALUES (
                'div001', {'primary': 'Test Division'}, 50, 37.60, 37.85,
                -122.55, -122.30,
                [{'name': 'Alt Div Name', 'type': 'alternate', 'language': NULL}]
                ::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
                'region', 'US', NULL, 'region', NULL, NULL
            )
        """)
        conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        conn.execute("INSERT INTO tile_assignments VALUES ('div001', '023130')")
        conn.execute("""
            CREATE TABLE place_containment (
                place_id VARCHAR, relations_json VARCHAR, tile_qk VARCHAR
            )
        """)
        raw_sql = _load_sql("overture_division_export_tiles.sql", {"repo": "https://example.com"})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw_sql)))
        (record_json,) = conn.execute("SELECT record_json FROM tile_export").fetchone()
        conn.close()
        variant = json.loads(record_json)["variants"][0]
        assert "language" not in variant, (
            f"overture_division variant.language must be omitted when "
            f"NULL, not emitted as JSON null; got {variant}"
        )

    def test_osm_variant_without_language_omits_key(self):
        """Same requirement as overture_place, for osm's export SQL. OSM's
        osm_variant_type_lang macro sets language NULL for 6 of its
        branches (alt_name, int_name, official_name, short_name, loc_name,
        old_name) -- the common case, not an edge case."""
        conn = duckdb.connect()
        conn.execute("""
            CREATE TABLE places (
                rkey             VARCHAR PRIMARY KEY,
                name             VARCHAR,
                importance       INTEGER,
                latitude         DOUBLE,
                longitude        DOUBLE,
                variants         STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT [],
                primary_category VARCHAR,
                tags             MAP(VARCHAR, VARCHAR)
            )
        """)
        conn.execute("""
            INSERT INTO places VALUES (
                'osm001', 'Test OSM Place', 50, 37.7749, -122.4194,
                [{'name': 'Alt OSM Name', 'type': 'alternate', 'language': NULL}]
                ::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[],
                NULL, map([]::VARCHAR[], []::VARCHAR[])
            )
        """)
        conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        conn.execute("INSERT INTO tile_assignments VALUES ('osm001', '023130')")
        conn.execute("""
            CREATE TABLE place_containment (
                place_id VARCHAR, relations_json VARCHAR, tile_qk VARCHAR
            )
        """)
        raw_sql = _load_sql("osm_export_tiles.sql", {})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw_sql)))
        (record_json,) = conn.execute("SELECT record_json FROM tile_export").fetchone()
        conn.close()
        variant = json.loads(record_json)["variants"][0]
        assert "language" not in variant, (
            f"osm variant.language must be omitted when NULL, not emitted "
            f"as JSON null; got {variant}"
        )


# ---------------------------------------------------------------------------
# Division containment relations JSON for test fixtures
# ---------------------------------------------------------------------------

# The four division boundaries that contain SF places (lat ~37.77, lon ~-122.42),
# ordered by level ascending (continent first — matches ORDER BY level ASC
# over garganorn.levels.LEVEL_VOCAB).
_SF_WITHIN_JSON = json.dumps({
    "within": [
        {"rkey": "org.atgeo.places.overture.division:div_continent_na"},
        {"rkey": "org.atgeo.places.overture.division:div_country_us"},
        {"rkey": "org.atgeo.places.overture.division:div_region_ca"},
        {"rkey": "org.atgeo.places.overture.division:div_locality_sf"},
    ]
})


def _create_place_containment(conn, entries):
    """Create the place_containment table and insert given (place_id, tile_qk, relations_json) rows.

    `entries` is a list of (place_id, tile_qk, relations_json) tuples.
    Pass an empty list to create an empty table.
    """
    conn.execute("""
        CREATE OR REPLACE TABLE place_containment (
            place_id      VARCHAR,
            relations_json VARCHAR,
            tile_qk       VARCHAR
        )
    """)
    for place_id, tile_qk, relations_json in entries:
        conn.execute(
            "INSERT INTO place_containment VALUES (?, ?, ?)",
            [place_id, relations_json, tile_qk],
        )


# ---------------------------------------------------------------------------
# Tests: Division containment in tile export pipelines
# ---------------------------------------------------------------------------

class TestContainmentInExport:
    """Tests division containment in tile export output: the export SQL
    LEFT JOINs place_containment to populate relations.within, and
    compute_containment()/run_pipeline()/main() wire the boundaries DB
    through the pipeline.
    """

    _OV_SUBS = {"repo": "places.atgeo.org"}
    _OSM_SUBS = {"repo": "places.atgeo.org"}

    # ------------------------------------------------------------------
    # Overture export includes relations.within when containment present
    # ------------------------------------------------------------------

    def test_overture_relations_with_containment(self, overture_parquet, density_parquet, tmp_path):
        """Overture export must include relations.within when place_containment populated."""
        db_path = tmp_path / "ov_containment_with.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Run the full Overture pipeline to get places + tile_assignments
        run_overture_import(conn, overture_parquet)
        run_tile_assignments(conn, pk_expr="id", min_zoom=6, max_zoom=17, max_per_tile=5000)

        # Populate place_containment for ov001, keyed on its actual tile_qk
        # so the (place_id, tile_qk) join in the export SQL matches it.
        ov001_qk = conn.execute(
            "SELECT tile_qk FROM tile_assignments WHERE place_id = 'ov001'"
        ).fetchone()[0]
        _create_place_containment(conn, [("ov001", ov001_qk, _SF_WITHIN_JSON)])

        # Run export
        raw_sql = _load_sql("overture_place_export_tiles.sql", self._OV_SUBS)
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw_sql)))

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        record = None
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            if parsed.get("rkey") == "ov001":
                record = parsed
                break

        assert record is not None, "ov001 must appear in tile_export"
        relations = record.get("relations", {})
        assert "within" in relations, (
            f"overture relations must have 'within' when place_containment populated; "
            f"got relations={relations!r}"
        )
        within = relations["within"]
        assert isinstance(within, list), f"relations.within must be a list; got {type(within)}"
        assert len(within) == 4, (
            f"relations.within must have 4 entries; got {len(within)}: {within}"
        )
        for entry in within:
            assert "rkey" in entry, f"within entry missing 'rkey': {entry}"
            assert entry["rkey"].startswith("org.atgeo.places.overture.division:"), \
                f"rkey must have division prefix; got {entry['rkey']!r}"
            assert "name" not in entry, f"'name' key must not appear in rkey-only output: {entry}"
            assert "level" not in entry, f"'level' key must not appear in rkey-only output: {entry}"

    # ------------------------------------------------------------------
    # OSM export includes relations.within when containment present
    # ------------------------------------------------------------------

    def test_osm_relations_with_containment(self, osm_parquet, density_parquet, tmp_path):
        """OSM export must include relations.within when place_containment populated."""
        db_path = tmp_path / "osm_containment_with.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Run the full OSM pipeline (includes importance and variants computation)
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        run_tile_assignments(conn, pk_expr="rkey", min_zoom=6, max_zoom=17, max_per_tile=5000)

        # Get a valid rkey from the imported places to use for containment
        rkeys = conn.execute("SELECT rkey FROM places ORDER BY rkey LIMIT 1").fetchall()
        assert rkeys, "OSM import must produce at least one place"
        target_rkey = rkeys[0][0]

        target_qk = conn.execute(
            "SELECT tile_qk FROM tile_assignments WHERE place_id = ?", [target_rkey]
        ).fetchone()[0]
        _create_place_containment(conn, [(target_rkey, target_qk, _SF_WITHIN_JSON)])

        # Run export
        raw_sql = _load_sql("osm_export_tiles.sql", self._OSM_SUBS)
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw_sql)))

        rows = conn.execute("SELECT record_json FROM tile_export").fetchall()
        conn.close()

        record = None
        for (record_json,) in rows:
            parsed = json.loads(record_json)
            # OSM rkeys are rewritten in the SQL (e.g. 'n1001' → 'node:1001')
            # Check original rkey match by looking at the rkey field
            rkey = parsed.get("rkey", "")
            expected_rkey = (
                target_rkey.replace("n", "node:", 1)
                if target_rkey.startswith("n")
                else target_rkey.replace("w", "way:", 1)
                if target_rkey.startswith("w")
                else target_rkey
            )
            if rkey == expected_rkey:
                record = parsed
                break

        assert record is not None, (
            f"place with rkey={target_rkey!r} must appear in tile_export"
        )
        relations = record.get("relations", {})
        assert "within" in relations, (
            f"osm relations must have 'within' when place_containment populated; "
            f"got relations={relations!r}"
        )
        within = relations["within"]
        assert isinstance(within, list), f"relations.within must be a list; got {type(within)}"
        assert len(within) == 4, (
            f"relations.within must have 4 entries; got {len(within)}: {within}"
        )
        for entry in within:
            assert "rkey" in entry, f"within entry missing 'rkey': {entry}"
            assert entry["rkey"].startswith("org.atgeo.places.overture.division:"), \
                f"rkey must have division prefix; got {entry['rkey']!r}"
            assert "name" not in entry, f"'name' key must not appear in rkey-only output: {entry}"
            assert "level" not in entry, f"'level' key must not appear in rkey-only output: {entry}"

    # ------------------------------------------------------------------
    # compute_containment import
    # ------------------------------------------------------------------

    def test_compute_containment_function_exists(self):
        """compute_containment must be importable from garganorn.quadtree."""
        from garganorn.quadtree import compute_containment  # noqa: F401

    # ------------------------------------------------------------------
    # compute_containment produces place_containment table
    # ------------------------------------------------------------------

    def test_compute_containment_produces_table(self, tmp_path, division_db_path):
        """compute_containment must create place_containment with correct columns and rows."""
        try:
            from garganorn.quadtree import compute_containment
        except (ImportError, AttributeError):
            pytest.fail(
                "compute_containment not importable from garganorn.quadtree; "
                "implement the function to make this test pass"
            )

        db_path = tmp_path / "containment_test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        # Build a minimal places table with SF coordinates
        conn.execute("""
            CREATE TABLE places (
                place_id VARCHAR,
                latitude     DOUBLE,
                longitude    DOUBLE,
                qk17         VARCHAR
            )
        """)
        conn.execute(
            "INSERT INTO places VALUES ('exp001', 37.7749, -122.4194, ST_QuadKey(-122.4194, 37.7749, 17))"
        )
        conn.execute(
            "INSERT INTO places VALUES ('exp002', 37.7694, -122.4862, ST_QuadKey(-122.4862, 37.7694, 17))"
        )

        # tile_assignments must already exist (no fallback in fixed code).
        conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        conn.execute("INSERT INTO tile_assignments SELECT place_id, left(qk17, 6) FROM places")

        # compute_containment takes parquet paths, not a connection
        import os
        places_pq = str(tmp_path / "places_produces.parquet")
        ta_pq = str(tmp_path / "ta_produces.parquet")
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
        conn.close()

        # Build covering explicitly: it's the orchestrator's responsibility,
        # not compute_containment's.
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering_produces")
        stage_covering(str(division_db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        from garganorn.stages import compute_containment as _stage_compute_containment
        containment_dir = str(tmp_path / "containment_produces")
        _stage_compute_containment(
            places_pq, ta_pq, str(division_db_path),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir, force=True,
        )

        # Read results from parquet output
        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert parquet_files, "compute_containment must create parquet files in containment_dir"

        check_con = duckdb.connect()
        # Verify schema
        col_names = {
            row[0]
            for row in check_con.execute(
                f"DESCRIBE SELECT * FROM read_parquet({parquet_files!r})"
            ).fetchall()
        }
        assert "place_id" in col_names, (
            f"containment parquet must have 'place_id' column; got columns: {col_names}"
        )
        assert "relations_json" in col_names, (
            f"containment parquet must have 'relations_json' column; got columns: {col_names}"
        )

        # Verify rows exist for SF places (they fall inside division boundaries)
        rows = check_con.execute(
            f"SELECT place_id, relations_json FROM read_parquet({parquet_files!r})"
        ).fetchall()
        check_con.close()

        assert len(rows) >= 1, (
            "compute_containment must produce at least one row for SF test places "
            f"that fall inside the division boundaries; got {len(rows)} rows"
        )
        for place_id, relations_json in rows:
            parsed = json.loads(relations_json)
            assert "within" in parsed, (
                f"relations_json for {place_id!r} must have 'within' key; got {parsed!r}"
            )
            within = parsed["within"]
            assert isinstance(within, list), f"within must be a list for {place_id}; got {type(within)}"
            assert len(within) >= 1, f"SF coordinates should be contained by at least 1 boundary"
            for entry in within:
                assert "rkey" in entry, f"within entry missing 'rkey': {entry}"
                assert entry["rkey"].startswith("org.atgeo.places.overture.division:"), \
                    f"rkey must be collection-qualified; got {entry['rkey']!r}"
                assert "name" in entry, f"'name' key missing from within entry: {entry}"
                assert "level" in entry, f"'level' key missing from within entry: {entry}"

    # ------------------------------------------------------------------
    # compute_containment matches every containing boundary
    # ------------------------------------------------------------------

    def test_compute_containment_matches_all_containing_boundaries(self, tmp_path, division_db_path):
        """compute_containment must match ALL boundaries that contain a place, not just some.

        The SF test point (37.7749, -122.4194) falls inside all four division boundaries
        defined in conftest.py::DIVISION_BOUNDARIES that cover North America:
          - div_continent_na (level 0): bbox [20,-130] to [55,-60]
          - div_country_us   (level 10): bbox [24,-125] to [50,-66]
          - div_region_ca    (level 25): bbox [34,-125] to [42,-118]
          - div_locality_sf  (level 50): bbox [37.6,-122.55] to [37.85,-122.3]

        div_borough_manhattan (level 50) does NOT contain the SF point,
        so exactly 4 entries are expected and no more. Any narrowing that drops a
        boundary whose extent does reach the point yields fewer than 4 and fails
        here.
        """
        try:
            from garganorn.quadtree import compute_containment
        except (ImportError, AttributeError):
            pytest.fail(
                "compute_containment not importable from garganorn.quadtree"
            )

        db_path = tmp_path / "containment_bbox_test.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")

        conn.execute("""
            CREATE TABLE places (
                place_id VARCHAR,
                latitude     DOUBLE,
                longitude    DOUBLE,
                qk17         VARCHAR
            )
        """)
        # SF city hall — inside North America, US, California, and San Francisco boundaries.
        conn.execute(
            "INSERT INTO places VALUES ('sf001', 37.7749, -122.4194, ST_QuadKey(-122.4194, 37.7749, 17))"
        )

        # tile_assignments must already exist (no fallback in fixed code).
        conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        conn.execute("INSERT INTO tile_assignments SELECT place_id, left(qk17, 6) FROM places")

        # compute_containment takes parquet paths, not a connection
        import os
        places_pq = str(tmp_path / "places_bbox.parquet")
        ta_pq = str(tmp_path / "ta_bbox.parquet")
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
        conn.close()

        # Build covering explicitly: it's the orchestrator's responsibility,
        # not compute_containment's.
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering_bbox")
        stage_covering(str(division_db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        from garganorn.stages import compute_containment as _stage_compute_containment
        containment_dir = str(tmp_path / "containment_bbox")
        _stage_compute_containment(
            places_pq, ta_pq, str(division_db_path),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir, force=True,
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert parquet_files, "compute_containment must produce parquet files with a valid boundaries_db"
        check_con = duckdb.connect()
        rows = check_con.execute(
            f"SELECT place_id, relations_json FROM read_parquet({parquet_files!r}) "
            f"WHERE place_id = 'sf001'"
        ).fetchall()
        check_con.close()

        assert len(rows) == 1, (
            f"Expected exactly 1 place_containment row for 'sf001', got {len(rows)}"
        )

        parsed = json.loads(rows[0][1])
        within = parsed["within"]

        # The SF point falls inside exactly 4 of the 5 division test boundaries.
        # If a bbox pre-filter incorrectly excludes any of these 4 boundaries,
        # this assertion will catch it.
        expected_rkeys = {
            "org.atgeo.places.overture.division:div_continent_na",
            "org.atgeo.places.overture.division:div_country_us",
            "org.atgeo.places.overture.division:div_region_ca",
            "org.atgeo.places.overture.division:div_locality_sf",
        }
        actual_rkeys = {entry["rkey"] for entry in within}
        assert actual_rkeys == expected_rkeys, (
            f"compute_containment produced wrong set of containing boundaries.\n"
            f"  Expected: {sorted(expected_rkeys)}\n"
            f"  Got:      {sorted(actual_rkeys)}\n"
            f"A missing boundary indicates the bbox pre-filter incorrectly excluded it "
            f"(false negative). An extra boundary indicates incorrect inclusion."
        )

    # ------------------------------------------------------------------
    # run_pipeline accepts boundaries_db keyword argument
    # ------------------------------------------------------------------

    def test_run_pipeline_accepts_boundaries_db(self):
        """run_pipeline must accept a boundaries_db keyword argument."""
        from garganorn.quadtree import run_pipeline
        sig = inspect.signature(run_pipeline)
        assert "boundaries_db" in sig.parameters, (
            f"run_pipeline must have a 'boundaries_db' parameter; "
            f"current parameters: {list(sig.parameters)}"
        )

    # ------------------------------------------------------------------
    # main() accepts --boundaries CLI argument
    # ------------------------------------------------------------------

    def test_main_accepts_boundaries_arg(self):
        """main() argparse must accept a --boundaries CLI argument."""
        import argparse
        import sys
        from unittest.mock import patch

        # Parse a minimal valid invocation that includes --boundaries.
        # If --boundaries is not defined, argparse will raise SystemExit(2).
        test_args = [
            "run",
            "--source", "overture_place",
            "--parquet", "/tmp/test.parquet",
            "--output", "/tmp/output",
            "--boundaries", "/tmp/boundaries.duckdb",
        ]
        with patch.object(sys, "argv", ["quadtree"] + test_args):
            try:
                # Re-parse using a fresh parser by importing and calling main's
                # internal parser logic. We do this by inspecting the source
                # rather than calling main() (which would trigger run_pipeline).
                # Instead, verify that argparse accepts --boundaries by constructing
                # the same parser that main() uses, which must include the argument.
                from garganorn import quadtree as _qt
                # Build the parser the same way main() does, then parse our args.
                # Since we can't easily extract the parser, we verify by calling
                # parse_known_args: if --boundaries is unrecognized it lands in extras.
                import argparse as _ap
                test_parser = _ap.ArgumentParser()
                # Minimal args that main() would define; add --boundaries.
                # The real test: does the actual main() parser accept it?
                # We simulate by running the whole argparse block.
                # Easiest approach: mock run_pipeline and call main() directly.
                with patch.object(_qt, "run_pipeline", return_value=None):
                    # Suppress SystemExit if --boundaries triggers an error
                    try:
                        _qt.main()
                    except SystemExit as exc:
                        # SystemExit(0) = success (e.g. --help); others = failure
                        # SystemExit(2) = argument parsing error (unrecognized --boundaries)
                        if exc.code == 2:
                            pytest.fail(
                                "main() argparse does not accept --boundaries; "
                                "add parser.add_argument('--boundaries', ...) to main()"
                            )
            except Exception:
                raise  # Don't swallow unexpected exceptions

    # ------------------------------------------------------------------
    # compute_containment with None boundaries_db creates empty table
    # ------------------------------------------------------------------

    def test_compute_containment_none_boundaries(self, tmp_path):
        """compute_containment(conn, None, ...) must create an empty place_containment table.

        When no boundaries DB is provided, the function should still create
        the table (so downstream SQL can LEFT JOIN it) but insert no rows.
        """
        from garganorn.quadtree import compute_containment

        db_path = tmp_path / "containment_none.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE places (
                place_id VARCHAR,
                latitude     DOUBLE,
                longitude    DOUBLE,
                qk17         VARCHAR
            )
        """)
        conn.execute(
            "INSERT INTO places VALUES ('p1', 37.7749, -122.4194, "
            "ST_QuadKey(-122.4194, 37.7749, 17))"
        )

        import os
        places_pq = str(tmp_path / "places_none.parquet")
        ta_pq = str(tmp_path / "ta_none.parquet")
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(
            f"COPY (SELECT place_id, left(qk17, 6) AS tile_qk FROM places) "
            f"TO '{ta_pq}' (FORMAT PARQUET)"
        )
        conn.close()

        from garganorn.stages import compute_containment as _stage_compute_containment
        containment_dir = str(tmp_path / "containment_none")
        _stage_compute_containment(
            places_pq, ta_pq, None, "place_id", "longitude", "latitude",
            containment_dir, force=True,
        )

        meta_path = os.path.join(containment_dir, "_meta.json")
        assert os.path.exists(meta_path), (
            "containment _meta.json must exist even with None boundaries_db"
        )
        with open(meta_path) as f:
            meta = json.load(f)
        assert meta.get("empty") is True, (
            f"_meta.json must have empty=True when boundaries_db is None; got {meta}"
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) == 0, (
            f"place_containment must have no parquet files when boundaries_db is None; "
            f"got {parquet_files}"
        )

    # ------------------------------------------------------------------
    # Place outside all boundaries produces no containment row
    # ------------------------------------------------------------------

    def test_compute_containment_place_outside_all_boundaries(self, tmp_path, division_db_path):
        """A place at (0, 0) in the ocean should produce no place_containment row.

        All test division boundaries cover parts of North America. A point in the
        Gulf of Guinea should not be contained by any of them. This validates
        that compute_containment does not produce spurious containment rows
        for places outside all boundaries.
        """
        from garganorn.quadtree import compute_containment

        db_path = tmp_path / "containment_ocean.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE places (
                place_id VARCHAR,
                latitude     DOUBLE,
                longitude    DOUBLE,
                qk17         VARCHAR
            )
        """)
        # Point at (0, 0) — Gulf of Guinea, outside all test boundaries
        conn.execute(
            "INSERT INTO places VALUES ('ocean001', 0.0, 0.0, "
            "ST_QuadKey(0.0, 0.0, 17))"
        )

        # tile_assignments must already exist (no fallback in fixed code).
        conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        conn.execute("INSERT INTO tile_assignments SELECT place_id, left(qk17, 6) FROM places")

        # compute_containment takes parquet paths, not a connection
        import os
        places_pq = str(tmp_path / "places_ocean.parquet")
        ta_pq = str(tmp_path / "ta_ocean.parquet")
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
        conn.close()

        # Build covering explicitly: it's the orchestrator's responsibility,
        # not compute_containment's.
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering_ocean")
        stage_covering(str(division_db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        from garganorn.stages import compute_containment as _stage_compute_containment
        containment_dir = str(tmp_path / "containment_ocean")
        _stage_compute_containment(
            places_pq, ta_pq, str(division_db_path),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir, force=True,
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        if parquet_files:
            check_con = duckdb.connect()
            rows = check_con.execute(
                f"SELECT place_id FROM read_parquet({parquet_files!r}) "
                f"WHERE place_id = 'ocean001'"
            ).fetchall()
            check_con.close()
        else:
            rows = []

        assert len(rows) == 0, (
            f"Place at (0, 0) should not be contained by any boundary; "
            f"got {len(rows)} containment rows"
        )

    # ------------------------------------------------------------------
    # Place near tile edge with boundary straddling the edge
    # ------------------------------------------------------------------

    def test_compute_containment_place_near_tile_edge(self, tmp_path, division_db_path):
        """A place near the edge of the SF city boundary must still be correctly
        contained when it falls inside the boundary polygon.

        The SF boundary (div_locality_sf) is POLYGON((-122.55 37.6, -122.55 37.85,
        -122.3 37.85, -122.3 37.6, -122.55 37.6)). We test:
          - A point just inside the SW corner: (37.61, -122.54) — should be in SF
          - A point just outside the SW corner: (37.59, -122.56) — should NOT be in SF
            but should still be in NA, US, CA (the z6 tile is the same: 023010)

        Both points are in the same z6 tile, so containment must be resolved
        by testing the point against the boundary polygon itself, not the tile.
        """
        from garganorn.quadtree import compute_containment

        db_path = tmp_path / "containment_edge.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE places (
                place_id VARCHAR,
                latitude     DOUBLE,
                longitude    DOUBLE,
                qk17         VARCHAR
            )
        """)
        # Just inside SF boundary SW corner
        conn.execute(
            "INSERT INTO places VALUES ('edge_in', 37.61, -122.54, "
            "ST_QuadKey(-122.54, 37.61, 17))"
        )
        # Just outside SF boundary SW corner (but still in CA)
        conn.execute(
            "INSERT INTO places VALUES ('edge_out', 37.59, -122.56, "
            "ST_QuadKey(-122.56, 37.59, 17))"
        )

        # tile_assignments must already exist (no fallback in fixed code).
        conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        conn.execute("INSERT INTO tile_assignments SELECT place_id, left(qk17, 6) FROM places")

        # compute_containment takes parquet paths, not a connection
        import os
        places_pq = str(tmp_path / "places_edge.parquet")
        ta_pq = str(tmp_path / "ta_edge.parquet")
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
        conn.close()

        # Build covering explicitly: it's the orchestrator's responsibility,
        # not compute_containment's.
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering_edge")
        stage_covering(str(division_db_path), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        from garganorn.stages import compute_containment as _stage_compute_containment
        containment_dir = str(tmp_path / "containment_edge")
        _stage_compute_containment(
            places_pq, ta_pq, str(division_db_path),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir, force=True,
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert parquet_files, "compute_containment must produce parquet files for edge test"

        check_con = duckdb.connect()
        # edge_in: should be in NA, US, CA, SF (4 boundaries)
        in_rows = check_con.execute(
            f"SELECT relations_json FROM read_parquet({parquet_files!r}) "
            f"WHERE place_id = 'edge_in'"
        ).fetchall()
        assert len(in_rows) == 1, f"Expected 1 row for edge_in; got {len(in_rows)}"
        in_within = json.loads(in_rows[0][0])["within"]
        in_rkeys = {e["rkey"] for e in in_within}
        expected_in_rkeys = {
            "org.atgeo.places.overture.division:div_continent_na",
            "org.atgeo.places.overture.division:div_country_us",
            "org.atgeo.places.overture.division:div_region_ca",
            "org.atgeo.places.overture.division:div_locality_sf",
        }
        assert in_rkeys == expected_in_rkeys, (
            f"edge_in should be in 4 boundaries; got {sorted(in_rkeys)}"
        )

        # edge_out: should be in NA, US, CA only (3 boundaries, not SF)
        out_rows = check_con.execute(
            f"SELECT relations_json FROM read_parquet({parquet_files!r}) "
            f"WHERE place_id = 'edge_out'"
        ).fetchall()
        check_con.close()
        assert len(out_rows) == 1, f"Expected 1 row for edge_out; got {len(out_rows)}"
        out_within = json.loads(out_rows[0][0])["within"]
        out_rkeys = {e["rkey"] for e in out_within}
        expected_out_rkeys = {
            "org.atgeo.places.overture.division:div_continent_na",
            "org.atgeo.places.overture.division:div_country_us",
            "org.atgeo.places.overture.division:div_region_ca",
        }
        assert out_rkeys == expected_out_rkeys, (
            f"edge_out should be in 3 boundaries (not SF); got {sorted(out_rkeys)}"
        )


# ---------------------------------------------------------------------------
# Record JSON byte-identical per source
# ---------------------------------------------------------------------------

class TestExportRecordParityPhase2:
    """stage_export takes places/tile_assignments parquet paths, not a
    connection, and two forced exports from the same artifacts produce
    gunzip-byte-identical tile files (pins the ORDER BY tile_qk, place_id
    invariant).
    """

    def test_stage_export_has_parquet_signature(self):
        """stage_export must accept (source, places_parquet, tile_assignments_parquet,
        containment_dir, tiles_root, ...)."""
        import inspect
        from garganorn.stages import stage_export
        sig = inspect.signature(stage_export)
        params = list(sig.parameters.keys())
        assert params[0] != "con", (
            f"stage_export's first param must not be 'con' — it takes a "
            f"parquet path; got {params[0]!r}"
        )
        assert "places_parquet" in params, (
            f"stage_export missing 'places_parquet' param; got {params}"
        )

    def test_two_forced_overture_exports_byte_identical(
        self, overture_parquet, density_parquet, tmp_path, monkeypatch
    ):
        """Two forced exports from the same overture artifacts must produce
        gunzip-byte-identical tile files (determinism: ORDER BY tile_qk, place_id).

        Two details make this assertion mean what it says:

        The clock is pinned. stage_export stamps a wall-clock generated_at at
        second granularity into every tile envelope (stages.py:1469-1471), so
        two runs straddling a clock tick differ by one digit inside the
        timestamp -- a difference that has nothing to do with export
        determinism. Unpinned, this test passes only when both pipeline runs
        happen to land in the same second, which is a coin flip on a loaded
        machine, not an assertion.

        The runs write to separate roots. The run-dir name derives from that
        same pinned instant, so sharing one root would make the second run
        overwrite the first in place and the comparison would compare each
        file to itself -- passing no matter what export did.
        """
        import gzip as _gzip
        from datetime import datetime as _datetime, timezone as _timezone
        import garganorn.stages as _stages
        from garganorn.quadtree import run_pipeline as _run_pipeline

        fixed = _datetime(2026, 1, 2, 3, 4, 5, tzinfo=_timezone.utc)

        class _FixedClock(_datetime):
            @classmethod
            def now(cls, tz=None):
                return fixed

        monkeypatch.setattr(_stages, "datetime", _FixedClock)

        def _run(root):
            root.mkdir()
            _run_pipeline(
                "overture_place", overture_parquet,
                (-122.55, 37.60, -122.30, 37.85),
                str(root), memory_limit="4GB", max_per_tile=100,
                density_parquet=density_parquet, force=True,
            )
            tiles_current = root / "overture_place" / "tiles" / "current"
            assert tiles_current.exists(), (
                f"tiles must be at {tiles_current}"
            )
            tiles = {}
            for p in tiles_current.rglob("*.json.gz"):
                with _gzip.open(p) as f:
                    tiles[str(p.relative_to(tiles_current))] = f.read()
            return tiles

        first_run = _run(tmp_path / "det_out_a")
        second_run = _run(tmp_path / "det_out_b")

        assert first_run, "first export produced no tiles; nothing was compared"
        assert first_run.keys() == second_run.keys(), (
            f"the two exports produced different tile sets: "
            f"only in first={sorted(first_run.keys() - second_run.keys())}, "
            f"only in second={sorted(second_run.keys() - first_run.keys())}"
        )
        for rel, first_bytes in first_run.items():
            assert second_run[rel] == first_bytes, (
                f"Tile {rel}: second forced export differs from first — "
                "ORDER BY tile_qk, place_id must make export deterministic"
            )


# ---------------------------------------------------------------------------
# Summary-band and regular-band copies of the same record are
# byte-identical, because both are written by the same export run over a
# single (unioned) assignments artifact -- not by two separate exports.
# ---------------------------------------------------------------------------

class TestSummaryByteIdenticalCopies:
    def test_regular_and_summary_copies_are_byte_identical(
        self, overture_parquet, density_parquet, tmp_path, monkeypatch
    ):
        """Run the full overture_place pipeline; at N=10,000 every fixture
        place lands in both the regular and summary band. Currently no
        summary band is produced at all, so this fails on the "no
        summary-band tile" assertion below -- the missing-feature failure.
        """
        import gzip as _gzip
        from datetime import datetime as _datetime, timezone as _timezone
        import garganorn.stages as _stages
        from garganorn.quadtree import run_pipeline as _run_pipeline

        fixed = _datetime(2026, 1, 2, 3, 4, 5, tzinfo=_timezone.utc)

        class _FixedClock(_datetime):
            @classmethod
            def now(cls, tz=None):
                return fixed

        monkeypatch.setattr(_stages, "datetime", _FixedClock)

        root = tmp_path / "summary_out"
        root.mkdir()
        _run_pipeline(
            "overture_place", overture_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            str(root), memory_limit="4GB", max_per_tile=100,
            density_parquet=density_parquet, force=True,
        )
        tiles_current = root / "overture_place" / "tiles" / "current"
        assert tiles_current.exists(), f"tiles must be at {tiles_current}"

        regular_records = {}
        summary_records = {}
        for gz_path in tiles_current.rglob("*.json.gz"):
            band_dir = gz_path.parent.name
            with _gzip.open(gz_path) as f:
                tile = json.loads(f.read())
            for rec in tile["records"]:
                rkey = rec["value"]["rkey"]
                target = summary_records if len(band_dir) < 6 else regular_records
                target.setdefault(rkey, []).append(rec)

        assert summary_records, (
            "no summary-band (<6-char directory) tile was produced -- the "
            "z1-z5 summary band is not being generated"
        )

        shared = set(regular_records) & set(summary_records)
        assert shared, (
            "no record appears in both the regular and summary band; "
            "at N=10,000 every fixture place should land in both"
        )
        rkey = sorted(shared)[0]
        reg_recs = regular_records[rkey]
        sum_recs = summary_records[rkey]
        assert len(reg_recs) == 1 and len(sum_recs) == 1, (
            f"{rkey!r} must appear exactly once per band; "
            f"got regular={len(reg_recs)}, summary={len(sum_recs)}"
        )
        assert reg_recs[0] == sum_recs[0], (
            f"regular and summary copies of {rkey!r} must be byte-identical; "
            f"regular={reg_recs[0]!r}\nsummary={sum_recs[0]!r}"
        )


# ---------------------------------------------------------------------------
# write_manifest_db from tile_assignments.parquet
# ---------------------------------------------------------------------------

class TestWriteManifestDbPhase2:
    """write_manifest_db reads tile_assignments from parquet, producing the
    same rkey/tile_qk rows and preserving the OSM rkey transform
    (n12345 → node:12345).
    """

    def test_write_manifest_db_accepts_parquet_path(self):
        """write_manifest_db must accept tile_assignments_parquet as first arg."""
        import inspect
        from garganorn.stages import write_manifest_db
        sig = inspect.signature(write_manifest_db)
        params = list(sig.parameters.keys())
        assert params[0] != "con", (
            f"write_manifest_db's first param must not be 'con' — it takes "
            f"a parquet path; got {params[0]!r}"
        )
        assert "tile_assignments_parquet" in params or "parquet" in params[0].lower(), (
            f"write_manifest_db must accept parquet path; got params: {params}"
        )

    def test_osm_rkey_transform_preserved_in_parquet_path(self, tmp_path):
        """OSM rkey transform (n12345→node:12345, w12345→way:12345) must be preserved
        when write_manifest_db reads from tile_assignments.parquet.
        """
        from garganorn.stages import write_manifest_db

        # Write a minimal tile_assignments.parquet with OSM-style place_ids
        ta_path = str(tmp_path / "osm_ta.parquet")
        con = duckdb.connect()
        con.execute(f"""
            COPY (
                SELECT place_id, tile_qk FROM (
                    VALUES ('n123456', '023130'), ('w789', '023131')
                ) t(place_id, tile_qk)
            ) TO '{ta_path}' (FORMAT PARQUET)
        """)
        con.close()

        run_dir = str(tmp_path / "run")
        import os as _os
        _os.makedirs(run_dir)

        # write_manifest_db(tile_assignments_parquet, output_dir, source)
        write_manifest_db(ta_path, run_dir, "osm")

        manifest_db = tmp_path / "run" / "manifest.duckdb"
        assert manifest_db.exists(), "manifest.duckdb must be written"

        # duckdb.connect() takes a file path, not SQL — attach separately.
        check = duckdb.connect()
        check.execute(f"ATTACH '{manifest_db}' AS m (READ_ONLY)")
        rows = {
            (r[0], r[1])
            for r in check.execute(
                "SELECT rkey, tile_qk FROM m.record_tiles ORDER BY rkey"
            ).fetchall()
        }
        assert ("node:123456", "023130") in rows, (
            f"OSM rkey 'n123456' must transform to 'node:123456'; got rows: {rows}"
        )
        assert ("way:789", "023131") in rows, (
            f"OSM rkey 'w789' must transform to 'way:789'; got rows: {rows}"
        )


# ---------------------------------------------------------------------------
# stage_export body
# ---------------------------------------------------------------------------

class TestStageExportPhase2Body:
    """Tests for the body of stage_export:
        stage_export(source, places_parquet, tile_assignments_parquet,
                     containment_dir, tiles_root, t0, export_workers=None)

    The `t0` positional parameter matches how run_pipeline calls it.
    """

    # ------------------------------------------------------------------
    # Fixture helpers
    # ------------------------------------------------------------------

    def _build_overture_fixtures(self, tmp_path):
        """Build minimal overture places.parquet + tile_assignments.parquet.

        Uses _make_overture_export_db to populate tables matching
        overture_place_export_tiles.sql, then COPYs them to parquet files.
        """
        import os
        places_pq = str(tmp_path / "places.parquet")
        ta_pq = str(tmp_path / "tile_assignments.parquet")
        conn = duckdb.connect()
        _make_overture_export_db(conn)
        conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
        conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
        conn.close()
        return places_pq, ta_pq

    # ------------------------------------------------------------------
    # Empty-containment substitution
    # ------------------------------------------------------------------

    def test_empty_containment_gives_empty_relations(self, tmp_path):
        """containment_dir with only _meta.json (no *.parquet) → relations=={}.

        The implementation must substitute the empty-containment subquery:
            (SELECT NULL::VARCHAR AS place_id, NULL::VARCHAR AS relations_json WHERE 1=0)
        NOT a read_parquet glob, which errors on DuckDB 1.2.1 when the glob matches nothing.

        Records are {uri, cid, value}-wrapped; relations lives on value, not
        on the wrapper.
        """
        import os, time
        from pathlib import Path
        from garganorn.stages import stage_export

        places_pq, ta_pq = self._build_overture_fixtures(tmp_path)
        containment_dir = _build_containment_dir(tmp_path, "containment")

        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)
        t0 = time.monotonic()

        stage_export("overture_place", places_pq, ta_pq, containment_dir, tiles_root, t0)

        # Every record's value must have relations == {}
        gz_files = list(Path(tiles_root).rglob("*.json.gz"))
        assert gz_files, "stage_export must produce at least one .json.gz"
        for gz_file in gz_files:
            with gzip.open(gz_file) as f:
                tile = json.loads(f.read())
            for rec in tile["records"]:
                assert set(rec.keys()) == {"uri", "cid", "value"}, (
                    f"record must be {{uri, cid, value}}-wrapped; got {list(rec)}"
                )
                value = rec["value"]
                assert value.get("relations") == {}, (
                    f"Empty containment must produce relations=={{}}; "
                    f"got {value.get('relations')!r} for rkey {value.get('rkey')!r}"
                )

    # ------------------------------------------------------------------
    # Determinism
    # ------------------------------------------------------------------

    def test_determinism_two_exports_byte_identical(self, tmp_path, monkeypatch):
        """Two exports from identical artifacts, with an injected fixed timestamp,
        produce byte-identical .json.gz files.

        Pins ordering determinism end to end: fixture data spans three
        export_partition_zoom partitions ('000000', '000111', '000222'), so
        this exercises pass 2's per-partition sort producing a total order
        across partitions, not just within-tile ordering. Also
        pins the query shape (one PARTITION_BY pass-1 statement, one sorted
        pass-2 SELECT per partition) independently for each of the two runs
        -- a fake implementation that keeps a single global sorted cursor
        would produce byte-identical output too, so query shape is the only
        way this test distinguishes it from the real two-pass mechanism.

        gzip mtime=0 is already set by `stage_export`.

        Tiles carry a run-scoped `generated_at`; without injecting the same
        `now` into both calls, two exports run at different wall-clock
        seconds would legitimately differ in that one field, which would
        make this test fail for a reason unrelated to the ordering/
        determinism property it exists to pin. Injecting `now` keeps the
        test measuring the invariant it's named for.
        """
        import os, time
        from datetime import datetime, timezone
        from pathlib import Path
        from garganorn.stages import stage_export
        import garganorn.stages as stages_module

        assignments = [
            (f"exp{tile_i}{rec_i}", tile_qk)
            for tile_i, tile_qk in enumerate([
                "0000000000", "0000001111", "0001110000", "0001111111", "0002220000",
            ])
            for rec_i in range(3)
        ]
        places_pq, ta_pq = _build_overture_tiles_fixture(tmp_path, assignments, "det")
        containment_dir = _build_containment_dir(tmp_path, "cd_det")
        t0 = time.monotonic()
        fixed_now = datetime(2026, 7, 9, 18, 0, 0, tzinfo=timezone.utc)

        # Two INDEPENDENT tiles_root dirs from identical input artifacts. Using
        # separate roots (rather than two runs into one root with force) avoids
        # (a) the export freshness gate skipping the second run and (b) the
        # key-collapse ambiguity of scanning two run dirs under one root — a
        # non-deterministic run must not be masked by whichever file rglob
        # visits last. Each root gets exactly one run dir.
        tiles_root_a = str(tmp_path / "tiles_a")
        tiles_root_b = str(tmp_path / "tiles_b")
        os.makedirs(tiles_root_a)
        os.makedirs(tiles_root_b)

        sql_log = spy_on_duckdb_connect(monkeypatch, stages_module)

        stage_export("overture_place", places_pq, ta_pq, containment_dir, tiles_root_a, t0,
                     now=fixed_now, export_partition_zoom=6)
        first_run_sql = list(sql_log)

        stage_export("overture_place", places_pq, ta_pq, containment_dir, tiles_root_b, t0,
                     now=fixed_now, export_partition_zoom=6)
        second_run_sql = sql_log[len(first_run_sql):]

        _assert_two_pass_query_shape(first_run_sql, expected_partitions=3)
        _assert_two_pass_query_shape(second_run_sql, expected_partitions=3)

        def _collect_tile_bytes(root):
            """Return {rel_path: bytes} for all .json.gz files, relative to the run dir."""
            result = {}
            for gz_file in Path(root).rglob("*.json.gz"):
                parts = gz_file.parts
                ts_idx = next(
                    (i for i, p in enumerate(parts) if re.match(r"^\d{8}T\d{6}$", p)),
                    None,
                )
                if ts_idx is not None:
                    rel = str(Path(*parts[ts_idx + 1:]))
                    result[rel] = gz_file.read_bytes()
            return result

        first_bytes = _collect_tile_bytes(tiles_root_a)
        second_bytes = _collect_tile_bytes(tiles_root_b)
        assert first_bytes, "First export must produce at least one .json.gz"

        assert set(second_bytes.keys()) == set(first_bytes.keys()), (
            f"Tile key sets differ between run 1 and run 2:\n"
            f"  only in run 2: {sorted(set(second_bytes) - set(first_bytes))}\n"
            f"  only in run 1: {sorted(set(first_bytes) - set(second_bytes))}"
        )
        for rel, first_content in first_bytes.items():
            assert second_bytes[rel] == first_content, (
                f"Tile {rel!r}: not byte-identical between two exports; "
                "ORDER BY tile_qk, place_id must produce deterministic output"
            )

    # ------------------------------------------------------------------
    # Run-dir lifecycle
    # ------------------------------------------------------------------

    def test_run_dir_leftover_incomplete_deleted(self, tmp_path):
        """A tiles/<ts>/ lacking manifest.json is deleted at next export.

        A run dir is complete iff manifest.json exists (written last).
        Stage must scan tiles/ at step 2 and delete any incomplete dirs (no manifest.json,
        not the current symlink target) before creating the new run.
        """
        import os, time
        from garganorn.stages import stage_export

        places_pq, ta_pq = self._build_overture_fixtures(tmp_path)
        containment_dir = _build_containment_dir(tmp_path, "containment")

        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)

        # Plant a leftover incomplete run dir (tile files present, manifest.json absent)
        leftover_ts = "20240101T000000"
        leftover_tile_dir = os.path.join(tiles_root, leftover_ts, "023130")
        os.makedirs(leftover_tile_dir)
        with open(os.path.join(leftover_tile_dir, "0231300.json.gz"), "wb") as f:
            f.write(gzip.compress(b'{"records":[]}', mtime=0))
        # Deliberately do NOT write manifest.json — this simulates a crash mid-export

        t0 = time.monotonic()

        stage_export("overture_place", places_pq, ta_pq, containment_dir, tiles_root, t0)

        # The incomplete leftover run dir must be deleted
        leftover_run_dir = os.path.join(tiles_root, leftover_ts)
        assert not os.path.exists(leftover_run_dir), (
            f"Incomplete run dir {leftover_run_dir!r} must be deleted by stage_export "
            "(run-dir completeness sweep)"
        )

    def test_manifest_json_written_after_manifest_duckdb(self, tmp_path):
        """manifest.json mtime must be >= manifest.duckdb mtime.

        manifest.json is written LAST as the run-dir completeness marker;
        manifest.duckdb is written first, then manifest.json.
        """
        import os, time
        from garganorn.stages import stage_export

        places_pq, ta_pq = self._build_overture_fixtures(tmp_path)
        containment_dir = _build_containment_dir(tmp_path, "containment")
        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)
        t0 = time.monotonic()

        stage_export("overture_place", places_pq, ta_pq, containment_dir, tiles_root, t0)

        # Find run dir via tiles/current symlink
        current = os.path.join(tiles_root, "current")
        assert os.path.islink(current), "tiles/current symlink must exist after stage_export"
        run_dir = os.path.realpath(current)

        manifest_json = os.path.join(run_dir, "manifest.json")
        manifest_duckdb = os.path.join(run_dir, "manifest.duckdb")
        assert os.path.exists(manifest_json), f"manifest.json must be written to {run_dir}"
        assert os.path.exists(manifest_duckdb), f"manifest.duckdb must be written to {run_dir}"
        assert os.path.getmtime(manifest_json) >= os.path.getmtime(manifest_duckdb), (
            "manifest.json mtime must be >= manifest.duckdb mtime "
            "(manifest.json is the completeness marker; it must land last)"
        )

    def test_tiles_current_symlink_points_at_new_run(self, tmp_path):
        """tiles/current symlink is updated to the new timestamp dir.

        The symlink must target a valid, existing timestamp dir containing manifest.json.
        """
        import os, time
        from garganorn.stages import stage_export

        places_pq, ta_pq = self._build_overture_fixtures(tmp_path)
        containment_dir = _build_containment_dir(tmp_path, "containment")
        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)
        t0 = time.monotonic()

        stage_export("overture_place", places_pq, ta_pq, containment_dir, tiles_root, t0)

        current = os.path.join(tiles_root, "current")
        assert os.path.islink(current), "tiles/current must be a symlink after stage_export"

        target = os.readlink(current)
        assert re.match(r"^\d{8}T\d{6}$", target), (
            f"tiles/current must point at a timestamp dir (YYYYMMDDTHHmmss); got {target!r}"
        )
        run_dir = os.path.join(tiles_root, target)
        assert os.path.isdir(run_dir), (
            f"Symlink target {target!r} must be an existing directory"
        )
        assert os.path.exists(os.path.join(run_dir, "manifest.json")), (
            f"Run dir {target!r} must contain manifest.json (completeness marker)"
        )

    def test_keep_2_sweep_retains_only_complete_dirs(self, tmp_path):
        """keep-2 sweep retains only the 2 newest COMPLETE run dirs.

        Complete = has manifest.json. Incomplete dirs are already deleted at step 2.
        Keep-2 sweep at step 5 removes complete run dirs older than the newest 2.
        """
        import os, time
        from garganorn.stages import stage_export

        places_pq, ta_pq = self._build_overture_fixtures(tmp_path)
        containment_dir = _build_containment_dir(tmp_path, "containment")
        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)

        # Pre-create 3 complete run dirs with older timestamps
        old_timestamps = ["20240101T000001", "20240101T000002", "20240101T000003"]
        for ts in old_timestamps:
            run_dir = os.path.join(tiles_root, ts)
            os.makedirs(os.path.join(run_dir, "023130"))
            with open(os.path.join(run_dir, "manifest.json"), "w") as f:
                json.dump({}, f)

        t0 = time.monotonic()

        stage_export("overture_place", places_pq, ta_pq, containment_dir, tiles_root, t0)

        # At most 2 complete run dirs survive (keep-2)
        complete_dirs = sorted([
            d for d in os.listdir(tiles_root)
            if re.match(r"^\d{8}T\d{6}$", d)
            and os.path.isdir(os.path.join(tiles_root, d))
            and not os.path.islink(os.path.join(tiles_root, d))
            and os.path.exists(os.path.join(tiles_root, d, "manifest.json"))
        ])
        assert len(complete_dirs) <= 2, (
            f"keep-2 sweep must retain at most 2 complete run dirs; "
            f"found {len(complete_dirs)}: {complete_dirs}"
        )


# ---------------------------------------------------------------------------
# Prefix-batched export: pass 1 materialises tile_export unsorted via COPY
# ... PARTITION_BY, pass 2 sorts and flushes one partition at a time.
# ---------------------------------------------------------------------------

def _build_containment_dir(tmp_path, name):
    """Empty containment_dir (_meta.json only, no *.parquet) -- the
    empty-containment shape stage_export must substitute instead of a
    read_parquet glob."""
    import os
    cd = str(tmp_path / name)
    os.makedirs(cd, exist_ok=True)
    with open(os.path.join(cd, "_meta.json"), "w") as f:
        json.dump({"empty": True, "params": {}, "inputs": []}, f)
    return cd


def _build_overture_tiles_fixture(tmp_path, assignments, name):
    """places.parquet + tile_assignments.parquet for overture_place, with an
    explicit place_id -> tile_qk mapping, decoupled from the place's own
    lat/lon (only tile_assignments controls partition routing).

    assignments: list of (place_id, tile_qk) pairs, place_ids unique.
    """
    places_rows = [
        (place_id, f"Place {place_id}", 37.7749, -122.4194, 50, "US")
        for place_id, _tile_qk in assignments
    ]
    conn = duckdb.connect()
    _make_overture_export_db(conn, places_rows, assignments=assignments)
    places_pq = str(tmp_path / f"{name}_places.parquet")
    ta_pq = str(tmp_path / f"{name}_ta.parquet")
    conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
    conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
    conn.close()
    return places_pq, ta_pq


def _build_osm_tiles_fixture(tmp_path, assignments, name):
    """places.parquet + tile_assignments.parquet for the osm source, matching
    the columns osm_export_tiles.sql selects off `places`.

    assignments: list of (rkey, tile_qk) pairs. rkeys must not start with
    n/w/r -- those prefixes trigger the node/way/relation rkey rewrite,
    which isn't what this fixture is testing.
    """
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE places (
            rkey             VARCHAR PRIMARY KEY,
            name             VARCHAR,
            importance       INTEGER,
            latitude         DOUBLE,
            longitude        DOUBLE,
            variants         STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT [],
            primary_category VARCHAR,
            tags             MAP(VARCHAR, VARCHAR)
        )
    """)
    for rkey, _tile_qk in assignments:
        conn.execute(
            "INSERT INTO places VALUES (?, ?, 50, 37.7749, -122.4194, [], NULL, "
            "map([]::VARCHAR[], []::VARCHAR[]))",
            [rkey, f"Place {rkey}"],
        )
    conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
    for rkey, tile_qk in assignments:
        conn.execute("INSERT INTO tile_assignments VALUES (?, ?)", [rkey, tile_qk])
    conn.execute("CREATE TABLE place_containment (place_id VARCHAR, relations_json VARCHAR)")

    places_pq = str(tmp_path / f"{name}_places.parquet")
    ta_pq = str(tmp_path / f"{name}_ta.parquet")
    conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
    conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
    conn.close()
    return places_pq, ta_pq


def _build_division_tiles_fixture(tmp_path, assignments, name):
    """places.parquet + tile_assignments.parquet for overture_division,
    matching the columns overture_division_export_tiles.sql selects off
    `places`. assignments: list of (id, tile_qk) pairs."""
    conn = duckdb.connect()
    conn.execute("""
        CREATE TABLE places (
            id            VARCHAR PRIMARY KEY,
            names         STRUCT("primary" VARCHAR),
            importance    INTEGER,
            min_latitude  DOUBLE, max_latitude  DOUBLE,
            min_longitude DOUBLE, max_longitude DOUBLE,
            variants      STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT [],
            subtype       VARCHAR, country VARCHAR, region VARCHAR,
            level         VARCHAR, wikidata VARCHAR, population BIGINT
        )
    """)
    for place_id, _tile_qk in assignments:
        conn.execute(
            "INSERT INTO places VALUES (?, {'primary': ?}, 50, 37.60, 37.85, "
            "-122.55, -122.30, [], 'region', 'US', NULL, 'region', NULL, NULL)",
            [place_id, f"Division {place_id}"],
        )
    conn.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
    for place_id, tile_qk in assignments:
        conn.execute("INSERT INTO tile_assignments VALUES (?, ?)", [place_id, tile_qk])
    conn.execute("CREATE TABLE place_containment (place_id VARCHAR, relations_json VARCHAR)")

    places_pq = str(tmp_path / f"{name}_places.parquet")
    ta_pq = str(tmp_path / f"{name}_ta.parquet")
    conn.execute(f"COPY places TO '{places_pq}' (FORMAT PARQUET)")
    conn.execute(f"COPY tile_assignments TO '{ta_pq}' (FORMAT PARQUET)")
    conn.close()
    return places_pq, ta_pq


def _read_tile(run_dir, qk):
    """Parsed JSON body of one tile's output file, or None if it wasn't written."""
    import os
    path = os.path.join(run_dir, qk[:6], f"{qk}.json.gz")
    if not os.path.exists(path):
        return None
    with gzip.open(path) as f:
        return json.loads(f.read())


def _tile_rkeys(run_dir, qk):
    """Set of rkeys in one tile's output file. Asserts the tile exists --
    callers that need to assert absence should use _read_tile directly."""
    tile = _read_tile(run_dir, qk)
    assert tile is not None, f"tile {qk} was not written to {run_dir}"
    return {rec["value"]["rkey"] for rec in tile["records"]}


_PARTITION_BY_MARKER = "PARTITION_BY"
_PASS2_ORDER_BY = "ORDER BY tile_qk, place_id"
_PRESERVE_INSERTION_ORDER_FALSE = "preserve_insertion_order = false"


def _assert_two_pass_query_shape(sql_log, expected_partitions):
    """Assert pass 1 materialises unsorted via exactly one COPY ...
    PARTITION_BY statement and pass 2 issues one ORDER BY tile_qk, place_id
    SELECT per partition found -- the mechanism the bounded-peak-spill
    guarantee rests on, not just output correctness.
    """
    partition_by_stmts = [s for s in sql_log if _PARTITION_BY_MARKER in s]
    assert len(partition_by_stmts) == 1, (
        f"expected exactly one PARTITION_BY statement (pass 1); "
        f"got {len(partition_by_stmts)} in: {sql_log}"
    )
    assert "ORDER BY" not in partition_by_stmts[0], (
        f"pass 1 must not sort:\n{partition_by_stmts[0]}"
    )
    pass2_stmts = [s for s in sql_log if _PASS2_ORDER_BY in s]
    assert len(pass2_stmts) == expected_partitions, (
        f"expected {expected_partitions} pass-2 sorted SELECTs (one per "
        f"partition found); got {len(pass2_stmts)} in: {sql_log}"
    )
    assert any(_PRESERVE_INSERTION_ORDER_FALSE in s for s in sql_log), (
        "SET preserve_insertion_order = false must still be issued -- it is "
        "the prerequisite for pass 1 not to sort"
    )


def _spy_build_tile_payload(monkeypatch, snapshot_dir=None):
    """Patch garganorn.stages.envelope.build_tile_payload to delegate to the
    real function while counting calls -- one per flush_tile invocation, so
    the count must equal the number of distinct tiles written. A missing
    per-partition current_qk/accumulated reset re-flushes the previous
    partition's last tile, producing one extra call.

    If snapshot_dir is given, os.listdir(snapshot_dir) is captured on the
    first call, before any teardown -- observes the staging dir mid-run,
    without hardcoding its name.

    Returns {'count': int, 'snapshot': list[str] | None}.
    """
    import os
    import garganorn.stages as stages_module

    real_build_tile_payload = stages_module.envelope.build_tile_payload
    result = {"count": 0, "snapshot": None}

    def _wrapper(*args, **kwargs):
        result["count"] += 1
        if snapshot_dir is not None and result["snapshot"] is None:
            result["snapshot"] = os.listdir(snapshot_dir)
        return real_build_tile_payload(*args, **kwargs)

    monkeypatch.setattr(stages_module.envelope, "build_tile_payload", _wrapper)
    return result


class TestBatchedExportPartitioning:
    """Tests for the two-pass prefix-partitioned export pipeline: pass 1
    materialises tile_export unsorted, partitioned by left(tile_qk,
    export_partition_zoom); pass 2 sorts and flushes one partition at a time.
    """

    def test_short_and_long_tile_qk_mixed_in_one_run(self, tmp_path, monkeypatch):
        """A tile_qk shorter than the partition depth must still land as one
        complete file: left(tile_qk, 6) on a 4-char string returns the whole
        string, so the tile is its own partition, not a fragment of one. The
        summary band produces sub-6-char keys in real runs; this test pins
        the slicing behavior directly rather than through that dependency.
        Mixed with 10-char tile_qks in the same run
        so partition-dir listing and the sort/flush loop are exercised at
        both widths, not just the width the real and fake implementations
        happen to agree on.
        """
        import os, time
        from garganorn.stages import stage_export
        import garganorn.stages as stages_module

        assignments = (
            [(f"short{i}", "0123") for i in range(5)]
            + [(f"long{i}", "0123440000") for i in range(3)]
            + [(f"other{i}", "0198765432") for i in range(2)]
        )
        places_pq, ta_pq = _build_overture_tiles_fixture(tmp_path, assignments, "mixed")
        containment_dir = _build_containment_dir(tmp_path, "cd_mixed")
        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)

        sql_log = spy_on_duckdb_connect(monkeypatch, stages_module)

        run_dir = stage_export(
            "overture_place", places_pq, ta_pq, containment_dir, tiles_root,
            time.monotonic(), export_partition_zoom=6,
        )

        assert _tile_rkeys(run_dir, "0123") == {f"short{i}" for i in range(5)}, (
            "the short (4-char) tile must contain exactly its 5 records"
        )
        assert _tile_rkeys(run_dir, "0123440000") == {f"long{i}" for i in range(3)}
        assert _tile_rkeys(run_dir, "0198765432") == {f"other{i}" for i in range(2)}

        # partitions at depth 6: '0123' (short tile is its own whole prefix),
        # '012344', '019876'
        _assert_two_pass_query_shape(sql_log, expected_partitions=3)

    def test_partition_boundary_flush_no_leak_no_duplicate(self, tmp_path, monkeypatch):
        """Input spans two partitions ('011000' and '011002', with the empty
        '011001' between them) where the last tile of the first partition
        and the first tile of the second are different tiles. Each tile's
        file must contain exactly its own records -- no leakage from the
        neighbouring partition, no duplication.

        Failing to reset current_qk/accumulated per partition would re-flush
        partition A's last tile (tile_a2) when partition B's cursor starts,
        corrupting tile_a2's file via a second concurrent write. That second
        write is byte-identical to the first, so no output assertion can see
        it -- only a build_tile_payload call count catches it.

        The sparse '011001' gap between the two populated prefixes also
        covers the requirement that pass 2 enumerate partitions by listing
        staging_dir rather than synthesising a 4^d prefix list -- a
        synthesised list would hand read_parquet an empty glob for '011001',
        which errors on this DuckDB version -- the same hazard stage_export's
        containment_expr comment records.
        """
        import os, time
        from pathlib import Path
        from garganorn.stages import stage_export

        tile_a1, tile_a2, tile_b1 = "0110000000", "0110001111", "0110020000"
        assignments = (
            [(f"a1_{i}", tile_a1) for i in range(2)]
            + [(f"a2_{i}", tile_a2) for i in range(1)]
            + [(f"b1_{i}", tile_b1) for i in range(3)]
        )
        places_pq, ta_pq = _build_overture_tiles_fixture(tmp_path, assignments, "boundary")
        containment_dir = _build_containment_dir(tmp_path, "cd_boundary")
        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)

        spy = _spy_build_tile_payload(monkeypatch)

        run_dir = stage_export(
            "overture_place", places_pq, ta_pq, containment_dir, tiles_root,
            time.monotonic(), export_partition_zoom=6,
        )

        assert _tile_rkeys(run_dir, tile_a1) == {"a1_0", "a1_1"}
        assert _tile_rkeys(run_dir, tile_a2) == {"a2_0"}
        assert _tile_rkeys(run_dir, tile_b1) == {"b1_0", "b1_1", "b1_2"}

        gz_files = list(Path(run_dir).rglob("*.json.gz"))
        assert len(gz_files) == 3, (
            f"expected exactly 3 tile files (one per tile_qk), got "
            f"{len(gz_files)}: {gz_files}"
        )
        assert spy["count"] == 3, (
            f"expected exactly one build_tile_payload call per tile (3 "
            f"tiles); got {spy['count']} -- a missing per-partition "
            f"current_qk/accumulated reset re-flushes the previous "
            f"partition's last tile"
        )

    def test_tile_spanning_two_fetchmany_batches(self, tmp_path):
        """A single tile with more than 1000 records (the fetchmany chunk
        size) must be written complete: the batch boundary is handled inside
        a single partition's pass-2 sort-and-flush loop.
        """
        import os, time
        from garganorn.stages import stage_export

        tile_qk = "0123000000"
        n = 1500
        assignments = [(f"p{i:05d}", tile_qk) for i in range(n)]
        places_pq, ta_pq = _build_overture_tiles_fixture(tmp_path, assignments, "fetchmany")
        containment_dir = _build_containment_dir(tmp_path, "cd_fetchmany")
        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)

        run_dir = stage_export(
            "overture_place", places_pq, ta_pq, containment_dir, tiles_root,
            time.monotonic(), export_partition_zoom=6,
        )

        rkeys = _tile_rkeys(run_dir, tile_qk)
        assert len(rkeys) == n, (
            f"tile must have all {n} records spanning more than one "
            f"fetchmany(1000) batch; got {len(rkeys)}"
        )
        assert rkeys == {f"p{i:05d}" for i in range(n)}

    @pytest.mark.parametrize("source, builder", [
        ("overture_place", _build_overture_tiles_fixture),
        ("osm", _build_osm_tiles_fixture),
        ("overture_division", _build_division_tiles_fixture),
    ])
    def test_partitioned_export_used_by_every_source(
        self, tmp_path, monkeypatch, source, builder
    ):
        """Every source takes the same two-pass partitioned export path --
        identical mechanism, no per-source branching. Checked against the
        query shape, not just output rkeys: an implementation that
        partitions overture_place but special-cases osm/overture_division
        would pass an output-only check.
        """
        import os, time
        from garganorn.stages import stage_export
        import garganorn.stages as stages_module

        assignments = [(f"{source}_{i}", "0123") for i in range(3)] + [
            (f"{source}_x", "0201")
        ]
        places_pq, ta_pq = builder(tmp_path, assignments, source)
        containment_dir = _build_containment_dir(tmp_path, f"cd_{source}")
        tiles_root = str(tmp_path / f"tiles_{source}")
        os.makedirs(tiles_root)

        sql_log = spy_on_duckdb_connect(monkeypatch, stages_module)

        run_dir = stage_export(
            source, places_pq, ta_pq, containment_dir, tiles_root,
            time.monotonic(), export_partition_zoom=6,
        )

        assert _tile_rkeys(run_dir, "0123") == {f"{source}_{i}" for i in range(3)}
        assert _tile_rkeys(run_dir, "0201") == {f"{source}_x"}
        # partitions at depth 6: '0123', '0201'
        _assert_two_pass_query_shape(sql_log, expected_partitions=2)

    def test_temp_directory_sentinel_and_root_untouched_after_run(self, tmp_path, monkeypatch):
        """stage_export's owned staging_dir lives under the caller's
        temp_directory, exists there DURING the run, and is fully cleaned up
        by the time the run returns. Anything else pre-existing under
        temp_directory -- like this sentinel file -- is left alone,
        mirroring the existing spill_dir contract (stage_covering has the
        analogous test in test_covering.py). The mid-run check does not
        assume a name for the staging dir -- the design does not fix one --
        so an implementation that puts staging next to run_dir instead of
        under temp_directory is caught here rather than passing vacuously.
        """
        import os, time
        from garganorn.stages import stage_export

        assignments = [(f"exp{i}", "0123") for i in range(3)]
        places_pq, ta_pq = _build_overture_tiles_fixture(tmp_path, assignments, "tempdir")
        containment_dir = _build_containment_dir(tmp_path, "cd_tempdir")
        tiles_root = str(tmp_path / "tiles")
        os.makedirs(tiles_root)

        temp_dir = tmp_path / "caller_temp"
        temp_dir.mkdir()
        sentinel = temp_dir / "sentinel.txt"
        sentinel.write_text("pre-existing, must survive")

        spy = _spy_build_tile_payload(monkeypatch, snapshot_dir=str(temp_dir))

        stage_export(
            "overture_place", places_pq, ta_pq, containment_dir, tiles_root,
            time.monotonic(), temp_directory=str(temp_dir), export_partition_zoom=6,
        )

        assert spy["snapshot"] is not None, "expected at least one tile flush"
        # >= 2 new entries, not just 1: DuckDB's own SET temp_directory spill
        # dir is already created eagerly by existing code regardless of this
        # feature, so a single new entry there proves nothing about pass 1's
        # staging_dir specifically -- only a *second*, distinct directory
        # does.
        new_entries = set(spy["snapshot"]) - {"sentinel.txt"}
        assert len(new_entries) >= 2, (
            "stage_export's owned staging dir must exist under temp_directory "
            "during the run, alongside (not instead of) the existing DuckDB "
            f"spill dir; mid-run listing was {spy['snapshot']}"
        )

        assert sentinel.exists() and sentinel.read_text() == "pre-existing, must survive", (
            "stage_export must not touch pre-existing content in the caller's temp_directory"
        )
        assert os.listdir(temp_dir) == ["sentinel.txt"], (
            f"stage_export must remove everything it owns under temp_directory "
            f"by the time it returns; leftover: {os.listdir(temp_dir)}"
        )

    def test_staging_artifacts_not_leaked_into_next_run_sharing_temp_directory(
        self, tmp_path, monkeypatch
    ):
        """A second run reusing the same caller-supplied temp_directory must
        not pick up the first run's staging partitions.

        Models 'residue from a prior killed run is cleared at start rather
        than accumulating' without depending on the staging directory's
        exact name (the design does not commit to one): if run 1's
        partition files survived under temp_directory, pass 2's directory
        listing for run 2 would pick them up as an extra, wrong tile.

        Also confirms run 1 actually created a staging dir under
        temp_directory (not just that nothing is left afterwards) -- an
        implementation that stages next to run_dir instead would leave
        temp_directory empty throughout and still pass a leak-only check.
        """
        import os, time
        from garganorn.stages import stage_export

        temp_dir = tmp_path / "caller_temp"
        temp_dir.mkdir()

        assignments_a = [(f"a{i}", "0000") for i in range(2)]
        places_a, ta_a = _build_overture_tiles_fixture(tmp_path, assignments_a, "run1")
        containment_a = _build_containment_dir(tmp_path, "cd_run1")
        tiles_root_a = str(tmp_path / "tiles_a")
        os.makedirs(tiles_root_a)

        spy_a = _spy_build_tile_payload(monkeypatch, snapshot_dir=str(temp_dir))

        run_dir_a = stage_export(
            "overture_place", places_a, ta_a, containment_a, tiles_root_a,
            time.monotonic(), temp_directory=str(temp_dir), export_partition_zoom=6,
        )
        assert _tile_rkeys(run_dir_a, "0000") == {"a0", "a1"}
        # >= 2, not just non-empty: DuckDB's own SET temp_directory spill dir
        # alone would already make temp_dir non-empty regardless of whether
        # pass 1's staging_dir exists.
        assert len(spy_a["snapshot"] or []) >= 2, (
            "run 1 must have created a staging dir under temp_directory, "
            "distinct from the existing DuckDB spill dir, before its first "
            f"flush; mid-run listing was {spy_a['snapshot']}"
        )

        assignments_b = [(f"b{i}", "3333") for i in range(2)]
        places_b, ta_b = _build_overture_tiles_fixture(tmp_path, assignments_b, "run2")
        containment_b = _build_containment_dir(tmp_path, "cd_run2")
        tiles_root_b = str(tmp_path / "tiles_b")
        os.makedirs(tiles_root_b)
        run_dir_b = stage_export(
            "overture_place", places_b, ta_b, containment_b, tiles_root_b,
            time.monotonic(), temp_directory=str(temp_dir), export_partition_zoom=6,
        )

        assert _tile_rkeys(run_dir_b, "3333") == {"b0", "b1"}, (
            "run 2 must contain only its own records"
        )
        assert _read_tile(run_dir_b, "0000") is None, (
            "run 2 must not resurrect run 1's tile '0000' -- staging "
            "partitions must not leak across runs sharing temp_directory"
        )


_EXPLAIN_ORDER_BY_OP = "ORDER_BY"
_EXPLAIN_MERGE_JOIN_OP = "PIECEWISE_MERGE_JOIN"


class TestExportPlanShape:
    """Pins the query-plan shape of the pass-1 SELECT off tile_export: no
    ORDER_BY and no PIECEWISE_MERGE_JOIN. Peak spill is bounded by one
    partition's slice only if pass 1 does not sort -- a merge join sorts
    internally and would defeat that guarantee just as an explicit ORDER BY
    would.

    Operator names pinned empirically against the installed DuckDB (1.4.4):
    `EXPLAIN SELECT a, b FROM t ORDER BY a, b` produces an ORDER_BY node;
    `EXPLAIN SELECT ... FROM t JOIN u ON t.a = u.a` (equi-join, no ORDER BY)
    produces HASH_JOIN, not PIECEWISE_MERGE_JOIN or ORDER_BY;
    `EXPLAIN SELECT ... FROM t JOIN u ON t.a < u.a` (inequality join, no
    hash-join shortcut available) produces PIECEWISE_MERGE_JOIN.
    """

    def test_tile_export_select_plan_has_no_order_by_or_merge_join(self):
        """tile_export's SELECT plan must contain no ORDER_BY or
        PIECEWISE_MERGE_JOIN operator -- ordering is pass 2's job."""
        conn = duckdb.connect()
        _make_overture_export_db(conn)
        raw_sql = _load_sql("overture_place_export_tiles.sql", {"repo": "places.atgeo.org"})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw_sql)))

        plan = conn.execute(
            "EXPLAIN SELECT tile_qk, place_id, rkey, record_json FROM tile_export"
        ).fetchall()[0][1]
        conn.close()

        assert _EXPLAIN_ORDER_BY_OP not in plan, (
            "pass 1's SELECT off tile_export must not sort -- peak spill is "
            "bounded by one partition only if pass 1 streams. Plan:\n" + plan
        )
        assert _EXPLAIN_MERGE_JOIN_OP not in plan, (
            "a merge join sorts internally, defeating the no-sort guarantee "
            "just as an explicit ORDER BY would. Plan:\n" + plan
        )

    def test_order_by_detection_fires_on_a_query_that_actually_sorts(self):
        """Sanity check on the assertion above: the ORDER_BY substring check
        must be capable of failing, or a typo'd operator name would make the
        prior test pass vacuously forever."""
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t AS SELECT i AS a FROM range(10) t(i)")
        plan = conn.execute("EXPLAIN SELECT a FROM t ORDER BY a").fetchall()[0][1]
        conn.close()
        assert _EXPLAIN_ORDER_BY_OP in plan, (
            f"expected {_EXPLAIN_ORDER_BY_OP} node in a sorted query's plan:\n{plan}"
        )

    def test_merge_join_detection_fires_on_a_query_that_actually_merge_joins(self):
        """Sanity check on the PIECEWISE_MERGE_JOIN assertion above: an
        inequality join predicate has no hash-join shortcut available, so
        DuckDB falls back to a merge join -- verified empirically against
        the installed DuckDB (1.4.4)."""
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t AS SELECT i AS a FROM range(10) t(i)")
        conn.execute("CREATE TABLE u AS SELECT i AS a FROM range(10) t(i)")
        plan = conn.execute(
            "EXPLAIN SELECT t.a FROM t JOIN u ON t.a < u.a"
        ).fetchall()[0][1]
        conn.close()
        assert _EXPLAIN_MERGE_JOIN_OP in plan, (
            f"expected {_EXPLAIN_MERGE_JOIN_OP} node in an inequality "
            f"join's plan:\n{plan}"
        )
