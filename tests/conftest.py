"""Shared fixtures for garganorn tests."""
import pytest
import duckdb

from garganorn.database import OverturePlaces, OvertureDivisions


# ---------------------------------------------------------------------------
# San Francisco area test data
# ---------------------------------------------------------------------------

OVERTURE_PLACES = [
    # id, name, latitude, longitude, address_freeform, locality, postcode, region, country
    ("ovr001", "Philz Coffee", 37.7749, -122.4194, "201 Berry St", "San Francisco", "94158", "US-CA", "US"),
    ("ovr002", "Dolores Park", 37.7596, -122.4269, "Dolores St & 19th St", "San Francisco", "94114", "US-CA", "US"),
    ("ovr003", "Coit Tower", 37.8024, -122.4058, "1 Telegraph Hill Blvd", "San Francisco", "94133", "US-CA", "US"),
    ("ovr004", "Anchor Brewing", 37.7688, -122.4125, "1705 Mariposa St", "San Francisco", "94107", "US-CA", "US"),
    ("ovr005", "Lombard Street", 37.8021, -122.4187, "Lombard St", "San Francisco", "94133", "US-CA", "US"),
    ("ovr006", "Diner North End", 37.7749, -122.4350, "1 North End Ave", "San Francisco", "94129", "US-CA", "US"),
    ("ovr007", "North End Pub", 37.7748, -122.4351, "2 North End Ave", "San Francisco", "94129", "US-CA", "US"),
]


def _create_overture_db(db_path):
    """Create an Overture DuckDB database with test data."""
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE places (
            id VARCHAR PRIMARY KEY,
            geometry GEOMETRY,
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            names STRUCT("primary" VARCHAR),
            categories STRUCT("primary" VARCHAR),
            addresses STRUCT(
                country VARCHAR,
                postcode VARCHAR,
                locality VARCHAR,
                freeform VARCHAR,
                region VARCHAR
            )[],
            websites VARCHAR[],
            socials VARCHAR[],
            emails VARCHAR[],
            phones VARCHAR[],
            brand STRUCT(names STRUCT("primary" VARCHAR)),
            confidence DOUBLE,
            version INTEGER,
            sources STRUCT(property VARCHAR, dataset VARCHAR, record_id VARCHAR, confidence DOUBLE)[],
            importance INTEGER,
            variants STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT []
        )
    """)

    ovr_importance = {
        "ovr001": 70,
        "ovr002": 55,
        "ovr003": 80,
        "ovr004": 55,
        "ovr005": 65,
        "ovr006": 70,
        "ovr007": 70,
    }

    # Variant data for Overture places
    ovr_variants = {
        "ovr003": [{"name": "Tour de Coit", "type": "alternate", "language": "fr"}],
    }

    for row in OVERTURE_PLACES:
        ovr_id, name, lat, lon, freeform, locality, postcode, region, country = row
        variants = ovr_variants.get(ovr_id, [])
        if variants:
            variant_sql = "[" + ", ".join(
                f"{{'name': '{v['name']}', 'type': '{v['type']}', 'language': '{v['language']}'}}"
                for v in variants
            ) + "]::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]"
        else:
            variant_sql = "[]::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]"
        conn.execute(f"""
            INSERT INTO places VALUES (
                ?, ST_Point(?, ?),
                {{'xmin': ?-0.001, 'ymin': ?-0.001, 'xmax': ?+0.001, 'ymax': ?+0.001}},
                {{'primary': ?}},
                {{'primary': NULL}},
                [{{'country': ?, 'postcode': ?, 'locality': ?, 'freeform': ?, 'region': ?}}],
                NULL, NULL, NULL, NULL,
                NULL,
                0.9, 1, NULL,
                ?,
                {variant_sql}
            )
        """, [ovr_id, lon, lat,
              lon, lat, lon, lat,
              name,
              country, postcode, locality, freeform, region,
              ovr_importance[ovr_id]])

    # Intentionally no RTREE index on the places table. All spatial filtering
    # uses bbox struct field comparisons, which do not require an explicit index.
    # RTREE is only used on the division boundaries table (ST_Contains queries).

    conn.close()


# ---------------------------------------------------------------------------
# Session-scoped path fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def overture_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("overture") / "overture.duckdb"
    _create_overture_db(db_path)
    return db_path


# ---------------------------------------------------------------------------
# Division boundary test data (Overture divisions schema)
# ---------------------------------------------------------------------------

# id, level, wkt_geom, min_lat, min_lon, max_lat, max_lon,
#   names (dict or None), subtype, country, region, wikidata, population, importance, variants
#
# level values are the atgeo containment vocabulary (garganorn.levels.LEVEL_VOCAB),
# not raw Overture admin_level. div_continent_na has subtype=None (continent has
# no producer entry in LEVEL_VOCAB) -- level 0 is a synthetic sentinel
# above country=10, chosen only to keep this pre-built boundaries.duckdb-shaped
# fixture's ascending order; the real import pipeline never emits it since
# admin_level's continent row (subtype NULL) would trip the fail-loud guard).
DIVISION_BOUNDARIES = [
    (
        "div_continent_na", 0,
        "POLYGON((-130 20, -130 55, -60 55, -60 20, -130 20))",
        20.0, -130.0, 55.0, -60.0,
        None,  # names=NULL (for test_null_names_returns_empty_name_and_variants)
        None, None, None, None, None,
        0, [],
    ),
    (
        "div_country_us", 10,
        "POLYGON((-125 24, -125 50, -66 50, -66 24, -125 24))",
        24.0, -125.0, 50.0, -66.0,
        {"primary": "United States", "common": {}, "rules": []},
        "country", "US", None, "Q30", 331000000,
        0, [],
    ),
    (
        "div_region_ca", 25,
        "POLYGON((-125 34, -125 42, -118 42, -118 34, -125 34))",
        34.0, -125.0, 42.0, -118.0,
        {"primary": "California", "common": {"fr": "Californie"}, "rules": []},
        "region", "US", "US-CA", "Q99", 39000000,
        0, [],
    ),
    (
        "div_locality_sf", 50,
        "POLYGON((-122.55 37.6, -122.55 37.85, -122.3 37.85, -122.3 37.6, -122.55 37.6))",
        37.6, -122.55, 37.85, -122.3,
        {"primary": "San Francisco", "common": {"es": "San Francisco", "zh": "\u65e7\u91d1\u5c71"}, "rules": []},
        "locality", "US", "US-CA", "Q62", 874961,
        0, [],
    ),
    (
        "div_borough_manhattan", 50,
        "POLYGON((-74.05 40.68, -74.05 40.88, -73.90 40.88, -73.90 40.68, -74.05 40.68))",
        40.68, -74.05, 40.88, -73.90,
        {"primary": "Manhattan", "common": {}, "rules": []},
        "locality", "US", "US-NY", "Q11299", 1629153,
        0, [],
    ),
]


def _create_division_db(db_path):
    """Create a division-schema boundary DB (enriched places table for OvertureDivisions)."""
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE places (
            id VARCHAR,
            geometry GEOMETRY,
            level INTEGER,
            names STRUCT(
                "primary" VARCHAR,
                common MAP(VARCHAR, VARCHAR),
                rules STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]
            ),
            subtype VARCHAR,
            country VARCHAR,
            region VARCHAR,
            wikidata VARCHAR,
            population BIGINT,
            min_latitude DOUBLE,
            max_latitude DOUBLE,
            min_longitude DOUBLE,
            max_longitude DOUBLE,
            importance INTEGER,
            variants STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
        )
    """)
    for row in DIVISION_BOUNDARIES:
        (bid, level, wkt, min_lat, min_lon, max_lat, max_lon,
         names, subtype, country, region, wikidata, population,
         importance, variants) = row

        if names is None:
            names_sql = "NULL"
            names_params = []
        else:
            # Build the common MAP from the dict
            common_dict = names.get("common", {})
            if common_dict:
                keys_list = list(common_dict.keys())
                vals_list = list(common_dict.values())
                common_sql = "map(" + str(keys_list) + "::VARCHAR[], " + str(vals_list) + "::VARCHAR[])"
                # Use DuckDB literal for simplicity
                keys_literal = "[" + ", ".join(f"'{k}'" for k in keys_list) + "]"
                vals_literal = "[" + ", ".join(f"'{v}'" for v in vals_list) + "]"
                common_sql = f"map({keys_literal}::VARCHAR[], {vals_literal}::VARCHAR[])"
            else:
                common_sql = "map([]::VARCHAR[], []::VARCHAR[])"
            primary_val = names.get("primary")
            names_sql = f"{{'primary': ?, 'common': {common_sql}, 'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]}}"
            names_params = [primary_val]

        if not names_params:
            conn.execute(f"""
                INSERT INTO places (id, geometry, level, names, subtype, country, region,
                    wikidata, population, min_latitude, max_latitude, min_longitude, max_longitude,
                    importance, variants)
                VALUES (
                    ?, ST_GeomFromText(?), ?,
                    {names_sql},
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?,
                    []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
                )
            """, [bid, wkt, level,
                  subtype, country, region, wikidata, population,
                  min_lat, max_lat, min_lon, max_lon,
                  importance])
        else:
            conn.execute(f"""
                INSERT INTO places (id, geometry, level, names, subtype, country, region,
                    wikidata, population, min_latitude, max_latitude, min_longitude, max_longitude,
                    importance, variants)
                VALUES (
                    ?, ST_GeomFromText(?), ?,
                    {names_sql},
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?,
                    []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
                )
            """, [bid, wkt, level] + names_params + [
                  subtype, country, region, wikidata, population,
                  min_lat, max_lat, min_lon, max_lon,
                  importance])

    conn.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
    conn.close()


@pytest.fixture(scope="session")
def division_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("division") / "division.duckdb"
    _create_division_db(db_path)
    return db_path


@pytest.fixture
def division_db(division_db_path):
    db = OvertureDivisions(division_db_path)
    db.connect()
    yield db
    db.close()


# ---------------------------------------------------------------------------
# Quadtree parquet fixtures (session-scoped)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def overture_parquet(tmp_path_factory):
    """Write a single Overture-schema parquet file and return a glob path for it."""

    base = tmp_path_factory.mktemp("overture_parquet")
    parquet_path = base / "overture_data.parquet"

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

    # ov001 — in-bbox, names.common has one entry (language 'en'); has address data
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov001',
            {'xmin': -122.420, 'ymin': 37.774, 'xmax': -122.418, 'ymax': 37.776},
            'POINT(-122.419 37.775)',
            {'primary': 'Blue Bottle Coffee',
             'common': map(['en'], ['Blue Bottle Coffee']),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'coffee_shop'},
            [{'country': 'US', 'postcode': '94103', 'locality': 'San Francisco', 'freeform': '66 Mint St', 'region': 'US-CA'}],
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov002 — in-bbox, names.rules has one entry
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov002',
            {'xmin': -122.487, 'ymin': 37.768, 'xmax': -122.485, 'ymax': 37.770},
            'POINT(-122.486 37.769)',
            {'primary': 'GG Park',
             'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  [{'language': 'en', 'value': 'GG Park', 'variant': 'short'}]},
            {'primary': 'park'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov003 — in-bbox, names.common and names.rules both NULL (primary
    # present so the row survives the non-empty-name import filter).
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov003',
            {'xmin': -122.411, 'ymin': 37.769, 'xmax': -122.409, 'ymax': 37.771},
            'POINT(-122.410 37.770)',
            {'primary': 'Place ov003',
             'common': NULL::MAP(VARCHAR, VARCHAR),
             'rules':  NULL::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'coffee_shop'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov004 — in-bbox, same category as ov001
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov004',
            {'xmin': -122.431, 'ymin': 37.779, 'xmax': -122.429, 'ymax': 37.781},
            'POINT(-122.430 37.780)',
            {'primary': 'Place ov004',
             'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'coffee_shop'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov005 — in-bbox, unique category
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov005',
            {'xmin': -122.401, 'ymin': 37.779, 'xmax': -122.399, 'ymax': 37.781},
            'POINT(-122.400 37.780)',
            {'primary': 'Place ov005',
             'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'unique_venue'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL

        )
    """)

    # ov006 — out of bbox
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov006',
            {'xmin': -123.001, 'ymin': 37.749, 'xmax': -122.999, 'ymax': 37.751},
            'POINT(-123.000 37.750)',
            {'primary': NULL::VARCHAR,
             'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'coffee_shop'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov007 — geometry IS NULL
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov007',
            {'xmin': -122.411, 'ymin': 37.769, 'xmax': -122.409, 'ymax': 37.771},
            NULL,
            {'primary': NULL::VARCHAR,
             'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'coffee_shop'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov008 — in-bbox, all-NULL-country addresses
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov008',
            {'xmin': -122.501, 'ymin': 37.649, 'xmax': -122.499, 'ymax': 37.651},
            'POINT(-122.500 37.650)',
            {'primary': 'No Country Place',
             'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'null_country_venue'},
            [{'country': NULL::VARCHAR, 'postcode': '94103', 'locality': 'San Francisco', 'freeform': '1 Market St', 'region': 'US-CA'}],
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov009 — in-bbox, mixed addresses
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov009',
            {'xmin': -122.351, 'ymin': 37.629, 'xmax': -122.349, 'ymax': 37.631},
            'POINT(-122.350 37.630)',
            {'primary': 'Mixed Address Place',
             'common': map([]::VARCHAR[], []::VARCHAR[]),
             'rules':  []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'mixed_addr_venue'},
            [
              {'country': NULL::VARCHAR, 'postcode': '94103', 'locality': 'San Francisco', 'freeform': '1 Market St', 'region': 'US-CA'},
              {'country': 'US', 'postcode': '94105', 'locality': 'San Francisco', 'freeform': '2 Market St', 'region': 'US-CA'}
            ],
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ------------------------------------------------------------------
    # Rows below characterize the `variants` column derivation in
    # overture_place_import.sql (names.common + names.rules -> variants).
    # Do not alter rows above; other assertions depend on them.
    # ------------------------------------------------------------------

    # ov010 — names.rules populated, names.common IS NULL (SQL NULL, not empty map).
    # Exercises the "common NULL, rules populated" combination and pins the
    # 'official' -> 'official' variant-type mapping.
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov010',
            {'xmin': -122.420, 'ymin': 37.774, 'xmax': -122.418, 'ymax': 37.776},
            'POINT(-122.419 37.775)',
            {'primary': 'Rules Only Place',
             'common': NULL::MAP(VARCHAR, VARCHAR),
             'rules':  [{'language': 'en', 'value': 'Rules Value', 'variant': 'official'}]},
            {'primary': 'coffee_shop'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov011 — names.common populated, names.rules IS NULL (SQL NULL, not empty list).
    # Exercises the "common populated, rules NULL" combination and pins the
    # default 'alternate' type assigned to every names.common entry.
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov011',
            {'xmin': -122.420, 'ymin': 37.774, 'xmax': -122.418, 'ymax': 37.776},
            'POINT(-122.419 37.775)',
            {'primary': 'Common Only Place',
             'common': map(['fr'], ['Nom Commun']),
             'rules':  NULL::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            {'primary': 'coffee_shop'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov012 — names.rules covers all four recognized variant strings plus one
    # unrecognized string ('colloquial'), pinning the full CASE mapping:
    # common->alternate, official->official, alternate->alternate,
    # short->short, anything else->alternate. Names are chosen so
    # alphabetic ORDER BY name has no ties, isolating the mapping from
    # tie-order ambiguity (see ov014 for the tie case).
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov012',
            {'xmin': -122.420, 'ymin': 37.774, 'xmax': -122.418, 'ymax': 37.776},
            'POINT(-122.419 37.775)',
            {'primary': 'All Variants Place',
             'common': NULL::MAP(VARCHAR, VARCHAR),
             'rules':  [
                 {'language': 'en', 'value': 'Zeta Common', 'variant': 'common'},
                 {'language': 'en', 'value': 'Alpha Official', 'variant': 'official'},
                 {'language': 'en', 'value': 'Mid Alternate', 'variant': 'alternate'},
                 {'language': 'en', 'value': 'Omega Short', 'variant': 'short'},
                 {'language': 'en', 'value': 'Beta Colloquial', 'variant': 'colloquial'}
             ]},
            {'primary': 'coffee_shop'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov013 — every candidate variant has a NULL or '' value, from both
    # names.common and names.rules. All must be excluded, leaving variants
    # == [] (not NULL) even though both source fields are non-empty.
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov013',
            {'xmin': -122.420, 'ymin': 37.774, 'xmax': -122.418, 'ymax': 37.776},
            'POINT(-122.419 37.775)',
            {'primary': 'Null Empty Values Place',
             'common': map(['en', 'fr'], [NULL::VARCHAR, '']),
             'rules':  [
                 {'language': 'de', 'value': NULL::VARCHAR, 'variant': 'short'},
                 {'language': 'es', 'value': '', 'variant': 'official'}
             ]},
            {'primary': 'coffee_shop'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov014 — several names across both names.common and names.rules,
    # including two entries that share the same name ('Golden Gate') so the
    # ORDER BY name tie behavior is captured. See test_import_overture.py
    # for how the tie is asserted (set equality among tied entries, not a
    # fixed order between them).
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov014',
            {'xmin': -122.420, 'ymin': 37.774, 'xmax': -122.418, 'ymax': 37.776},
            'POINT(-122.419 37.775)',
            {'primary': 'Order Tie Place',
             'common': map(['en', 'fr'], ['Golden Gate', 'Porte Doree']),
             'rules':  [
                 {'language': 'es', 'value': 'Golden Gate', 'variant': 'official'},
                 {'language': 'de', 'value': 'Zelle', 'variant': 'short'}
             ]},
            {'primary': 'coffee_shop'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov015 — the same (name, type, language) tuple appears twice within
    # names.rules alone. Pins whether variants dedupes exact-duplicate
    # entries (currently: no, both copies survive).
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov015',
            {'xmin': -122.420, 'ymin': 37.774, 'xmax': -122.418, 'ymax': 37.776},
            'POINT(-122.419 37.775)',
            {'primary': 'Duplicate Within Rules Place',
             'common': NULL::MAP(VARCHAR, VARCHAR),
             'rules':  [
                 {'language': 'en', 'value': 'Repeat Name', 'variant': 'alternate'},
                 {'language': 'en', 'value': 'Repeat Name', 'variant': 'alternate'}
             ]},
            {'primary': 'coffee_shop'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    # ov016 — the same (name, type, language) tuple arises once from
    # names.common (which is always typed 'alternate') and once from an
    # explicit names.rules entry with variant='alternate'. Pins whether
    # variants dedupes across the two source fields (currently: no).
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov016',
            {'xmin': -122.420, 'ymin': 37.774, 'xmax': -122.418, 'ymax': 37.776},
            'POINT(-122.419 37.775)',
            {'primary': 'Duplicate Across Common And Rules Place',
             'common': map(['en'], ['Repeat Name']),
             'rules':  [
                 {'language': 'en', 'value': 'Repeat Name', 'variant': 'alternate'}
             ]},
            {'primary': 'coffee_shop'},
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )
    """)

    conn.execute(f"COPY tmp_ov TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()

    return str(base / "*.parquet")


@pytest.fixture(scope="session")
def density_parquet(overture_parquet, tmp_path_factory):
    """Extract z15 density tiles from Overture parquet; return path to density.parquet.

    This fixture depends on overture_parquet and runs stage_density_extract()
    to produce a shared density parquet file that all importance tests can use.
    The fixture is session-scoped so the density extraction runs once per test run.

    The density parquet has schema (tile_qk15 VARCHAR, density_score DOUBLE,
    tile_xmin DOUBLE, tile_ymin DOUBLE, tile_xmax DOUBLE, tile_ymax DOUBLE).
    """
    import time
    from garganorn.stages import stage_density_extract

    base = tmp_path_factory.mktemp("density_parquet")
    output_path = base / "density.parquet"

    stage_density_extract(overture_parquet, str(output_path), time.monotonic())

    return str(output_path)


@pytest.fixture(scope="session")
def osm_parquet(tmp_path_factory):
    """Write OSM-schema node and way parquet files; return dict with 'node' and 'way' globs."""

    base = tmp_path_factory.mktemp("osm_parquet")
    node_path = base / "node_data.parquet"
    way_path = base / "way_data.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")

    # --- Node parquet ---
    conn.execute("""
        CREATE TABLE tmp_nodes (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            lat     DOUBLE,
            lon     DOUBLE
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1001,
            map(['name','amenity'], ['Tartine Manufactory','cafe']),
            37.7612, -122.4195
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1002,
            map(['name','leisure'], ['Dolores Park','park']),
            37.7596, -122.4269
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1003,
            map(['amenity'], ['cafe']),
            37.7700, -122.4100
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1004,
            map(['name','shop'], ['Faraway Place','bakery']),
            37.9000, -123.5000
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1005,
            map(['name','amenity','alt_name','name:fr'],
                ['Alt Name Cafe','cafe','The Old Spot','Café Alt']),
            37.7750, -122.4200
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            9001,
            map([]::VARCHAR[], []::VARCHAR[]),
            37.8199, -122.4786
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            9002,
            map([]::VARCHAR[], []::VARCHAR[]),
            37.8197, -122.4788
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1006,
            map(['name','traffic_calming'], ['No Category Node','bump']),
            37.7760, -122.4150
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1007,
            map(['name','old_name','official_name','short_name','loc_name','int_name','amenity'],
                ['Multi Variant Place','Former Name','Official Title','MVP','Local Spot','International Name','cafe']),
            37.7770, -122.4160
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1008,
            map(['name','amenity','name:prefix','name:prefix:ru','alt_name'],
                ['Drop Suffix Place','cafe','wieś','город','Kept Name']),
            37.7780, -122.4170
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1009,
            map(['name','amenity','alt_name'],
                ['Semicolon Place','cafe','Foo;Bar']),
            37.7790, -122.4180
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1010,
            map(['name','amenity','name:zh'],
                ['Duplicate Case','cafe','Duplicate Case;真名']),
            37.7800, -122.4190
        )
    """)

    conn.execute("""
        INSERT INTO tmp_nodes VALUES (
            1011,
            map(['name','amenity','old_name:en','name:abbr','name:-2024','name:carnaval'],
                ['Extended And Override Place','cafe','Historic English Name','EAOP',
                 'Renamed 2024','Carnival Name']),
            37.7810, -122.4200
        )
    """)

    conn.execute(f"COPY tmp_nodes TO '{node_path}' (FORMAT PARQUET)")

    # --- Way parquet ---
    conn.execute("""
        CREATE TABLE tmp_ways (
            id      BIGINT,
            tags    MAP(VARCHAR, VARCHAR),
            nds     STRUCT(ref BIGINT)[]
        )
    """)

    conn.execute("""
        INSERT INTO tmp_ways VALUES (
            2001,
            map(['name','bridge','tourism'], ['Golden Gate Bridge','yes','attraction']),
            [{'ref': 9001}, {'ref': 9002}]::STRUCT(ref BIGINT)[]
        )
    """)

    conn.execute("""
        INSERT INTO tmp_ways VALUES (
            2002,
            map(['name','tourism','alt_name','old_name:en'],
                ['Way Variant Bridge','attraction','Old Bridge Name','Historic Bridge Name']),
            [{'ref': 9001}, {'ref': 9002}]::STRUCT(ref BIGINT)[]
        )
    """)

    conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
    conn.close()

    return {
        "node": str(base / "node_data.parquet"),
        "way": str(base / "way_data.parquet"),
    }


@pytest.fixture(scope="session")
def division_parquet(tmp_path_factory):
    """Write Overture division and division_area parquet files for testing.

    Returns a tuple of (division_parquet_path, division_area_parquet_path).
    """
    base = tmp_path_factory.mktemp("division_parquet")
    division_path = base / "division.parquet"
    division_area_path = base / "division_area.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")

    # Division parquet (metadata)
    conn.execute("""
        CREATE TABLE tmp_division (
            id VARCHAR,
            names STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR), rules STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]),
            subtype VARCHAR,
            country VARCHAR,
            region VARCHAR,
            wikidata VARCHAR,
            population BIGINT,
            parent_division_id VARCHAR,
            hierarchies STRUCT(division_id VARCHAR, subtype VARCHAR, name VARCHAR)[][]
        )
    """)

    # Insert SF locality. Its chain's div_region_ca member is deliberately
    # misnamed ("Not California") to pin that a record's own names.primary
    # in the imported set wins over the name carried in the hierarchies
    # struct. div_missing_county never
    # appears as its own tmp_division row, so the containment semi-join has
    # an ancestor to drop.
    conn.execute("""
        INSERT INTO tmp_division VALUES (
            'div_locality_sf',
            {'primary': 'San Francisco', 'common': map(['en'], ['San Francisco']), 'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            'locality',
            'US',
            'US-CA',
            'Q62',
            874961,
            NULL,
            [[
                {'division_id': 'div_country_us', 'subtype': 'country', 'name': 'United States'},
                {'division_id': 'div_missing_county', 'subtype': 'county', 'name': 'Phantom County'},
                {'division_id': 'div_region_ca', 'subtype': 'region', 'name': 'Not California'},
                {'division_id': 'div_locality_sf', 'subtype': 'locality', 'name': 'San Francisco'}
            ]]::STRUCT(division_id VARCHAR, subtype VARCHAR, name VARCHAR)[][]
        )
    """)

    # Insert US country
    conn.execute("""
        INSERT INTO tmp_division VALUES (
            'div_country_us',
            {'primary': 'United States', 'common': map(['en'], ['United States']), 'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            'country',
            'US',
            NULL,
            'Q30',
            331000000,
            NULL,
            [[
                {'division_id': 'div_country_us', 'subtype': 'country', 'name': 'United States'}
            ]]::STRUCT(division_id VARCHAR, subtype VARCHAR, name VARCHAR)[][]
        )
    """)

    # Insert region
    conn.execute("""
        INSERT INTO tmp_division VALUES (
            'div_region_ca',
            {'primary': 'California', 'common': map(['en'], ['California']), 'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
            'region',
            'US',
            'US-CA',
            'Q99',
            39538223,
            'div_country_us',
            [[
                {'division_id': 'div_country_us', 'subtype': 'country', 'name': 'United States'},
                {'division_id': 'div_region_ca', 'subtype': 'region', 'name': 'California'}
            ]]::STRUCT(division_id VARCHAR, subtype VARCHAR, name VARCHAR)[][]
        )
    """)

    conn.execute(f"COPY tmp_division TO '{division_path}' (FORMAT PARQUET)")

    # Division area parquet (geometries). admin_level here mirrors the genuine
    # upstream Overture division_area schema (source input, not touched by the
    # atgeo level vocabulary). overture_division_import.sql drops the
    # admin_level plumbing since level is derived from division.subtype
    # instead, so this column is dead input; kept here only because it is a
    # faithful copy of Overture's real column set.
    conn.execute("""
        CREATE TABLE tmp_division_area (
            division_id VARCHAR,
            admin_level INTEGER,
            is_land BOOLEAN,
            geometry VARCHAR,
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)
        )
    """)

    # SF locality area
    conn.execute("""
        INSERT INTO tmp_division_area VALUES (
            'div_locality_sf',
            3,
            true,
            'POLYGON((-122.55 37.6, -122.55 37.85, -122.30 37.85, -122.30 37.6, -122.55 37.6))',
            {'xmin': -122.55, 'ymin': 37.6, 'xmax': -122.30, 'ymax': 37.85}
        )
    """)

    # US country area
    conn.execute("""
        INSERT INTO tmp_division_area VALUES (
            'div_country_us',
            1,
            true,
            'POLYGON((-125 25, -125 49, -65 49, -65 25, -125 25))',
            {'xmin': -125.0, 'ymin': 25.0, 'xmax': -65.0, 'ymax': 49.0}
        )
    """)

    # California region area
    conn.execute("""
        INSERT INTO tmp_division_area VALUES (
            'div_region_ca',
            2,
            true,
            'POLYGON((-124.5 32.5, -124.5 42.0, -114.1 42.0, -114.1 32.5, -124.5 32.5))',
            {'xmin': -124.5, 'ymin': 32.5, 'xmax': -114.1, 'ymax': 42.0}
        )
    """)

    conn.execute(f"COPY tmp_division_area TO '{division_area_path}' (FORMAT PARQUET)")
    conn.close()

    return (str(division_path), str(division_area_path))
