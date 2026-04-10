"""Shared fixtures for garganorn tests."""
import pytest
import duckdb

from garganorn.database import FoursquareOSP, OvertureMaps, OpenStreetMap
from tests.quadtree_helpers import FSQ_ROWS


# ---------------------------------------------------------------------------
# San Francisco area test data
# ---------------------------------------------------------------------------

FSQ_PLACES = [
    # fsq_place_id, name, latitude, longitude, address, locality, postcode, region, admin_region, post_town, po_box, country
    ("fsq001", "Blue Bottle Coffee", 37.7749, -122.4194, "66 Mint St", "San Francisco", "94103", "CA", "CA", None, None, "US"),
    ("fsq002", "Golden Gate Park", 37.7694, -122.4862, "501 Stanyan St", "San Francisco", "94117", "CA", "CA", None, None, "US"),
    ("fsq003", "Ferry Building Marketplace", 37.7955, -122.3937, "1 Ferry Building", "San Francisco", "94111", "CA", "CA", None, None, "US"),
    ("fsq004", "Tartine Bakery", 37.7612, -122.4242, "600 Guerrero St", "San Francisco", "94110", "CA", "CA", None, None, "US"),
    ("fsq005", "Alcatraz Island", 37.8270, -122.4230, None, "San Francisco", "94133", "CA", "CA", None, None, "US"),
    # Token-blending test fixtures:
    # Query "North End Diner" → "Diner North End" should rank above "North End Pub"
    # Full-string JW favors "North End Pub" (longer prefix match); token JW favors "Diner North End"
    ("fsq006", "Diner North End", 37.7749, -122.4350, "1 North End Ave", "San Francisco", "94129", "CA", "CA", None, None, "US"),
    ("fsq007", "North End Pub", 37.7748, -122.4351, "2 North End Ave", "San Francisco", "94129", "CA", "CA", None, None, "US"),
    # Multi-token scaling test fixtures (Strategy E):
    # 5-token names for verifying blending at higher token counts
    ("fsq008", "San Francisco International Airport Terminal", 37.6213, -122.3790, "International Terminal", "San Francisco", "94128", "CA", "CA", None, None, "US"),
    ("fsq009", "University Medical Center", 37.7629, -122.4577, "505 Parnassus Ave", "San Francisco", "94143", "CA", "CA", None, None, "US"),
    ("fsq010", "North Beach Community Garden Center", 37.8008, -122.4105, "1 Garden Ln", "San Francisco", "94133", "CA", "CA", None, None, "US"),
    # Cutoff survival test fixtures (Strategy E):
    # "Restaurant Park Avenue" has low full_jw but high token_jw for query "Park Avenue Restaurant".
    # The 25 "Park Avenue ..." variants compete via full_jw; the reordered name must survive any top-N cutoff.
    ("fsq011", "Restaurant Park Avenue", 37.7500, -122.4100, "1 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq012", "Park Avenue Cafe", 37.7501, -122.4101, "2 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq013", "Park Avenue Bar", 37.7502, -122.4102, "3 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq014", "Park Avenue Grill", 37.7503, -122.4103, "4 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq015", "Park Avenue Bistro", 37.7504, -122.4104, "5 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq016", "Park Avenue Bakery", 37.7505, -122.4105, "6 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq017", "Park Avenue Diner", 37.7506, -122.4106, "7 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq018", "Park Avenue Lounge", 37.7507, -122.4107, "8 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq019", "Park Avenue Tavern", 37.7508, -122.4108, "9 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq020", "Park Avenue Kitchen", 37.7509, -122.4109, "10 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq021", "Park Avenue Deli", 37.7510, -122.4110, "11 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq022", "Park Avenue Pizza", 37.7511, -122.4111, "12 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq023", "Park Avenue Sushi", 37.7512, -122.4112, "13 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq024", "Park Avenue Noodles", 37.7513, -122.4113, "14 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq025", "Park Avenue Burgers", 37.7514, -122.4114, "15 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq026", "Park Avenue Tacos", 37.7515, -122.4115, "16 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq027", "Park Avenue Steakhouse", 37.7516, -122.4116, "17 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq028", "Park Avenue Ramen", 37.7517, -122.4117, "18 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq029", "Park Avenue Tapas", 37.7518, -122.4118, "19 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq030", "Park Avenue Curry", 37.7519, -122.4119, "20 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq031", "Park Avenue Wok", 37.7520, -122.4120, "21 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq032", "Park Avenue Smokehouse", 37.7521, -122.4121, "22 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq033", "Park Avenue Seafood", 37.7522, -122.4122, "23 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq034", "Park Avenue Brunch", 37.7523, -122.4123, "24 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
    ("fsq035", "Park Avenue Patisserie", 37.7524, -122.4124, "25 Park Ave", "San Francisco", "94107", "CA", "CA", None, None, "US"),
]

OVERTURE_PLACES = [
    # id, name, latitude, longitude, address_freeform, locality, postcode, region, country
    ("ovr001", "Philz Coffee", 37.7749, -122.4194, "201 Berry St", "San Francisco", "94158", "US-CA", "US"),
    ("ovr002", "Dolores Park", 37.7596, -122.4269, "Dolores St & 19th St", "San Francisco", "94114", "US-CA", "US"),
    ("ovr003", "Coit Tower", 37.8024, -122.4058, "1 Telegraph Hill Blvd", "San Francisco", "94133", "US-CA", "US"),
    ("ovr004", "Anchor Brewing", 37.7688, -122.4125, "1705 Mariposa St", "San Francisco", "94107", "US-CA", "US"),
    ("ovr005", "Lombard Street", 37.8021, -122.4187, "Lombard St", "San Francisco", "94133", "US-CA", "US"),
    # Token-blending test fixtures:
    # Query "North End Diner" → "Diner North End" should rank above "North End Pub"
    # Full-string JW favors "North End Pub" (longer prefix match); token JW favors "Diner North End"
    ("ovr006", "Diner North End", 37.7749, -122.4350, "1 North End Ave", "San Francisco", "94129", "US-CA", "US"),
    ("ovr007", "North End Pub", 37.7748, -122.4351, "2 North End Ave", "San Francisco", "94129", "US-CA", "US"),
]


def _generate_trigrams(name):
    """Generate distinct trigrams from a place name (lowercased full string)."""
    s = name.lower()
    trigrams = set()
    for i in range(len(s) - 2):
        trigrams.add(s[i:i+3])
    return trigrams


def _create_fsq_db(db_path):
    """Create a FSQ DuckDB database with test data and trigram name_index."""
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE places (
            fsq_place_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            geom GEOMETRY,
            address VARCHAR,
            locality VARCHAR,
            postcode VARCHAR,
            region VARCHAR,
            admin_region VARCHAR,
            post_town VARCHAR,
            po_box VARCHAR,
            country VARCHAR,
            date_created DATE,
            date_refreshed DATE,
            date_closed DATE,
            tel VARCHAR,
            website VARCHAR,
            email VARCHAR,
            facebook_id VARCHAR,
            instagram VARCHAR,
            twitter VARCHAR,
            fsq_category_ids VARCHAR[],
            fsq_category_labels VARCHAR[],
            placemaker_url VARCHAR,
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            importance INTEGER,
            variants STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT []
        )
    """)

    fsq_importance = {
        "fsq001": 75,
        "fsq002": 60,
        "fsq003": 85,
        "fsq004": 55,
        "fsq005": 90,
        "fsq006": 70,
        "fsq007": 70,
        # Multi-token scaling fixtures
        "fsq008": 70,
        "fsq009": 70,
        "fsq010": 70,
        # Cutoff survival fixtures
        "fsq011": 70,
        "fsq012": 70,
        "fsq013": 70,
        "fsq014": 70,
        "fsq015": 70,
        "fsq016": 70,
        "fsq017": 70,
        "fsq018": 70,
        "fsq019": 70,
        "fsq020": 70,
        "fsq021": 70,
        "fsq022": 70,
        "fsq023": 70,
        "fsq024": 70,
        "fsq025": 70,
        "fsq026": 70,
        "fsq027": 70,
        "fsq028": 70,
        "fsq029": 70,
        "fsq030": 70,
        "fsq031": 70,
        "fsq032": 70,
        "fsq033": 70,
        "fsq034": 70,
        "fsq035": 70,
    }

    for row in FSQ_PLACES:
        fsq_id, name, lat, lon, address, locality, postcode, region, admin_region, post_town, po_box, country = row
        conn.execute("""
            INSERT INTO places VALUES (
                ?, ?, ?, ?,
                ST_Point(?, ?),
                ?, ?, ?, ?, ?, ?, ?,
                ?,
                '2021-01-01', '2022-01-01', NULL,
                NULL, NULL, NULL, NULL, NULL, NULL,
                ARRAY[]::VARCHAR[], ARRAY[]::VARCHAR[],
                NULL,
                {'xmin': ?-0.001, 'ymin': ?-0.001, 'xmax': ?+0.001, 'ymax': ?+0.001},
                ?,
                []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
            )
        """, [fsq_id, name, lat, lon, lon, lat,
              address, locality, postcode, region, admin_region, post_town, po_box,
              country,
              lon, lat, lon, lat,
              fsq_importance[fsq_id]])

    conn.execute("""
        CREATE TABLE name_index (
            trigram VARCHAR,
            fsq_place_id VARCHAR,
            name VARCHAR,
            norm_name VARCHAR,
            importance INTEGER,
            is_variant BOOLEAN DEFAULT FALSE
        )
    """)
    for row in FSQ_PLACES:
        fsq_id, name, lat, lon, address, locality, postcode, region, _, _, _, country = row
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?, FALSE)
            """, [trigram, fsq_id, name, FoursquareOSP._strip_accents(name.lower()),
                  fsq_importance[fsq_id]])

    conn.close()


def _create_overture_db(db_path):
    """Create an Overture DuckDB database with test data and trigram name_index."""
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

    conn.execute("""
        CREATE TABLE name_index (
            trigram VARCHAR,
            id VARCHAR,
            name VARCHAR,
            norm_name VARCHAR,
            importance INTEGER,
            is_variant BOOLEAN DEFAULT FALSE
        )
    """)
    for row in OVERTURE_PLACES:
        ovr_id, name, lat, lon, freeform, locality, postcode, region, country = row
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?, FALSE)
            """, [trigram, ovr_id, name, OvertureMaps._strip_accents(name.lower()),
                  ovr_importance[ovr_id]])

    # Index variant names for Overture places
    for ovr_id, variants in ovr_variants.items():
        importance = ovr_importance[ovr_id]
        for v in variants:
            variant_name = v["name"]
            norm_variant = OvertureMaps._strip_accents(variant_name.lower())
            for trigram in _generate_trigrams(variant_name):
                conn.execute("""
                    INSERT INTO name_index VALUES (?, ?, ?, ?, ?, TRUE)
                """, [trigram, ovr_id, variant_name, norm_variant, importance])

    conn.close()


# ---------------------------------------------------------------------------
# Session-scoped path fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def fsq_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("fsq") / "fsq.duckdb"
    _create_fsq_db(db_path)
    return db_path


@pytest.fixture(scope="session")
def overture_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("overture") / "overture.duckdb"
    _create_overture_db(db_path)
    return db_path


# ---------------------------------------------------------------------------
# Function-scoped DB instance fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def fsq_db(fsq_db_path):
    db = FoursquareOSP(fsq_db_path)
    db.connect()
    yield db
    db.close()


@pytest.fixture
def overture_db(overture_db_path):
    db = OvertureMaps(overture_db_path)
    db.connect()
    yield db
    db.close()


# ---------------------------------------------------------------------------
# OSM test data
# ---------------------------------------------------------------------------

OSM_PLACES = [
    # osm_type, osm_id, name, latitude, longitude, primary_category, tags (dict)
    ("n", 240109189, "Tartine Manufactory", 37.7612, -122.4195,
     "amenity=cafe",
     {"cuisine": "coffee", "addr:street": "Alabama St", "addr:housenumber": "595",
      "addr:city": "San Francisco", "addr:postcode": "94110", "addr:country": "US"}),
    ("n", 1234567, "UCSF Medical Center", 37.7631, -122.4576,
     "amenity=hospital",
     {"healthcare": "hospital", "addr:street": "Parnassus Ave", "addr:housenumber": "505",
      "addr:city": "San Francisco", "addr:country": "US"}),
    ("w", 50637691, "Dolores Park", 37.7596, -122.4269,
     "leisure=park",
     {}),
    ("n", 9876543, "Bi-Rite Market", 37.7614, -122.4253,
     "shop=supermarket",
     {"addr:street": "18th St", "addr:housenumber": "3639",
      "addr:city": "San Francisco", "addr:postcode": "94110", "addr:country": "US"}),
    ("w", 88776655, "Caltrain Station", 37.7764, -122.3942,
     "railway=station",
     {}),
    # Token-blending test fixtures:
    # Query "North End Diner" → "Diner North End" should rank above "North End Pub"
    # Full-string JW favors "North End Pub" (longer prefix match); token JW favors "Diner North End"
    ("n", 11110001, "Diner North End", 37.7749, -122.4350,
     "amenity=restaurant",
     {}),
    ("n", 11110002, "North End Pub", 37.7748, -122.4351,
     "amenity=pub",
     {}),
]

OSM_IMPORTANCE = {
    "n240109189": 65,
    "n1234567": 80,
    "w50637691": 55,
    "n9876543": 60,
    "w88776655": 70,
    "n11110001": 70,
    "n11110002": 70,
}


def _create_osm_db(db_path):
    """Create an OSM DuckDB database with test data and trigram name_index."""
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE places (
            osm_type VARCHAR,
            osm_id BIGINT,
            rkey VARCHAR,
            name VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            geom GEOMETRY,
            primary_category VARCHAR,
            tags MAP(VARCHAR, VARCHAR),
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            importance INTEGER,
            variants STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[] DEFAULT []
        )
    """)

    # Variant data for OSM places
    osm_variants = {
        "n240109189": [
            {"name": "Tartine Manufactory SF", "type": "alternate", "language": "en"},
            {"name": "Old Tartine", "type": "historical", "language": None},
            {"name": "Tartine MFY", "type": "short", "language": None},
        ],
    }

    for row in OSM_PLACES:
        osm_type, osm_id, name, lat, lon, primary_category, tags = row
        rkey = osm_type + str(osm_id)
        importance = OSM_IMPORTANCE[rkey]
        # Build MAP literal: MAP {'key1': 'val1', 'key2': 'val2'}
        if tags:
            map_entries = ", ".join(f"'{k}': '{v}'" for k, v in tags.items())
            map_literal = f"MAP {{{map_entries}}}"
        else:
            map_literal = "MAP()::MAP(VARCHAR, VARCHAR)"
        variants = osm_variants.get(rkey, [])
        if variants:
            def _fmt_variant(v):
                lang = f"'{v['language']}'" if v.get("language") else "NULL"
                return f"{{'name': '{v['name']}', 'type': '{v['type']}', 'language': {lang}}}"
            variant_sql = "[" + ", ".join(_fmt_variant(v) for v in variants) + "]::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]"
        else:
            variant_sql = "[]::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]"
        conn.execute(f"""
            INSERT INTO places VALUES (
                ?, ?, ?, ?, ?, ?,
                ST_Point(?, ?),
                ?,
                {map_literal},
                {{'xmin': ?-0.001, 'ymin': ?-0.001, 'xmax': ?+0.001, 'ymax': ?+0.001}},
                ?,
                {variant_sql}
            )
        """, [osm_type, osm_id, rkey, name, lat, lon, lon, lat,
              primary_category,
              lon, lat, lon, lat,
              importance])

    conn.execute("""
        CREATE TABLE name_index (
            trigram VARCHAR,
            rkey VARCHAR,
            name VARCHAR,
            norm_name VARCHAR,
            importance INTEGER,
            is_variant BOOLEAN DEFAULT FALSE
        )
    """)
    for row in OSM_PLACES:
        osm_type, osm_id, name, lat, lon, primary_category, tags = row
        rkey = osm_type + str(osm_id)
        importance = OSM_IMPORTANCE[rkey]
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?, FALSE)
            """, [trigram, rkey, name, OpenStreetMap._strip_accents(name.lower()),
                  importance])

    # Index variant names for OSM places
    for rkey, variants in osm_variants.items():
        importance = OSM_IMPORTANCE[rkey]
        for v in variants:
            variant_name = v["name"]
            norm_variant = OpenStreetMap._strip_accents(variant_name.lower())
            for trigram in _generate_trigrams(variant_name):
                conn.execute("""
                    INSERT INTO name_index VALUES (?, ?, ?, ?, ?, TRUE)
                """, [trigram, rkey, variant_name, norm_variant, importance])

    conn.close()


# ---------------------------------------------------------------------------
# OSM session-scoped path fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def osm_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("osm") / "osm.duckdb"
    _create_osm_db(db_path)
    return db_path


# ---------------------------------------------------------------------------
# OSM function-scoped DB instance fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def osm_db(osm_db_path):
    db = OpenStreetMap(osm_db_path)
    db.connect()
    yield db
    db.close()


from garganorn.boundaries import BoundaryLookup, OvertureDivision


# ---------------------------------------------------------------------------
# Division boundary test data (Overture divisions schema)
# ---------------------------------------------------------------------------

# id, admin_level, wkt_geom, min_lat, min_lon, max_lat, max_lon,
#   names (dict or None), subtype, country, region, wikidata, population, importance, variants
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
        "div_country_us", 1,
        "POLYGON((-125 24, -125 50, -66 50, -66 24, -125 24))",
        24.0, -125.0, 50.0, -66.0,
        {"primary": "United States", "common": {}, "rules": []},
        "country", "US", None, "Q30", 331000000,
        0, [],
    ),
    (
        "div_region_ca", 2,
        "POLYGON((-125 34, -125 42, -118 42, -118 34, -125 34))",
        34.0, -125.0, 42.0, -118.0,
        {"primary": "California", "common": {"fr": "Californie"}, "rules": []},
        "region", "US", "US-CA", "Q99", 39000000,
        0, [],
    ),
    (
        "div_locality_sf", 3,
        "POLYGON((-122.55 37.6, -122.55 37.85, -122.3 37.85, -122.3 37.6, -122.55 37.6))",
        37.6, -122.55, 37.85, -122.3,
        {"primary": "San Francisco", "common": {"es": "San Francisco", "zh": "\u65e7\u91d1\u5c71"}, "rules": []},
        "locality", "US", "US-CA", "Q62", 874961,
        0, [],
    ),
    (
        "div_borough_manhattan", 4,
        "POLYGON((-74.05 40.68, -74.05 40.88, -73.90 40.88, -73.90 40.68, -74.05 40.68))",
        40.68, -74.05, 40.88, -73.90,
        {"primary": "Manhattan", "common": {}, "rules": []},
        "locality", "US", "US-NY", "Q11299", 1629153,
        0, [],
    ),
]


def _create_division_db(db_path):
    """Create a division-schema boundary DB (enriched places table for OvertureDivision)."""
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE places (
            id VARCHAR,
            geometry GEOMETRY,
            admin_level INTEGER,
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
        (bid, admin_level, wkt, min_lat, min_lon, max_lat, max_lon,
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
                INSERT INTO places (id, geometry, admin_level, names, subtype, country, region,
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
            """, [bid, wkt, admin_level,
                  subtype, country, region, wikidata, population,
                  min_lat, max_lat, min_lon, max_lon,
                  importance])
        else:
            conn.execute(f"""
                INSERT INTO places (id, geometry, admin_level, names, subtype, country, region,
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
            """, [bid, wkt, admin_level] + names_params + [
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
def boundary_lookup(division_db_path):
    bl = BoundaryLookup(division_db_path)
    bl.connect()
    yield bl
    bl.close()


@pytest.fixture
def division_db(division_db_path):
    db = OvertureDivision(division_db_path)
    db.connect()
    yield db
    db.close()


# ---------------------------------------------------------------------------
# Quadtree parquet fixtures (session-scoped)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def fsq_parquet(tmp_path_factory):
    """Write a single FSQ-schema parquet file and return a glob path for it."""

    base = tmp_path_factory.mktemp("fsq_parquet")
    parquet_path = base / "fsq_data.parquet"

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE tmp_fsq (
            fsq_place_id        VARCHAR,
            name                VARCHAR,
            latitude            DOUBLE,
            longitude           DOUBLE,
            bbox                STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            geom                VARCHAR,
            date_refreshed      DATE,
            date_closed         DATE,
            date_created        DATE,
            address             VARCHAR,
            locality            VARCHAR,
            region              VARCHAR,
            postcode            VARCHAR,
            country             VARCHAR,
            admin_region        VARCHAR,
            post_town           VARCHAR,
            po_box              VARCHAR,
            tel                 VARCHAR,
            website             VARCHAR,
            email               VARCHAR,
            facebook_id         VARCHAR,
            instagram           VARCHAR,
            twitter             VARCHAR,
            fsq_category_ids    VARCHAR[],
            fsq_category_labels VARCHAR[],
            placemaker_url      VARCHAR
        )
    """)

    for row in FSQ_ROWS:
        fsq_id, name, lat, lon, date_ref, date_closed, geom_wkt, cat_ids, _ = row
        bbox_xmin = lon - 0.001
        bbox_xmax = lon + 0.001
        bbox_ymin = lat - 0.001
        bbox_ymax = lat + 0.001
        cat_str = "[" + ", ".join(f"'{c}'" for c in cat_ids) + "]"

        closed_val = f"'{date_closed}'" if date_closed else "NULL"
        geom_val = f"'{geom_wkt}'" if geom_wkt else "NULL"

        conn.execute(f"""
            INSERT INTO tmp_fsq VALUES (
                '{fsq_id}', '{name}', {lat}, {lon},
                {{'xmin': {bbox_xmin}, 'ymin': {bbox_ymin},
                  'xmax': {bbox_xmax}, 'ymax': {bbox_ymax}}},
                {geom_val},
                '{date_ref}',
                {closed_val},
                NULL,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                {cat_str}::VARCHAR[],
                NULL::VARCHAR[], NULL
            )
        """)

    conn.execute(f"COPY tmp_fsq TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()

    return str(base / "*.parquet")


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

    # ov003 — in-bbox, names IS NULL
    conn.execute("""
        INSERT INTO tmp_ov VALUES (
            'ov003',
            {'xmin': -122.411, 'ymin': 37.769, 'xmax': -122.409, 'ymax': 37.771},
            'POINT(-122.410 37.770)',
            NULL,
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
            {'primary': NULL::VARCHAR,
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
            {'primary': NULL::VARCHAR,
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

    conn.execute(f"COPY tmp_ov TO '{parquet_path}' (FORMAT PARQUET)")
    conn.close()

    return str(base / "*.parquet")


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
            map(['name','highway'], ['No Category Node','crossing']),
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

    conn.execute(f"COPY tmp_ways TO '{way_path}' (FORMAT PARQUET)")
    conn.close()

    return {
        "node": str(base / "node_data.parquet"),
        "way": str(base / "way_data.parquet"),
    }
