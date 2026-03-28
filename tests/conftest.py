"""Shared fixtures for garganorn tests."""
import pytest
import duckdb

from garganorn.database import FoursquareOSP, OvertureMaps, OpenStreetMap


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
            importance INTEGER
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
                ?
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
            importance INTEGER
        )
    """)
    for row in FSQ_PLACES:
        fsq_id, name, lat, lon, address, locality, postcode, region, _, _, _, country = row
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?)
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
            importance INTEGER
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

    for row in OVERTURE_PLACES:
        ovr_id, name, lat, lon, freeform, locality, postcode, region, country = row
        conn.execute("""
            INSERT INTO places VALUES (
                ?, ST_Point(?, ?),
                {'xmin': ?-0.001, 'ymin': ?-0.001, 'xmax': ?+0.001, 'ymax': ?+0.001},
                {'primary': ?},
                {'primary': NULL},
                [{'country': ?, 'postcode': ?, 'locality': ?, 'freeform': ?, 'region': ?}],
                NULL, NULL, NULL, NULL,
                NULL,
                0.9, 1, NULL,
                ?
            )
        """, [ovr_id, lon, lat,
              lon, lat, lon, lat,
              name,
              country, postcode, locality, freeform, region,
              ovr_importance[ovr_id]])

    # Note: The production schema documents a RTREE index on bbox, but DuckDB
    # requires a GEOMETRY column for RTREE. The production queries use bbox
    # struct field range comparisons, which don't require an explicit index.
    # We skip creating the RTREE index here.

    conn.execute("""
        CREATE TABLE name_index (
            trigram VARCHAR,
            id VARCHAR,
            name VARCHAR,
            norm_name VARCHAR,
            importance INTEGER
        )
    """)
    for row in OVERTURE_PLACES:
        ovr_id, name, lat, lon, freeform, locality, postcode, region, country = row
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?)
            """, [trigram, ovr_id, name, OvertureMaps._strip_accents(name.lower()),
                  ovr_importance[ovr_id]])

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
            importance INTEGER
        )
    """)

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
        conn.execute(f"""
            INSERT INTO places VALUES (
                ?, ?, ?, ?, ?, ?,
                ST_Point(?, ?),
                ?,
                {map_literal},
                {{'xmin': ?-0.001, 'ymin': ?-0.001, 'xmax': ?+0.001, 'ymax': ?+0.001}},
                ?
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
            importance INTEGER
        )
    """)
    for row in OSM_PLACES:
        osm_type, osm_id, name, lat, lon, primary_category, tags = row
        rkey = osm_type + str(osm_id)
        importance = OSM_IMPORTANCE[rkey]
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?)
            """, [trigram, rkey, name, OpenStreetMap._strip_accents(name.lower()),
                  importance])

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


from garganorn.boundaries import BoundaryLookup, WhosOnFirst

# ---------------------------------------------------------------------------
# WoF boundary test data
# ---------------------------------------------------------------------------

# wof_id, name, placetype, level, lat, lon, country, wkt_geom,
# min_lat, min_lon, max_lat, max_lon, names_json, concordances
WOF_BOUNDARIES = [
    (102191575, "North America", "continent", 0, 40.0, -100.0, "XX",
     "POLYGON((-130 20, -130 55, -60 55, -60 20, -130 20))",
     20.0, -130.0, 55.0, -60.0, None, None),
    (85633793, "United States", "country", 10, 39.0, -98.0, "US",
     "POLYGON((-125 24, -125 50, -66 50, -66 24, -125 24))",
     24.0, -125.0, 50.0, -66.0, None, '{"wk:id": "Q30"}'),
    (85688637, "California", "region", 25, 37.0, -120.0, "US",
     "POLYGON((-125 34, -125 42, -118 42, -118 34, -125 34))",
     34.0, -125.0, 42.0, -118.0,
     '[{"name": "California", "language": "eng", "variant": "preferred"}, '
     '{"name": "Californie", "language": "fra", "variant": "preferred"}]',
     '{"wk:id": "Q99", "gn:id": "5332921"}'),
    (85922583, "San Francisco", "locality", 50, 37.7749, -122.4194, "US",
     "POLYGON((-122.55 37.6, -122.55 37.85, -122.3 37.85, -122.3 37.6, -122.55 37.6))",
     37.6, -122.55, 37.85, -122.3,
     '[{"name": "San Francisco", "language": "eng", "variant": "preferred"}, '
     '{"name": "\u65e7\u91d1\u5c71", "language": "zho", "variant": "preferred"}]',
     '{"wk:id": "Q62", "gn:id": "5391959"}'),
    (85977539, "Manhattan", "borough", 55, 40.7831, -73.9712, "US",
     "POLYGON((-74.05 40.68, -74.05 40.88, -73.90 40.88, -73.90 40.68, -74.05 40.68))",
     40.68, -74.05, 40.88, -73.90, None, None),
]


def _create_wof_db(db_path):
    """Create a WoF boundary DuckDB with test polygons."""
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE boundaries (
            wof_id BIGINT,
            rkey VARCHAR,
            name VARCHAR,
            placetype VARCHAR,
            level INTEGER,
            latitude DOUBLE,
            longitude DOUBLE,
            geom GEOMETRY,
            country VARCHAR,
            min_latitude DOUBLE,
            min_longitude DOUBLE,
            max_latitude DOUBLE,
            max_longitude DOUBLE,
            names_json VARCHAR,
            concordances VARCHAR
        )
    """)
    for row in WOF_BOUNDARIES:
        wof_id, name, placetype, level, lat, lon, country, wkt, \
            min_lat, min_lon, max_lat, max_lon, names_json, concordances = row
        conn.execute("""
            INSERT INTO boundaries VALUES (
                ?, ?::VARCHAR, ?, ?, ?, ?, ?,
                ST_GeomFromText(?),
                ?, ?, ?, ?, ?, ?, ?
            )
        """, [wof_id, wof_id, name, placetype, level, lat, lon, wkt,
              country, min_lat, min_lon, max_lat, max_lon,
              names_json, concordances])
    conn.execute("CREATE INDEX boundaries_rtree ON boundaries USING RTREE (geom)")
    conn.execute("CREATE INDEX idx_rkey ON boundaries(rkey)")
    conn.close()


@pytest.fixture(scope="session")
def wof_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("wof") / "wof.duckdb"
    _create_wof_db(db_path)
    return db_path


@pytest.fixture
def boundary_lookup(wof_db_path):
    bl = BoundaryLookup(wof_db_path)
    bl.connect()
    yield bl
    bl.close()


@pytest.fixture
def wof_db(wof_db_path):
    db = WhosOnFirst(wof_db_path)
    db.connect()
    yield db
    db.close()
