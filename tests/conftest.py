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
]

OVERTURE_PLACES = [
    # id, name, latitude, longitude, address_freeform, locality, postcode, region, country
    ("ovr001", "Philz Coffee", 37.7749, -122.4194, "201 Berry St", "San Francisco", "94158", "US-CA", "US"),
    ("ovr002", "Dolores Park", 37.7596, -122.4269, "Dolores St & 19th St", "San Francisco", "94114", "US-CA", "US"),
    ("ovr003", "Coit Tower", 37.8024, -122.4058, "1 Telegraph Hill Blvd", "San Francisco", "94133", "US-CA", "US"),
    ("ovr004", "Anchor Brewing", 37.7688, -122.4125, "1705 Mariposa St", "San Francisco", "94107", "US-CA", "US"),
    ("ovr005", "Lombard Street", 37.8021, -122.4187, "Lombard St", "San Francisco", "94133", "US-CA", "US"),
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
            latitude VARCHAR,
            longitude VARCHAR,
            address VARCHAR,
            locality VARCHAR,
            postcode VARCHAR,
            region VARCHAR,
            country VARCHAR,
            importance INTEGER
        )
    """)
    for row in FSQ_PLACES:
        fsq_id, name, lat, lon, address, locality, postcode, region, _, _, _, country = row
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [trigram, fsq_id, name,
                  f"{lat:.6f}", f"{lon:.6f}",
                  address, locality, postcode, region, country,
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
            latitude VARCHAR,
            longitude VARCHAR,
            importance INTEGER
        )
    """)
    for row in OVERTURE_PLACES:
        ovr_id, name, lat, lon, freeform, locality, postcode, region, country = row
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?, ?)
            """, [trigram, ovr_id, name,
                  f"{lat:.6f}", f"{lon:.6f}", ovr_importance[ovr_id]])

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
    # osm_type, osm_id, name, latitude, longitude, tags (VARCHAR[])
    ("n", 240109189, "Tartine Manufactory", 37.7612, -122.4195,
     ["amenity=cafe", "cuisine=coffee", "addr:street=Alabama St", "addr:housenumber=595", "addr:city=San Francisco", "addr:postcode=94110", "addr:country=US"]),
    ("n", 1234567, "UCSF Medical Center", 37.7631, -122.4576,
     ["amenity=hospital", "healthcare=hospital", "addr:street=Parnassus Ave", "addr:housenumber=505", "addr:city=San Francisco", "addr:country=US"]),
    ("w", 50637691, "Dolores Park", 37.7596, -122.4269,
     ["leisure=park"]),
    ("n", 9876543, "Bi-Rite Market", 37.7614, -122.4253,
     ["shop=supermarket", "addr:street=18th St", "addr:housenumber=3639", "addr:city=San Francisco", "addr:postcode=94110", "addr:country=US"]),
    ("w", 88776655, "Caltrain Station", 37.7764, -122.3942,
     ["railway=station"]),
]

OSM_IMPORTANCE = {
    "n240109189": 65,
    "n1234567": 80,
    "w50637691": 55,
    "n9876543": 60,
    "w88776655": 70,
}


def _create_osm_db(db_path):
    """Create an OSM DuckDB database with test data and trigram name_index."""
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE places (
            osm_type VARCHAR,
            osm_id BIGINT,
            name VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            geom GEOMETRY,
            tags VARCHAR[],
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            importance INTEGER
        )
    """)

    for row in OSM_PLACES:
        osm_type, osm_id, name, lat, lon, tags = row
        rkey = osm_type + str(osm_id)
        importance = OSM_IMPORTANCE[rkey]
        tags_literal = "[" + ", ".join(f"'{t}'" for t in tags) + "]"
        conn.execute(f"""
            INSERT INTO places VALUES (
                ?, ?, ?, ?, ?,
                ST_Point(?, ?),
                {tags_literal}::VARCHAR[],
                {{'xmin': ?-0.001, 'ymin': ?-0.001, 'xmax': ?+0.001, 'ymax': ?+0.001}},
                ?
            )
        """, [osm_type, osm_id, name, lat, lon, lon, lat,
              lon, lat, lon, lat,
              importance])

    conn.execute("""
        CREATE TABLE name_index (
            trigram VARCHAR,
            rkey VARCHAR,
            name VARCHAR,
            latitude VARCHAR,
            longitude VARCHAR,
            importance INTEGER
        )
    """)
    for row in OSM_PLACES:
        osm_type, osm_id, name, lat, lon, tags = row
        rkey = osm_type + str(osm_id)
        importance = OSM_IMPORTANCE[rkey]
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?, ?)
            """, [trigram, rkey, name,
                  f"{lat:.6f}", f"{lon:.6f}", importance])

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
