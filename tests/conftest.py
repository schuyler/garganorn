"""Shared fixtures for garganorn tests."""
import pytest
import duckdb

from garganorn.database import FoursquareOSP, OvertureMaps


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


def _create_fsq_db(db_path, with_name_index=True):
    """Create a FSQ DuckDB database with test data."""
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
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)
        )
    """)

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
                {'xmin': ?-0.001, 'ymin': ?-0.001, 'xmax': ?+0.001, 'ymax': ?+0.001}
            )
        """, [fsq_id, name, lat, lon, lon, lat,
              address, locality, postcode, region, admin_region, post_town, po_box,
              country,
              lon, lat, lon, lat])

    if with_name_index:
        # Token-only name_index (no dm_code column) so connect() sets
        # has_phonetic_index=False and tests exercise the token self-join path.
        conn.execute("""
            CREATE TABLE name_index (
                token VARCHAR,
                fsq_place_id VARCHAR,
                name VARCHAR,
                latitude VARCHAR,
                longitude VARCHAR,
                address VARCHAR,
                locality VARCHAR,
                postcode VARCHAR,
                region VARCHAR,
                country VARCHAR,
                importance DOUBLE
            )
        """)
        for row in FSQ_PLACES:
            fsq_id, name, lat, lon, address, locality, postcode, region, _, _, _, country = row
            for word in name.lower().split():
                if len(word) > 1:
                    conn.execute("""
                        INSERT INTO name_index VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [word, fsq_id, name,
                          f"{lat:.6f}", f"{lon:.6f}",
                          address, locality, postcode, region, country, 1.0])

    conn.close()


def _create_overture_db(db_path, with_name_index=True):
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
            sources STRUCT(property VARCHAR, dataset VARCHAR, record_id VARCHAR, confidence DOUBLE)[]
        )
    """)

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
                0.9, 1, NULL
            )
        """, [ovr_id, lon, lat,
              lon, lat, lon, lat,
              name,
              country, postcode, locality, freeform, region])

    # Note: The production schema documents a RTREE index on bbox, but DuckDB
    # requires a GEOMETRY column for RTREE. The production queries use bbox
    # struct field range comparisons, which don't require an explicit index.
    # We skip creating the RTREE index here.

    if with_name_index:
        # Token-only name_index (no dm_code column) so connect() sets
        # has_phonetic_index=False and tests exercise the token self-join path.
        conn.execute("""
            CREATE TABLE name_index (
                token VARCHAR,
                id VARCHAR,
                name VARCHAR,
                latitude VARCHAR,
                longitude VARCHAR,
                importance DOUBLE
            )
        """)
        for row in OVERTURE_PLACES:
            ovr_id, name, lat, lon, freeform, locality, postcode, region, country = row
            for word in name.lower().split():
                if len(word) > 1:
                    conn.execute("""
                        INSERT INTO name_index VALUES (?, ?, ?, ?, ?, ?)
                    """, [word, ovr_id, name,
                          f"{lat:.6f}", f"{lon:.6f}", 1.0])

    conn.close()


# ---------------------------------------------------------------------------
# Session-scoped path fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def fsq_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("fsq") / "fsq.duckdb"
    _create_fsq_db(db_path, with_name_index=True)
    return db_path


@pytest.fixture(scope="session")
def fsq_db_path_no_index(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("fsq_no_idx") / "fsq_no_idx.duckdb"
    _create_fsq_db(db_path, with_name_index=False)
    return db_path


@pytest.fixture(scope="session")
def overture_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("overture") / "overture.duckdb"
    _create_overture_db(db_path, with_name_index=True)
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
def fsq_db_no_index(fsq_db_path_no_index):
    db = FoursquareOSP(fsq_db_path_no_index)
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
# Trigram index fixtures
# ---------------------------------------------------------------------------

def _generate_trigrams(name):
    """Generate distinct trigrams from a place name (lowercased full string)."""
    s = name.lower()
    trigrams = set()
    for i in range(len(s) - 2):
        trigrams.add(s[i:i+3])
    return trigrams


def _create_fsq_trigram_db(db_path):
    """Create a FSQ DuckDB database with trigram name_index."""
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
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)
        )
    """)

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
                {'xmin': ?-0.001, 'ymin': ?-0.001, 'xmax': ?+0.001, 'ymax': ?+0.001}
            )
        """, [fsq_id, name, lat, lon, lon, lat,
              address, locality, postcode, region, admin_region, post_town, po_box,
              country,
              lon, lat, lon, lat])

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
            importance DOUBLE
        )
    """)

    for row in FSQ_PLACES:
        fsq_id, name, lat, lon, address, locality, postcode, region, _, _, _, country = row
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [trigram, fsq_id, name, f"{lat:.6f}", f"{lon:.6f}",
                  address, locality, postcode, region, country, 1.0])

    conn.close()


def _create_overture_trigram_db(db_path):
    """Create an Overture DuckDB database with trigram name_index."""
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
            sources STRUCT(property VARCHAR, dataset VARCHAR, record_id VARCHAR, confidence DOUBLE)[]
        )
    """)

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
                0.9, 1, NULL
            )
        """, [ovr_id, lon, lat,
              lon, lat, lon, lat,
              name,
              country, postcode, locality, freeform, region])

    conn.execute("""
        CREATE TABLE name_index (
            trigram VARCHAR,
            id VARCHAR,
            name VARCHAR,
            latitude VARCHAR,
            longitude VARCHAR,
            importance DOUBLE
        )
    """)

    for row in OVERTURE_PLACES:
        ovr_id, name, lat, lon, freeform, locality, postcode, region, country = row
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?, ?)
            """, [trigram, ovr_id, name, f"{lat:.6f}", f"{lon:.6f}", 1.0])

    conn.close()


@pytest.fixture(scope="session")
def fsq_trigram_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("fsq_trigram") / "fsq_trigram.duckdb"
    _create_fsq_trigram_db(db_path)
    return db_path


@pytest.fixture(scope="session")
def overture_trigram_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("overture_trigram") / "overture_trigram.duckdb"
    _create_overture_trigram_db(db_path)
    return db_path


@pytest.fixture
def fsq_trigram_db(fsq_trigram_db_path):
    db = FoursquareOSP(fsq_trigram_db_path)
    db.connect()
    yield db
    db.close()


@pytest.fixture
def overture_trigram_db(overture_trigram_db_path):
    db = OvertureMaps(overture_trigram_db_path)
    db.connect()
    yield db
    db.close()
