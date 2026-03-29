"""Tests for importance INTEGER schema and tiebreaking behavior.

Red phase: these tests FAIL against the current code (importance DOUBLE,
uniform value 1.0) and PASS after the Green phase changes fixtures to
INTEGER with varied values.
"""
import pytest
import duckdb

from garganorn.database import FoursquareOSP, OvertureMaps


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_trigrams(name):
    """Generate distinct trigrams from a place name (lowercased full string)."""
    s = name.lower()
    trigrams = set()
    for i in range(len(s) - 2):
        trigrams.add(s[i:i+3])
    return trigrams


# ---------------------------------------------------------------------------
# Schema type tests: importance column must be INTEGER, not DOUBLE
# ---------------------------------------------------------------------------

def test_fsq_places_importance_is_integer(fsq_db_path):
    """places.importance must be INTEGER type in the FSQ database."""
    conn = duckdb.connect(str(fsq_db_path), read_only=True)
    rows = conn.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'places' AND column_name = 'importance'"
    ).fetchall()
    conn.close()
    assert rows, "importance column not found in places"
    data_type = rows[0][0].upper()
    assert data_type == "INTEGER", (
        f"places.importance should be INTEGER, got {data_type}"
    )


def test_overture_places_importance_is_integer(overture_db_path):
    """places.importance must be INTEGER type in the Overture database."""
    conn = duckdb.connect(str(overture_db_path), read_only=True)
    rows = conn.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'places' AND column_name = 'importance'"
    ).fetchall()
    conn.close()
    assert rows, "importance column not found in places"
    data_type = rows[0][0].upper()
    assert data_type == "INTEGER", (
        f"places.importance should be INTEGER, got {data_type}"
    )


# ---------------------------------------------------------------------------
# Value range tests: name_index.importance must be INTEGER type with varied values
# ---------------------------------------------------------------------------

def test_fsq_name_index_importance_is_integer(fsq_db_path):
    """name_index.importance must be INTEGER type in the FSQ database."""
    conn = duckdb.connect(str(fsq_db_path), read_only=True)
    rows = conn.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'name_index' AND column_name = 'importance'"
    ).fetchall()
    conn.close()
    assert rows, "importance column not found in name_index"
    data_type = rows[0][0].upper()
    assert data_type == "INTEGER", (
        f"name_index.importance should be INTEGER, got {data_type}"
    )


def test_fsq_name_index_importance_values_in_range(fsq_db_path):
    """All importance values in FSQ name_index must be in [0, 100] and not all uniform."""
    conn = duckdb.connect(str(fsq_db_path), read_only=True)
    # Check range
    out_of_range = conn.execute(
        "SELECT COUNT(*) FROM name_index WHERE importance < 0 OR importance > 100"
    ).fetchone()[0]
    # Check that values are non-uniform (i.e., not all the same)
    distinct_count = conn.execute(
        "SELECT COUNT(DISTINCT importance) FROM name_index"
    ).fetchone()[0]
    conn.close()
    assert out_of_range == 0, (
        f"{out_of_range} rows in FSQ name_index have importance outside [0, 100]"
    )
    assert distinct_count > 1, (
        "FSQ name_index importance values are all the same; expected varied values"
    )


def test_overture_name_index_importance_is_integer(overture_db_path):
    """name_index.importance must be INTEGER type in the Overture database."""
    conn = duckdb.connect(str(overture_db_path), read_only=True)
    rows = conn.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'name_index' AND column_name = 'importance'"
    ).fetchall()
    conn.close()
    assert rows, "importance column not found in name_index"
    data_type = rows[0][0].upper()
    assert data_type == "INTEGER", (
        f"name_index.importance should be INTEGER, got {data_type}"
    )


def test_overture_name_index_importance_values_in_range(overture_db_path):
    """All importance values in Overture name_index must be in [0, 100] and not all uniform."""
    conn = duckdb.connect(str(overture_db_path), read_only=True)
    # Check range
    out_of_range = conn.execute(
        "SELECT COUNT(*) FROM name_index WHERE importance < 0 OR importance > 100"
    ).fetchone()[0]
    # Check that values are non-uniform (i.e., not all the same)
    distinct_count = conn.execute(
        "SELECT COUNT(DISTINCT importance) FROM name_index"
    ).fetchone()[0]
    conn.close()
    assert out_of_range == 0, (
        f"{out_of_range} rows in Overture name_index have importance outside [0, 100]"
    )
    assert distinct_count > 1, (
        "Overture name_index importance values are all the same; expected varied values"
    )


# ---------------------------------------------------------------------------
# Tiebreaking fixture: two FSQ places with same name, different importance
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fsq_tiebreak_db_path(tmp_path_factory):
    """FSQ trigram database with two places sharing the same name but different importance."""
    db_path = tmp_path_factory.mktemp("fsq_tiebreak") / "fsq_tiebreak.duckdb"
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

    # Two places with identical names but different importance
    places = [
        ("tie001", "Test Place", 37.7749, -122.4194, 80),
        ("tie002", "Test Place", 37.7750, -122.4195, 55),
    ]
    for fsq_id, name, lat, lon, importance in places:
        conn.execute("""
            INSERT INTO places VALUES (
                ?, ?, ?, ?,
                ST_Point(?, ?),
                NULL, 'San Francisco', '94103', 'CA', 'CA', NULL, NULL, 'US',
                '2021-01-01', '2022-01-01', NULL,
                NULL, NULL, NULL, NULL, NULL, NULL,
                ARRAY[]::VARCHAR[], ARRAY[]::VARCHAR[],
                NULL,
                {'xmin': ?-0.001, 'ymin': ?-0.001, 'xmax': ?+0.001, 'ymax': ?+0.001},
                ?,
                []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
            )
        """, [fsq_id, name, lat, lon, lon, lat,
              lon, lat, lon, lat,
              importance])

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

    for fsq_id, name, lat, lon, importance in places:
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (?, ?, ?, ?, ?, FALSE)
            """, [trigram, fsq_id, name, FoursquareOSP._strip_accents(name.lower()), importance])

    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Tiebreaking test: higher importance comes first when Jaccard scores are equal
# ---------------------------------------------------------------------------

def test_trigram_tiebreaking_by_importance(fsq_tiebreak_db_path):
    """When two places have identical names (equal JW score),
    the one with higher importance must appear first in results."""
    db = FoursquareOSP(fsq_tiebreak_db_path)
    db.connect()
    try:
        results = db.nearest(q="Test Place", limit=10)
    finally:
        db.close()

    assert len(results) >= 2, "Expected at least 2 results for 'Test Place'"

    rkeys = [r["rkey"] for r in results]
    assert rkeys[0] == "tie001", (
        f"Expected tie001 (importance=80) first, got {rkeys[0]}"
    )
    assert rkeys[1] == "tie002", (
        f"Expected tie002 (importance=55) second, got {rkeys[1]}"
    )
