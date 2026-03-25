"""Tests for importance threshold filtering via compute_importance_floor.

Red phase: these tests FAIL against the current code because:
  - compute_importance_floor does not exist yet in garganorn.database
  - nearest() does not compute or apply an importance floor
"""
import math
import pytest
import duckdb

from garganorn.database import FoursquareOSP

# ---------------------------------------------------------------------------
# Conditional import: unit tests will fail until function is implemented
# ---------------------------------------------------------------------------

try:
    from garganorn.database import compute_importance_floor
except ImportError:
    compute_importance_floor = None


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _generate_trigrams(name):
    """Generate distinct trigrams from a place name (lowercased full string)."""
    s = name.lower()
    trigrams = set()
    for i in range(len(s) - 2):
        trigrams.add(s[i:i+3])
    return trigrams


# ---------------------------------------------------------------------------
# Custom fixture: 3 places named "Test Coffee" with varying importance
# Used for integration tests that need to control importance values exactly.
# th001=80 (high), th002=10 (very low), th003=60 (above floor 27)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fsq_threshold_db_path(tmp_path_factory):
    """FSQ trigram database with 3 'Test Coffee' places at different importance."""
    db_path = tmp_path_factory.mktemp("fsq_threshold") / "fsq_threshold.duckdb"
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

    # Three places: same name, different importance
    # th001=80 (above global floor 52), th002=10 (below all floors), th003=60 (above floor 52)
    places = [
        ("th001", "Test Coffee", 37.7749, -122.4194, 80),
        ("th002", "Test Coffee", 37.7750, -122.4195, 10),
        ("th003", "Test Coffee", 37.7748, -122.4193, 60),
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
                ?
            )
        """, [fsq_id, name, lat, lon, lon, lat,
              lon, lat, lon, lat,
              importance])

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

    for fsq_id, name, lat, lon, importance in places:
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (
                    ?, ?, ?, ?, ?,
                    NULL, 'San Francisco', '94103', 'CA', 'US',
                    ?
                )
            """, [trigram, fsq_id, name, f"{lat:.6f}", f"{lon:.6f}", importance])

    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Custom fixture: all-low-importance places (none above global floor 52)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fsq_all_low_importance_db_path(tmp_path_factory):
    """FSQ trigram database where all places have importance below the global floor."""
    db_path = tmp_path_factory.mktemp("fsq_low_imp") / "fsq_low_imp.duckdb"
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

    # All places have importance < 52 (below global floor)
    places = [
        ("lo001", "Test Coffee", 37.7749, -122.4194, 5),
        ("lo002", "Test Coffee", 37.7750, -122.4195, 10),
        ("lo003", "Test Coffee", 37.7748, -122.4193, 20),
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
                ?
            )
        """, [fsq_id, name, lat, lon, lon, lat,
              lon, lat, lon, lat,
              importance])

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

    for fsq_id, name, lat, lon, importance in places:
        for trigram in _generate_trigrams(name):
            conn.execute("""
                INSERT INTO name_index VALUES (
                    ?, ?, ?, ?, ?,
                    NULL, 'San Francisco', '94103', 'CA', 'US',
                    ?
                )
            """, [trigram, fsq_id, name, f"{lat:.6f}", f"{lon:.6f}", importance])

    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Unit tests for compute_importance_floor
# ---------------------------------------------------------------------------

class TestComputeImportanceFloor:
    """Unit tests for the compute_importance_floor function.

    All tests fail until compute_importance_floor is added to garganorn.database.
    """

    def _require(self):
        assert compute_importance_floor is not None, (
            "compute_importance_floor not yet implemented in garganorn.database"
        )

    def test_compute_floor_small_area(self):
        """Very small area (100 km²) → floor 0."""
        self._require()
        assert compute_importance_floor(100) == 0

    def test_compute_floor_city_scale(self):
        """City-scale area (2500 km²) → floor 5."""
        self._require()
        assert compute_importance_floor(2500) == 5

    def test_compute_floor_region(self):
        """Region-scale area (70000 km²) → floor 17."""
        self._require()
        assert compute_importance_floor(70000) == 17

    def test_compute_floor_globe(self):
        """Full-globe area → floor capped at 45."""
        self._require()
        assert compute_importance_floor(510_000_000) == 45

    def test_compute_floor_zero_area(self):
        """Zero area → floor 0."""
        self._require()
        assert compute_importance_floor(0) == 0

    def test_compute_floor_negative_area(self):
        """Negative area → floor 0 (clamped)."""
        self._require()
        assert compute_importance_floor(-100) == 0

    def test_compute_floor_capped_at_45(self):
        """Astronomically large area → floor capped at 45."""
        self._require()
        assert compute_importance_floor(1e30) == 45


# ---------------------------------------------------------------------------
# Integration tests: text-only query applies importance floor
# ---------------------------------------------------------------------------

class TestTextOnlyImportanceFloor:
    """Text-only (no lat/lon) queries use global area ≈ 510M km² → floor 52."""

    def test_text_only_applies_importance_floor(self, fsq_threshold_db_path):
        """Text-only nearest() should exclude places with importance < 52.

        th001=80 and th003=60 qualify; th002=10 must NOT appear.
        """
        db = FoursquareOSP(fsq_threshold_db_path)
        db.connect()
        try:
            results = db.nearest(q="Test Coffee")
        finally:
            db.close()

        rkeys = {r["rkey"] for r in results}
        assert "th001" in rkeys, "th001 (importance=80) should be in results"
        assert "th003" in rkeys, "th003 (importance=60) should be in results"
        assert "th002" not in rkeys, (
            "th002 (importance=10) should be excluded by importance floor 52"
        )


# ---------------------------------------------------------------------------
# Integration tests: spatial query with small bbox applies low floor
# ---------------------------------------------------------------------------

class TestSpatialQueryImportanceFloor:
    """Spatial queries derive floor from bbox area."""

    def test_spatial_query_low_floor(self, fsq_threshold_db_path):
        """5km spatial query → area ≈ 100 km² → floor 0 → all places returned.

        With floor 0, none of the places are filtered.
        """
        db = FoursquareOSP(fsq_threshold_db_path)
        db.connect()
        try:
            results = db.nearest(
                latitude=37.77, longitude=-122.42,
                q="Test Coffee",
                expand_m=5000,
            )
        finally:
            db.close()

        rkeys = {r["rkey"] for r in results}
        assert "th001" in rkeys, "th001 (importance=80) should be in results"
        assert "th002" in rkeys, "th002 (importance=10) should be in results (floor=0)"
        assert "th003" in rkeys, "th003 (importance=60) should be in results"

    def test_spatial_large_expand_applies_floor(self, fsq_threshold_db_path):
        """500km spatial query → area ≈ 1M km² → floor 27 → th002 (importance=10) excluded.

        expand_m=500000 at SF lat ≈ 1,000,000 km²,
        floor = min(int(4*ln(1 + 1000000/1000)), 100) = min(27, 100) = 27.
        th002 (importance=10) is below 27 and must be excluded.
        th001 (importance=80) and th003 (importance=60) are above 27 and must appear.
        """
        db = FoursquareOSP(fsq_threshold_db_path)
        db.connect()
        try:
            results = db.nearest(
                latitude=37.77, longitude=-122.42,
                q="Test Coffee",
                expand_m=500000,
            )
        finally:
            db.close()

        rkeys = {r["rkey"] for r in results}
        assert "th001" in rkeys, "th001 (importance=80) should be in results"
        assert "th003" in rkeys, "th003 (importance=60) should be in results"
        assert "th002" not in rkeys, (
            "th002 (importance=10) should be excluded by importance floor 27"
        )


# ---------------------------------------------------------------------------
# Edge case: no results when all places are below the floor
# ---------------------------------------------------------------------------

class TestNoResultsWhenAllBelowFloor:
    """When all places have importance < global floor, text-only query returns empty."""

    def test_no_results_when_all_below_floor(self, fsq_all_low_importance_db_path):
        """Text-only query with all places below floor 52 → empty result list."""
        db = FoursquareOSP(fsq_all_low_importance_db_path)
        db.connect()
        try:
            results = db.nearest(q="Test Coffee")
        finally:
            db.close()

        assert results == [], (
            f"Expected no results when all importances < 52, got {[r['rkey'] for r in results]}"
        )
