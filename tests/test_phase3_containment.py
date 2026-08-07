"""Phase 3 failing tests: BoundaryLookup division migration + compute_containment rkey-only output."""
import inspect

import pytest
import duckdb


from garganorn.boundaries import BoundaryLookup


# ---------------------------------------------------------------------------
# Division-schema boundary test data
# ---------------------------------------------------------------------------

DIVISION_BOUNDARIES = [
    # id, level, lat, lon, wkt_geom, min_lat, min_lon, max_lat, max_lon
    # level values are the atgeo containment vocabulary (garganorn.levels.LEVEL_VOCAB):
    # country=10, region=25, locality=50. div_continent_na has no vocabulary entry
    # (docs/pipeline-implementation-decisions.md, "OQ-P2-2 — containment level
    # vocabulary": continent has no producer entry); 0 is a synthetic sentinel
    # kept only to preserve this pre-built boundaries.duckdb-shaped fixture's
    # ascending order.
    # div_borough_manhattan's subtype is "locality" in the real pipeline data too
    # (Overture has no borough subtype in current data, per the same
    # level-vocabulary decisions), so it also maps to 50.
    ("div_continent_na", 0, 40.0, -100.0,
     "POLYGON((-130 20, -130 55, -60 55, -60 20, -130 20))",
     20.0, -130.0, 55.0, -60.0),
    ("div_country_us", 10, 39.0, -98.0,
     "POLYGON((-125 24, -125 50, -66 50, -66 24, -125 24))",
     24.0, -125.0, 50.0, -66.0),
    ("div_region_ca", 25, 37.0, -120.0,
     "POLYGON((-125 34, -125 42, -118 42, -118 34, -125 34))",
     34.0, -125.0, 42.0, -118.0),
    ("div_locality_sf", 50, 37.7749, -122.4194,
     "POLYGON((-122.55 37.6, -122.55 37.85, -122.3 37.85, -122.3 37.6, -122.55 37.6))",
     37.6, -122.55, 37.85, -122.3),
    ("div_borough_manhattan", 50, 40.7831, -73.9712,
     "POLYGON((-74.05 40.68, -74.05 40.88, -73.90 40.88, -73.90 40.68, -74.05 40.68))",
     40.68, -74.05, 40.88, -73.90),
]


def _create_division_db(db_path):
    """Create a division-schema boundary DB (table 'places' with id, geometry, level)."""
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE places (
            id VARCHAR,
            geometry GEOMETRY,
            level INTEGER,
            min_latitude DOUBLE,
            max_latitude DOUBLE,
            min_longitude DOUBLE,
            max_longitude DOUBLE
        )
    """)
    for row in DIVISION_BOUNDARIES:
        bid, level, lat, lon, wkt, min_lat, min_lon, max_lat, max_lon = row
        conn.execute("""
            INSERT INTO places VALUES (
                ?, ST_GeomFromText(?), ?, ?, ?, ?, ?
            )
        """, [bid, wkt, level, min_lat, max_lat, min_lon, max_lon])
    conn.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
    conn.close()


@pytest.fixture(scope="session")
def division_db_path(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("division") / "division.duckdb"
    _create_division_db(db_path)
    return db_path


@pytest.fixture
def division_lookup(division_db_path):
    bl = BoundaryLookup(division_db_path)
    bl.connect()
    yield bl
    bl.close()


# ---------------------------------------------------------------------------
# Test 1: BoundaryLookup.COLLECTION equals division collection
# ---------------------------------------------------------------------------

class TestBoundaryLookupCollection:
    def test_collection_is_division(self):
        """COLLECTION class attribute should be the Overture division collection."""
        assert BoundaryLookup.COLLECTION == "org.atgeo.places.overture.division"


# ---------------------------------------------------------------------------
# Tests 2-3: containment() returns rkey-only dicts with division prefix
# ---------------------------------------------------------------------------

class TestDivisionContainment:
    def test_containment_returns_rkey_only(self, division_lookup):
        """containment() dicts must have collection+rkey only -- no 'name', no 'level'."""
        result = division_lookup.containment(37.7749, -122.4194)
        assert len(result) > 0, "Expected at least one containing boundary"
        for entry in result:
            assert "collection" in entry
            assert "rkey" in entry
            assert "name" not in entry, f"'name' key must not appear in containment output: {entry}"
            assert "level" not in entry, f"'level' key must not appear in containment output: {entry}"

    def test_containment_rkeys_have_division_prefix(self, division_lookup):
        """Each entry must have collection org.atgeo.places.overture.division."""
        result = division_lookup.containment(37.7749, -122.4194)
        assert len(result) > 0
        for entry in result:
            assert entry["collection"] == "org.atgeo.places.overture.division", \
                f"unexpected collection: {entry['collection']}"

    def test_containment_returns_expected_ids(self, division_lookup):
        """Point in SF should match continent, country, region, and locality."""
        result = division_lookup.containment(37.7749, -122.4194)
        rkeys = [r["rkey"] for r in result]
        assert "div_continent_na" in rkeys
        assert "div_country_us" in rkeys
        assert "div_region_ca" in rkeys
        assert "div_locality_sf" in rkeys
        # Manhattan should not be included
        assert "div_borough_manhattan" not in rkeys


# ---------------------------------------------------------------------------
# Test 4: compute_containment() accepts collection_prefix parameter
# ---------------------------------------------------------------------------

class TestComputeContainmentSignature:
    def test_accepts_collection_prefix_parameter(self):
        """compute_containment must accept a 'collection_prefix' keyword argument."""
        from garganorn.quadtree import compute_containment
        sig = inspect.signature(compute_containment)
        assert "collection_prefix" in sig.parameters, \
            f"compute_containment signature missing 'collection_prefix': {sig}"

    def test_collection_prefix_defaults_to_division(self):
        """Default value of collection_prefix should be 'org.atgeo.places.overture.division'."""
        from garganorn.quadtree import compute_containment
        sig = inspect.signature(compute_containment)
        param = sig.parameters["collection_prefix"]
        assert param.default == "org.atgeo.places.overture.division", \
            f"Expected default 'org.atgeo.places.overture.division', got {param.default!r}"


# ---------------------------------------------------------------------------
# Tests 5-9 (TestComputeContainmentOutput, TestComputeContainmentAdaptive):
# Deleted with the covering-rewrite (Phase 1).
#   - TestComputeContainmentOutput: superseded by TestContainmentBehaviorPorts
#     in tests/test_containment_covering.py (called without covering_dir, which
#     the new implementation treats as Q3 graceful degradation → empty result).
#   - TestComputeContainmentAdaptive: asserted max_boundaries/max_zoom on the
#     deleted recursion; behavior superseded by TestBruteForceOracle (§7.2.3).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Test 10: run_pipeline() boundary export filter
# ---------------------------------------------------------------------------

class TestBoundaryExportFilter:
    def test_run_pipeline_has_boundary_filter(self):
        """export_boundaries_db CTAS should include a level-vocabulary filter.

        pipeline-implementation-decisions.md ("OQ-P2-2 — containment level
        vocabulary"): the OLD invariant (admin_level BETWEEN 0 AND 2
        OR subtype = 'locality') is replaced by the level-vocabulary threshold
        `level <= 50` (country..locality; per the decisions above). The filter is expressed via the
        LEVEL_VOCAB constant so it can't drift from the vocabulary, but the
        source text still contains the literal comparison against 50
        (LEVEL_VOCAB['locality']) since that's what the CASE/threshold compiles
        to in the CTAS.
        """
        from garganorn import stages
        source = inspect.getsource(stages.stage_division_import)
        assert "level <= 50" in source or "level<=50" in source.lower(), \
            "stage_division_import should filter by level <= 50 (country..locality)"
        assert "admin_level BETWEEN 0 AND 2" not in source, \
            "stage_division_import must no longer filter by raw admin_level"
