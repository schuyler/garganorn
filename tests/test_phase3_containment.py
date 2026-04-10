"""Phase 3 failing tests: BoundaryLookup division migration + compute_containment rkey-only output."""
import inspect

import pytest
import duckdb

from garganorn.boundaries import BoundaryLookup


# ---------------------------------------------------------------------------
# Division-schema boundary test data
# ---------------------------------------------------------------------------

DIVISION_BOUNDARIES = [
    # id, admin_level, lat, lon, wkt_geom, min_lat, min_lon, max_lat, max_lon
    ("div_continent_na", 0, 40.0, -100.0,
     "POLYGON((-130 20, -130 55, -60 55, -60 20, -130 20))",
     20.0, -130.0, 55.0, -60.0),
    ("div_country_us", 1, 39.0, -98.0,
     "POLYGON((-125 24, -125 50, -66 50, -66 24, -125 24))",
     24.0, -125.0, 50.0, -66.0),
    ("div_region_ca", 2, 37.0, -120.0,
     "POLYGON((-125 34, -125 42, -118 42, -118 34, -125 34))",
     34.0, -125.0, 42.0, -118.0),
    ("div_locality_sf", 3, 37.7749, -122.4194,
     "POLYGON((-122.55 37.6, -122.55 37.85, -122.3 37.85, -122.3 37.6, -122.55 37.6))",
     37.6, -122.55, 37.85, -122.3),
    ("div_borough_manhattan", 4, 40.7831, -73.9712,
     "POLYGON((-74.05 40.68, -74.05 40.88, -73.90 40.88, -73.90 40.68, -74.05 40.68))",
     40.68, -74.05, 40.88, -73.90),
]


def _create_division_db(db_path):
    """Create a division-schema boundary DB (table 'places' with id, geometry, admin_level)."""
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("""
        CREATE TABLE places (
            id VARCHAR,
            geometry GEOMETRY,
            admin_level INTEGER,
            min_latitude DOUBLE,
            max_latitude DOUBLE,
            min_longitude DOUBLE,
            max_longitude DOUBLE
        )
    """)
    for row in DIVISION_BOUNDARIES:
        bid, admin_level, lat, lon, wkt, min_lat, min_lon, max_lat, max_lon = row
        conn.execute("""
            INSERT INTO places VALUES (
                ?, ST_GeomFromText(?), ?, ?, ?, ?, ?
            )
        """, [bid, wkt, admin_level, min_lat, max_lat, min_lon, max_lon])
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
        """containment() dicts must have 'rkey' only -- no 'name', no 'level'."""
        result = division_lookup.containment(37.7749, -122.4194)
        assert len(result) > 0, "Expected at least one containing boundary"
        for entry in result:
            assert "rkey" in entry
            assert "name" not in entry, f"'name' key must not appear in containment output: {entry}"
            assert "level" not in entry, f"'level' key must not appear in containment output: {entry}"

    def test_containment_rkeys_have_division_prefix(self, division_lookup):
        """Each rkey must start with 'org.atgeo.places.overture.division:'."""
        result = division_lookup.containment(37.7749, -122.4194)
        assert len(result) > 0
        for entry in result:
            assert entry["rkey"].startswith("org.atgeo.places.overture.division:"), \
                f"rkey missing division prefix: {entry['rkey']}"

    def test_containment_returns_expected_ids(self, division_lookup):
        """Point in SF should match continent, country, region, and locality."""
        result = division_lookup.containment(37.7749, -122.4194)
        rkeys = [r["rkey"] for r in result]
        assert "org.atgeo.places.overture.division:div_continent_na" in rkeys
        assert "org.atgeo.places.overture.division:div_country_us" in rkeys
        assert "org.atgeo.places.overture.division:div_region_ca" in rkeys
        assert "org.atgeo.places.overture.division:div_locality_sf" in rkeys
        # Manhattan should not be included
        assert "org.atgeo.places.overture.division:div_borough_manhattan" not in rkeys


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
# Test 5: compute_containment() output produces rkey-only relations
# ---------------------------------------------------------------------------

class TestComputeContainmentOutput:
    def test_output_relations_are_rkey_only(self, division_db_path):
        """Relations JSON from compute_containment must contain only {rkey: ...} dicts."""
        import json
        from garganorn.quadtree import compute_containment

        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")

        # Create a minimal places table with one point inside SF
        con.execute("""
            CREATE TABLE places (
                pk VARCHAR,
                longitude DOUBLE,
                latitude DOUBLE,
                qk17 VARCHAR
            )
        """)
        # qk17 prefix "023010" is arbitrary; compute_containment iterates quadkeys
        con.execute("""
            INSERT INTO places VALUES ('p1', -122.4194, 37.7749, '02301000000000000')
        """)

        compute_containment(
            con, str(division_db_path),
            pk_expr="pk", lon_expr="longitude", lat_expr="latitude",
            collection_prefix="org.atgeo.places.overture.division",
        )

        rows = con.execute("SELECT * FROM place_containment").fetchall()
        assert len(rows) > 0, "Expected at least one containment row"
        for pk, relations_json in rows:
            data = json.loads(relations_json)
            within = data.get("within", [])
            assert len(within) > 0, f"Expected non-empty within list for pk={pk}"
            for entry in within:
                assert set(entry.keys()) == {"rkey"}, \
                    f"Relation must have only 'rkey', got keys {set(entry.keys())}: {entry}"
                assert entry["rkey"].startswith("org.atgeo.places.overture.division:"), \
                    f"rkey missing division prefix: {entry['rkey']}"
