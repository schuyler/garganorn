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


# ---------------------------------------------------------------------------
# Tests 6-9: compute_containment() adaptive subdivision kwargs
# ---------------------------------------------------------------------------

class TestComputeContainmentAdaptive:
    def test_accepts_max_boundaries_parameter(self):
        """compute_containment must accept a 'max_boundaries' keyword argument."""
        from garganorn.quadtree import compute_containment
        sig = inspect.signature(compute_containment)
        assert "max_boundaries" in sig.parameters

    def test_accepts_max_zoom_parameter(self):
        """compute_containment must accept a 'max_zoom' keyword argument."""
        from garganorn.quadtree import compute_containment
        sig = inspect.signature(compute_containment)
        assert "max_zoom" in sig.parameters

    def test_max_boundaries_default_is_200(self):
        """Default value of max_boundaries should be 200."""
        from garganorn.quadtree import compute_containment
        sig = inspect.signature(compute_containment)
        assert sig.parameters["max_boundaries"].default == 200

    def test_max_zoom_default_is_14(self):
        """Default value of max_zoom should be 14."""
        from garganorn.quadtree import compute_containment
        sig = inspect.signature(compute_containment)
        assert sig.parameters["max_zoom"].default == 14

    def test_prefix_length_correctness(self, division_db_path):
        """Two places sharing z6 but in different z7 tiles must both get correct containment."""
        import json
        from garganorn.quadtree import compute_containment

        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE places (pk VARCHAR, longitude DOUBLE, latitude DOUBLE, qk17 VARCHAR)
        """)
        # NW point in z7=0230100, SE point in z7=0230103
        con.execute("INSERT INTO places VALUES ('nw', -123.25, 39.29, '02301002210310312')")
        con.execute("INSERT INTO places VALUES ('se', -120.4375, 37.098, '02301032210132310')")

        compute_containment(con, str(division_db_path), pk_expr="pk",
                           lon_expr="longitude", lat_expr="latitude",
                           max_boundaries=1, max_zoom=10)

        rows = {r[0]: json.loads(r[1]) for r in con.execute("SELECT * FROM place_containment").fetchall()}
        con.close()

        # Both points should have containment results
        assert "nw" in rows, "NW point should have containment"
        assert "se" in rows, "SE point should have containment"

        # Both should be contained in continent, country, region (3 boundaries)
        for pk in ("nw", "se"):
            rkeys = sorted(r["rkey"] for r in rows[pk]["within"])
            assert len(rkeys) >= 3, f"{pk} should have at least 3 boundaries, got {len(rkeys)}"
            expected = [
                "org.atgeo.places.overture.division:div_continent_na",
                "org.atgeo.places.overture.division:div_country_us",
                "org.atgeo.places.overture.division:div_region_ca",
            ]
            assert rkeys == sorted(expected), \
                f"{pk}: expected {sorted(expected)}, got {rkeys}"

    def test_subdivision_produces_same_results(self, division_db_path):
        """With max_boundaries=2, forced subdivision must produce identical containment results."""
        import json
        from garganorn.quadtree import compute_containment

        # Run without subdivision (default max_boundaries=200, well above 4-5 boundaries)
        con1 = duckdb.connect(":memory:")
        con1.execute("INSTALL spatial; LOAD spatial;")
        con1.execute("""
            CREATE TABLE places (pk VARCHAR, longitude DOUBLE, latitude DOUBLE, qk17 VARCHAR)
        """)
        con1.execute("INSERT INTO places VALUES ('sf', -122.4194, 37.7749, '02301020333300320')")
        con1.execute("INSERT INTO places VALUES ('nyc', -73.9712, 40.7831, '03201011013023231')")
        compute_containment(con1, str(division_db_path), pk_expr="pk",
                            lon_expr="longitude", lat_expr="latitude")
        baseline = {r[0]: json.loads(r[1]) for r in con1.execute("SELECT * FROM place_containment").fetchall()}
        con1.close()

        # Run WITH subdivision (max_boundaries=2, forces both tiles to subdivide)
        con2 = duckdb.connect(":memory:")
        con2.execute("INSTALL spatial; LOAD spatial;")
        con2.execute("""
            CREATE TABLE places (pk VARCHAR, longitude DOUBLE, latitude DOUBLE, qk17 VARCHAR)
        """)
        con2.execute("INSERT INTO places VALUES ('sf', -122.4194, 37.7749, '02301020333300320')")
        con2.execute("INSERT INTO places VALUES ('nyc', -73.9712, 40.7831, '03201011013023231')")
        compute_containment(con2, str(division_db_path), pk_expr="pk",
                            lon_expr="longitude", lat_expr="latitude",
                            max_boundaries=2, max_zoom=10)
        subdivided = {r[0]: json.loads(r[1]) for r in con2.execute("SELECT * FROM place_containment").fetchall()}
        con2.close()

        # Both should have the same containment results
        assert set(baseline.keys()) == set(subdivided.keys()), \
            f"Different place IDs: baseline={set(baseline.keys())}, subdivided={set(subdivided.keys())}"
        for pk in baseline:
            baseline_rkeys = sorted(r["rkey"] for r in baseline[pk]["within"])
            subdivided_rkeys = sorted(r["rkey"] for r in subdivided[pk]["within"])
            assert baseline_rkeys == subdivided_rkeys, \
                f"Different rkeys for {pk}: baseline={baseline_rkeys}, subdivided={subdivided_rkeys}"


# ---------------------------------------------------------------------------
# Test 10: run_pipeline() boundary export filter
# ---------------------------------------------------------------------------

class TestBoundaryExportFilter:
    def test_run_pipeline_has_boundary_filter(self):
        """stage_boundary_export CTAS should include a subtype/admin_level filter."""
        from garganorn import stages
        source = inspect.getsource(stages.stage_boundary_export)
        # The boundary export CTAS should filter by admin_level and subtype
        assert "admin_level BETWEEN 0 AND 2" in source or "admin_level between 0 and 2" in source.lower(), \
            "stage_boundary_export should filter by admin_level"
        assert "subtype = 'locality'" in source or "subtype='locality'" in source, \
            "stage_boundary_export should filter by subtype='locality'"
