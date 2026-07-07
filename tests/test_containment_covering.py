"""Red tests: compute_containment covering rewrite (§7.2 of covering-containment-design.md).

Tests call compute_containment with the new Phase-1 signature (covering_dir,
containment_dir instead of max_boundaries, max_zoom).  Against the current
implementation they fail with TypeError.  Tests that also need garganorn.covering
to build a real covering directory import it inside the test body and fail with
ModuleNotFoundError.

§7.2 item mapping:
  1. Ports of surviving behavior tests      → TestContainmentBehaviorPorts
  2. Ordering (within by level ASC)         → TestContainmentOrdering
  3. Brute-force oracle parity              → TestBruteForceOracle
  4. Containment artifact layout            → TestContainmentArtifacts
  5. Q3 graceful degradation               → TestQ3Degradation
  6. End-to-end export integration          → TestExportIntegration
  7. Edge-arm D7 antimeridian              → TestAntimeridianEdgeArm
"""
import gzip
import inspect
import json
import os

import duckdb
import pytest

from garganorn.stages import compute_containment
from garganorn.quadtree import run_pipeline

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_COLLECTION_PREFIX = "org.atgeo.places.overture.division"

# Minimal boundaries DB schema (what bnd.places must look like for compute_containment)
_SIMPLE_BOUNDARIES = [
    # (id, admin_level, wkt, min_lat, min_lon, max_lat, max_lon)
    (
        "div_continent_na", 0,
        "POLYGON((-130 20, -130 55, -60 55, -60 20, -130 20))",
        20.0, -130.0, 55.0, -60.0,
    ),
    (
        "div_country_us", 1,
        "POLYGON((-125 24, -125 50, -66 50, -66 24, -125 24))",
        24.0, -125.0, 50.0, -66.0,
    ),
    (
        "div_region_ca", 2,
        "POLYGON((-125 34, -125 42, -118 42, -118 34, -125 34))",
        34.0, -125.0, 42.0, -118.0,
    ),
    (
        "div_locality_sf", 3,
        "POLYGON((-122.55 37.6, -122.55 37.85, -122.3 37.85, -122.3 37.6, -122.55 37.6))",
        37.6, -122.55, 37.85, -122.3,
    ),
    (
        "div_borough_manhattan", 4,
        "POLYGON((-74.05 40.68, -74.05 40.88, -73.90 40.88, -73.90 40.68, -74.05 40.68))",
        40.68, -74.05, 40.88, -73.90,
    ),
]


def _create_simple_boundaries_db(db_path):
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
    for bid, admin_level, wkt, min_lat, min_lon, max_lat, max_lon in _SIMPLE_BOUNDARIES:
        conn.execute(
            "INSERT INTO places VALUES (?, ST_GeomFromText(?), ?, ?, ?, ?, ?)",
            [bid, wkt, admin_level, min_lat, max_lat, min_lon, max_lon],
        )
    conn.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
    conn.close()


@pytest.fixture(scope="module")
def simple_boundaries_db(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("cc_bnd") / "boundaries.duckdb"
    _create_simple_boundaries_db(db_path)
    return db_path


def _make_places_con(places, tile_qk_map=None):
    """Return a fresh in-memory DuckDB connection with a places table and tile_assignments.

    places: list of (place_id, lon, lat, qk17)
    tile_qk_map: dict of {place_id: tile_qk}; defaults to left(qk17, 6) for each place.
    """
    con = duckdb.connect(":memory:")
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("""
        CREATE TABLE places (
            pk VARCHAR,
            longitude DOUBLE,
            latitude DOUBLE,
            qk17 VARCHAR
        )
    """)
    for place_id, lon, lat, qk17 in places:
        con.execute("INSERT INTO places VALUES (?, ?, ?, ?)", [place_id, lon, lat, qk17])
    # tile_assignments maps place_id → tile_qk (used by the COPY query in new compute_containment)
    con.execute("""
        CREATE TABLE tile_assignments (
            place_id VARCHAR,
            tile_qk VARCHAR
        )
    """)
    if tile_qk_map is None:
        tile_qk_map = {pid: qk17[:6] for pid, _, _, qk17 in places}
    for pid, tqk in tile_qk_map.items():
        con.execute("INSERT INTO tile_assignments VALUES (?, ?)", [pid, tqk])
    return con


# ---------------------------------------------------------------------------
# §7.2 item 1 — ports of surviving behavior tests
# ---------------------------------------------------------------------------

class TestContainmentBehaviorPorts:
    """§7.2 item 1: rkey-only relations, division prefix, SF point expected IDs,
    collection_prefix kwarg.  All fail TypeError with old compute_containment signature.
    """

    def test_new_signature_has_covering_dir_not_max_boundaries(self):
        """New compute_containment must have covering_dir/containment_dir, not max_boundaries."""
        sig = inspect.signature(compute_containment)
        assert "covering_dir" in sig.parameters, (
            f"compute_containment missing 'covering_dir' in new signature: {sig}"
        )
        assert "containment_dir" in sig.parameters, (
            f"compute_containment missing 'containment_dir' in new signature: {sig}"
        )
        assert "max_boundaries" not in sig.parameters, (
            f"compute_containment should NOT have 'max_boundaries' (old recursion removed): {sig}"
        )
        assert "max_zoom" not in sig.parameters, (
            f"compute_containment should NOT have 'max_zoom' (old recursion removed): {sig}"
        )

    def test_collection_prefix_default_and_new_signature(self):
        """collection_prefix default unchanged; covering_dir/containment_dir present in new sig.

        Fails on old code because covering_dir is asserted first (which old code lacks).
        """
        sig = inspect.signature(compute_containment)
        # The following assertion fails on old code (new signature not yet written)
        assert "covering_dir" in sig.parameters, (
            f"covering_dir missing — new signature not implemented: {sig}"
        )
        # These should both hold in the new code too
        assert "collection_prefix" in sig.parameters, "collection_prefix param missing"
        default = sig.parameters["collection_prefix"].default
        assert default == _COLLECTION_PREFIX, (
            f"Expected default '{_COLLECTION_PREFIX}', got {default!r}"
        )

    def test_rkey_only_relations_sf_point(self, simple_boundaries_db, tmp_path):
        """SF point: place_containment relations have only 'rkey' keys, division prefix."""
        con = _make_places_con(
            [("p_sf", -122.4194, 37.7749, "02301020333300320")]
        )
        covering_dir = str(tmp_path / "rkeys_covering")
        from garganorn.covering import stage_covering
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        # New signature: covering_dir and containment_dir replace max_boundaries/max_zoom.
        # Fails with TypeError on old code — right red reason.
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            collection_prefix=_COLLECTION_PREFIX,
            covering_dir=covering_dir,
            containment_dir=str(tmp_path / "rkeys_containment"),
        )
        rows = con.execute("SELECT place_id, relations_json FROM place_containment").fetchall()
        assert len(rows) > 0, "SF point should have containment rows"
        for _, relations_json in rows:
            data = json.loads(relations_json)
            within = data.get("within", [])
            assert len(within) > 0
            for entry in within:
                assert set(entry.keys()) == {"rkey"}, (
                    f"Relation must have only 'rkey', got {set(entry.keys())}: {entry}"
                )
                assert entry["rkey"].startswith(_COLLECTION_PREFIX + ":"), (
                    f"rkey missing division prefix: {entry['rkey']}"
                )

    def test_sf_point_expected_boundary_ids(self, simple_boundaries_db, tmp_path):
        """SF point: rkeys include continent, country, region, locality; exclude Manhattan."""
        con = _make_places_con(
            [("p_sf", -122.4194, 37.7749, "02301020333300320")]
        )
        from garganorn.covering import stage_covering
        sfid_covering = str(tmp_path / "sfid_covering")
        stage_covering(str(simple_boundaries_db), sfid_covering, cover_min_zoom=4, cover_max_zoom=12)
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=sfid_covering,
            containment_dir=str(tmp_path / "sfid_containment"),
        )
        rows = con.execute("SELECT place_id, relations_json FROM place_containment").fetchall()
        assert len(rows) > 0
        rkeys = {
            e["rkey"]
            for _, rel_json in rows
            for e in json.loads(rel_json)["within"]
        }
        assert f"{_COLLECTION_PREFIX}:div_continent_na" in rkeys
        assert f"{_COLLECTION_PREFIX}:div_country_us" in rkeys
        assert f"{_COLLECTION_PREFIX}:div_region_ca" in rkeys
        assert f"{_COLLECTION_PREFIX}:div_locality_sf" in rkeys
        assert f"{_COLLECTION_PREFIX}:div_borough_manhattan" not in rkeys


# ---------------------------------------------------------------------------
# §7.2 item 2 — ordering: within by level ASC, NULL levels last
# ---------------------------------------------------------------------------

class TestContainmentOrdering:
    """§7.2 item 2: within list ordered by level ASC; NULL admin_level rows appear last."""

    def test_within_ordered_by_level_asc(self, simple_boundaries_db, tmp_path):
        """The within list is sorted by level ASC (continent=0 first, locality=3 last)."""
        con = _make_places_con(
            [("p_sf", -122.4194, 37.7749, "02301020333300320")]
        )
        from garganorn.covering import stage_covering
        ord_covering = str(tmp_path / "ord_covering")
        stage_covering(str(simple_boundaries_db), ord_covering, cover_min_zoom=4, cover_max_zoom=12)
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=ord_covering,
            containment_dir=str(tmp_path / "ord_containment"),
        )
        rows = con.execute("SELECT relations_json FROM place_containment").fetchall()
        assert len(rows) > 0
        for (rel_json,) in rows:
            within = json.loads(rel_json)["within"]
            assert len(within) > 1, "SF point should be in multiple boundaries"
            rkeys = [e["rkey"] for e in within]
            # continent (level=0) must come before locality (level=3)
            continent_idx = next(
                (i for i, r in enumerate(rkeys) if r.endswith(":div_continent_na")), None
            )
            locality_idx = next(
                (i for i, r in enumerate(rkeys) if r.endswith(":div_locality_sf")), None
            )
            assert continent_idx is not None, "div_continent_na not in within"
            assert locality_idx is not None, "div_locality_sf not in within"
            assert continent_idx < locality_idx, (
                f"continent (idx={continent_idx}) should come before locality (idx={locality_idx})"
            )

    def test_null_admin_level_boundaries_last(self, tmp_path):
        """Boundary with NULL admin_level appears after non-NULL levels in within."""
        # Create boundaries DB with one NULL-level boundary
        null_db_path = tmp_path / "null_level.duckdb"
        conn = duckdb.connect(str(null_db_path))
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
        # admin_level=2 boundary
        conn.execute("""
            INSERT INTO places VALUES (
                'r_named', ST_GeomFromText('POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))'),
                2, -10.0, 10.0, -10.0, 10.0
            )
        """)
        # NULL admin_level boundary (locality equivalent)
        conn.execute("""
            INSERT INTO places VALUES (
                'r_null', ST_GeomFromText('POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))'),
                NULL, -10.0, 10.0, -10.0, 10.0
            )
        """)
        conn.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
        conn.close()

        con = _make_places_con([("p0", 0.0, 0.0, "30000000000000000")])
        from garganorn.covering import stage_covering
        null_covering = str(tmp_path / "null_covering")
        stage_covering(str(null_db_path), null_covering, cover_min_zoom=4, cover_max_zoom=12)
        compute_containment(
            con,
            str(null_db_path),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=null_covering,
            containment_dir=str(tmp_path / "null_containment"),
        )
        rows = con.execute("SELECT relations_json FROM place_containment").fetchall()
        assert len(rows) > 0
        for (rel_json,) in rows:
            within = json.loads(rel_json)["within"]
            rkeys = [e["rkey"] for e in within]
            named_idx = next((i for i, r in enumerate(rkeys) if r.endswith(":r_named")), None)
            null_idx = next((i for i, r in enumerate(rkeys) if r.endswith(":r_null")), None)
            if named_idx is not None and null_idx is not None:
                assert named_idx < null_idx, (
                    "Non-NULL level boundary should appear before NULL level boundary in within"
                )


# ---------------------------------------------------------------------------
# §7.2 item 3 — brute-force oracle parity
# ---------------------------------------------------------------------------

class TestBruteForceOracle:
    """§7.2 item 3: compute_containment pair set == brute-force ST_Contains for all places."""

    def test_parity_with_direct_st_contains_simple_boundaries(self, simple_boundaries_db, tmp_path):
        """(place_id, boundary_id) pairs from compute_containment match direct ST_Contains."""
        # Import garganorn.covering to build the covering dir.
        # Fails with ModuleNotFoundError until covering.py exists — right red reason.
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "oracle_covering")
        stage_covering(
            str(simple_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )

        places = [
            ("p_sf", -122.4194, 37.7749, "02301020333300320"),
            ("p_nyc", -73.9712, 40.7831, "03201011013023231"),
            ("p_ocean", -150.0, 20.0, "00000000000000000"),
        ]
        # Compute qk17s via DuckDB for ocean point
        tmp_con = duckdb.connect(":memory:")
        tmp_con.execute("INSTALL spatial; LOAD spatial;")
        corrected = []
        for pid, lon, lat, _ in places:
            qk17 = tmp_con.execute(
                "SELECT ST_QuadKey(?, ?, 17)", [lon, lat]
            ).fetchone()[0]
            corrected.append((pid, lon, lat, qk17))

        con = _make_places_con(corrected)
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=str(tmp_path / "oracle_containment"),
        )

        # collect compute_containment pairs
        cc_pairs = set()
        for place_id, rel_json in con.execute(
            "SELECT place_id, relations_json FROM place_containment"
        ).fetchall():
            for entry in json.loads(rel_json)["within"]:
                boundary_id = entry["rkey"].split(":", 1)[1]
                cc_pairs.add((place_id, boundary_id))

        # brute-force oracle
        oracle_con = duckdb.connect(":memory:")
        oracle_con.execute("INSTALL spatial; LOAD spatial;")
        oracle_con.execute(f"ATTACH '{simple_boundaries_db}' AS bnd (READ_ONLY)")
        oracle_pairs = set()
        for pid, lon, lat, _ in corrected:
            matching_boundaries = oracle_con.execute(
                "SELECT id FROM bnd.places WHERE ST_Contains(geometry, ST_Point(?, ?))",
                [lon, lat],
            ).fetchall()
            for (bid,) in matching_boundaries:
                oracle_pairs.add((pid, bid))

        assert cc_pairs == oracle_pairs, (
            f"compute_containment pairs != brute-force oracle.\n"
            f"  In CC but not oracle: {cc_pairs - oracle_pairs}\n"
            f"  In oracle but not CC: {oracle_pairs - cc_pairs}"
        )

    def test_no_duplicate_rkeys_in_within(self, simple_boundaries_db, tmp_path):
        """Each boundary appears at most once in a place's within list (no DISTINCT needed)."""
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "nodup_covering")
        stage_covering(
            str(simple_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        con = _make_places_con(
            [("p_sf", -122.4194, 37.7749, "02301020333300320")]
        )
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=str(tmp_path / "nodup_containment"),
        )
        rows = con.execute("SELECT relations_json FROM place_containment").fetchall()
        assert len(rows) > 0
        for (rel_json,) in rows:
            within = json.loads(rel_json)["within"]
            rkeys = [e["rkey"] for e in within]
            assert len(rkeys) == len(set(rkeys)), (
                f"Duplicate rkeys in within list: {[r for r in rkeys if rkeys.count(r) > 1]}"
            )


# ---------------------------------------------------------------------------
# §7.2 item 4 — containment artifact layout
# ---------------------------------------------------------------------------

class TestContainmentArtifacts:
    """§7.2 item 4: containment/<qk4>.parquet written, schema and sort correct."""

    def test_containment_parquets_written(self, simple_boundaries_db, tmp_path):
        """compute_containment writes containment/<qk4>.parquet files."""
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "art_covering")
        stage_covering(
            str(simple_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )

        containment_dir = str(tmp_path / "art_containment")
        con = _make_places_con(
            [("p_sf", -122.4194, 37.7749, "02301020333300320")]
        )
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=containment_dir,
        )

        assert os.path.isdir(containment_dir), "containment_dir not created"
        parquets = [f for f in os.listdir(containment_dir) if f.endswith(".parquet")]
        assert len(parquets) > 0, "No containment/<qk4>.parquet files written"

        # verify file names are length-4 quadkey prefixes
        for fname in parquets:
            stem = fname[:-8]
            assert len(stem) == 4, f"{fname!r}: stem not length 4"
            assert all(c in "0123" for c in stem), f"{fname!r}: non-quadkey chars"

    def test_containment_parquet_schema(self, simple_boundaries_db, tmp_path):
        """Each containment parquet has columns (tile_qk, place_id, relations_json)."""
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "sch_covering")
        stage_covering(
            str(simple_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        containment_dir = str(tmp_path / "sch_containment")
        con = _make_places_con(
            [("p_sf", -122.4194, 37.7749, "02301020333300320")]
        )
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=containment_dir,
        )
        parquets = [os.path.join(containment_dir, f)
                    for f in os.listdir(containment_dir) if f.endswith(".parquet")]
        assert len(parquets) > 0
        check_con = duckdb.connect(":memory:")
        cols = {
            row[0]
            for row in check_con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [parquets[:1]]
            ).fetchall()
        }
        assert "tile_qk" in cols
        assert "place_id" in cols
        assert "relations_json" in cols

    def test_containment_parquet_sorted_tile_qk_place_id(self, simple_boundaries_db, tmp_path):
        """Each containment parquet is sorted by (tile_qk, place_id)."""
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "srt_covering")
        stage_covering(
            str(simple_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        containment_dir = str(tmp_path / "srt_containment")
        places = [
            ("p_sf", -122.4194, 37.7749, "02301020333300320"),
            ("p_nyc", -73.9712, 40.7831, "03201011013023231"),
        ]
        tmp_con = duckdb.connect(":memory:")
        tmp_con.execute("INSTALL spatial; LOAD spatial;")
        corrected = [
            (pid, lon, lat, tmp_con.execute("SELECT ST_QuadKey(?,?,17)", [lon, lat]).fetchone()[0])
            for pid, lon, lat, _ in places
        ]
        con = _make_places_con(corrected)
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=containment_dir,
        )
        parquet_files = [
            f for f in os.listdir(containment_dir) if f.endswith(".parquet")
        ]
        check_con = duckdb.connect(":memory:")
        for fname in parquet_files:
            path = os.path.join(containment_dir, fname)
            rows = check_con.execute(
                "SELECT tile_qk, place_id FROM read_parquet(?)", [path]
            ).fetchall()
            assert rows == sorted(rows), f"{fname}: not sorted by (tile_qk, place_id)"

    def test_tile_qk_in_parquet_matches_tile_assignments(self, simple_boundaries_db, tmp_path):
        """tile_qk in containment parquets agrees with tile_assignments for each place."""
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "tqk_covering")
        stage_covering(
            str(simple_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        containment_dir = str(tmp_path / "tqk_containment")
        tmp_con = duckdb.connect(":memory:")
        tmp_con.execute("INSTALL spatial; LOAD spatial;")
        qk17 = tmp_con.execute("SELECT ST_QuadKey(?,?,17)", [-122.4194, 37.7749]).fetchone()[0]
        tile_qk = qk17[:6]
        con = _make_places_con(
            [("p_sf", -122.4194, 37.7749, qk17)],
            tile_qk_map={"p_sf": tile_qk},
        )
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=containment_dir,
        )
        parquets = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        check_con = duckdb.connect(":memory:")
        tile_qks_in_parquets = {
            r[0]
            for r in check_con.execute(
                "SELECT DISTINCT tile_qk FROM read_parquet(?)", [parquets]
            ).fetchall()
        }
        # Every tile_qk in containment must have been in tile_assignments
        ta_tile_qks = {r[0] for r in con.execute("SELECT DISTINCT tile_qk FROM tile_assignments").fetchall()}
        assert tile_qks_in_parquets.issubset(ta_tile_qks), (
            f"Containment tile_qk values not in tile_assignments: "
            f"{tile_qks_in_parquets - ta_tile_qks}"
        )


# ---------------------------------------------------------------------------
# §7.2 item 5 — Q3 graceful degradation
# ---------------------------------------------------------------------------

class TestQ3Degradation:
    """§7.2 item 5: boundaries_db=None, missing/empty covering_dir → empty place_containment."""

    def test_boundaries_db_none_gives_empty_containment(self, tmp_path):
        """boundaries_db=None → place_containment is created empty (Q3 preserved)."""
        con = _make_places_con([("p1", 0.0, 0.0, "22222222222222222")])
        compute_containment(
            con,
            None,  # boundaries_db=None
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=str(tmp_path / "q3_no_bnd_covering"),
            containment_dir=str(tmp_path / "q3_no_bnd_containment"),
        )
        rows = con.execute("SELECT * FROM place_containment").fetchall()
        assert rows == [], (
            f"boundaries_db=None should yield empty place_containment, got: {rows}"
        )

    def test_missing_covering_dir_gives_empty_containment(self, simple_boundaries_db, tmp_path):
        """covering_dir that does not exist → place_containment is created empty."""
        con = _make_places_con([("p1", -122.4194, 37.7749, "02301020333300320")])
        missing_dir = str(tmp_path / "does_not_exist")
        assert not os.path.exists(missing_dir)
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=missing_dir,
            containment_dir=str(tmp_path / "q3_no_cov_containment"),
        )
        rows = con.execute("SELECT * FROM place_containment").fetchall()
        assert rows == [], (
            "Missing covering_dir should yield empty place_containment, got non-empty"
        )

    def test_empty_covering_dir_gives_empty_containment(self, simple_boundaries_db, tmp_path):
        """An empty covering_dir (no parquet files) → place_containment is empty."""
        covering_dir = str(tmp_path / "empty_covering")
        os.makedirs(covering_dir)  # exists but empty
        con = _make_places_con([("p1", -122.4194, 37.7749, "02301020333300320")])
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=str(tmp_path / "q3_empty_containment"),
        )
        rows = con.execute("SELECT * FROM place_containment").fetchall()
        assert rows == [], (
            "Empty covering_dir should yield empty place_containment, got non-empty"
        )

    def test_place_outside_all_boundaries_absent_from_output(self, simple_boundaries_db, tmp_path):
        """Place with no containing boundary must not appear in place_containment."""
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "absent_covering")
        stage_covering(
            str(simple_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        # Point in the Pacific Ocean — outside all _SIMPLE_BOUNDARIES
        tmp_con = duckdb.connect(":memory:")
        tmp_con.execute("INSTALL spatial; LOAD spatial;")
        qk17 = tmp_con.execute("SELECT ST_QuadKey(?,?,17)", [-150.0, 20.0]).fetchone()[0]
        con = _make_places_con([("p_ocean", -150.0, 20.0, qk17)])
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=str(tmp_path / "absent_containment"),
        )
        rows = con.execute(
            "SELECT * FROM place_containment WHERE place_id = 'p_ocean'"
        ).fetchall()
        assert rows == [], (
            f"Place in the ocean should have no containment rows, got: {rows}"
        )


# ---------------------------------------------------------------------------
# §7.2 item 6 — end-to-end export integration
# ---------------------------------------------------------------------------

class TestExportIntegration:
    """§7.2 item 6: mini end-to-end run_pipeline producing tile JSON with relations."""

    def test_run_pipeline_with_covering_builds_covering_dir(
        self, simple_boundaries_db, fsq_parquet, density_parquet, tmp_path
    ):
        """run_pipeline with boundaries_db must build a covering directory next to boundaries.duckdb.

        The new code calls ensure_covering(boundaries_db) before the containment stage,
        which creates <dirname(boundaries_db)>/covering/.  The old code does not call
        stage_covering at all, so this directory does not exist.

        Fails behavioral assertion (no covering dir) on old code.
        """
        output_dir = str(tmp_path / "integration_out")
        os.makedirs(output_dir)

        run_pipeline(
            "foursquare",
            fsq_parquet,
            (-122.55, 37.60, -122.30, 37.85),
            output_dir,
            memory_limit="4GB",
            max_per_tile=100,
            boundaries_db=str(simple_boundaries_db),
            density_parquet=density_parquet,
        )

        # New orchestration: ensure_covering writes covering/ next to boundaries.duckdb
        expected_covering = os.path.join(
            os.path.dirname(str(simple_boundaries_db)), "covering"
        )
        assert os.path.isdir(expected_covering), (
            f"run_pipeline should have called ensure_covering and created {expected_covering!r}; "
            "old code does not call stage_covering — this fails on the old implementation"
        )

    def test_place_containment_view_join_compatible_with_export(
        self, simple_boundaries_db, tmp_path
    ):
        """place_containment VIEW created by compute_containment is join-compatible with export SQL.

        The export SQL joins place_containment on place_id. Verify the VIEW exists
        and has the expected columns after compute_containment runs.
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "compat_covering")
        stage_covering(
            str(simple_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        con = _make_places_con(
            [("p_sf", -122.4194, 37.7749, "02301020333300320")]
        )
        compute_containment(
            con,
            str(simple_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=str(tmp_path / "compat_containment"),
        )

        # place_containment should be a VIEW or TABLE with (place_id, relations_json)
        cols = {
            row[0]
            for row in con.execute("DESCRIBE SELECT * FROM place_containment").fetchall()
        }
        assert "place_id" in cols, "place_containment missing 'place_id' column"
        assert "relations_json" in cols, "place_containment missing 'relations_json' column"

        # join on place_id should work
        result = con.execute("""
            SELECT pc.place_id, pc.relations_json
            FROM place_containment pc
            JOIN places p ON p.pk = pc.place_id
        """).fetchall()
        assert len(result) > 0, "JOIN between place_containment and places returned no rows"


# ---------------------------------------------------------------------------
# §7.2 item 7 — edge-arm antimeridian D7
# ---------------------------------------------------------------------------

class TestAntimeridianEdgeArm:
    """§7.2 item 7: D7 antimeridian edge-arm tests.

    What these tests pin:

    (a) LOBE-INCLUSION TESTS (test_east_lobe_place_in_containment,
        test_west_lobe_place_in_containment, and the lobe assertions in
        test_all_three_places_via_compute_containment): these pin the D7 OR-logic
        in the edge arm's WHERE clause.  If the CASE used AND instead of OR
        (i.e. ``p.lon >= b.min_longitude AND p.lon <= b.max_longitude``), a point
        at lon=175 would evaluate to ``175 >= 170 AND 175 <= -170`` = false,
        wrongly dropping the lobe place.  These tests fail on that bug and are
        the behaviorally observable test of the D7 predicate.

    (b) GAP ASSERTIONS (test_gap_place_absent_from_containment and the gap
        assertions in test_all_three_places_via_compute_containment): these are
        end-to-end sanity checks.  The gap place (lon=0) must NOT appear under
        ami_boundary's rkey — this is guaranteed structurally, not by the D7 CASE
        branch.  The covering seed SQL (§2.3) uses the same D7 OR-condition on the
        boundary's bbox; gap tiles (xmax < 170 AND xmin > -170) never pass it, so
        ami_boundary contributes no covering tiles in the gap region and no
        (gap_place, ami_boundary) row can enter the edge arm join.  Even if such a
        row somehow entered the join, ST_Contains(ami_boundary, gap_point) = false
        (the geometry has no interior in the gap), so the D7 CASE exclusion would
        be masked by ST_Contains anyway.  The gap assertions therefore test
        end-to-end correctness (gap place gets gap_boundary's rkey, never
        ami_boundary's), NOT the D7 CASE's exclusion branch, which is structurally
        unreachable for gap points and masked by ST_Contains even where reachable.
    """

    @pytest.fixture(scope="class")
    def antimeridian_boundaries_db(self, tmp_path_factory):
        """Create boundaries DB with one antimeridian-crossing boundary.

        The boundary min_longitude=170, max_longitude=-170 represents two lobes:
          - East lobe:  [170°, 180°]  (near +170°)
          - West lobe:  [-180°, -170°] (near -170°)
          - Gap:        [-170°, 170°]  (everything else)

        Geometry: MultiPolygon with one polygon per lobe.
        """
        db_path = tmp_path_factory.mktemp("ami_bnd") / "boundaries.duckdb"
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
        # Antimeridian-crossing boundary: lobes [170,180] and [-180,-170], lat [-15, 15]
        # MultiPolygon WKT — each lobe as a separate polygon
        ami_wkt = (
            "MULTIPOLYGON("
            "((170 -15, 180 -15, 180 15, 170 15, 170 -15)),"   # east lobe
            "((-180 -15, -170 -15, -170 15, -180 15, -180 -15)))"  # west lobe
        )
        conn.execute(
            "INSERT INTO places VALUES (?, ST_GeomFromText(?), ?, ?, ?, ?, ?)",
            ["ami_boundary", ami_wkt, 1, -15.0, 15.0, 170.0, -170.0],
        )
        # Non-antimeridian boundary covering the gap area around lon=0.
        # Ensures the gap place has a valid containment result (gap_boundary's rkey)
        # so the gap assertion is a meaningful end-to-end check rather than
        # vacuously asserting absence from an empty result set.
        gap_wkt = "POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))"
        conn.execute(
            "INSERT INTO places VALUES (?, ST_GeomFromText(?), ?, ?, ?, ?, ?)",
            ["gap_boundary", gap_wkt, 2, -10.0, 10.0, -10.0, 10.0],
        )
        conn.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
        conn.close()
        return db_path

    def test_east_lobe_place_in_containment(self, antimeridian_boundaries_db, tmp_path):
        """Place in east lobe (lon=175) is matched by edge arm and appears in containment."""
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "ami_e_covering")
        stage_covering(
            str(antimeridian_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        tmp_con = duckdb.connect(":memory:")
        tmp_con.execute("INSTALL spatial; LOAD spatial;")
        qk17_east = tmp_con.execute("SELECT ST_QuadKey(?,?,17)", [175.0, 0.0]).fetchone()[0]
        con = _make_places_con([("p_east", 175.0, 0.0, qk17_east)])
        compute_containment(
            con,
            str(antimeridian_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=str(tmp_path / "ami_e_containment"),
        )
        rows = con.execute(
            "SELECT place_id FROM place_containment WHERE place_id = 'p_east'"
        ).fetchall()
        assert len(rows) == 1, (
            f"East lobe place (lon=175) should be in containment; got {rows}"
        )

    def test_west_lobe_place_in_containment(self, antimeridian_boundaries_db, tmp_path):
        """Place in west lobe (lon=-175) is matched by edge arm and appears in containment."""
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "ami_w_covering")
        stage_covering(
            str(antimeridian_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        tmp_con = duckdb.connect(":memory:")
        tmp_con.execute("INSTALL spatial; LOAD spatial;")
        qk17_west = tmp_con.execute("SELECT ST_QuadKey(?,?,17)", [-175.0, 0.0]).fetchone()[0]
        con = _make_places_con([("p_west", -175.0, 0.0, qk17_west)])
        compute_containment(
            con,
            str(antimeridian_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=str(tmp_path / "ami_w_containment"),
        )
        rows = con.execute(
            "SELECT place_id FROM place_containment WHERE place_id = 'p_west'"
        ).fetchall()
        assert len(rows) == 1, (
            f"West lobe place (lon=-175) should be in containment; got {rows}"
        )

    def test_gap_place_absent_from_containment(self, antimeridian_boundaries_db, tmp_path):
        """Gap place (lon=0) appears with gap_boundary's rkey; never with ami_boundary's.

        This is an end-to-end sanity check, not a test of the D7 CASE exclusion branch.
        ami_boundary seeds no covering tiles in the gap region (the seed SQL's D7 condition
        on the boundary bbox excludes gap tiles entirely), so no (gap_place, ami_boundary)
        row can enter the edge arm join regardless of the CASE predicate.  gap_boundary
        provides the gap place's rkey, confirming that covering was built and containment
        ran successfully for the gap region.
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "ami_g_covering")
        stage_covering(
            str(antimeridian_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        tmp_con = duckdb.connect(":memory:")
        tmp_con.execute("INSTALL spatial; LOAD spatial;")
        qk17_gap = tmp_con.execute("SELECT ST_QuadKey(?,?,17)", [0.0, 0.0]).fetchone()[0]
        con = _make_places_con([("p_gap", 0.0, 0.0, qk17_gap)])
        compute_containment(
            con,
            str(antimeridian_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=str(tmp_path / "ami_g_containment"),
        )
        rows = con.execute(
            "SELECT relations_json FROM place_containment WHERE place_id = 'p_gap'"
        ).fetchall()
        rkeys = {
            e["rkey"]
            for (rel_json,) in rows
            for e in json.loads(rel_json)["within"]
        }
        ami_rkey = f"{_COLLECTION_PREFIX}:ami_boundary"
        gap_rkey = f"{_COLLECTION_PREFIX}:gap_boundary"
        assert ami_rkey not in rkeys, (
            f"Gap place (lon=0) must NOT appear under ami_boundary (antimeridian CASE "
            f"must exclude it); rkeys found: {rkeys}"
        )
        assert gap_rkey in rkeys, (
            f"Gap place (lon=0) should appear under gap_boundary (confirms covering was "
            f"seeded for the gap region); rkeys found: {rkeys}"
        )

    def test_all_three_places_via_compute_containment(
        self, antimeridian_boundaries_db, tmp_path
    ):
        """Lobe places appear with ami_boundary; gap place appears with gap_boundary only.

        Runs compute_containment with the new signature (covering_dir + containment_dir).
        Fails with TypeError on old code — right red reason.

        What this test pins:
        - Lobe assertions (p_east, p_west): behaviorally pin the D7 OR-logic in the
          edge arm CASE.  A buggy AND condition would drop both lobe places.
        - Gap assertion (p_gap): end-to-end sanity.  ami_boundary has no covering tiles
          in the gap region (structural exclusion by the seed SQL), so the gap place is
          absent from ami_boundary's results regardless of the CASE predicate.
          gap_boundary's rkey on the gap place confirms covering was seeded correctly.
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "all3_covering")
        stage_covering(
            str(antimeridian_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        tmp_con = duckdb.connect(":memory:")
        tmp_con.execute("INSTALL spatial; LOAD spatial;")
        qk17_east = tmp_con.execute("SELECT ST_QuadKey(?,?,17)", [175.0, 0.0]).fetchone()[0]
        qk17_west = tmp_con.execute("SELECT ST_QuadKey(?,?,17)", [-175.0, 0.0]).fetchone()[0]
        qk17_gap = tmp_con.execute("SELECT ST_QuadKey(?,?,17)", [0.0, 0.0]).fetchone()[0]

        con = _make_places_con([
            ("p_east", 175.0, 0.0, qk17_east),
            ("p_west", -175.0, 0.0, qk17_west),
            ("p_gap", 0.0, 0.0, qk17_gap),
        ])
        compute_containment(
            con,
            str(antimeridian_boundaries_db),
            pk_expr="pk",
            lon_expr="longitude",
            lat_expr="latitude",
            covering_dir=covering_dir,
            containment_dir=str(tmp_path / "all3_containment"),
        )

        ami_rkey = f"{_COLLECTION_PREFIX}:ami_boundary"
        gap_rkey = f"{_COLLECTION_PREFIX}:gap_boundary"

        # Collect per-place rkeys from place_containment
        place_rkeys = {}
        for place_id, rel_json in con.execute(
            "SELECT place_id, relations_json FROM place_containment"
        ).fetchall():
            place_rkeys[place_id] = {e["rkey"] for e in json.loads(rel_json)["within"]}

        assert ami_rkey in place_rkeys.get("p_east", set()), (
            "East lobe place (lon=175) should appear with ami_boundary"
        )
        assert ami_rkey in place_rkeys.get("p_west", set()), (
            "West lobe place (lon=-175) should appear with ami_boundary"
        )
        assert ami_rkey not in place_rkeys.get("p_gap", set()), (
            "Gap place (lon=0) must NOT appear with ami_boundary "
            "(D7 CASE must exclude it even when gap_boundary seeds covering tiles nearby)"
        )
        assert gap_rkey in place_rkeys.get("p_gap", set()), (
            "Gap place (lon=0) should appear with gap_boundary "
            "(confirms the covering was seeded for the gap region)"
        )
