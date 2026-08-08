"""Red tests: compute_containment covering rewrite + Phase 2 relocation.

Contains existing Phase-1 red tests and Phase-2 containment relocation tests.

Tests call compute_containment with the new Phase-1 signature (covering_dir,
containment_dir instead of max_boundaries, max_zoom).  Against the current
implementation they fail with TypeError.  Tests that also need garganorn.covering
to build a real covering directory import it inside the test body and fail with
ModuleNotFoundError.

Test class mapping:
  1. Ports of surviving behavior tests      → TestContainmentBehaviorPorts
  2. Ordering (within by level ASC)         → TestContainmentOrdering
  3. Brute-force oracle parity              → TestBruteForceOracle
  4. Containment artifact layout            → TestContainmentArtifacts
  5. Q3 graceful degradation               → TestQ3Degradation
  6. End-to-end export integration          → TestExportIntegration
  7. Edge-arm D7 antimeridian              → TestAntimeridianEdgeArm
"""
import collections
import gzip
import inspect
import json
import os
import time

import duckdb
import pytest

from garganorn.stages import compute_containment
from garganorn.quadtree import run_pipeline

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_COLLECTION_PREFIX = "org.atgeo.places.overture.division"

# Minimal boundaries DB schema (what bnd.places must look like for compute_containment)
# level values are the atgeo containment vocabulary (garganorn.levels.LEVEL_VOCAB):
# country=10, region=25, locality=50. div_continent_na and div_borough_manhattan
# have no vocabulary entry among this fixture's modeled subtypes (continent has no
# producer entry in the containment level vocabulary; this fixture's "borough" is a locality-shaped
# boundary used only to test partial containment, not vocabulary mapping) -- 0 and 50
# respectively are chosen to preserve pre-existing ascending/partial-containment
# behavior without asserting a specific subtype mapping for them.
_SIMPLE_BOUNDARIES = [
    # (id, level, wkt, min_lat, min_lon, max_lat, max_lon)
    (
        "div_continent_na", 0,
        "POLYGON((-130 20, -130 55, -60 55, -60 20, -130 20))",
        20.0, -130.0, 55.0, -60.0,
    ),
    (
        "div_country_us", 10,
        "POLYGON((-125 24, -125 50, -66 50, -66 24, -125 24))",
        24.0, -125.0, 50.0, -66.0,
    ),
    (
        "div_region_ca", 25,
        "POLYGON((-125 34, -125 42, -118 42, -118 34, -125 34))",
        34.0, -125.0, 42.0, -118.0,
    ),
    (
        "div_locality_sf", 50,
        "POLYGON((-122.55 37.6, -122.55 37.85, -122.3 37.85, -122.3 37.6, -122.55 37.6))",
        37.6, -122.55, 37.85, -122.3,
    ),
    (
        "div_borough_manhattan", 50,
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
            level INTEGER,
            min_latitude DOUBLE,
            max_latitude DOUBLE,
            min_longitude DOUBLE,
            max_longitude DOUBLE
        )
    """)
    for bid, level, wkt, min_lat, min_lon, max_lat, max_lon in _SIMPLE_BOUNDARIES:
        conn.execute(
            "INSERT INTO places VALUES (?, ST_GeomFromText(?), ?, ?, ?, ?, ?)",
            [bid, wkt, level, min_lat, max_lat, min_lon, max_lon],
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
# Ports of surviving behavior tests
# ---------------------------------------------------------------------------

class TestContainmentBehaviorPorts:
    """Rkey-only relations, division prefix, SF point expected IDs,
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
        places_parquet = _make_parquet_places(
            tmp_path, [("p_sf", -122.4194, 37.7749)], "rkeys_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_sf", "023010")], "rkeys_ta.parquet"
        )
        covering_dir = str(tmp_path / "rkeys_covering")
        from garganorn.covering import stage_covering
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        containment_dir = str(tmp_path / "rkeys_containment")
        compute_containment(
            places_parquet, ta_parquet, str(simple_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            collection_prefix=_COLLECTION_PREFIX,
            covering_dir=covering_dir,
            force=True,
        )
        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir) if f.endswith(".parquet")
        ]
        check_con = duckdb.connect()
        rows = check_con.execute(
            f"SELECT place_id, relations_json FROM read_parquet({parquet_files!r})"
        ).fetchall()
        check_con.close()
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
        places_parquet = _make_parquet_places(
            tmp_path, [("p_sf", -122.4194, 37.7749)], "sfid_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_sf", "023010")], "sfid_ta.parquet"
        )
        from garganorn.covering import stage_covering
        sfid_covering = str(tmp_path / "sfid_covering")
        stage_covering(str(simple_boundaries_db), sfid_covering, cover_min_zoom=4, cover_max_zoom=12)
        containment_dir = str(tmp_path / "sfid_containment")
        compute_containment(
            places_parquet, ta_parquet, str(simple_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=sfid_covering,
            force=True,
        )
        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir) if f.endswith(".parquet")
        ]
        check_con = duckdb.connect()
        rows = check_con.execute(
            f"SELECT place_id, relations_json FROM read_parquet({parquet_files!r})"
        ).fetchall()
        check_con.close()
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
# Ordering: within by level ASC (Phase 2 sig).
#
# The containment level vocabulary is total by construction
# (garganorn.levels.LEVEL_VOCAB covers every subtype the fail-loud guard
# admits), so "NULL levels sort last" is no longer a live case -- there are no
# NULL levels to sort. See TestNoNullLevels below, which replaces the old
# NULL-levels-last test.
# ---------------------------------------------------------------------------

class TestContainmentOrdering:
    """Within list ordered by level ASC, ties broken by id.

    Ported to Phase 2 signature.  Fails RED with TypeError.
    """

    def test_within_ordered_by_level_asc(self, simple_boundaries_db, tmp_path):
        """The within list is sorted by level ASC (continent=0 first, locality=3 last).

        Uses Phase 2 parquet inputs.  Fails RED with TypeError.
        """
        from garganorn.covering import stage_covering
        ord_covering = str(tmp_path / "ord_covering")
        stage_covering(str(simple_boundaries_db), ord_covering, cover_min_zoom=4, cover_max_zoom=12)

        places_parquet = _make_parquet_places(
            tmp_path, [("p_sf", -122.4194, 37.7749)], "ord_places.parquet"
        )
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_sf", "023130")], "ord_ta.parquet"
        )
        containment_dir = str(tmp_path / "ord_containment")

        # Fails RED with TypeError (Phase 2 signature not yet implemented)
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(simple_boundaries_db),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
            covering_dir=ord_covering,
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        check_con = duckdb.connect(":memory:")
        rows = check_con.execute(
            "SELECT relations_json FROM read_parquet(?)", [parquet_files]
        ).fetchall()
        assert len(rows) > 0
        for (rel_json,) in rows:
            within = json.loads(rel_json)["within"]
            assert len(within) > 1, "SF point should be in multiple boundaries"
            rkeys = [e["rkey"] for e in within]
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

    def test_no_null_levels_in_boundaries_db(self, tmp_path):
        """No boundary ever has a NULL level (repurposed from the old NULL-levels-last test).

        "NULL levels sort last" is void because level
        is total by construction (`count(*) WHERE level IS
        NULL = 0` in `places`). A boundaries DB built directly (bypassing the
        import CTAS + fail-loud guard) could still contain a NULL level if
        hand-constructed, as this one deliberately does, to prove downstream
        containment code must not special-case NULL levels: this boundary set
        still resolves and orders correctly even though the level vocabulary
        can never actually put a NULL there in production.
        """
        null_db_path = tmp_path / "null_level.duckdb"
        conn = duckdb.connect(str(null_db_path))
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
        conn.execute("""
            INSERT INTO places VALUES (
                'r_named', ST_GeomFromText('POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))'),
                25, -10.0, 10.0, -10.0, 10.0
            )
        """)
        conn.execute("""
            INSERT INTO places VALUES (
                'r_null', ST_GeomFromText('POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))'),
                NULL, -10.0, 10.0, -10.0, 10.0
            )
        """)
        conn.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
        conn.close()

        from garganorn.covering import stage_covering
        null_covering = str(tmp_path / "null_covering")
        stage_covering(str(null_db_path), null_covering, cover_min_zoom=4, cover_max_zoom=12)

        places_parquet = _make_parquet_places(
            tmp_path, [("p0", 0.0, 0.0)], "null_places.parquet"
        )
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p0", "30000000000000000"[:6])], "null_ta.parquet"
        )
        containment_dir = str(tmp_path / "null_containment")

        # Fails RED with TypeError
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(null_db_path),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
            covering_dir=null_covering,
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        check_con = duckdb.connect(":memory:")
        rows = check_con.execute(
            "SELECT relations_json FROM read_parquet(?)", [parquet_files]
        ).fetchall()
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


class TestNoNullLevels:
    """count(*) WHERE level IS NULL = 0 in places.

    Exercises the real overture_division import pipeline (division_parquet fixture,
    tests/conftest.py) end to end: LEVEL_VOCAB maps every observed subtype
    (country, region, locality in this fixture), so the resulting boundaries.duckdb
    `places.level` column must be total -- zero NULLs -- once garganorn/levels.py
    exists and the import CTAS derives level from subtype instead of carrying
    raw (96%-NULL) admin_level through.
    """

    def test_places_level_has_no_nulls(self, division_parquet, tmp_path):
        from garganorn import stages

        output = str(tmp_path / "places.parquet")
        bbox = (-122.55, 37.60, -122.30, 37.85)
        stages.stage_import("overture_division", division_parquet, bbox, output)

        boundaries = str(tmp_path / "boundaries.duckdb")
        con = duckdb.connect()
        con.execute(f"ATTACH '{boundaries}' AS bnd (READ_ONLY)")
        null_count = con.execute(
            "SELECT count(*) FROM bnd.places WHERE level IS NULL"
        ).fetchone()[0]
        con.close()
        assert null_count == 0, (
            f"boundaries.duckdb places.level must never be NULL; found {null_count} NULL rows"
        )


# ---------------------------------------------------------------------------
# Brute-force oracle parity (Phase 2 signature)
# ---------------------------------------------------------------------------

class TestBruteForceOracle:
    """compute_containment pair set == brute-force ST_Contains.

    Ported to Phase 2 signature: compute_containment(places_parquet,
    tile_assignments_parquet, ...).  Fails RED because the current
    implementation still takes 'con' as its first argument.
    """

    def test_parity_with_direct_st_contains_simple_boundaries(self, simple_boundaries_db, tmp_path):
        """(place_id, boundary_id) pairs from compute_containment match direct ST_Contains.

        Uses Phase 2 parquet inputs.  Fails RED with TypeError (wrong signature).
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "oracle_covering")
        stage_covering(
            str(simple_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )

        # Compute correct qk17 values
        tmp_con = duckdb.connect(":memory:")
        tmp_con.execute("INSTALL spatial; LOAD spatial;")
        raw_places = [
            ("p_sf", -122.4194, 37.7749),
            ("p_nyc", -73.9712, 40.7831),
            ("p_ocean", -150.0, 20.0),
        ]
        places_with_qk = [
            (pid, lon, lat)
            for pid, lon, lat in raw_places
        ]

        places_parquet = _make_parquet_places(tmp_path, places_with_qk, "oracle_places.parquet")

        # tile_qk = left(qk17, 6) for each place
        ta_rows = []
        for pid, lon, lat in places_with_qk:
            qk17 = tmp_con.execute("SELECT ST_QuadKey(?,?,17)", [lon, lat]).fetchone()[0]
            if qk17:
                ta_rows.append((pid, qk17[:6]))
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, ta_rows, "oracle_ta.parquet"
        )

        containment_dir = str(tmp_path / "oracle_containment")
        # Phase 2 signature: places_parquet, tile_assignments_parquet, boundaries_db, ...
        # Fails RED with TypeError because current implementation takes 'con' first.
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(simple_boundaries_db),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
            covering_dir=covering_dir,
        )

        # Read results from containment parquet files
        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        cc_pairs = set()
        if parquet_files:
            check_con = duckdb.connect(":memory:")
            for place_id, rel_json in check_con.execute(
                "SELECT place_id, relations_json FROM read_parquet(?)",
                [parquet_files],
            ).fetchall():
                for entry in json.loads(rel_json)["within"]:
                    boundary_id = entry["rkey"].split(":", 1)[1]
                    cc_pairs.add((place_id, boundary_id))

        # brute-force oracle via ST_Contains
        oracle_con = duckdb.connect(":memory:")
        oracle_con.execute("INSTALL spatial; LOAD spatial;")
        oracle_con.execute(f"ATTACH '{simple_boundaries_db}' AS bnd (READ_ONLY)")
        oracle_pairs = set()
        for pid, lon, lat in places_with_qk:
            for (bid,) in oracle_con.execute(
                "SELECT id FROM bnd.places WHERE ST_Contains(geometry, ST_Point(?, ?))",
                [lon, lat],
            ).fetchall():
                oracle_pairs.add((pid, bid))

        assert cc_pairs == oracle_pairs, (
            f"compute_containment pairs != brute-force oracle.\n"
            f"  In CC but not oracle: {cc_pairs - oracle_pairs}\n"
            f"  In oracle but not CC: {oracle_pairs - cc_pairs}"
        )

    def test_no_duplicate_rkeys_in_within(self, simple_boundaries_db, tmp_path):
        """Each boundary appears at most once in a place's within list.

        Uses Phase 2 parquet inputs.  Fails RED with TypeError.
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "nodup_covering")
        stage_covering(
            str(simple_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )

        places_parquet = _make_parquet_places(
            tmp_path, [("p_sf", -122.4194, 37.7749)], "nodup_places.parquet"
        )
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_sf", "023130")], "nodup_ta.parquet"
        )
        containment_dir = str(tmp_path / "nodup_containment")

        # Fails RED with TypeError (wrong signature)
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(simple_boundaries_db),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
            covering_dir=covering_dir,
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) > 0, "No containment parquets written"
        check_con = duckdb.connect(":memory:")
        rows = check_con.execute(
            "SELECT relations_json FROM read_parquet(?)", [parquet_files]
        ).fetchall()
        assert len(rows) > 0
        for (rel_json,) in rows:
            within = json.loads(rel_json)["within"]
            rkeys = [e["rkey"] for e in within]
            assert len(rkeys) == len(set(rkeys)), (
                f"Duplicate rkeys in within list: {[r for r in rkeys if rkeys.count(r) > 1]}"
            )


# ---------------------------------------------------------------------------
# Containment artifact layout (Phase 2 signature)
# ---------------------------------------------------------------------------

class TestContainmentArtifacts:
    """containment/<qk4>.parquet written, schema and sort correct.

    Ported to Phase 2 signature.  Fails RED with TypeError.
    """

    def test_containment_parquets_written(self, simple_boundaries_db, tmp_path):
        """compute_containment writes containment/<qk6>.parquet files (default partition_zoom).

        Uses Phase 2 parquet inputs.  Fails RED with TypeError.
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "art_covering")
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        places_parquet = _make_parquet_places(
            tmp_path, [("p_sf", -122.4194, 37.7749)], "art_places.parquet"
        )
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_sf", "023130")], "art_ta.parquet"
        )
        containment_dir = str(tmp_path / "art_containment")

        # Fails RED with TypeError (Phase 2 signature not yet implemented)
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(simple_boundaries_db),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
            covering_dir=covering_dir,
        )

        assert os.path.isdir(containment_dir), "containment_dir not created"
        parquets = [f for f in os.listdir(containment_dir) if f.endswith(".parquet")]
        assert len(parquets) > 0, "No containment/<qk6>.parquet files written"
        for fname in parquets:
            stem = fname[:-8]
            assert len(stem) == 6, f"{fname!r}: stem not length 6"
            assert all(c in "0123" for c in stem), f"{fname!r}: non-quadkey chars"

    def test_containment_parquet_schema(self, simple_boundaries_db, tmp_path):
        """Each containment parquet has columns (tile_qk, place_id, relations_json).

        Uses Phase 2 parquet inputs.  Fails RED with TypeError.
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "sch_covering")
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        places_parquet = _make_parquet_places(
            tmp_path, [("p_sf", -122.4194, 37.7749)], "sch_places.parquet"
        )
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_sf", "023130")], "sch_ta.parquet"
        )
        containment_dir = str(tmp_path / "sch_containment")

        # Fails RED with TypeError
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(simple_boundaries_db),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
            covering_dir=covering_dir,
        )

        parquets = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
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
        """Each containment parquet is sorted by (tile_qk, place_id).

        Uses Phase 2 parquet inputs.  Fails RED with TypeError.
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "srt_covering")
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        places_parquet = _make_parquet_places(
            tmp_path,
            [("p_sf", -122.4194, 37.7749), ("p_nyc", -73.9712, 40.7831)],
            "srt_places.parquet",
        )
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path,
            [("p_sf", "023130"), ("p_nyc", "032010")],
            "srt_ta.parquet",
        )
        containment_dir = str(tmp_path / "srt_containment")

        # Fails RED with TypeError
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(simple_boundaries_db),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
            covering_dir=covering_dir,
        )

        parquet_files = [f for f in os.listdir(containment_dir) if f.endswith(".parquet")]
        check_con = duckdb.connect(":memory:")
        for fname in parquet_files:
            path = os.path.join(containment_dir, fname)
            rows = check_con.execute(
                "SELECT tile_qk, place_id FROM read_parquet(?)", [path]
            ).fetchall()
            assert rows == sorted(rows), f"{fname}: not sorted by (tile_qk, place_id)"

    def test_tile_qk_in_parquet_matches_tile_assignments(self, simple_boundaries_db, tmp_path):
        """tile_qk in containment parquets agrees with tile_assignments for each place.

        Uses Phase 2 parquet inputs.  Fails RED with TypeError.
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "tqk_covering")
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        tile_qk = "023130"
        places_parquet = _make_parquet_places(
            tmp_path, [("p_sf", -122.4194, 37.7749)], "tqk_places.parquet"
        )
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_sf", tile_qk)], "tqk_ta.parquet"
        )
        containment_dir = str(tmp_path / "tqk_containment")

        # Fails RED with TypeError
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(simple_boundaries_db),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
            covering_dir=covering_dir,
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
        assert tile_qks_in_parquets.issubset({tile_qk}), (
            f"Containment tile_qk values not in expected set: "
            f"{tile_qks_in_parquets - {tile_qk}}"
        )


# ---------------------------------------------------------------------------
# Q3 graceful degradation
# ---------------------------------------------------------------------------

class TestQ3Degradation:
    """boundaries_db=None, missing/empty covering_dir → empty place_containment."""

    def test_boundaries_db_none_gives_empty_containment(self, tmp_path):
        """boundaries_db=None → containment is created empty (Q3 preserved)."""
        places_parquet = _make_parquet_places(
            tmp_path, [("p1", 0.0, 0.0)], "q3_no_bnd_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p1", "222222")], "q3_no_bnd_ta.parquet"
        )
        containment_dir = str(tmp_path / "q3_no_bnd_containment")
        compute_containment(
            places_parquet, ta_parquet, None,
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=str(tmp_path / "q3_no_bnd_covering"),
            force=True,
        )
        parquet_files = [f for f in os.listdir(containment_dir) if f.endswith(".parquet")]
        assert parquet_files == [], (
            f"boundaries_db=None should yield no containment parquets, got: {parquet_files}"
        )
        meta = json.loads(open(os.path.join(containment_dir, "_meta.json")).read())
        assert meta.get("empty") is True, (
            f"boundaries_db=None should set empty=True in _meta.json, got: {meta}"
        )

    def test_missing_covering_dir_gives_empty_containment(self, simple_boundaries_db, tmp_path):
        """covering_dir that does not exist → containment is created empty."""
        places_parquet = _make_parquet_places(
            tmp_path, [("p1", -122.4194, 37.7749)], "q3_no_cov_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p1", "023010")], "q3_no_cov_ta.parquet"
        )
        missing_dir = str(tmp_path / "does_not_exist")
        assert not os.path.exists(missing_dir)
        containment_dir = str(tmp_path / "q3_no_cov_containment")
        compute_containment(
            places_parquet, ta_parquet, str(simple_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=missing_dir,
            force=True,
        )
        parquet_files = [f for f in os.listdir(containment_dir) if f.endswith(".parquet")]
        assert parquet_files == [], (
            "Missing covering_dir should yield no containment parquets, got non-empty"
        )
        meta = json.loads(open(os.path.join(containment_dir, "_meta.json")).read())
        assert meta.get("empty") is True

    def test_empty_covering_dir_gives_empty_containment(self, simple_boundaries_db, tmp_path):
        """An empty covering_dir (no parquet files) → containment is empty."""
        covering_dir = str(tmp_path / "empty_covering")
        os.makedirs(covering_dir)  # exists but empty
        places_parquet = _make_parquet_places(
            tmp_path, [("p1", -122.4194, 37.7749)], "q3_empty_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p1", "023010")], "q3_empty_ta.parquet"
        )
        containment_dir = str(tmp_path / "q3_empty_containment")
        compute_containment(
            places_parquet, ta_parquet, str(simple_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir,
            force=True,
        )
        parquet_files = [f for f in os.listdir(containment_dir) if f.endswith(".parquet")]
        assert parquet_files == [], (
            "Empty covering_dir should yield no containment parquets, got non-empty"
        )
        meta = json.loads(open(os.path.join(containment_dir, "_meta.json")).read())
        assert meta.get("empty") is True

    def test_place_outside_all_boundaries_absent_from_output(self, simple_boundaries_db, tmp_path):
        """Place with no containing boundary must not appear in containment."""
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
        tmp_con.close()

        places_parquet = _make_parquet_places(
            tmp_path, [("p_ocean", -150.0, 20.0)], "absent_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_ocean", qk17[:6])], "absent_ta.parquet"
        )
        containment_dir = str(tmp_path / "absent_containment")
        compute_containment(
            places_parquet, ta_parquet, str(simple_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir,
            force=True,
        )
        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        if parquet_files:
            check_con = duckdb.connect()
            rows = check_con.execute(
                f"SELECT * FROM read_parquet({parquet_files!r}) WHERE place_id = 'p_ocean'"
            ).fetchall()
            check_con.close()
            assert rows == [], (
                f"Place in the ocean should have no containment rows, got: {rows}"
            )


# ---------------------------------------------------------------------------
# End-to-end export integration
# ---------------------------------------------------------------------------

class TestExportIntegration:
    """Mini end-to-end run_pipeline producing tile JSON with relations."""

    def test_run_pipeline_with_covering_builds_covering_dir(
        self, simple_boundaries_db, overture_parquet, density_parquet, tmp_path
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
            "overture_place",
            overture_parquet,
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
        """Containment parquets are join-compatible with export SQL.

        Verifies that containment output has (place_id, relations_json) columns
        and can be joined with places data.
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "compat_covering")
        stage_covering(
            str(simple_boundaries_db),
            covering_dir,
            cover_min_zoom=4,
            cover_max_zoom=12,
        )
        places_parquet = _make_parquet_places(
            tmp_path, [("p_sf", -122.4194, 37.7749)], "compat_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_sf", "023010")], "compat_ta.parquet"
        )
        containment_dir = str(tmp_path / "compat_containment")
        compute_containment(
            places_parquet, ta_parquet, str(simple_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir,
            force=True,
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) > 0, "containment parquets must exist"
        check_con = duckdb.connect()
        cols = {
            row[0]
            for row in check_con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [parquet_files[:1]]
            ).fetchall()
        }
        assert "place_id" in cols, "place_containment missing 'place_id' column"
        assert "relations_json" in cols, "place_containment missing 'relations_json' column"

        # join on place_id should work
        result = check_con.execute(f"""
            SELECT pc.place_id, pc.relations_json
            FROM read_parquet({parquet_files!r}) pc
            JOIN read_parquet('{places_parquet}') p ON p.place_id = pc.place_id
        """).fetchall()
        assert len(result) > 0, "JOIN between containment parquets and places returned no rows"
        check_con.close()


# ---------------------------------------------------------------------------
# Edge-arm antimeridian D7
# ---------------------------------------------------------------------------

class TestAntimeridianEdgeArm:
    """D7 antimeridian edge-arm tests.

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
        branch.  The covering seed SQL uses the same D7 OR-condition on the
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
                level INTEGER,
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
            ["ami_boundary", ami_wkt, 10, -15.0, 15.0, 170.0, -170.0],
        )
        # Non-antimeridian boundary covering the gap area around lon=0.
        # Ensures the gap place has a valid containment result (gap_boundary's rkey)
        # so the gap assertion is a meaningful end-to-end check rather than
        # vacuously asserting absence from an empty result set.
        gap_wkt = "POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))"
        conn.execute(
            "INSERT INTO places VALUES (?, ST_GeomFromText(?), ?, ?, ?, ?, ?)",
            ["gap_boundary", gap_wkt, 25, -10.0, 10.0, -10.0, 10.0],
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
        tmp_con.close()

        places_parquet = _make_parquet_places(
            tmp_path, [("p_east", 175.0, 0.0)], "ami_e_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_east", qk17_east[:6])], "ami_e_ta.parquet"
        )
        containment_dir = str(tmp_path / "ami_e_containment")
        compute_containment(
            places_parquet, ta_parquet, str(antimeridian_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir,
            force=True,
        )
        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) > 0, "No containment parquets for east lobe place"
        check_con = duckdb.connect()
        rows = check_con.execute(
            f"SELECT place_id FROM read_parquet({parquet_files!r}) WHERE place_id = 'p_east'"
        ).fetchall()
        check_con.close()
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
        tmp_con.close()

        places_parquet = _make_parquet_places(
            tmp_path, [("p_west", -175.0, 0.0)], "ami_w_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_west", qk17_west[:6])], "ami_w_ta.parquet"
        )
        containment_dir = str(tmp_path / "ami_w_containment")
        compute_containment(
            places_parquet, ta_parquet, str(antimeridian_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir,
            force=True,
        )
        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) > 0, "No containment parquets for west lobe place"
        check_con = duckdb.connect()
        rows = check_con.execute(
            f"SELECT place_id FROM read_parquet({parquet_files!r}) WHERE place_id = 'p_west'"
        ).fetchall()
        check_con.close()
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
        tmp_con.close()

        places_parquet = _make_parquet_places(
            tmp_path, [("p_gap", 0.0, 0.0)], "ami_g_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_gap", qk17_gap[:6])], "ami_g_ta.parquet"
        )
        containment_dir = str(tmp_path / "ami_g_containment")
        compute_containment(
            places_parquet, ta_parquet, str(antimeridian_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir,
            force=True,
        )
        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        rkeys = set()
        if parquet_files:
            check_con = duckdb.connect()
            for (rel_json,) in check_con.execute(
                f"SELECT relations_json FROM read_parquet({parquet_files!r}) WHERE place_id = 'p_gap'"
            ).fetchall():
                for e in json.loads(rel_json)["within"]:
                    rkeys.add(e["rkey"])
            check_con.close()
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
        tmp_con.close()

        places_parquet = _make_parquet_places(
            tmp_path,
            [("p_east", 175.0, 0.0), ("p_west", -175.0, 0.0), ("p_gap", 0.0, 0.0)],
            "all3_places.parquet",
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path,
            [
                ("p_east", qk17_east[:6]),
                ("p_west", qk17_west[:6]),
                ("p_gap", qk17_gap[:6]),
            ],
            "all3_ta.parquet",
        )
        containment_dir = str(tmp_path / "all3_containment")
        compute_containment(
            places_parquet, ta_parquet, str(antimeridian_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir,
            force=True,
        )

        ami_rkey = f"{_COLLECTION_PREFIX}:ami_boundary"
        gap_rkey = f"{_COLLECTION_PREFIX}:gap_boundary"

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir)
            if f.endswith(".parquet")
        ]
        place_rkeys = {}
        if parquet_files:
            check_con = duckdb.connect()
            for place_id, rel_json in check_con.execute(
                f"SELECT place_id, relations_json FROM read_parquet({parquet_files!r})"
            ).fetchall():
                place_rkeys[place_id] = {e["rkey"] for e in json.loads(rel_json)["within"]}
            check_con.close()

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


# ---------------------------------------------------------------------------
# Phase 2 containment relocation tests (RED)
# ---------------------------------------------------------------------------

def _make_parquet_places(tmp_path, places, filename="places.parquet"):
    """Write places.parquet with (place_id, qk17, latitude, longitude)."""
    path = str(tmp_path / filename)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows_sql = ", ".join(
        f"('{pid}', {lat}, {lon}, ST_QuadKey({lon}, {lat}, 17))"
        for pid, lon, lat in places
    )
    con.execute(f"""
        COPY (
            SELECT place_id, latitude, longitude, qk17
            FROM (VALUES {rows_sql}) t(place_id, latitude, longitude, qk17)
        ) TO '{path}' (FORMAT PARQUET)
    """)
    con.close()
    return path


def _make_parquet_tile_assignments(tmp_path, assignments, filename="tile_assignments.parquet"):
    """Write tile_assignments.parquet with (place_id, tile_qk)."""
    path = str(tmp_path / filename)
    con = duckdb.connect()
    rows_sql = ", ".join(f"('{pid}', '{tqk}')" for pid, tqk in assignments)
    con.execute(f"""
        COPY (
            SELECT place_id, tile_qk
            FROM (VALUES {rows_sql}) t(place_id, tile_qk)
        ) TO '{path}' (FORMAT PARQUET)
    """)
    con.close()
    return path


class TestContainmentRelocationPhase2:
    """compute_containment must use parquet inputs and write to <src>/containment/.

    All tests fail in Red phase because compute_containment still takes 'con'
    as its first parameter.
    """

    _SF_PLACES = [
        ("p001", -122.4194, 37.7749),
        ("p002", -122.4862, 37.7694),
    ]

    def test_compute_containment_no_con_parameter(self):
        """compute_containment must not take 'con' as its first parameter."""
        params = list(inspect.signature(compute_containment).parameters.keys())
        assert params[0] != "con", (
            f"compute_containment must not have 'con' as first param; got {params[0]!r}. "
            "Phase 2 drops the connection argument."
        )

    def test_compute_containment_has_places_parquet_param(self):
        """compute_containment must accept a places_parquet parameter."""
        params = list(inspect.signature(compute_containment).parameters.keys())
        assert "places_parquet" in params, (
            f"compute_containment missing places_parquet param; params: {params}"
        )

    def test_compute_containment_writes_to_containment_dir(
        self, simple_boundaries_db, tmp_path
    ):
        """compute_containment must write its output under <src>/containment/ (not run dir)."""
        places_parquet = _make_parquet_places(tmp_path, self._SF_PLACES)
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path,
            [("p001", "023130"), ("p002", "023130")],
        )
        containment_dir = str(tmp_path / "containment")
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(simple_boundaries_db),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
        )
        assert os.path.isdir(containment_dir), (
            f"compute_containment must create containment directory at {containment_dir}"
        )
        meta_path = os.path.join(containment_dir, "_meta.json")
        assert os.path.exists(meta_path), (
            f"containment/_meta.json must exist at {meta_path}"
        )

    def test_compute_containment_q3_boundaries_none(self, tmp_path):
        """With boundaries_db=None, compute_containment must still write containment/_meta.json."""
        places_parquet = _make_parquet_places(tmp_path, self._SF_PLACES)
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p001", "023130")],
        )
        containment_dir = str(tmp_path / "containment_q3")
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            None,              # boundaries_db=None → Q3 degradation
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
        )
        meta_path = os.path.join(containment_dir, "_meta.json")
        assert os.path.exists(meta_path), (
            "Q3 degradation: containment/_meta.json must exist even when boundaries_db=None"
        )

    def test_compute_containment_freshness_against_places_parquet(
        self, simple_boundaries_db, tmp_path
    ):
        """Touching places.parquet must make containment stale and trigger rebuild."""
        places_parquet = _make_parquet_places(tmp_path, self._SF_PLACES)
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p001", "023130")],
        )
        containment_dir = str(tmp_path / "containment")
        compute_containment(
            places_parquet, tile_assignments_parquet,
            str(simple_boundaries_db), "place_id", "longitude", "latitude",
            containment_dir,
        )
        meta_path = os.path.join(containment_dir, "_meta.json")
        mtime1 = os.path.getmtime(meta_path)
        # Touch places.parquet to make containment stale
        time.sleep(0.05)
        os.utime(places_parquet, None)
        time.sleep(0.05)
        compute_containment(
            places_parquet, tile_assignments_parquet,
            str(simple_boundaries_db), "place_id", "longitude", "latitude",
            containment_dir,
        )
        mtime2 = os.path.getmtime(meta_path)
        assert mtime2 > mtime1, (
            "compute_containment must rebuild when places.parquet is touched"
        )


# ---------------------------------------------------------------------------
# Dir-swap atomicity matrix (mirrors covering atomicity tests)
# ---------------------------------------------------------------------------

class TestContainmentDirSwapAtomicity:
    """Dir-swap atomicity matrix for containment.

    Mirrors tests/test_covering.py::TestFreshnessAtomicity:
      1. Leftover .tmp before build → clobbered, correct output
      2. Leftover .old with dir missing → cleared, correct output
      3. Partial .tmp (no _meta.json) from crash → next build correct

    All tests fail RED because compute_containment does not yet accept parquet inputs.
    """

    _SF_PLACES = [("p001", -122.4194, 37.7749)]
    _SF_ASSIGNMENTS = [("p001", "023130")]

    def _build(self, tmp_path, simple_boundaries_db, covering_dir, containment_dir,
                places_file="at_places.parquet", ta_file="at_ta.parquet"):
        places_parquet = _make_parquet_places(tmp_path, self._SF_PLACES, places_file)
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, self._SF_ASSIGNMENTS, ta_file
        )
        # Fails RED: wrong signature (con expected, not places_parquet)
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(simple_boundaries_db),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
            covering_dir=covering_dir,
        )

    def test_leftover_tmp_before_build_is_clobbered(
        self, simple_boundaries_db, tmp_path
    ):
        """Leftover containment.tmp/ before build must be clobbered, not reused.

        Fails RED with TypeError (Phase 2 signature not yet implemented).
        """
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "at_covering")
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        containment_dir = str(tmp_path / "containment")
        # Plant stale containment.tmp/ (leftover from previous crash during build)
        stale_tmp = tmp_path / "containment.tmp"
        stale_tmp.mkdir()
        (stale_tmp / "stale.parquet").write_bytes(b"garbage-leftover")

        self._build(tmp_path, simple_boundaries_db, covering_dir, containment_dir)

        assert not stale_tmp.exists(), (
            "stale containment.tmp/ must be clobbered before build"
        )
        assert os.path.isdir(containment_dir), "containment/ must exist after build"
        assert os.path.exists(os.path.join(containment_dir, "_meta.json")), (
            "_meta.json must exist after successful build"
        )

    def test_leftover_old_with_dir_missing_is_cleared(
        self, simple_boundaries_db, tmp_path
    ):
        """Leftover containment.old/ (containment/ missing) → build must clear and succeed.

        Fails RED with TypeError.
        """
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "at2_covering")
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        containment_dir = str(tmp_path / "containment2")
        # Simulate crash state: .old exists but final dir does not
        stale_old = tmp_path / "containment2.old"
        stale_old.mkdir()
        (stale_old / "old_data.parquet").write_bytes(b"old-data")

        self._build(
            tmp_path, simple_boundaries_db, covering_dir, containment_dir,
            "at2_places.parquet", "at2_ta.parquet"
        )

        assert not stale_old.exists(), "stale containment.old/ must be removed after build"
        assert os.path.isdir(containment_dir), "containment/ must be created"

    def test_partial_tmp_from_crash_is_rebuilt(
        self, simple_boundaries_db, tmp_path
    ):
        """Partial containment.tmp/ (no _meta.json) from crash → next build correct.

        Fails RED with TypeError.
        """
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "at3_covering")
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        containment_dir = str(tmp_path / "containment3")
        # Partial .tmp: has a parquet file but no _meta.json (crash mid-build)
        partial_tmp = tmp_path / "containment3.tmp"
        partial_tmp.mkdir()
        (partial_tmp / "0230.parquet").write_bytes(b"partial-crash-data")
        # No _meta.json → directory is stale; must be rebuilt from scratch

        self._build(
            tmp_path, simple_boundaries_db, covering_dir, containment_dir,
            "at3_places.parquet", "at3_ta.parquet"
        )

        assert not partial_tmp.exists(), "partial .tmp must be cleaned up"
        assert os.path.isdir(containment_dir), "containment/ must be created"
        meta_path = os.path.join(containment_dir, "_meta.json")
        assert os.path.exists(meta_path), "_meta.json must exist after successful rebuild"


# ---------------------------------------------------------------------------
# Q3 export relations: {} + idempotency
# ---------------------------------------------------------------------------

class TestContainmentQ3ExportAndIdempotency:
    """Q3 empty containment → relations:{} in export; idempotency.

    Uses Phase 2 signature.  All tests fail RED with TypeError.
    """

    def test_compute_containment_idempotent_q3(self, tmp_path):
        """Calling compute_containment twice (boundaries_db=None) must not error; second call
        is a no-op (containment dir already fresh).

        Fails RED with TypeError (Phase 2 signature not yet implemented).
        """
        places_parquet = _make_parquet_places(
            tmp_path, [("p001", -122.4, 37.7)], "idem_places.parquet"
        )
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p001", "023130")], "idem_ta.parquet"
        )
        containment_dir = str(tmp_path / "idem_containment")

        # First call
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            None,  # boundaries_db=None → Q3 degradation
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
        )
        meta_path = os.path.join(containment_dir, "_meta.json")
        assert os.path.exists(meta_path), "_meta.json must exist after first call"
        mtime1 = os.path.getmtime(meta_path)

        # Second call must be a no-op (containment is fresh)
        time.sleep(0.05)
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            None,
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
        )
        mtime2 = os.path.getmtime(meta_path)
        assert mtime2 == mtime1, (
            "Second call with fresh inputs must be a no-op (idempotency)"
        )

    def test_compute_containment_idempotent_with_boundaries(
        self, simple_boundaries_db, tmp_path
    ):
        """compute_containment is idempotent even with boundaries_db.

        Ported from TestComputeContainmentIdempotency::test_compute_containment_with_boundaries_idempotent.
        Fails RED with TypeError.
        """
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "idem2_covering")
        stage_covering(
            str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12
        )

        places_parquet = _make_parquet_places(
            tmp_path, [("p001", -122.4194, 37.7749)], "idem2_places.parquet"
        )
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p001", "023130")], "idem2_ta.parquet"
        )
        containment_dir = str(tmp_path / "idem2_containment")

        # First call
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(simple_boundaries_db),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
            covering_dir=covering_dir,
        )
        meta_path = os.path.join(containment_dir, "_meta.json")
        mtime1 = os.path.getmtime(meta_path)

        # Second call (idempotency)
        time.sleep(0.05)
        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            str(simple_boundaries_db),
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
            covering_dir=covering_dir,
        )
        mtime2 = os.path.getmtime(meta_path)
        assert mtime2 == mtime1, (
            "Second call with fresh inputs and boundaries must be a no-op"
        )

    def test_q3_containment_dir_only_has_meta_json(self, tmp_path):
        """Q3: boundaries_db=None → containment/ contains only _meta.json (no parquets).

        Fails RED with TypeError.
        """
        places_parquet = _make_parquet_places(
            tmp_path, [("p001", 0.0, 0.0)], "q3_places.parquet"
        )
        tile_assignments_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p001", "300000")], "q3_ta.parquet"
        )
        containment_dir = str(tmp_path / "q3_containment")

        compute_containment(
            places_parquet,
            tile_assignments_parquet,
            None,  # Q3 degradation
            "place_id",
            "longitude",
            "latitude",
            containment_dir,
        )

        assert os.path.isdir(containment_dir), "containment/ must be created even in Q3"
        meta_path = os.path.join(containment_dir, "_meta.json")
        assert os.path.exists(meta_path), "_meta.json must exist in Q3 mode"

        # In Q3 mode, no .parquet files (no containment data)
        parquets = [f for f in os.listdir(containment_dir) if f.endswith(".parquet")]
        assert len(parquets) == 0, (
            f"Q3 containment must have no .parquet files; found: {parquets}"
        )

        # The meta should record "empty": true
        with open(meta_path) as f:
            meta = json.load(f)
        assert meta.get("empty") is True, (
            f"Q3 containment _meta.json must have 'empty': true; got: {meta}"
        )


# ---------------------------------------------------------------------------
# compute_containment OOM fix: partition_zoom kwarg
# ---------------------------------------------------------------------------

class TestContainmentPartitionZoom:
    """RED tests for the new partition_zoom kwarg (default 6, was hardcoded z4).

    All three tests fail against current code with TypeError: the current
    compute_containment signature has no partition_zoom parameter at all.
    """

    def test_partition_zoom_param_accepted(self, simple_boundaries_db, tmp_path):
        """compute_containment accepts partition_zoom=4 (explicit opt-in to old behavior).

        Fails RED with TypeError: unexpected keyword argument 'partition_zoom' --
        the current code has no such parameter.
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "pz4_covering")
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        places_parquet = _make_parquet_places(
            tmp_path, [("p_sf", -122.4194, 37.7749)], "pz4_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_sf", "023010")], "pz4_ta.parquet"
        )
        containment_dir = str(tmp_path / "pz4_containment")

        compute_containment(
            places_parquet, ta_parquet, str(simple_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir,
            partition_zoom=4,
            force=True,
        )

        parquets = [f for f in os.listdir(containment_dir) if f.endswith(".parquet")]
        assert len(parquets) > 0, "No containment parquets written"
        for fname in parquets:
            stem = fname[:-8]
            assert len(stem) == 4, (
                f"{fname!r}: explicit partition_zoom=4 should produce 4-char stems, got {stem!r}"
            )

    def test_partition_zoom_default_produces_finer_stems(self, simple_boundaries_db, tmp_path):
        """With no partition_zoom argument, output stems default to 6 chars, not 4.

        Fails RED for two reasons: current code produces 4-char stems
        unconditionally (so asserting 6 fails), and there is no partition_zoom
        kwarg yet for a default to apply to.
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "pzdef_covering")
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        places_parquet = _make_parquet_places(
            tmp_path, [("p_sf", -122.4194, 37.7749)], "pzdef_places.parquet"
        )
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_sf", "023010")], "pzdef_ta.parquet"
        )
        containment_dir = str(tmp_path / "pzdef_containment")

        compute_containment(
            places_parquet, ta_parquet, str(simple_boundaries_db),
            "place_id", "longitude", "latitude", containment_dir,
            covering_dir=covering_dir,
            force=True,
        )

        parquets = [f for f in os.listdir(containment_dir) if f.endswith(".parquet")]
        assert len(parquets) > 0, "No containment parquets written"
        for fname in parquets:
            stem = fname[:-8]
            assert len(stem) == 6, (
                f"{fname!r}: default partition_zoom should produce 6-char stems, got {stem!r}"
            )

    def test_partition_zoom_output_rows_invariant(self, simple_boundaries_db, tmp_path):
        """Repartitioning at z4 vs z6 must produce the identical set of output rows.

        Fixture: p_a (-122.4194, 37.7749) and p_e (-120.0, 35.0) share qk17[:4] ==
        "0230" (same z4 covering file, same z4 output batch) but differ at
        qk17[:6] ("023010" vs "023012" -- different z6 output batches). Verified
        empirically at test-write time with ST_QuadKey(lon, lat, 17) against these
        exact coordinates. Both points fall inside div_continent_na's,
        div_country_us's, and div_region_ca's bboxes; p_a additionally falls
        inside div_locality_sf's bbox, p_e does not -- so both produce
        non-trivial, comparable containment relations.

        Fails RED with TypeError: partition_zoom is not a recognized keyword
        argument on the current compute_containment signature (fails on the
        first, partition_zoom=4, sub-run).
        """
        from garganorn.covering import stage_covering

        covering_dir = str(tmp_path / "pzinv_covering")
        stage_covering(str(simple_boundaries_db), covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        places = [("p_a", -122.4194, 37.7749), ("p_e", -120.0, 35.0)]
        places_parquet = _make_parquet_places(tmp_path, places, "pzinv_places.parquet")
        ta_parquet = _make_parquet_tile_assignments(
            tmp_path, [("p_a", "023010"), ("p_e", "023012")], "pzinv_ta.parquet"
        )

        def _run(partition_zoom, dirname):
            containment_dir = str(tmp_path / dirname)
            kwargs = dict(covering_dir=covering_dir, force=True)
            if partition_zoom is not None:
                kwargs["partition_zoom"] = partition_zoom
            compute_containment(
                places_parquet, ta_parquet, str(simple_boundaries_db),
                "place_id", "longitude", "latitude", containment_dir,
                **kwargs,
            )
            parquet_files = [
                os.path.join(containment_dir, f)
                for f in os.listdir(containment_dir)
                if f.endswith(".parquet")
            ]
            if not parquet_files:
                return collections.Counter()
            check_con = duckdb.connect()
            rows = check_con.execute(
                f"SELECT tile_qk, place_id, relations_json FROM read_parquet({parquet_files!r})"
            ).fetchall()
            check_con.close()
            return collections.Counter(rows)

        rows_z4 = _run(4, "pzinv_z4_containment")
        rows_z6 = _run(None, "pzinv_z6_containment")

        assert len(rows_z4) > 0, "z4 run produced no containment rows"
        assert rows_z4 == rows_z6, (
            f"Repartitioning changed output rows.\n"
            f"  Only in z4 run: {rows_z4 - rows_z6}\n"
            f"  Only in z6 run: {rows_z6 - rows_z4}"
        )
