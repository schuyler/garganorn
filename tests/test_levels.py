"""RED tests: garganorn.levels.LEVEL_VOCAB and the atgeo containment level vocabulary.

phase2b-design.md Part A (OQ-P2-2). garganorn/levels.py does not exist yet, so
every test in this module that imports LEVEL_VOCAB fails at collection/setup
with ImportError until it is implemented -- that failure IS the RED signal for
this feature (§6 acceptance item 1). Tests are structured so the ImportError
is legible: a module-level try/except records it and _check_levels() surfaces
it as a clear pytest.fail rather than an opaque collection error, mirroring
the pattern tests/test_covering.py uses for garganorn.covering.

Covers phase2b-design.md §6 (combined acceptance checklist) items 1, 2, 3, 5:
  1. Vocabulary correctness -- LEVEL_VOCAB covers exactly the 9 observed
     subtypes + borough; fail-loud raises on an injected unknown subtype.
  2. Ordering -- containment query returning multiple levels is ordered
     ascending by level, ties broken by id.
  3. Filter delta -- boundaries.duckdb now includes localadmin and
     admin_level=3 counties; hoods excluded.
  5. No NULL levels -- count(*) WHERE level IS NULL = 0 in places.

(Item 4, cross-artifact tile/boundaries.duckdb agreement, belongs to Part B's
combined fixture per §6 preamble and is out of scope for this Part-A-only
test module.)
"""
import inspect

import duckdb
import pytest

try:
    from garganorn.levels import LEVEL_VOCAB
    _LEVELS_ERROR = None
except ImportError as _exc:
    LEVEL_VOCAB = None
    _LEVELS_ERROR = _exc


def _check_levels():
    """Call at the start of every test that needs LEVEL_VOCAB; surfaces the ImportError."""
    if _LEVELS_ERROR is not None:
        pytest.fail(
            f"garganorn.levels.LEVEL_VOCAB not importable: {_LEVELS_ERROR}",
            pytrace=False,
        )


# ---------------------------------------------------------------------------
# §6 item 1 — vocabulary correctness
# ---------------------------------------------------------------------------

class TestLevelVocabCorrectness:
    """LEVEL_VOCAB covers exactly the 9 observed subtypes + borough (phase2b-design.md §A.3)."""

    _EXPECTED_SUBTYPES = {
        "country", "dependency", "region", "county", "localadmin",
        "locality", "borough", "macrohood", "neighborhood", "microhood",
    }

    _EXPECTED_VALUES = {
        "country": 10,
        "dependency": 15,
        "region": 25,
        "county": 35,
        "localadmin": 45,
        "locality": 50,
        "borough": 55,
        "macrohood": 60,
        "neighborhood": 65,
        "microhood": 70,
    }

    def test_level_vocab_importable(self):
        """garganorn.levels.LEVEL_VOCAB must exist and be importable."""
        _check_levels()
        assert LEVEL_VOCAB is not None

    def test_level_vocab_keys_are_exactly_the_ten_subtypes(self):
        """LEVEL_VOCAB's key set is exactly the 9 observed subtypes plus borough.

        9 subtypes empirically observed in the division theme (phase2b-design.md
        §A.2, global, zero NULLs): locality, neighborhood, microhood, macrohood,
        county, localadmin, region, country, dependency. Plus borough (normative
        in §1.7 though absent from current Overture data, §A.3).
        """
        _check_levels()
        assert set(LEVEL_VOCAB.keys()) == self._EXPECTED_SUBTYPES, (
            f"LEVEL_VOCAB keys {set(LEVEL_VOCAB.keys())} != "
            f"expected {self._EXPECTED_SUBTYPES}"
        )

    def test_level_vocab_values_match_stride_five_table(self):
        """Each subtype maps to its §A.3 stride-5 value; no admin_level involved."""
        _check_levels()
        assert LEVEL_VOCAB == self._EXPECTED_VALUES, (
            f"LEVEL_VOCAB {LEVEL_VOCAB} != expected {self._EXPECTED_VALUES}"
        )

    def test_level_vocab_values_are_unique(self):
        """No two subtypes share a level value (each subtype sorts distinctly)."""
        _check_levels()
        values = list(LEVEL_VOCAB.values())
        assert len(values) == len(set(values)), (
            f"LEVEL_VOCAB has duplicate level values: {LEVEL_VOCAB}"
        )

    def test_level_vocab_values_ascending_in_hierarchy_order(self):
        """Values increase monotonically from country (most general) to microhood (most specific)."""
        _check_levels()
        hierarchy_order = [
            "country", "dependency", "region", "county", "localadmin",
            "locality", "borough", "macrohood", "neighborhood", "microhood",
        ]
        values_in_order = [LEVEL_VOCAB[s] for s in hierarchy_order]
        assert values_in_order == sorted(values_in_order), (
            f"LEVEL_VOCAB values not monotonically ascending in hierarchy order: "
            f"{list(zip(hierarchy_order, values_in_order))}"
        )


# ---------------------------------------------------------------------------
# §6 item 1 (continued) — fail-loud enforcement (phase2b-design.md §A.6)
# ---------------------------------------------------------------------------

class TestFailLoudUnmappedSubtype:
    """stage_division_import must raise RuntimeError on an unmapped/NULL subtype.

    phase2b-design.md §A.6: the guard runs in stage_division_import() after the
    division_all CTAS and before any artifact write, using
    `SELECT DISTINCT subtype FROM division_all WHERE subtype IS NULL OR
    subtype NOT IN (...)`. This test injects a division row with a subtype
    LEVEL_VOCAB does not recognize and asserts the whole stage raises rather
    than silently defaulting or guessing.
    """

    _BBOX = (-180, -90, 180, 90)

    def _make_bad_division_parquet(self, tmp_path, bad_subtype):
        """Write a minimal division/division_area parquet pair with one bad-subtype row."""
        division_path = tmp_path / "division_bad.parquet"
        division_area_path = tmp_path / "division_area_bad.parquet"

        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE tmp_division (
                id VARCHAR,
                names STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR),
                             rules STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]),
                subtype VARCHAR,
                country VARCHAR,
                region VARCHAR,
                wikidata VARCHAR,
                population BIGINT,
                parent_division_id VARCHAR
            )
        """)
        subtype_sql = f"'{bad_subtype}'" if bad_subtype is not None else "NULL"
        conn.execute(f"""
            INSERT INTO tmp_division VALUES (
                'div_bad',
                {{'primary': 'Bad Division', 'common': map([]::VARCHAR[], []::VARCHAR[]),
                  'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]}},
                {subtype_sql},
                'US', NULL, NULL, 1000, NULL
            )
        """)
        conn.execute(f"COPY tmp_division TO '{division_path}' (FORMAT PARQUET)")

        conn.execute("""
            CREATE TABLE tmp_division_area (
                division_id VARCHAR,
                admin_level INTEGER,
                is_land BOOLEAN,
                geometry VARCHAR,
                bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)
            )
        """)
        conn.execute("""
            INSERT INTO tmp_division_area VALUES (
                'div_bad',
                NULL,
                true,
                'POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))',
                {'xmin': -10.0, 'ymin': -10.0, 'xmax': 10.0, 'ymax': 10.0}
            )
        """)
        conn.execute(f"COPY tmp_division_area TO '{division_area_path}' (FORMAT PARQUET)")
        conn.close()
        return (str(division_path), str(division_area_path))

    def test_unknown_subtype_raises_runtime_error(self, tmp_path):
        """A division row with subtype='nation_state' (not in LEVEL_VOCAB) raises RuntimeError."""
        from garganorn import stages

        parquet_glob = self._make_bad_division_parquet(tmp_path, "nation_state")
        output = str(tmp_path / "places.parquet")
        with pytest.raises(RuntimeError, match="unmapped division subtype"):
            stages.stage_import("overture_division", parquet_glob, self._BBOX, output)

    def test_null_subtype_raises_runtime_error(self, tmp_path):
        """A division row with subtype=NULL raises RuntimeError (never defaults or guesses)."""
        from garganorn import stages

        parquet_glob = self._make_bad_division_parquet(tmp_path, None)
        output = str(tmp_path / "places.parquet")
        with pytest.raises(RuntimeError, match="unmapped division subtype"):
            stages.stage_import("overture_division", parquet_glob, self._BBOX, output)

    def test_fail_loud_check_precedes_artifact_write(self, tmp_path):
        """No places.parquet is written when the fail-loud guard raises (no partial output)."""
        import pathlib
        from garganorn import stages

        parquet_glob = self._make_bad_division_parquet(tmp_path, "nation_state")
        output = str(tmp_path / "places.parquet")
        with pytest.raises(RuntimeError):
            stages.stage_import("overture_division", parquet_glob, self._BBOX, output)
        assert not pathlib.Path(output).exists(), (
            "places.parquet must not be written when the fail-loud guard raises"
        )


# ---------------------------------------------------------------------------
# §6 item 2 — ordering: ascending by level, ties broken by id
# ---------------------------------------------------------------------------

class TestBoundaryLookupOrderingByLevel:
    """BoundaryLookup.containment() orders ascending by level, ties broken by id.

    phase2b-design.md §A.7c: boundaries.py line 49 `ORDER BY admin_level ASC`
    becomes `ORDER BY level ASC, id ASC`. Uses a hand-built boundaries.duckdb
    (not the conftest fixture) with two same-level boundaries at different ids
    to exercise the tie-break specifically.
    """

    @pytest.fixture
    def tied_level_db(self, tmp_path):
        from garganorn.boundaries import BoundaryLookup

        db_path = tmp_path / "tied_level.duckdb"
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
        # Two overlapping boundaries at the SAME level (25 = region), different ids.
        # Point (0,0) falls inside both -- the tie-break (id ASC) determines order.
        rows = [
            ("region_b", 25, "POLYGON((-10 -10, -10 10, 10 10, 10 -10, -10 -10))"),
            ("region_a", 25, "POLYGON((-5 -5, -5 5, 5 5, 5 -5, -5 -5))"),
            ("country_z", 10, "POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20))"),
        ]
        for bid, level, wkt in rows:
            conn.execute(
                "INSERT INTO places VALUES (?, ST_GeomFromText(?), ?, -20.0, 20.0, -20.0, 20.0)",
                [bid, wkt, level],
            )
        conn.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
        conn.close()

        bl = BoundaryLookup(db_path)
        bl.connect()
        yield bl
        bl.close()

    def test_ascending_by_level(self, tied_level_db):
        """country_z (level 10) sorts before the two level-25 regions."""
        result = tied_level_db.containment(0.0, 0.0)
        ids = [r["rkey"].split(":")[-1] for r in result]
        assert ids[0] == "country_z", f"expected country_z first, got {ids}"

    def test_ties_broken_by_id_ascending(self, tied_level_db):
        """Two boundaries at the same level (25) are ordered by id ASC: region_a before region_b."""
        result = tied_level_db.containment(0.0, 0.0)
        ids = [r["rkey"].split(":")[-1] for r in result]
        region_ids = [i for i in ids if i.startswith("region_")]
        assert region_ids == ["region_a", "region_b"], (
            f"same-level boundaries must tie-break by id ASC; got {region_ids}"
        )


# ---------------------------------------------------------------------------
# §6 item 3 — filter delta: localadmin + admin_level=3 counties included, hoods excluded
# ---------------------------------------------------------------------------

class TestBoundaryFilterDelta:
    """boundaries.duckdb export filter (level <= 50) delta vs the old admin_level filter.

    phase2b-design.md §A.5: proposed filter `level <= 50` (country..locality)
    includes localadmin (+21,380 landed) and previously-excluded counties, and
    still excludes all hoods (borough/macrohood/neighborhood/microhood, level
    >= 55). Uses a synthetic division_parquet fixture covering one subtype per
    level tier to exercise the row-inclusion boundary directly, rather than
    the row-count reconciliation numbers themselves (those are a §A.5
    empirical measurement, not a unit-testable invariant).
    """

    _BBOX = (-180, -90, 180, 90)

    def _make_all_subtypes_parquet(self, tmp_path):
        """One division per subtype, all with valid land geometry, global bbox."""
        division_path = tmp_path / "division_all_subtypes.parquet"
        division_area_path = tmp_path / "division_area_all_subtypes.parquet"

        subtypes = [
            "country", "dependency", "region", "county", "localadmin",
            "locality", "macrohood", "neighborhood", "microhood",
        ]

        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE tmp_division (
                id VARCHAR,
                names STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR),
                             rules STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]),
                subtype VARCHAR,
                country VARCHAR,
                region VARCHAR,
                wikidata VARCHAR,
                population BIGINT,
                parent_division_id VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE tmp_division_area (
                division_id VARCHAR,
                admin_level INTEGER,
                is_land BOOLEAN,
                geometry VARCHAR,
                bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)
            )
        """)
        for i, subtype in enumerate(subtypes):
            div_id = f"div_{subtype}"
            # Distinct, non-overlapping small squares so each division has a
            # unique, unambiguous land area.
            x0 = -170 + i * 10
            wkt = f"POLYGON(({x0} -5, {x0} 5, {x0+5} 5, {x0+5} -5, {x0} -5))"
            conn.execute(
                """
                INSERT INTO tmp_division VALUES (
                    ?, {'primary': ?, 'common': map([]::VARCHAR[], []::VARCHAR[]),
                        'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
                    ?, 'US', NULL, NULL, 1000, NULL
                )
                """,
                [div_id, subtype.title(), subtype],
            )
            conn.execute(
                """
                INSERT INTO tmp_division_area VALUES (
                    ?, NULL, true, ?,
                    {'xmin': ?, 'ymin': -5.0, 'xmax': ?, 'ymax': 5.0}
                )
                """,
                [div_id, wkt, float(x0), float(x0 + 5)],
            )

        conn.execute(f"COPY tmp_division TO '{division_path}' (FORMAT PARQUET)")
        conn.execute(f"COPY tmp_division_area TO '{division_area_path}' (FORMAT PARQUET)")
        conn.close()
        return (str(division_path), str(division_area_path))

    def test_localadmin_included_in_boundaries_db(self, tmp_path):
        """localadmin must appear in boundaries.duckdb (was excluded by the old admin_level filter)."""
        from garganorn import stages

        parquet_glob = self._make_all_subtypes_parquet(tmp_path)
        output = str(tmp_path / "places.parquet")
        stages.stage_import("overture_division", parquet_glob, self._BBOX, output)

        boundaries = str(tmp_path / "boundaries.duckdb")
        con = duckdb.connect()
        con.execute(f"ATTACH '{boundaries}' AS bnd (READ_ONLY)")
        ids = {r[0] for r in con.execute("SELECT id FROM bnd.places").fetchall()}
        con.close()
        assert "div_localadmin" in ids, (
            f"localadmin must be included in boundaries.duckdb (level <= 50); found ids: {ids}"
        )

    def test_admin_hierarchy_through_locality_included(self, tmp_path):
        """country/dependency/region/county/locality (level <= 50) are all included."""
        from garganorn import stages

        parquet_glob = self._make_all_subtypes_parquet(tmp_path)
        output = str(tmp_path / "places.parquet")
        stages.stage_import("overture_division", parquet_glob, self._BBOX, output)

        boundaries = str(tmp_path / "boundaries.duckdb")
        con = duckdb.connect()
        con.execute(f"ATTACH '{boundaries}' AS bnd (READ_ONLY)")
        ids = {r[0] for r in con.execute("SELECT id FROM bnd.places").fetchall()}
        con.close()
        for subtype in ["country", "dependency", "region", "county", "locality"]:
            assert f"div_{subtype}" in ids, (
                f"{subtype} (level <= 50) must be included in boundaries.duckdb; found ids: {ids}"
            )

    def test_hoods_excluded_from_boundaries_db(self, tmp_path):
        """macrohood/neighborhood/microhood (level >= 60) must be excluded, matching today's scope."""
        from garganorn import stages

        parquet_glob = self._make_all_subtypes_parquet(tmp_path)
        output = str(tmp_path / "places.parquet")
        stages.stage_import("overture_division", parquet_glob, self._BBOX, output)

        boundaries = str(tmp_path / "boundaries.duckdb")
        con = duckdb.connect()
        con.execute(f"ATTACH '{boundaries}' AS bnd (READ_ONLY)")
        ids = {r[0] for r in con.execute("SELECT id FROM bnd.places").fetchall()}
        con.close()
        for subtype in ["macrohood", "neighborhood", "microhood"]:
            assert f"div_{subtype}" not in ids, (
                f"{subtype} (level >= 60) must be excluded from boundaries.duckdb; found ids: {ids}"
            )


# ---------------------------------------------------------------------------
# §6 item 5 — no NULL levels in places (import-time, covers Part A directly)
#
# Note: tests/test_containment_covering.py::TestNoNullLevels covers the same
# acceptance item against boundaries.duckdb via the division_parquet conftest
# fixture (country/region/locality only). This class additionally exercises
# the full 9-subtype spread (including the hoods) to confirm LEVEL_VOCAB
# leaves nothing NULL even for subtypes the boundaries.duckdb filter later
# excludes -- the guarantee is at the `places` CTAS, before the filter runs.
# ---------------------------------------------------------------------------

class TestNoNullLevelsAllSubtypes:
    """places.level must be non-NULL for every subtype LEVEL_VOCAB maps, including hoods."""

    _BBOX = (-180, -90, 180, 90)

    def test_no_null_levels_across_all_nine_subtypes(self, tmp_path):
        from garganorn import stages

        helper = TestBoundaryFilterDelta()
        parquet_glob = helper._make_all_subtypes_parquet(tmp_path)
        output = str(tmp_path / "places.parquet")
        stages.stage_import("overture_division", parquet_glob, self._BBOX, output)

        boundaries = str(tmp_path / "boundaries.duckdb")
        con = duckdb.connect()
        con.execute(f"ATTACH '{boundaries}' AS bnd (READ_ONLY)")
        null_count = con.execute(
            "SELECT count(*) FROM bnd.places WHERE level IS NULL"
        ).fetchone()[0]
        row_count = con.execute("SELECT count(*) FROM bnd.places").fetchone()[0]
        con.close()
        assert row_count > 0, "expected at least one row in boundaries.duckdb"
        assert null_count == 0, (
            f"boundaries.duckdb places.level must never be NULL; found {null_count} NULL rows"
        )


# ---------------------------------------------------------------------------
# County collapse (§A.4): all counties get level=35 regardless of raw admin_level
# ---------------------------------------------------------------------------

class TestCountyCollapse:
    """All county-subtype divisions get level=35; admin_level is not a secondary discriminator.

    phase2b-design.md §A.4: county collapses to a single level value. This test
    feeds two county divisions with different (now-vestigial) division_area
    admin_level values and confirms both land on level=35.
    """

    _BBOX = (-180, -90, 180, 90)

    def _make_two_counties_parquet(self, tmp_path):
        division_path = tmp_path / "division_counties.parquet"
        division_area_path = tmp_path / "division_area_counties.parquet"

        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("""
            CREATE TABLE tmp_division (
                id VARCHAR,
                names STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR),
                             rules STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]),
                subtype VARCHAR,
                country VARCHAR,
                region VARCHAR,
                wikidata VARCHAR,
                population BIGINT,
                parent_division_id VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE tmp_division_area (
                division_id VARCHAR,
                admin_level INTEGER,
                is_land BOOLEAN,
                geometry VARCHAR,
                bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)
            )
        """)
        # county_al2 mimics the common case (admin_level=2, 38,674 of 38,903 per §A.2);
        # county_al3 mimics the arbitrarily-excluded-today case (admin_level=3, §A.5 delta).
        counties = [("county_al2", 2, -170.0), ("county_al3", 3, -150.0)]
        for div_id, admin_level, x0 in counties:
            conn.execute(
                """
                INSERT INTO tmp_division VALUES (
                    ?, {'primary': ?, 'common': map([]::VARCHAR[], []::VARCHAR[]),
                        'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]},
                    'county', 'US', NULL, NULL, 1000, NULL
                )
                """,
                [div_id, div_id],
            )
            wkt = f"POLYGON(({x0} -5, {x0} 5, {x0+5} 5, {x0+5} -5, {x0} -5))"
            conn.execute(
                """
                INSERT INTO tmp_division_area VALUES (
                    ?, ?, true, ?,
                    {'xmin': ?, 'ymin': -5.0, 'xmax': ?, 'ymax': 5.0}
                )
                """,
                [div_id, admin_level, wkt, x0, x0 + 5],
            )

        conn.execute(f"COPY tmp_division TO '{division_path}' (FORMAT PARQUET)")
        conn.execute(f"COPY tmp_division_area TO '{division_area_path}' (FORMAT PARQUET)")
        conn.close()
        return (str(division_path), str(division_area_path))

    def test_counties_collapse_to_level_35_regardless_of_admin_level(self, tmp_path):
        from garganorn import stages

        parquet_glob = self._make_two_counties_parquet(tmp_path)
        output = str(tmp_path / "places.parquet")
        stages.stage_import("overture_division", parquet_glob, self._BBOX, output)

        boundaries = str(tmp_path / "boundaries.duckdb")
        con = duckdb.connect()
        con.execute(f"ATTACH '{boundaries}' AS bnd (READ_ONLY)")
        rows = con.execute(
            "SELECT id, level FROM bnd.places WHERE id IN ('county_al2', 'county_al3') ORDER BY id"
        ).fetchall()
        con.close()
        assert rows == [("county_al2", 35), ("county_al3", 35)], (
            f"both counties must collapse to level=35 regardless of source admin_level; got {rows}"
        )
