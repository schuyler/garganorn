"""garganorn.stages.stage_division_containment.

places_parquet is built via the real stage_import("overture_division", ...)
path (not a hand-rolled fixture), so level and names."primary" come from
the actual import CTAS -- the same guarantee R5 pins. division_parquet is
tests/conftest.py's session-scoped fixture, extended with a hierarchies
column; its div_locality_sf chain carries
div_missing_county (absent from the imported set, for R2) and a
deliberately misnamed div_region_ca entry (for R5).
"""
import json
import os
import time

import duckdb

from garganorn import stages as _stages
from garganorn.stages import stage_division_containment

_COLLECTION_PREFIX = "org.atgeo.places.overture.division"
_BBOX = (-122.55, 37.60, -122.30, 37.85)


def _make_places_parquet(division_parquet, tmp_path, name="places.parquet"):
    output = str(tmp_path / name)
    _stages.stage_import("overture_division", division_parquet, _BBOX, output)
    return output


def _write_tile_assignments(path, rows):
    """rows: list of (place_id, tile_qk)."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE t (place_id VARCHAR, tile_qk VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?, ?)", rows)
    con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    con.close()


def _containment_parquets(containment_dir):
    return [
        os.path.join(containment_dir, f)
        for f in os.listdir(containment_dir)
        if f.endswith(".parquet")
    ]


def _rows(containment_dir):
    parquets = _containment_parquets(containment_dir)
    if not parquets:
        return []
    con = duckdb.connect(":memory:")
    rows = con.execute(
        "SELECT tile_qk, place_id, relations_json FROM read_parquet(?)", [parquets]
    ).fetchall()
    con.close()
    return rows


# ---------------------------------------------------------------------------
# R1 -- a locality's within names its region and country; a division whose
# chain has no ancestors has no containment row (the export layer's
# LEFT JOIN ... coalesce(..., '{}') is what turns that absence into
# `relations: {}` on the wire -- see overture_division_export_tiles.sql,
# out of scope here).
# ---------------------------------------------------------------------------

class TestWithinNamesAncestors:
    def test_locality_within_names_region_and_country_country_has_no_row(
        self, division_parquet, tmp_path
    ):
        places_parquet = _make_places_parquet(division_parquet, tmp_path)
        ta_path = str(tmp_path / "tile_assignments.parquet")
        _write_tile_assignments(ta_path, [
            ("div_locality_sf", "023010"),
            ("div_region_ca", "023011"),
            ("div_country_us", "023012"),
        ])
        containment_dir = str(tmp_path / "containment")
        stage_division_containment(
            division_parquet[0], places_parquet, ta_path, containment_dir, force=True,
        )

        rows = _rows(containment_dir)
        sf_row = next(r for r in rows if r[1] == "div_locality_sf")
        rkeys = {e["rkey"] for e in json.loads(sf_row[2])["within"]}
        assert f"{_COLLECTION_PREFIX}:div_region_ca" in rkeys
        assert f"{_COLLECTION_PREFIX}:div_country_us" in rkeys

        assert not any(r[1] == "div_country_us" for r in rows), (
            "div_country_us's chain holds only itself; with no ancestors "
            "surviving the semi-join it must have no containment row"
        )


# ---------------------------------------------------------------------------
# R2 -- an ancestor absent from the imported set is dropped, and the
# surviving ancestors are unaffected by the gap.
# ---------------------------------------------------------------------------

class TestMissingAncestorDropped:
    def test_missing_county_absent_surviving_ancestors_unaffected(
        self, division_parquet, tmp_path
    ):
        places_parquet = _make_places_parquet(division_parquet, tmp_path)
        ta_path = str(tmp_path / "tile_assignments.parquet")
        _write_tile_assignments(ta_path, [("div_locality_sf", "023010")])
        containment_dir = str(tmp_path / "containment")
        stage_division_containment(
            division_parquet[0], places_parquet, ta_path, containment_dir, force=True,
        )

        within = json.loads(_rows(containment_dir)[0][2])["within"]
        rkeys = {e["rkey"] for e in within}
        assert f"{_COLLECTION_PREFIX}:div_missing_county" not in rkeys
        assert rkeys == {
            f"{_COLLECTION_PREFIX}:div_country_us",
            f"{_COLLECTION_PREFIX}:div_region_ca",
        }


# ---------------------------------------------------------------------------
# R3 -- no division lists its own id in its own within.
# ---------------------------------------------------------------------------

class TestNoSelfReference:
    def test_no_row_lists_its_own_place_id(self, division_parquet, tmp_path):
        places_parquet = _make_places_parquet(division_parquet, tmp_path)
        ta_path = str(tmp_path / "tile_assignments.parquet")
        _write_tile_assignments(ta_path, [
            ("div_locality_sf", "023010"),
            ("div_region_ca", "023011"),
        ])
        containment_dir = str(tmp_path / "containment")
        stage_division_containment(
            division_parquet[0], places_parquet, ta_path, containment_dir, force=True,
        )

        rows = _rows(containment_dir)
        assert rows, "fixture assumption violated: no containment rows produced"
        for tile_qk, place_id, relations_json in rows:
            own_rkey = f"{_COLLECTION_PREFIX}:{place_id}"
            rkeys = {e["rkey"] for e in json.loads(relations_json)["within"]}
            assert own_rkey not in rkeys, f"{place_id} lists itself in its own within"


# ---------------------------------------------------------------------------
# R4 -- a division referenced by more than one tile, including one in the
# summary band, has byte-identical relations_json in every tile carrying
# it, and exactly one row per (tile_qk, place_id). Models
# tests/test_overture_division.py::TestDivisionMultiTileContainmentJoin::
# test_three_tile_division_exports_three_records, the existing N-vs-N^2
# guard, at the containment stage instead of the export stage.
# ---------------------------------------------------------------------------

class TestMultiTileByteIdentical:
    def test_three_tile_division_has_identical_relations_one_row_per_tile(
        self, division_parquet, tmp_path
    ):
        places_parquet = _make_places_parquet(division_parquet, tmp_path)
        ta_path = str(tmp_path / "tile_assignments.parquet")
        tile_qks = ["023010", "023011", "02"]  # two grid tiles, one summary-band tile
        _write_tile_assignments(ta_path, [("div_locality_sf", qk) for qk in tile_qks])
        containment_dir = str(tmp_path / "containment")
        stage_division_containment(
            division_parquet[0], places_parquet, ta_path, containment_dir, force=True,
        )

        rows = [r for r in _rows(containment_dir) if r[1] == "div_locality_sf"]
        assert sorted(r[0] for r in rows) == sorted(tile_qks), (
            f"expected one row per referencing tile, got {rows}"
        )
        assert len({r[2] for r in rows}) == 1, (
            "a division's relations_json must be identical in every tile referencing it"
        )


# ---------------------------------------------------------------------------
# R5 -- within entry shape, ordering, rkey construction, and name sourced
# from the ancestor's own imported record rather than the hierarchies
# struct.
# ---------------------------------------------------------------------------

class TestWithinEntryShape:
    def test_key_set_ordering_rkey_and_name_from_imported_record(
        self, division_parquet, tmp_path
    ):
        places_parquet = _make_places_parquet(division_parquet, tmp_path)
        ta_path = str(tmp_path / "tile_assignments.parquet")
        _write_tile_assignments(ta_path, [("div_locality_sf", "023010")])
        containment_dir = str(tmp_path / "containment")
        stage_division_containment(
            division_parquet[0], places_parquet, ta_path, containment_dir, force=True,
        )

        within = json.loads(_rows(containment_dir)[0][2])["within"]
        for entry in within:
            assert set(entry.keys()) == {"rkey", "name", "level"}

        levels = [e["level"] for e in within]
        assert levels == sorted(levels), f"within not ordered by level ASC: {within}"

        by_rkey = {e["rkey"]: e for e in within}
        country = by_rkey[f"{_COLLECTION_PREFIX}:div_country_us"]
        region = by_rkey[f"{_COLLECTION_PREFIX}:div_region_ca"]
        assert country["level"] == 10
        assert region["level"] == 25
        # The hierarchies struct names div_region_ca "Not California" in
        # div_locality_sf's chain (tests/conftest.py); the record's own
        # names.primary from places_parquet must win.
        assert region["name"] == "California"


# ---------------------------------------------------------------------------
# R5 (continued) -- within a shared level, ancestors are ordered by
# ancestor_id ASC, not hierarchies chain position. The fixture chain lists
# the two counties zulu-then-alpha, the opposite of their ascending id
# order, so only an explicit ancestor_id tiebreak -- not chain order --
# can produce the alpha-before-zulu result asserted here.
# ---------------------------------------------------------------------------

class TestSameLevelAncestorTiebreak:
    def test_ancestors_at_shared_level_ordered_by_ancestor_id_ascending(self, tmp_path):
        division_path = str(tmp_path / "tiebreak_division.parquet")
        area_path = str(tmp_path / "tiebreak_division_area.parquet")
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE d (
                id VARCHAR,
                names STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR), rules STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]),
                subtype VARCHAR, country VARCHAR, region VARCHAR, wikidata VARCHAR,
                population BIGINT, parent_division_id VARCHAR,
                hierarchies STRUCT(division_id VARCHAR, subtype VARCHAR, name VARCHAR)[][]
            )
        """)
        zulu = {"division_id": "div_county_zulu", "subtype": "county", "name": "Zulu County"}
        alpha = {"division_id": "div_county_alpha", "subtype": "county", "name": "Alpha County"}
        locality = {"division_id": "div_locality_z", "subtype": "locality", "name": "Z Locality"}
        con.executemany("INSERT INTO d VALUES (?, ?, ?, 'US', 'US-ZZ', NULL, ?, NULL, ?)", [
            ("div_locality_z", {"primary": "Z Locality", "common": {}, "rules": []},
             "locality", 1000, [[zulu, alpha, locality]]),
            ("div_county_zulu", {"primary": "Zulu County", "common": {}, "rules": []},
             "county", 2000, [[zulu]]),
            ("div_county_alpha", {"primary": "Alpha County", "common": {}, "rules": []},
             "county", 3000, [[alpha]]),
        ])
        con.execute(f"COPY d TO '{division_path}' (FORMAT PARQUET)")

        con.execute("""
            CREATE TABLE a (
                division_id VARCHAR, admin_level INTEGER, is_land BOOLEAN,
                geometry VARCHAR, bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)
            )
        """)
        wkt = "POLYGON((-122.55 37.6, -122.55 37.85, -122.30 37.85, -122.30 37.6, -122.55 37.6))"
        bbox = {"xmin": -122.55, "ymin": 37.6, "xmax": -122.30, "ymax": 37.85}
        con.executemany("INSERT INTO a VALUES (?, 2, true, ?, ?)", [
            (div_id, wkt, bbox)
            for div_id in ("div_locality_z", "div_county_zulu", "div_county_alpha")
        ])
        con.execute(f"COPY a TO '{area_path}' (FORMAT PARQUET)")
        con.close()

        places_parquet = _make_places_parquet(
            (division_path, area_path), tmp_path, name="tiebreak_places.parquet",
        )
        ta_path = str(tmp_path / "tiebreak_ta.parquet")
        _write_tile_assignments(ta_path, [("div_locality_z", "030010")])
        containment_dir = str(tmp_path / "tiebreak_containment")
        stage_division_containment(
            division_path, places_parquet, ta_path, containment_dir, force=True,
        )

        within = json.loads(_rows(containment_dir)[0][2])["within"]
        counties = [e for e in within if e["level"] == 35]
        assert len(counties) == 2, (
            f"fixture assumption violated: expected 2 same-level ancestors, got {counties}"
        )
        assert [e["rkey"] for e in counties] == [
            f"{_COLLECTION_PREFIX}:div_county_alpha",
            f"{_COLLECTION_PREFIX}:div_county_zulu",
        ], f"same-level ancestors not ordered by ancestor_id ASC: {counties}"


# ---------------------------------------------------------------------------
# R7 -- freshness: a second call over unchanged inputs is a no-op;
# force=True rebuilds. Mirrors
# tests/test_division_tile_references.py::TestFreshness.
# ---------------------------------------------------------------------------

class TestFreshness:
    def test_second_call_is_noop_force_rebuilds(self, division_parquet, tmp_path):
        places_parquet = _make_places_parquet(division_parquet, tmp_path)
        ta_path = str(tmp_path / "tile_assignments.parquet")
        _write_tile_assignments(ta_path, [("div_locality_sf", "023010")])
        containment_dir = str(tmp_path / "containment")

        stage_division_containment(division_parquet[0], places_parquet, ta_path, containment_dir)
        meta_path = os.path.join(containment_dir, "_meta.json")
        assert os.path.exists(meta_path), "stage did not write containment/_meta.json"
        mtime1 = os.path.getmtime(meta_path)

        time.sleep(0.05)
        stage_division_containment(division_parquet[0], places_parquet, ta_path, containment_dir)
        mtime2 = os.path.getmtime(meta_path)
        assert mtime2 == mtime1, (
            "second call over unchanged inputs must not rebuild containment/ "
            "(freshness gate not honored)"
        )

        time.sleep(0.05)
        stage_division_containment(
            division_parquet[0], places_parquet, ta_path, containment_dir, force=True,
        )
        mtime3 = os.path.getmtime(meta_path)
        assert mtime3 > mtime2, "force=True must rebuild containment/ even when fresh"


# ---------------------------------------------------------------------------
# Output contract: file naming, columns, sort order, and _meta.json
# params/inputs.
# ---------------------------------------------------------------------------

class TestOutputContract:
    def test_file_naming_columns_sort_order_and_meta(self, division_parquet, tmp_path):
        places_parquet = _make_places_parquet(division_parquet, tmp_path)
        ta_path = str(tmp_path / "tile_assignments.parquet")
        _write_tile_assignments(ta_path, [
            ("div_locality_sf", "023011"),
            ("div_locality_sf", "023010"),
            ("div_region_ca", "023010"),  # second row in the 023010 stem, sort order can flip
            ("div_region_ca", "02"),  # summary-band tile, shorter than six chars
        ])
        containment_dir = str(tmp_path / "containment")
        stage_division_containment(
            division_parquet[0], places_parquet, ta_path, containment_dir, force=True,
        )

        parquet_names = {os.path.basename(p) for p in _containment_parquets(containment_dir)}
        assert parquet_names == {"023010.parquet", "023011.parquet", "02.parquet"}, (
            f"expected one file per left(tile_qk, 6) stem, got {parquet_names}"
        )

        con = duckdb.connect(":memory:")
        parquets = _containment_parquets(containment_dir)
        described = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [parquets]).fetchall()
        cols = {row[0]: row[1] for row in described}
        assert list(cols.keys()) == ["tile_qk", "place_id", "relations_json"], f"unexpected columns: {cols}"
        assert cols["tile_qk"] == "VARCHAR"
        assert cols["place_id"] == "VARCHAR"
        assert cols["relations_json"] == "VARCHAR"

        for path in parquets:
            rows = con.execute(
                "SELECT tile_qk, place_id FROM read_parquet(?)", [path]
            ).fetchall()
            assert rows == sorted(rows), f"{path}: not sorted by (tile_qk, place_id)"
        con.close()

        meta = json.loads(open(os.path.join(containment_dir, "_meta.json")).read())
        assert meta["params"] == {"collection_prefix": _COLLECTION_PREFIX}
        assert places_parquet in meta["inputs"]
        assert ta_path in meta["inputs"]
        assert any(p.endswith("division.parquet") for p in meta["inputs"]), (
            f"inputs must include the raw division parquet glob paths: {meta['inputs']}"
        )
