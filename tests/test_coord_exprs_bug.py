"""Tests for _coord_exprs's alias parameter.

_coord_exprs(source, alias=...) qualifies struct field references as
`alias.bbox.xmin` etc. for overture sources, and as a table prefix for
column-based sources like osm -- so compute_containment's f-string
interpolation sites produce valid SQL.
"""
import json
import os
import tempfile

import duckdb
import pytest

from garganorn.quadtree import _coord_exprs
from garganorn.stages import compute_containment


# ---------------------------------------------------------------------------
# _coord_exprs alias parameter
# ---------------------------------------------------------------------------

class TestCoordExprsAlias:
    """_coord_exprs(source, alias="") embeds the alias in struct field references."""

    def test_overture_no_alias_unchanged(self):
        """Without alias, overture expressions are bare struct field references."""
        lon, lat = _coord_exprs("overture_place")
        assert "bbox.xmin" in lon
        assert "bbox.xmax" in lon
        assert "bbox.ymin" in lat
        assert "bbox.ymax" in lat

    def test_overture_with_alias_prefixes_struct_fields(self):
        """With alias='p', each struct field reference is qualified as p.bbox.*."""
        lon, lat = _coord_exprs("overture_place", alias="p")
        # Expressions must reference p.bbox.*, not bare bbox.*
        assert "p.bbox.xmin" in lon, f"Expected 'p.bbox.xmin' in lon expr: {lon!r}"
        assert "p.bbox.xmax" in lon, f"Expected 'p.bbox.xmax' in lon expr: {lon!r}"
        assert "p.bbox.ymin" in lat, f"Expected 'p.bbox.ymin' in lat expr: {lat!r}"
        assert "p.bbox.ymax" in lat, f"Expected 'p.bbox.ymax' in lat expr: {lat!r}"

    def test_overture_with_alias_no_bare_bbox_refs(self):
        """With alias='p', no unqualified bbox.* appears in the expression."""
        lon, lat = _coord_exprs("overture_place", alias="p")
        # A bare `bbox.xmin` (not preceded by 'p.') would indicate the alias
        # was not applied. The simplest check: the expression should not start
        # with "(bbox." after stripping whitespace.
        assert not lon.lstrip("(").startswith("bbox."), \
            f"lon expr starts with bare 'bbox.': {lon!r}"
        assert not lat.lstrip("(").startswith("bbox."), \
            f"lat expr starts with bare 'bbox.': {lat!r}"

    def test_non_overture_alias_prefixes_columns(self):
        """For non-struct sources, alias is applied as a table prefix."""
        lon_no_alias, lat_no_alias = _coord_exprs("osm")
        lon_alias, lat_alias = _coord_exprs("osm", alias="p")
        assert lon_no_alias == "longitude"
        assert lat_no_alias == "latitude"
        assert lon_alias == "p.longitude"
        assert lat_alias == "p.latitude"

    def test_osm_alias_prefixes_columns(self):
        """With alias='p', OSM expressions are qualified as p.longitude / p.latitude."""
        lon, lat = _coord_exprs("osm", alias="p")
        assert lon == "p.longitude", f"Expected 'p.longitude', got: {lon!r}"
        assert lat == "p.latitude", f"Expected 'p.latitude', got: {lat!r}"

    def test_overture_alias_expressions_are_valid_duckdb_sql(self):
        """Aliased expressions must parse and execute correctly in DuckDB."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE places (
                id    VARCHAR,
                bbox  STRUCT(xmin DOUBLE, xmax DOUBLE, ymin DOUBLE, ymax DOUBLE)
            )
        """)
        con.execute("""
            INSERT INTO places VALUES (
                'p1',
                {'xmin': -122.42, 'xmax': -122.40, 'ymin': 37.77, 'ymax': 37.79}
            )
        """)
        lon_expr, lat_expr = _coord_exprs("overture_place", alias="p")
        # This SELECT should execute without ParserException
        row = con.execute(f"SELECT {lon_expr}, {lat_expr} FROM places p").fetchone()
        assert row is not None
        assert abs(row[0] - (-122.41)) < 0.01, f"Unexpected lon: {row[0]}"
        assert abs(row[1] - 37.78) < 0.01, f"Unexpected lat: {row[1]}"


# ---------------------------------------------------------------------------
# compute_containment with overture-style bbox struct column
# ---------------------------------------------------------------------------

def _make_division_db(path):
    """Create a minimal division-schema boundaries DuckDB at *path*.

    The geometry is a simple polygon covering the San Francisco area, which
    contains the test place at (-122.42, 37.78).
    """
    con = duckdb.connect(path)
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("""
        CREATE TABLE places (
            id            VARCHAR,
            geometry      GEOMETRY,
            level         INTEGER,
            names         STRUCT("primary" VARCHAR),
            min_latitude  DOUBLE,
            max_latitude  DOUBLE,
            min_longitude DOUBLE,
            max_longitude DOUBLE
        )
    """)
    con.execute("""
        INSERT INTO places VALUES (
            '85922583',
            ST_GeomFromText('POLYGON((-122.55 37.6, -122.55 37.85, -122.3 37.85, -122.3 37.6, -122.55 37.6))'),
            50,
            {'primary': 'San Francisco'},
            37.6, 37.85, -122.55, -122.3
        )
    """)
    con.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
    con.close()


class TestComputeContainmentOverture:
    """compute_containment must execute without ParserException for overture places."""

    def test_overture_bbox_struct_does_not_raise_parser_error(self, tmp_path):
        """compute_containment with overture bbox struct places must not raise.

        _coord_exprs's alias parameter qualifies struct field references as
        alias.bbox.*, avoiding the hard-coded `p.` prefix in compute_containment's
        interpolation sites.

        Fixture provides a valid 17-char qk17, tile_assignments, and covering_dir
        (preconditions the code does not fall back to building itself).
        """
        division_path = str(tmp_path / "division.duckdb")
        _make_division_db(division_path)

        # Compute valid qk17 for lon=-122.42, lat=37.78 (centroid of the test bbox).
        _tmp_con = duckdb.connect(":memory:")
        _tmp_con.execute("INSTALL spatial; LOAD spatial;")
        qk17 = _tmp_con.execute(
            "SELECT ST_QuadKey(?, ?, 17)", [-122.42, 37.78]
        ).fetchone()[0]
        _tmp_con.close()

        # Build covering from the division DB (orchestrator responsibility).
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering")
        stage_covering(division_path, covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE places (
                id    VARCHAR,
                bbox  STRUCT(xmin DOUBLE, xmax DOUBLE, ymin DOUBLE, ymax DOUBLE),
                qk17  VARCHAR
            )
        """)
        # Place inside the SF polygon (lon=-122.42, lat=37.78), valid 17-char qk17.
        con.execute(
            "INSERT INTO places VALUES ('ovr001', "
            "{'xmin': -122.425, 'xmax': -122.415, 'ymin': 37.775, 'ymax': 37.785}, ?)",
            [qk17],
        )

        # Pre-create tile_assignments (precondition; no fallback in fixed code).
        con.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        con.execute("INSERT INTO tile_assignments VALUES ('ovr001', ?)", [qk17[:6]])

        # Write places and tile_assignments to parquet
        places_parquet = str(tmp_path / "ovr001_places.parquet")
        ta_parquet = str(tmp_path / "ovr001_ta.parquet")
        con.execute(f"COPY (SELECT id, bbox, qk17 FROM places) TO '{places_parquet}' (FORMAT PARQUET)")
        con.execute(f"COPY (SELECT place_id, tile_qk FROM tile_assignments) TO '{ta_parquet}' (FORMAT PARQUET)")

        lon_expr, lat_expr = _coord_exprs("overture_place", alias="p")
        containment_dir = str(tmp_path / "containment")
        compute_containment(
            places_parquet, ta_parquet, division_path,
            "id", lon_expr, lat_expr, containment_dir,
            covering_dir=covering_dir,
            force=True,
        )

        # Verify the containment parquets were populated
        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir) if f.endswith(".parquet")
        ]
        check_con = duckdb.connect()
        rows = check_con.execute(
            f"SELECT place_id, relations_json FROM read_parquet({parquet_files!r})"
        ).fetchall()
        check_con.close()
        assert len(rows) == 1, f"Expected 1 containment row, got {len(rows)}"
        assert rows[0][0] == "ovr001"
        assert "org.atgeo.places.overture.division:85922583" in rows[0][1]

    def test_overture_containment_result_has_correct_division_rkey(self, tmp_path):
        """The containment record for an overture place references the expected division rkey."""
        division_path = str(tmp_path / "division.duckdb")
        _make_division_db(division_path)

        # Compute valid qk17 for lon=-122.42, lat=37.78 (centroid of the test bbox).
        _tmp_con = duckdb.connect(":memory:")
        _tmp_con.execute("INSTALL spatial; LOAD spatial;")
        qk17 = _tmp_con.execute(
            "SELECT ST_QuadKey(?, ?, 17)", [-122.42, 37.78]
        ).fetchone()[0]
        _tmp_con.close()

        # Build covering from the division DB (orchestrator responsibility).
        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering")
        stage_covering(division_path, covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE places (
                id    VARCHAR,
                bbox  STRUCT(xmin DOUBLE, xmax DOUBLE, ymin DOUBLE, ymax DOUBLE),
                qk17  VARCHAR
            )
        """)
        con.execute(
            "INSERT INTO places VALUES ('ovr002', "
            "{'xmin': -122.425, 'xmax': -122.415, 'ymin': 37.775, 'ymax': 37.785}, ?)",
            [qk17],
        )

        # Pre-create tile_assignments (precondition; no fallback in fixed code).
        con.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        con.execute("INSERT INTO tile_assignments VALUES ('ovr002', ?)", [qk17[:6]])

        # Write places and tile_assignments to parquet
        places_parquet = str(tmp_path / "ovr002_places.parquet")
        ta_parquet = str(tmp_path / "ovr002_ta.parquet")
        con.execute(f"COPY (SELECT id, bbox, qk17 FROM places) TO '{places_parquet}' (FORMAT PARQUET)")
        con.execute(f"COPY (SELECT place_id, tile_qk FROM tile_assignments) TO '{ta_parquet}' (FORMAT PARQUET)")

        lon_expr, lat_expr = _coord_exprs("overture_place", alias="p")
        containment_dir = str(tmp_path / "containment")
        compute_containment(
            places_parquet, ta_parquet, division_path,
            "id", lon_expr, lat_expr, containment_dir,
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
        assert len(rows) == 1
        relations = json.loads(rows[0][1])
        within = relations.get("within", [])
        rkeys = [r["rkey"] for r in within]
        assert any("85922583" in rk for rk in rkeys), \
            f"Expected division rkey containing '85922583' in relations: {rkeys}"


# ---------------------------------------------------------------------------
# Representative-candidate-point bug: a division as its own candidate
# ---------------------------------------------------------------------------

class TestComputeContainmentDivisionCrescent:
    """A division acting as its own containment CANDIDATE (overture_division
    rows tested against boundaries.duckdb) must use its true interior point,
    not its bbox midpoint, to decide which other division it falls within.

    Regression for the representative-candidate-point bug: for a crescent/
    non-convex division whose bbox spans a border with a neighboring
    division, the bbox midpoint can land in the neighbor's territory
    instead of the division's own, misattributing the `within` relation
    (this is what happens for Norway/Chile/Indonesia-shaped divisions in
    production). This test models that directly: a candidate crescent
    division's true land sits just west of a border between County A (its
    real parent) and County B (its neighbor); its bbox is built so the
    midpoint falls just east of the border, inside County B.

    _coord_exprs("overture_division") must read interior_lon/interior_lat,
    so the candidate resolves to County A. Were it to read the bbox midpoint
    again, the point would land in County B and the containment edge test --
    which now runs against a fragment clipped to the candidate's own tile --
    would match nothing at all.
    """

    _COUNTY_A_WKT = "POLYGON((0 0, 1 0, 1 2, 0 2, 0 0))"   # west of the x=1 border
    _COUNTY_B_WKT = "POLYGON((1 0, 2 0, 2 2, 1 2, 1 0))"   # east of the x=1 border
    # The crescent candidate's bbox midpoint -- just inside County B, across
    # the border from the candidate's true land. Reading containment coords
    # from here is the bug this test guards against.
    _MID_X, _MID_Y = 1.02, 1.0
    # The candidate's true interior point: inside County A but hard against
    # the x=1 border. That placement is load-bearing. No quadkey tile edge
    # ever falls on x=1 (a boundary needs -180 + k*360/2^L == 1, which has no
    # integer solution), so every tile containing this point straddles the
    # border and County A classifies all of them edge leaves. An edge leaf's
    # stored geometry is the boundary clipped to the tile, so the arm's
    # ST_Covers test still depends on the true lon/lat. Move this point deep
    # into County A and the covering row becomes an interior cell, whose
    # stored geometry is just the tile's own envelope -- ST_Covers against
    # that envelope passes for any point in the tile, which makes the test
    # pass no matter what _coord_exprs returns.
    _INTERIOR_X, _INTERIOR_Y = 0.999, 1.0
    # bbox spans the crescent's true land and its reach east into County B,
    # so the midpoint lands at _MID_X while still containing the interior point.
    _BBOX_XMIN, _BBOX_XMAX = 0.989, 1.051

    def _make_boundaries_db(self, path):
        con = duckdb.connect(path)
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE places (
                id            VARCHAR,
                geometry      GEOMETRY,
                level         INTEGER,
                names         STRUCT("primary" VARCHAR),
                min_latitude  DOUBLE,
                max_latitude  DOUBLE,
                min_longitude DOUBLE,
                max_longitude DOUBLE
            )
        """)
        for div_id, name, wkt in [
            ("county_a", "County A", self._COUNTY_A_WKT),
            ("county_b", "County B", self._COUNTY_B_WKT),
        ]:
            con.execute(f"""
                INSERT INTO places
                SELECT '{div_id}', g, 35, {{'primary': '{name}'}},
                       ST_YMin(g), ST_YMax(g), ST_XMin(g), ST_XMax(g)
                FROM (SELECT ST_GeomFromText('{wkt}') AS g)
            """)
        con.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
        con.close()

    def test_bbox_midpoint_misattributes_crescent_division_to_wrong_neighbor(self, tmp_path):
        division_path = str(tmp_path / "division.duckdb")
        self._make_boundaries_db(division_path)

        from garganorn.covering import stage_covering
        covering_dir = str(tmp_path / "covering")
        stage_covering(division_path, covering_dir, cover_min_zoom=4, cover_max_zoom=12)

        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        # qk17 tracks the interior point, matching what
        # overture_division_import.sql produces. The bbox below still spans
        # the border with its midpoint inside County B -- that is the
        # crescent shape being modelled; only the tile key follows the point.
        qk17 = con.execute(
            "SELECT ST_QuadKey(?, ?, 17)", [self._INTERIOR_X, self._INTERIOR_Y]
        ).fetchone()[0]

        con.execute("""
            CREATE TABLE places (
                id            VARCHAR,
                bbox          STRUCT(xmin DOUBLE, xmax DOUBLE, ymin DOUBLE, ymax DOUBLE),
                qk17          VARCHAR,
                interior_lon  DOUBLE,
                interior_lat  DOUBLE
            )
        """)
        # interior_lon/interior_lat are the candidate's true interior point,
        # west of the border inside County A -- what _coord_exprs reads.
        con.execute(
            "INSERT INTO places VALUES ('crescent_child', "
            "{'xmin': ?, 'xmax': ?, 'ymin': ?, 'ymax': ?}, ?, ?, ?)",
            [self._BBOX_XMIN, self._BBOX_XMAX,
             self._MID_Y - 0.01, self._MID_Y + 0.01, qk17,
             self._INTERIOR_X, self._INTERIOR_Y],
        )
        con.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        con.execute("INSERT INTO tile_assignments VALUES ('crescent_child', ?)", [qk17[:6]])

        places_parquet = str(tmp_path / "crescent_places.parquet")
        ta_parquet = str(tmp_path / "crescent_ta.parquet")
        con.execute(
            f"COPY (SELECT id, bbox, qk17, interior_lon, interior_lat FROM places) "
            f"TO '{places_parquet}' (FORMAT PARQUET)"
        )
        con.execute(f"COPY (SELECT place_id, tile_qk FROM tile_assignments) TO '{ta_parquet}' (FORMAT PARQUET)")

        lon_expr, lat_expr = _coord_exprs("overture_division", alias="p")
        containment_dir = str(tmp_path / "containment")
        compute_containment(
            places_parquet, ta_parquet, division_path,
            "id", lon_expr, lat_expr, containment_dir,
            covering_dir=covering_dir,
            force=True,
        )

        parquet_files = [
            os.path.join(containment_dir, f)
            for f in os.listdir(containment_dir) if f.endswith(".parquet")
        ]
        check_con = duckdb.connect()
        rows = (
            check_con.execute(
                f"SELECT place_id, relations_json FROM read_parquet({parquet_files!r})"
            ).fetchall()
            if parquet_files else []
        )
        check_con.close()

        assert len(rows) == 1, (
            f"Expected 1 containment row for crescent_child, got {len(rows)}: {rows}"
        )
        relations = json.loads(rows[0][1])
        rkeys = [r["rkey"] for r in relations.get("within", [])]
        assert any("county_a" in rk for rk in rkeys), (
            f"Expected crescent_child to be within County A (its true "
            f"containing division), got: {rkeys}. The bbox-midpoint "
            f"_coord_exprs('overture_division') expression lands in "
            f"County B's territory instead -- this is the "
            f"representative-candidate-point bug."
        )
