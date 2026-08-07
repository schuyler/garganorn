"""Red-phase tests for the _coord_exprs table-alias bug.

Bug: _coord_exprs("overture_place") returns "(bbox.xmin + bbox.xmax) / 2.0" with no
table prefix. compute_containment interpolates these as `p.{lon_expr}` / `p.{lat_expr}`,
producing invalid SQL `p.(bbox.xmin + bbox.xmax) / 2.0`.

Approved fix: add alias parameter to _coord_exprs so struct field references
are qualified as `alias.bbox.xmin` etc., and drop the hard-coded `p.` prefix
from the compute_containment f-string interpolation sites.

These tests MUST FAIL against the current (unfixed) code. That is the point.
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
        lon_no_alias, lat_no_alias = _coord_exprs("foursquare")
        lon_alias, lat_alias = _coord_exprs("foursquare", alias="p")
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
            37.6, 37.85, -122.55, -122.3
        )
    """)
    con.execute("CREATE INDEX places_rtree ON places USING RTREE (geometry)")
    con.close()


class TestComputeContainmentOverture:
    """compute_containment must execute without ParserException for overture places."""

    def test_overture_bbox_struct_does_not_raise_parser_error(self, tmp_path):
        """compute_containment with overture bbox struct places must not raise.

        The fix adds an alias parameter to _coord_exprs so struct field references
        are qualified as alias.bbox.*, and drops the hard-coded `p.` prefix from
        compute_containment interpolation sites.

        Fixture provides a valid 17-char qk17, tile_assignments, and covering_dir
        (§3.2 preconditions; no fallbacks in the fixed code).
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

        # Build covering from the division DB (§4: orchestrator responsibility).
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

        # Pre-create tile_assignments (§3.2 precondition; no fallback in fixed code).
        con.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        con.execute("INSERT INTO tile_assignments VALUES ('ovr001', ?)", [qk17[:6]])

        # Write places and tile_assignments to parquet for Phase 2 API
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
        assert "85922583" in rows[0][1]
        assert "org.atgeo.places.overture.division" in rows[0][1]

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

        # Build covering from the division DB (§4: orchestrator responsibility).
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

        # Pre-create tile_assignments (§3.2 precondition; no fallback in fixed code).
        con.execute("CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)")
        con.execute("INSERT INTO tile_assignments VALUES ('ovr002', ?)", [qk17[:6]])

        # Write places and tile_assignments to parquet for Phase 2 API
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
        assert "85922583" in rkeys, \
            f"Expected division rkey '85922583' in relations: {rkeys}"
        assert all(r["collection"] == "org.atgeo.places.overture.division" for r in within)
