"""Red-phase tests for the _coord_exprs table-alias bug.

Bug: _coord_exprs("overture") returns "(bbox.xmin + bbox.xmax) / 2.0" with no
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

from garganorn.quadtree import _coord_exprs, compute_containment


# ---------------------------------------------------------------------------
# _coord_exprs alias parameter
# ---------------------------------------------------------------------------

class TestCoordExprsAlias:
    """_coord_exprs(source, alias="") embeds the alias in struct field references."""

    def test_overture_no_alias_unchanged(self):
        """Without alias, overture expressions are bare struct field references."""
        lon, lat = _coord_exprs("overture")
        assert "bbox.xmin" in lon
        assert "bbox.xmax" in lon
        assert "bbox.ymin" in lat
        assert "bbox.ymax" in lat

    def test_overture_with_alias_prefixes_struct_fields(self):
        """With alias='p', each struct field reference is qualified as p.bbox.*."""
        lon, lat = _coord_exprs("overture", alias="p")
        # Expressions must reference p.bbox.*, not bare bbox.*
        assert "p.bbox.xmin" in lon, f"Expected 'p.bbox.xmin' in lon expr: {lon!r}"
        assert "p.bbox.xmax" in lon, f"Expected 'p.bbox.xmax' in lon expr: {lon!r}"
        assert "p.bbox.ymin" in lat, f"Expected 'p.bbox.ymin' in lat expr: {lat!r}"
        assert "p.bbox.ymax" in lat, f"Expected 'p.bbox.ymax' in lat expr: {lat!r}"

    def test_overture_with_alias_no_bare_bbox_refs(self):
        """With alias='p', no unqualified bbox.* appears in the expression."""
        lon, lat = _coord_exprs("overture", alias="p")
        # A bare `bbox.xmin` (not preceded by 'p.') would indicate the alias
        # was not applied. The simplest check: the expression should not start
        # with "(bbox." after stripping whitespace.
        assert not lon.lstrip("(").startswith("bbox."), \
            f"lon expr starts with bare 'bbox.': {lon!r}"
        assert not lat.lstrip("(").startswith("bbox."), \
            f"lat expr starts with bare 'bbox.': {lat!r}"

    def test_non_overture_alias_prefixes_columns(self):
        """For non-struct sources, alias is applied as a table prefix."""
        lon_no_alias, lat_no_alias = _coord_exprs("fsq")
        lon_alias, lat_alias = _coord_exprs("fsq", alias="p")
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
        lon_expr, lat_expr = _coord_exprs("overture", alias="p")
        # This SELECT should execute without ParserException
        row = con.execute(f"SELECT {lon_expr}, {lat_expr} FROM places p").fetchone()
        assert row is not None
        assert abs(row[0] - (-122.41)) < 0.01, f"Unexpected lon: {row[0]}"
        assert abs(row[1] - 37.78) < 0.01, f"Unexpected lat: {row[1]}"


# ---------------------------------------------------------------------------
# compute_containment with overture-style bbox struct column
# ---------------------------------------------------------------------------

def _make_wof_db(path):
    """Create a minimal WoF boundaries DuckDB at *path*.

    The geometry is a simple polygon covering the San Francisco area, which
    contains the test place at (-122.42, 37.78).
    """
    con = duckdb.connect(path)
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("""
        CREATE TABLE boundaries (
            wof_id        BIGINT,
            rkey          VARCHAR,
            name          VARCHAR,
            placetype     VARCHAR,
            level         INTEGER,
            latitude      DOUBLE,
            longitude     DOUBLE,
            geom          GEOMETRY,
            country       VARCHAR,
            min_latitude  DOUBLE,
            min_longitude DOUBLE,
            max_latitude  DOUBLE,
            max_longitude DOUBLE,
            names_json    VARCHAR,
            concordances  VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO boundaries VALUES (
            85922583, '85922583', 'San Francisco', 'locality', 50,
            37.7749, -122.4194,
            ST_GeomFromText('POLYGON((-122.55 37.6, -122.55 37.85, -122.3 37.85, -122.3 37.6, -122.55 37.6))'),
            'US',
            37.6, -122.55, 37.85, -122.3,
            NULL, NULL
        )
    """)
    con.execute("CREATE INDEX boundaries_rtree ON boundaries USING RTREE (geom)")
    con.close()


class TestComputeContainmentOverture:
    """compute_containment must execute without ParserException for overture places."""

    def test_overture_bbox_struct_does_not_raise_parser_error(self, tmp_path):
        """compute_containment with overture bbox struct places must not raise.

        Currently raises duckdb.ParserException due to `p.(bbox.xmin + bbox.xmax) / 2.0`
        being invalid SQL. After the fix, it should succeed.
        """
        wof_path = str(tmp_path / "wof.duckdb")
        _make_wof_db(wof_path)

        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE places (
                id    VARCHAR,
                bbox  STRUCT(xmin DOUBLE, xmax DOUBLE, ymin DOUBLE, ymax DOUBLE),
                qk17  VARCHAR
            )
        """)
        # Place inside the SF polygon (lon=-122.42, lat=37.78)
        con.execute("""
            INSERT INTO places VALUES (
                'ovr001',
                {'xmin': -122.425, 'xmax': -122.415, 'ymin': 37.775, 'ymax': 37.785},
                '0230102310231023'
            )
        """)

        lon_expr, lat_expr = _coord_exprs("overture", alias="p")
        pk_expr = "p.id"

        # This call currently fails with TypeError because _coord_exprs does not
        # yet accept an alias parameter. After the fix, it should succeed.
        compute_containment(con, wof_path, pk_expr, lon_expr, lat_expr)

        # Verify the containment table was populated
        rows = con.execute("SELECT place_id, relations_json FROM place_containment").fetchall()
        assert len(rows) == 1, f"Expected 1 containment row, got {len(rows)}"
        assert rows[0][0] == "ovr001"
        assert "San Francisco" in rows[0][1]

    def test_overture_containment_result_has_correct_wof_rkey(self, tmp_path):
        """The containment record for an overture place references the expected WoF rkey."""
        wof_path = str(tmp_path / "wof.duckdb")
        _make_wof_db(wof_path)

        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE places (
                id    VARCHAR,
                bbox  STRUCT(xmin DOUBLE, xmax DOUBLE, ymin DOUBLE, ymax DOUBLE),
                qk17  VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO places VALUES (
                'ovr002',
                {'xmin': -122.425, 'xmax': -122.415, 'ymin': 37.775, 'ymax': 37.785},
                '0230102310231023'
            )
        """)

        lon_expr, lat_expr = _coord_exprs("overture", alias="p")
        pk_expr = "p.id"

        compute_containment(con, wof_path, pk_expr, lon_expr, lat_expr)

        rows = con.execute("SELECT place_id, relations_json FROM place_containment").fetchall()
        assert len(rows) == 1
        relations = json.loads(rows[0][1])
        within = relations.get("within", [])
        rkeys = [r["rkey"] for r in within]
        assert any("85922583" in rk for rk in rkeys), \
            f"Expected WoF rkey 85922583 in relations: {rkeys}"
