"""Tests for the Overture divisions pipeline and export."""

import inspect
import json
import os
import pathlib
import time
from unittest.mock import patch, MagicMock, call

import duckdb
import pytest

import garganorn.stages as _stages

from tests.quadtree_helpers import REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit


# ---------------------------------------------------------------------------
# SOURCES registration
# ---------------------------------------------------------------------------

class TestSources:
    """overture_division must be registered in SOURCES."""

    def test_overture_division_key_exists(self):
        from garganorn.quadtree import SOURCES
        assert "overture_division" in SOURCES, (
            f"SOURCES missing 'overture_division'; keys: {list(SOURCES.keys())}"
        )

    def test_overture_division_pk_is_id(self):
        from garganorn.quadtree import SOURCES
        assert SOURCES["overture_division"].source_pk == "id", (
            f"Expected SOURCES['overture_division'].source_pk == 'id', "
            f"got {SOURCES['overture_division'].source_pk!r}"
        )

    def test_overture_division_has_attribution(self):
        from garganorn.quadtree import SOURCES
        assert hasattr(SOURCES["overture_division"], "attribution"), (
            f"SOURCES['overture_division'] must have 'attribution' attribute"
        )
        assert SOURCES["overture_division"].attribution == "https://docs.overturemaps.org/attribution/", (
            f"Expected overture attribution URL, "
            f"got {SOURCES['overture_division'].attribution!r}"
        )

    def test_overture_division_has_collection(self):
        from garganorn.quadtree import SOURCES
        assert hasattr(SOURCES["overture_division"], "collection"), (
            f"SOURCES['overture_division'] must have 'collection' attribute"
        )
        assert SOURCES["overture_division"].collection == "org.atgeo.places.overture.division", (
            f"Expected 'org.atgeo.places.overture.division', "
            f"got {SOURCES['overture_division'].collection!r}"
        )


# ---------------------------------------------------------------------------
# ATTRIBUTION registration (deprecated - replaced by SOURCES)
# ---------------------------------------------------------------------------

class TestAttribution:
    """overture_division must be registered in ATTRIBUTION (deprecated)."""

    def test_overture_division_key_exists(self):
        from garganorn.quadtree import SOURCES
        assert "overture_division" in SOURCES, (
            f"SOURCES missing 'overture_division'; keys: {list(SOURCES.keys())}"
        )

    def test_overture_division_attribution_url(self):
        from garganorn.quadtree import SOURCES
        assert SOURCES["overture_division"].attribution == "https://docs.overturemaps.org/attribution/", (
            f"Expected overture attribution URL, "
            f"got {SOURCES['overture_division'].attribution!r}"
        )


# ---------------------------------------------------------------------------
# _coord_exprs for overture_division
# ---------------------------------------------------------------------------

class TestCoordExprs:
    """_coord_exprs must return the division's own interior-point columns
    for overture_division, not overture_place's bbox-midpoint expression.

    A bbox midpoint can land outside a non-convex/crescent/multi-part
    (MULTIPOLYGON) division's own geometry -- e.g. Norway, Chile, Indonesia.
    The fix computes ST_PointOnSurface(geometry) once at import time and
    stores it as interior_lon/interior_lat columns; _coord_exprs must
    reference those columns for overture_division instead of bbox.
    """

    def test_returns_interior_point_no_alias(self):
        from garganorn.quadtree import _coord_exprs
        lon_expr, lat_expr = _coord_exprs("overture_division")
        assert lon_expr == "interior_lon", (
            f"Expected 'interior_lon', got {lon_expr!r}"
        )
        assert lat_expr == "interior_lat", (
            f"Expected 'interior_lat', got {lat_expr!r}"
        )

    def test_returns_interior_point_with_alias(self):
        from garganorn.quadtree import _coord_exprs
        lon_expr, lat_expr = _coord_exprs("overture_division", alias="p")
        assert lon_expr == "p.interior_lon", (
            f"Expected 'p.interior_lon', got {lon_expr!r}"
        )
        assert lat_expr == "p.interior_lat", (
            f"Expected 'p.interior_lat', got {lat_expr!r}"
        )

    def test_differs_from_overture_place_expressions(self):
        """overture_division must no longer share overture_place's bbox
        midpoint expression -- that shared expression is the bug."""
        from garganorn.quadtree import _coord_exprs
        ov_lon, ov_lat = _coord_exprs("overture_place")
        div_lon, div_lat = _coord_exprs("overture_division")
        assert div_lon != ov_lon, (
            f"overture_division lon_expr must differ from overture_place's "
            f"bbox-midpoint expression; got the same: {div_lon!r}"
        )
        assert div_lat != ov_lat, (
            f"overture_division lat_expr must differ from overture_place's "
            f"bbox-midpoint expression; got the same: {div_lat!r}"
        )


# ---------------------------------------------------------------------------
# SQL file existence
# ---------------------------------------------------------------------------

class TestSQLFiles:
    """The import and export SQL files must exist on disk."""

    def test_import_sql_exists(self):
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_division_import.sql"
        assert sql_path.exists(), f"Import SQL file not found: {sql_path}"

    def test_export_sql_exists(self):
        sql_path = REPO_ROOT / "garganorn" / "sql" / "overture_division_export_tiles.sql"
        assert sql_path.exists(), f"Export SQL file not found: {sql_path}"


# ---------------------------------------------------------------------------
# Pipeline computes importance/variants inline for all sources (Phase 2)
# ---------------------------------------------------------------------------

class TestPipelineSkipsImportanceVariants:
    """run_pipeline computes importance/variants inline for all sources (Phase 2).

    Phase 2 eliminated separate importance and variants stages. All sources
    now compute these values inline during import. overture_division uses
    a hybrid formula (density+population) and sets variants=[] inline in
    its import SQL.
    """

    def test_importance_computed_inline_for_overture_division(self):
        """run_pipeline computes importance inline during import for all sources.

        Phase 2 eliminated separate importance/variants stages. We verify this
        by checking that run_pipeline passes density_parquet and idf_parquet
        to stage_import (which computes importance inline in the SQL CTAS).
        """
        import inspect
        from garganorn.quadtree import run_pipeline

        source_code = inspect.getsource(run_pipeline)

        # The import call (which computes importance inline) must pass density_parquet
        # and idf_parquet.  The call is currently routed through the transitional
        # helper (_transitional_import_phase1) until G7 rewrites quadtree.py; the
        # behavioral invariant — that importance is computed inline in the import SQL
        # rather than via a separate stage — is unchanged.
        assert "density_parquet=" in source_code, (
            "run_pipeline must pass density_parquet to the import call for inline importance"
        )
        # overture_division doesn't use IDF, but other sources do
        assert "idf_parquet=" in source_code, (
            "run_pipeline must pass idf_parquet to the import call for inline importance"
        )

    def test_variants_computed_inline_for_overture_division(self):
        """run_pipeline computes variants inline during import for all sources.

        Phase 2 eliminated the separate variants stage. We verify this by
        checking that run_pipeline no longer calls a separate variants stage.
        overture_division sets variants=[] inline in its import SQL.
        """
        import inspect
        from garganorn.quadtree import run_pipeline

        source_code = inspect.getsource(run_pipeline)

        # Phase 2: run_pipeline must NOT call a separate variants stage
        assert "stage_variants" not in source_code, (
            "run_pipeline must not call stage_variants (Phase 2 eliminated this stage)"
        )
        # Variants are computed inline in the import SQL.  The import call is
        # currently routed through _transitional_import_phase1 (TEMPORARY, until
        # G7 rewrites quadtree.py); verify no separate variants stage exists.
        assert "stage_variants(" not in source_code, (
            "run_pipeline must not call stage_variants (variants computed inline)"
        )

    def test_overture_division_registered_in_sources(self):
        """overture_division must be in SOURCES for the pipeline to accept it."""
        from garganorn.quadtree import SOURCES
        assert "overture_division" in SOURCES, (
            "overture_division must be in SOURCES before pipeline can run"
        )


# ---------------------------------------------------------------------------
# Export: strip null-valued keys from attributes
# ---------------------------------------------------------------------------

_DIV_EXPORT_PLACES = [
    # (id, name, subtype, country, region, level, wikidata, population)
    # level values are the atgeo containment vocabulary (garganorn.levels.LEVEL_VOCAB),
    # re-derived from subtype: country=10, region=25, county=35.
    ("div001", "Testland", "country", "US", None, 10, None, 1000000),
    ("div002", "Testregion", "region", "US", "CA", 25, "Q123", None),
    # all optional fields null
    ("div003", "Nowhere", "county", None, None, 35, None, None),
]


def _make_division_export_db(conn, places_rows=None):
    """Populate conn with minimal places, tile_assignments, place_containment."""
    if places_rows is None:
        places_rows = _DIV_EXPORT_PLACES

    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute("""
        CREATE TABLE places (
            id VARCHAR,
            geometry GEOMETRY,
            names STRUCT("primary" VARCHAR, common MAP(VARCHAR, VARCHAR),
                         rules STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]),
            subtype VARCHAR,
            country VARCHAR,
            region VARCHAR,
            level INTEGER,
            wikidata VARCHAR,
            population BIGINT,
            min_latitude DOUBLE,
            max_latitude DOUBLE,
            min_longitude DOUBLE,
            max_longitude DOUBLE,
            bbox STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE),
            qk17 VARCHAR,
            importance INTEGER,
            variants STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
        )
    """)

    for (div_id, name, subtype, country, region, level,
         wikidata, population) in places_rows:
        # NOTE: f-string interpolation is safe here — all values are
        # hardcoded test data from _DIV_EXPORT_PLACES above.
        conn.execute(f"""
            INSERT INTO places VALUES (
                '{div_id}',
                ST_GeomFromText('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))'),
                {{'primary': '{name}', 'common': map([]::VARCHAR[], []::VARCHAR[]),
                  'rules': []::STRUCT(language VARCHAR, value VARCHAR, variant VARCHAR)[]}},
                {f"'{subtype}'" if subtype else "NULL"},
                {f"'{country}'" if country else "NULL"},
                {f"'{region}'" if region else "NULL"},
                {level},
                {f"'{wikidata}'" if wikidata else "NULL"},
                {population if population else "NULL"},
                37.7, 37.8, -122.5, -122.4,
                {{'xmin': -122.5, 'ymin': 37.7, 'xmax': -122.4, 'ymax': 37.8}},
                ST_QuadKey(-122.45, 37.75, 17),
                0,
                []::STRUCT(name VARCHAR, type VARCHAR, language VARCHAR)[]
            )
        """)

    conn.execute("""
        CREATE TABLE tile_assignments (place_id VARCHAR, tile_qk VARCHAR)
    """)
    for (div_id, *_) in places_rows:
        conn.execute(f"INSERT INTO tile_assignments VALUES ('{div_id}', '023010')")

    conn.execute("""
        CREATE TABLE place_containment (
            place_id VARCHAR, relations_json VARCHAR, tile_qk VARCHAR
        )
    """)


class TestExportStripJsonNulls:
    """overture_division export must strip null-valued keys from attributes."""

    def test_export_strips_null_attributes(self, tmp_path):
        """Attributes dict must not contain null-valued keys."""
        db_path = tmp_path / "test_division_strip_nulls.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_division_export_db(conn)

        raw_sql = _load_sql("overture_division_export_tiles.sql",
                            {"repo": "https://example.com"})
        sql = _strip_spatial_install(_strip_memory_limit(raw_sql))
        conn.execute(sql)

        rows = conn.execute(
            "SELECT record_json FROM tile_export ORDER BY record_json"
        ).fetchall()
        conn.close()

        assert len(rows) == 3

        for (record_json,) in rows:
            record = json.loads(record_json)
            attrs = record["attributes"]
            rkey = record["rkey"]

            null_keys = [k for k, v in attrs.items() if v is None]
            assert not null_keys, (
                f"Record {rkey} has null-valued attribute keys: {null_keys}. "
                f"Attributes: {attrs}"
            )

            if rkey == "div001":
                assert attrs["country"] == "US"
                assert attrs["population"] == 1000000
                assert "region" not in attrs
                assert "wikidata" not in attrs
            elif rkey == "div002":
                assert attrs["country"] == "US"
                assert attrs["region"] == "CA"
                assert attrs["wikidata"] == "Q123"
                assert "population" not in attrs
            elif rkey == "div003":
                assert attrs["subtype"] == "county"
                assert attrs["level"] == 35  # LEVEL_VOCAB["county"]
                assert set(attrs.keys()) == {"subtype", "level"}


# ---------------------------------------------------------------------------
# Export: a division referenced by N tiles exports N records, not N squared
# ---------------------------------------------------------------------------

class TestDivisionMultiTileContainmentJoin:
    """One record per division per referencing tile, whatever N is.

    Once a division is referenced by every tile its geometry reaches, both
    tile_assignments and place_containment carry N rows for it — the latter
    because compute_containment groups by (tile_qk, place_id). A containment
    join keyed on place_id alone pairs each with each and exports N^2 copies.
    Nothing else in the suite builds a division with more than one tile
    reference, so nothing else would notice.
    """

    def test_three_tile_division_exports_three_records(self, tmp_path):
        db_path = tmp_path / "test_division_multi_tile.duckdb"
        conn = duckdb.connect(str(db_path))
        _make_division_export_db(conn, places_rows=[_DIV_EXPORT_PLACES[0]])

        tile_qks = ["023010", "023011", "023012"]
        relations = json.dumps({"within": [
            {"rkey": "org.atgeo.places.overture.division:div000",
             "name": "Testcontinent", "level": 0}
        ]})

        # Replace the helper's single assignment with one per tile, and give
        # each the matching containment row compute_containment would emit —
        # same place_id, same relations_json, different tile_qk.
        conn.execute("DELETE FROM tile_assignments")
        for tile_qk in tile_qks:
            conn.execute("INSERT INTO tile_assignments VALUES ('div001', ?)", [tile_qk])
            conn.execute(
                "INSERT INTO place_containment VALUES ('div001', ?, ?)",
                [relations, tile_qk],
            )

        raw_sql = _load_sql("overture_division_export_tiles.sql",
                            {"repo": "https://example.com"})
        conn.execute(_strip_spatial_install(_strip_memory_limit(raw_sql)))

        rows = conn.execute(
            "SELECT tile_qk, record_json FROM tile_export ORDER BY tile_qk"
        ).fetchall()
        conn.close()

        assert [tile_qk for tile_qk, _ in rows] == tile_qks, (
            f"expected one row per referencing tile, got {len(rows)} rows: "
            f"{[t for t, _ in rows]}"
        )
        # Every copy must be byte-identical: the spec's dedup-by-rkey rule
        # drops all but one, so any per-tile divergence is silent data loss.
        assert len({record_json for _, record_json in rows}) == 1, (
            "a division's record must be identical in every tile referencing it"
        )
        assert json.loads(rows[0][1])["relations"] == json.loads(relations)


# ---------------------------------------------------------------------------
# Phase 2 division import artifact tests (RED)
# ---------------------------------------------------------------------------

class TestDivisionImportArtifactPhase2:
    """stage_import for overture_division must write both places.parquet
    and boundaries.duckdb with unchanged schema.

    Fails in Red phase because stage_import still takes 'con' as first arg.
    """

    _BBOX = (-122.55, 37.60, -122.30, 37.85)

    def test_stage_import_no_con_parameter(self):
        """stage_import must not take 'con' as its first parameter."""
        params = list(inspect.signature(_stages.stage_import).parameters.keys())
        assert params[0] != "con", (
            f"stage_import must not have 'con' as first param; got {params[0]!r}"
        )

    def test_stage_division_import_writes_places_parquet(self, division_parquet, tmp_path):
        """stage_import for overture_division must write places.parquet."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("overture_division", division_parquet, self._BBOX, output)
        assert pathlib.Path(output).exists(), f"places.parquet not written to {output}"

    def test_stage_division_import_writes_boundaries_duckdb(self, division_parquet, tmp_path):
        """stage_import for overture_division must write boundaries.duckdb in the same dir."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("overture_division", division_parquet, self._BBOX, output)
        boundaries = tmp_path / "boundaries.duckdb"
        assert boundaries.exists(), f"boundaries.duckdb not written alongside places.parquet"

    def test_boundaries_duckdb_schema_replaces_admin_level_with_level(self, division_parquet, tmp_path):
        """boundaries.duckdb must have a level column, not admin_level.

        The atgeo level vocabulary replaces the raw Overture admin_level
        integer throughout the boundaries.duckdb export.
        """
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("overture_division", division_parquet, self._BBOX, output)
        boundaries = str(tmp_path / "boundaries.duckdb")
        con = duckdb.connect()
        con.execute(f"ATTACH '{boundaries}' AS bnd (READ_ONLY)")
        cols = {r[0] for r in con.execute("DESCRIBE bnd.places").fetchall()}
        con.close()
        assert "level" in cols, (
            f"boundaries.duckdb must have level column; found: {cols}"
        )
        assert "admin_level" not in cols, (
            f"boundaries.duckdb must not have admin_level column; found: {cols}"
        )

    def test_boundaries_duckdb_has_rtree_index(self, division_parquet, tmp_path):
        """boundaries.duckdb must have an R-tree index on geometry."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("overture_division", division_parquet, self._BBOX, output)
        boundaries = str(tmp_path / "boundaries.duckdb")
        con = duckdb.connect()
        con.execute(f"ATTACH '{boundaries}' AS bnd (READ_ONLY)")
        # duckdb_indexes() in DuckDB 1.2.1 has no index_type column; use index_name.
        # The R-tree index is created with name "bnd_places_rtree".
        indexes = con.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE database_name = 'bnd'"
        ).fetchall()
        con.close()
        index_names = {r[0].lower() for r in indexes}
        assert any("rtree" in name for name in index_names), (
            f"boundaries.duckdb must have R-tree index; found index names: {index_names}"
        )

    def test_boundaries_duckdb_places_hilbert_sorted(self, division_parquet, tmp_path):
        """bnd.places must be physically sorted by ST_Hilbert(geometry) over the
        world BOX_2D (zone maps require sorted columns)."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("overture_division", division_parquet, self._BBOX, output)
        boundaries = str(tmp_path / "boundaries.duckdb")
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"ATTACH '{boundaries}' AS bnd (READ_ONLY)")
        rows = [r[0] for r in con.execute("""
            SELECT ST_Hilbert(geometry,
                {'min_x': -180.0, 'min_y': -90.0,
                 'max_x': 180.0, 'max_y': 90.0}::BOX_2D)
            FROM bnd.places
        """).fetchall()]
        con.close()
        assert len(rows) >= 3, f"expected fixture divisions in bnd.places; got {len(rows)} rows"
        assert rows == sorted(rows), "bnd.places must be sorted by ST_Hilbert(geometry)"

    def test_places_parquet_no_geometry_column(self, division_parquet, tmp_path):
        """division places.parquet must not contain 'geometry' column."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("overture_division", division_parquet, self._BBOX, output)
        con = duckdb.connect()
        cols = {r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{output}')"
        ).fetchall()}
        con.close()
        assert "geometry" not in cols, (
            f"division places.parquet must not contain 'geometry' column; found: {cols}"
        )

    def test_single_meta_gates_both_artifacts(self, division_parquet, tmp_path):
        """Deleting boundaries.duckdb must make stage_import stale (single meta gates both)."""
        output = str(tmp_path / "places.parquet")
        _stages.stage_import("overture_division", division_parquet, self._BBOX, output)
        mtime1 = os.path.getmtime(output)
        # Remove boundaries.duckdb to simulate it being stale/missing
        boundaries = tmp_path / "boundaries.duckdb"
        boundaries.unlink()
        time.sleep(0.05)
        # Rerun without force — must rebuild because boundaries.duckdb is gone
        _stages.stage_import("overture_division", division_parquet, self._BBOX, output)
        mtime2 = os.path.getmtime(output)
        assert mtime2 > mtime1, (
            "stage_import must rebuild when boundaries.duckdb is missing "
            "(single meta gates both artifacts)"
        )


# ---------------------------------------------------------------------------
# _assert_interior_points (representative-candidate-point fix, RED)
# ---------------------------------------------------------------------------

class TestAssertInteriorPoints:
    """_assert_interior_points must raise RuntimeError when any row's
    (interior_lon, interior_lat) is not ST_Within its own geometry, and
    must not raise when every row's point is genuinely interior.

    Exercised directly against a synthetic (geometry, interior_lon,
    interior_lat) table -- no need to reconstruct division_all's full
    column set, per the design note in stages._assert_interior_points.
    """

    _SQUARE_WKT = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"
    # Two disjoint squares -- an exterior point (15, 15), the midpoint
    # between their centers (5, 5) and (25, 25), sits in the gap between
    # them, outside both parts, modeling the Norway/Chile/Indonesia
    # multi-part failure mode this guard exists to catch.
    _MULTIPOLYGON_WKT = (
        "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)),"
        "((20 20, 30 20, 30 30, 20 30, 20 20)))"
    )

    def _make_table(self):
        con = duckdb.connect(":memory:")
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("""
            CREATE TABLE t (
                geometry GEOMETRY,
                interior_lon DOUBLE,
                interior_lat DOUBLE
            )
        """)
        return con

    def test_exterior_point_raises(self):
        con = self._make_table()
        con.execute(f"""
            INSERT INTO t VALUES (ST_GeomFromText('{self._SQUARE_WKT}'), 50.0, 50.0)
        """)
        with pytest.raises(RuntimeError):
            _stages._assert_interior_points(con, "t", "test import")

    def test_multipolygon_exterior_point_raises(self):
        con = self._make_table()
        con.execute(f"""
            INSERT INTO t VALUES (ST_GeomFromText('{self._MULTIPOLYGON_WKT}'), 15.0, 15.0)
        """)
        with pytest.raises(RuntimeError):
            _stages._assert_interior_points(con, "t", "test import")

    def test_null_lon_raises(self):
        con = self._make_table()
        con.execute(f"""
            INSERT INTO t VALUES (ST_GeomFromText('{self._SQUARE_WKT}'), NULL, 5.0)
        """)
        with pytest.raises(RuntimeError):
            _stages._assert_interior_points(con, "t", "test import")

    def test_null_lat_raises(self):
        con = self._make_table()
        con.execute(f"""
            INSERT INTO t VALUES (ST_GeomFromText('{self._SQUARE_WKT}'), 5.0, NULL)
        """)
        with pytest.raises(RuntimeError):
            _stages._assert_interior_points(con, "t", "test import")

    def test_nan_lon_raises(self):
        con = self._make_table()
        con.execute(f"""
            INSERT INTO t VALUES (ST_GeomFromText('{self._SQUARE_WKT}'), 'NaN'::DOUBLE, 5.0)
        """)
        with pytest.raises(RuntimeError):
            _stages._assert_interior_points(con, "t", "test import")

    def test_nan_lat_raises(self):
        con = self._make_table()
        con.execute(f"""
            INSERT INTO t VALUES (ST_GeomFromText('{self._SQUARE_WKT}'), 5.0, 'NaN'::DOUBLE)
        """)
        with pytest.raises(RuntimeError):
            _stages._assert_interior_points(con, "t", "test import")

    def test_interior_point_does_not_raise(self):
        con = self._make_table()
        con.execute(f"""
            INSERT INTO t VALUES (ST_GeomFromText('{self._SQUARE_WKT}'), 5.0, 5.0)
        """)
        _stages._assert_interior_points(con, "t", "test import")
