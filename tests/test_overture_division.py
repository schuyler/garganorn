"""Tests for the Overture divisions pipeline and export."""

import json
import pathlib
from unittest.mock import patch, MagicMock, call

import duckdb
import pytest

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
    """_coord_exprs must return bbox midpoint expressions for overture_division."""

    def test_returns_bbox_midpoint_no_alias(self):
        from garganorn.quadtree import _coord_exprs
        lon_expr, lat_expr = _coord_exprs("overture_division")
        assert "bbox.xmin" in lon_expr and "bbox.xmax" in lon_expr, (
            f"Expected bbox midpoint lon expression, got {lon_expr!r}"
        )
        assert "bbox.ymin" in lat_expr and "bbox.ymax" in lat_expr, (
            f"Expected bbox midpoint lat expression, got {lat_expr!r}"
        )

    def test_returns_bbox_midpoint_with_alias(self):
        from garganorn.quadtree import _coord_exprs
        lon_expr, lat_expr = _coord_exprs("overture_division", alias="p")
        assert "p.bbox.xmin" in lon_expr and "p.bbox.xmax" in lon_expr, (
            f"Expected aliased bbox midpoint lon expression, got {lon_expr!r}"
        )
        assert "p.bbox.ymin" in lat_expr and "p.bbox.ymax" in lat_expr, (
            f"Expected aliased bbox midpoint lat expression, got {lat_expr!r}"
        )

    def test_matches_overture_expressions(self):
        """overture_division coord exprs should match overture_place's (same bbox schema)."""
        from garganorn.quadtree import _coord_exprs
        ov_lon, ov_lat = _coord_exprs("overture_place")
        div_lon, div_lat = _coord_exprs("overture_division")
        assert div_lon == ov_lon, (
            f"overture_division lon_expr {div_lon!r} != overture_place {ov_lon!r}"
        )
        assert div_lat == ov_lat, (
            f"overture_division lat_expr {div_lat!r} != overture_place {ov_lat!r}"
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

        # Phase 2: run_pipeline must pass density_parquet and idf_parquet to stage_import
        assert "stage_import(" in source_code, "run_pipeline must call stage_import"
        assert "density_parquet=" in source_code, (
            "run_pipeline must pass density_parquet to stage_import for inline importance"
        )
        # overture_division doesn't use IDF, but other sources do
        assert "idf_parquet=" in source_code, (
            "run_pipeline must pass idf_parquet to stage_import for inline importance"
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
        # Verify that stage_import is called (variants are computed inline in import SQL)
        assert "stage_import(" in source_code, (
            "run_pipeline must call stage_import (variants computed inline)"
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
    # (id, name, subtype, country, region, admin_level, wikidata, population)
    ("div001", "Testland", "country", "US", None, 2, None, 1000000),
    ("div002", "Testregion", "region", "US", "CA", 4, "Q123", None),
    # all optional fields null
    ("div003", "Nowhere", "county", None, None, 6, None, None),
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
            admin_level INTEGER,
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

    for (div_id, name, subtype, country, region, admin_level,
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
                {admin_level},
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
        CREATE TABLE place_containment (place_id VARCHAR, relations_json VARCHAR)
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
                assert attrs["admin_level"] == 6
                assert set(attrs.keys()) == {"subtype", "admin_level"}
