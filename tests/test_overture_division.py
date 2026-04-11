"""Tests for the Overture divisions pipeline and export."""

import json
import pathlib
from unittest.mock import patch, MagicMock, call

import duckdb
import pytest

from tests.quadtree_helpers import REPO_ROOT, _load_sql, _strip_spatial_install, _strip_memory_limit


# ---------------------------------------------------------------------------
# SOURCE_PK registration
# ---------------------------------------------------------------------------

class TestSourcePK:
    """overture_division must be registered in SOURCE_PK."""

    def test_overture_division_key_exists(self):
        from garganorn.quadtree import SOURCE_PK
        assert "overture_division" in SOURCE_PK, (
            f"SOURCE_PK missing 'overture_division'; keys: {list(SOURCE_PK.keys())}"
        )

    def test_overture_division_pk_is_id(self):
        from garganorn.quadtree import SOURCE_PK
        assert SOURCE_PK.get("overture_division") == "id", (
            f"Expected SOURCE_PK['overture_division'] == 'id', "
            f"got {SOURCE_PK.get('overture_division')!r}"
        )


# ---------------------------------------------------------------------------
# ATTRIBUTION registration
# ---------------------------------------------------------------------------

class TestAttribution:
    """overture_division must be registered in ATTRIBUTION."""

    def test_overture_division_key_exists(self):
        from garganorn.quadtree import ATTRIBUTION
        assert "overture_division" in ATTRIBUTION, (
            f"ATTRIBUTION missing 'overture_division'; keys: {list(ATTRIBUTION.keys())}"
        )

    def test_overture_division_attribution_url(self):
        from garganorn.quadtree import ATTRIBUTION
        assert ATTRIBUTION.get("overture_division") == "https://docs.overturemaps.org/attribution/", (
            f"Expected overture attribution URL, "
            f"got {ATTRIBUTION.get('overture_division')!r}"
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
        """overture_division coord exprs should match overture's (same bbox schema)."""
        from garganorn.quadtree import _coord_exprs
        ov_lon, ov_lat = _coord_exprs("overture")
        div_lon, div_lat = _coord_exprs("overture_division")
        assert div_lon == ov_lon, (
            f"overture_division lon_expr {div_lon!r} != overture {ov_lon!r}"
        )
        assert div_lat == ov_lat, (
            f"overture_division lat_expr {div_lat!r} != overture {ov_lat!r}"
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
# Pipeline skips importance/variants for overture_division
# ---------------------------------------------------------------------------

class TestPipelineSkipsImportanceVariants:
    """run_pipeline must skip importance and variants stages for overture_division.

    The current code unconditionally runs importance and variants for all
    sources. Phase 2 must add a conditional to skip these for
    overture_division (which computes importance=0 and variants=[] inline
    in its import SQL).
    """

    def test_importance_skipped_for_overture_division(self):
        """The run_pipeline code path for overture_division must not call
        run_sql with 'importance' stage.

        We verify this by inspecting the source code of run_pipeline for a
        conditional that guards the importance/variants calls. The current
        code has no such guard, so this test fails until the conditional is
        added.
        """
        import inspect
        from garganorn.quadtree import run_pipeline

        source_code = inspect.getsource(run_pipeline)

        # The implementation must contain a conditional that checks for
        # overture_division before running importance/variants.
        # Look for the skip pattern described in the design.
        assert "overture_division" in source_code, (
            "run_pipeline source code does not mention 'overture_division'; "
            "expected a conditional to skip importance/variants stages"
        )

    def test_variants_skipped_for_overture_division(self):
        """The run_pipeline code must skip variants for overture_division.

        Same approach: inspect source code for the conditional guard.
        """
        import inspect
        from garganorn.quadtree import run_pipeline

        source_code = inspect.getsource(run_pipeline)

        # The current code unconditionally calls:
        #   run_sql("variants", f"{source}_variants.sql")
        # After Phase 2, there must be a conditional wrapping this call
        # that excludes overture_division.
        # We check that overture_division appears near importance/variants logic.
        has_skip = (
            'overture_division' in source_code
            and ('not in' in source_code or 'skip' in source_code.lower()
                 or '!=' in source_code)
        )
        assert has_skip, (
            "run_pipeline does not contain a conditional to skip "
            "importance/variants for overture_division"
        )

    def test_overture_division_registered_in_source_pk(self):
        """overture_division must be in SOURCE_PK for the pipeline to accept it."""
        from garganorn.quadtree import SOURCE_PK
        assert "overture_division" in SOURCE_PK, (
            "overture_division must be in SOURCE_PK before pipeline can run"
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
            attrs = record["value"]["attributes"]
            rkey = record["value"]["rkey"]

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
