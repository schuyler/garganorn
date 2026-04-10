"""Tests for Phase 2 of the Overture divisions migration.

Red phase: these tests verify that overture_division is registered as a
pipeline source in quadtree.py and that the expected SQL files exist.
All tests should fail until the Green phase implementation.
"""

import pathlib
from unittest.mock import patch, MagicMock, call

import pytest

from tests.quadtree_helpers import REPO_ROOT


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
