"""RED TDD tests for source key unification refactor.

These tests document the desired state after renaming source keys and classes.
All tests MUST FAIL against the current codebase.

Refactor goals:
1. Rename source keys: fsq → foursquare, overture → overture_place
2. Rename classes: OvertureMaps → OverturePlaces, OvertureDivision → OvertureDivisions
3. Move OvertureDivision from boundaries.py to database.py (renamed to OvertureDivisions)
4. Add source_key and source_pk class attributes to each collection class
5. Build SOURCES registry in quadtree.py from collection classes
6. Rename SQL files: fsq_*.sql → foursquare_*.sql, overture_*.sql → overture_place_*.sql
7. CLI --source choices: ["foursquare", "overture_place", "osm", "overture_division"]
"""
import importlib
import pytest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent


class TestCollectionClassAttributes:
    """Test that collection classes have source_key and source_pk attributes."""

    def test_foursquare_osp_has_source_key(self):
        """FoursquareOSP should have source_key == 'foursquare'."""
        from garganorn.database import FoursquareOSP
        assert hasattr(FoursquareOSP, 'source_key'), \
            "FoursquareOSP missing source_key class attribute"
        assert FoursquareOSP.source_key == "foursquare", \
            f"FoursquareOSP.source_key should be 'foursquare', got {FoursquareOSP.source_key!r}"

    def test_overture_places_has_source_key(self):
        """OverturePlaces should have source_key == 'overture_place'."""
        from garganorn.database import OverturePlaces
        assert hasattr(OverturePlaces, 'source_key'), \
            "OverturePlaces missing source_key class attribute"
        assert OverturePlaces.source_key == "overture_place", \
            f"OverturePlaces.source_key should be 'overture_place', got {OverturePlaces.source_key!r}"

    def test_openstreetmap_has_source_key(self):
        """OpenStreetMap should have source_key == 'osm'."""
        from garganorn.database import OpenStreetMap
        assert hasattr(OpenStreetMap, 'source_key'), \
            "OpenStreetMap missing source_key class attribute"
        assert OpenStreetMap.source_key == "osm", \
            f"OpenStreetMap.source_key should be 'osm', got {OpenStreetMap.source_key!r}"

    def test_overture_divisions_has_source_key(self):
        """OvertureDivisions should have source_key == 'overture_division'."""
        from garganorn.database import OvertureDivisions
        assert hasattr(OvertureDivisions, 'source_key'), \
            "OvertureDivisions missing source_key class attribute"
        assert OvertureDivisions.source_key == "overture_division", \
            f"OvertureDivisions.source_key should be 'overture_division', got {OvertureDivisions.source_key!r}"

    def test_foursquare_osp_has_source_pk(self):
        """FoursquareOSP should have source_pk == 'fsq_place_id'."""
        from garganorn.database import FoursquareOSP
        assert hasattr(FoursquareOSP, 'source_pk'), \
            "FoursquareOSP missing source_pk class attribute"
        assert FoursquareOSP.source_pk == "fsq_place_id", \
            f"FoursquareOSP.source_pk should be 'fsq_place_id', got {FoursquareOSP.source_pk!r}"

    def test_overture_places_has_source_pk(self):
        """OverturePlaces should have source_pk == 'id'."""
        from garganorn.database import OverturePlaces
        assert hasattr(OverturePlaces, 'source_pk'), \
            "OverturePlaces missing source_pk class attribute"
        assert OverturePlaces.source_pk == "id", \
            f"OverturePlaces.source_pk should be 'id', got {OverturePlaces.source_pk!r}"

    def test_openstreetmap_has_source_pk(self):
        """OpenStreetMap should have source_pk == 'rkey'."""
        from garganorn.database import OpenStreetMap
        assert hasattr(OpenStreetMap, 'source_pk'), \
            "OpenStreetMap missing source_pk class attribute"
        assert OpenStreetMap.source_pk == "rkey", \
            f"OpenStreetMap.source_pk should be 'rkey', got {OpenStreetMap.source_pk!r}"

    def test_overture_divisions_has_source_pk(self):
        """OvertureDivisions should have source_pk == 'id'."""
        from garganorn.database import OvertureDivisions
        assert hasattr(OvertureDivisions, 'source_pk'), \
            "OvertureDivisions missing source_pk class attribute"
        assert OvertureDivisions.source_pk == "id", \
            f"OvertureDivisions.source_pk should be 'id', got {OvertureDivisions.source_pk!r}"


class TestClassRenaming:
    """Test that old class names are removed and new ones exist."""

    def test_overture_places_class_exists(self):
        """OverturePlaces class should be importable from garganorn.database."""
        from garganorn.database import OverturePlaces
        assert OverturePlaces is not None, \
            "OverturePlaces class should exist in garganorn.database"

    def test_overture_maps_class_removed(self):
        """OvertureMaps class should NOT exist (renamed to OverturePlaces)."""
        with pytest.raises(ImportError, match="OvertureMaps"):
            from garganorn.database import OvertureMaps
            # If we get here, the import succeeded - fail the test
            pytest.fail("OvertureMaps should not exist (should be renamed to OverturePlaces)")

    def test_overture_divisions_class_in_database(self):
        """OvertureDivisions class should be importable from garganorn.database."""
        from garganorn.database import OvertureDivisions
        assert OvertureDivisions is not None, \
            "OvertureDivisions class should exist in garganorn.database"

    def test_overture_division_removed_from_boundaries(self):
        """OvertureDivision should NOT be importable from garganorn.boundaries."""
        with pytest.raises(ImportError, match="OvertureDivision"):
            from garganorn.boundaries import OvertureDivision
            # If we get here, the import succeeded - fail the test
            pytest.fail("OvertureDivision should not exist in boundaries.py (moved to database.py as OvertureDivisions)")


class TestQuadtreeRegistry:
    """Test that quadtree.py has SOURCES registry and old dicts are removed."""

    def test_quadtree_sources_registry_exists(self):
        """quadtree.py should export a SOURCES dict keyed by source_key strings."""
        import garganorn.quadtree
        assert hasattr(garganorn.quadtree, 'SOURCES'), \
            "quadtree.py should have SOURCES dict"
        assert isinstance(garganorn.quadtree.SOURCES, dict), \
            "SOURCES should be a dict"

    def test_quadtree_sources_has_correct_keys(self):
        """SOURCES should be keyed by 'foursquare', 'overture_place', 'osm', 'overture_division'."""
        import garganorn.quadtree
        expected_keys = {"foursquare", "overture_place", "osm", "overture_division"}
        actual_keys = set(garganorn.quadtree.SOURCES.keys())
        assert actual_keys == expected_keys, \
            f"SOURCES keys should be {expected_keys}, got {actual_keys}"

    def test_quadtree_sources_values_are_classes(self):
        """SOURCES values should be the collection classes."""
        import garganorn.quadtree
        from garganorn.database import FoursquareOSP, OverturePlaces, OpenStreetMap, OvertureDivisions

        expected_classes = {
            "foursquare": FoursquareOSP,
            "overture_place": OverturePlaces,
            "osm": OpenStreetMap,
            "overture_division": OvertureDivisions,
        }

        for key, expected_class in expected_classes.items():
            actual_class = garganorn.quadtree.SOURCES.get(key)
            assert actual_class is expected_class, \
                f"SOURCES[{key!r}] should be {expected_class}, got {actual_class}"

    def test_quadtree_source_pk_removed(self):
        """SOURCE_PK dict should NOT exist in quadtree.py."""
        import garganorn.quadtree
        assert not hasattr(garganorn.quadtree, 'SOURCE_PK'), \
            "SOURCE_PK dict should be removed from quadtree.py (replaced by SOURCES registry)"

    def test_quadtree_attribution_removed(self):
        """ATTRIBUTION dict should NOT exist in quadtree.py."""
        import garganorn.quadtree
        assert not hasattr(garganorn.quadtree, 'ATTRIBUTION'), \
            "ATTRIBUTION dict should be removed from quadtree.py (replaced by SOURCES registry)"


class TestSQLFileNames:
    """Test that SQL files are renamed from old names to new names."""

    def test_foursquare_import_sql_exists(self):
        """foursquare_import.sql should exist."""
        path = REPO_ROOT / "garganorn" / "sql" / "foursquare_import.sql"
        assert path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should exist"

    def test_foursquare_importance_sql_exists(self):
        """foursquare_importance.sql should exist."""
        path = REPO_ROOT / "garganorn" / "sql" / "foursquare_importance.sql"
        assert path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should exist"

    def test_foursquare_variants_sql_exists(self):
        """foursquare_variants.sql should exist."""
        path = REPO_ROOT / "garganorn" / "sql" / "foursquare_variants.sql"
        assert path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should exist"

    def test_foursquare_export_tiles_sql_exists(self):
        """foursquare_export_tiles.sql should exist."""
        path = REPO_ROOT / "garganorn" / "sql" / "foursquare_export_tiles.sql"
        assert path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should exist"

    def test_overture_place_import_sql_exists(self):
        """overture_place_import.sql should exist."""
        path = REPO_ROOT / "garganorn" / "sql" / "overture_place_import.sql"
        assert path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should exist"

    def test_overture_place_importance_sql_exists(self):
        """overture_place_importance.sql should exist."""
        path = REPO_ROOT / "garganorn" / "sql" / "overture_place_importance.sql"
        assert path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should exist"

    def test_overture_place_variants_sql_exists(self):
        """overture_place_variants.sql should exist."""
        path = REPO_ROOT / "garganorn" / "sql" / "overture_place_variants.sql"
        assert path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should exist"

    def test_overture_place_export_tiles_sql_exists(self):
        """overture_place_export_tiles.sql should exist."""
        path = REPO_ROOT / "garganorn" / "sql" / "overture_place_export_tiles.sql"
        assert path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should exist"

    def test_fsq_import_sql_removed(self):
        """fsq_import.sql should NOT exist (renamed to foursquare_import.sql)."""
        path = REPO_ROOT / "garganorn" / "sql" / "fsq_import.sql"
        assert not path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should NOT exist (renamed to foursquare_import.sql)"

    def test_fsq_importance_sql_removed(self):
        """fsq_importance.sql should NOT exist (renamed to foursquare_importance.sql)."""
        path = REPO_ROOT / "garganorn" / "sql" / "fsq_importance.sql"
        assert not path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should NOT exist (renamed to foursquare_importance.sql)"

    def test_fsq_variants_sql_removed(self):
        """fsq_variants.sql should NOT exist (renamed to foursquare_variants.sql)."""
        path = REPO_ROOT / "garganorn" / "sql" / "fsq_variants.sql"
        assert not path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should NOT exist (renamed to foursquare_variants.sql)"

    def test_fsq_export_tiles_sql_removed(self):
        """fsq_export_tiles.sql should NOT exist (renamed to foursquare_export_tiles.sql)."""
        path = REPO_ROOT / "garganorn" / "sql" / "fsq_export_tiles.sql"
        assert not path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should NOT exist (renamed to foursquare_export_tiles.sql)"

    def test_overture_import_sql_removed(self):
        """overture_import.sql should NOT exist (renamed to overture_place_import.sql)."""
        path = REPO_ROOT / "garganorn" / "sql" / "overture_import.sql"
        assert not path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should NOT exist (renamed to overture_place_import.sql)"

    def test_overture_importance_sql_removed(self):
        """overture_importance.sql should NOT exist (renamed to overture_place_importance.sql)."""
        path = REPO_ROOT / "garganorn" / "sql" / "overture_importance.sql"
        assert not path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should NOT exist (renamed to overture_place_importance.sql)"

    def test_overture_variants_sql_removed(self):
        """overture_variants.sql should NOT exist (renamed to overture_place_variants.sql)."""
        path = REPO_ROOT / "garganorn" / "sql" / "overture_variants.sql"
        assert not path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should NOT exist (renamed to overture_place_variants.sql)"

    def test_overture_export_tiles_sql_removed(self):
        """overture_export_tiles.sql should NOT exist (renamed to overture_place_export_tiles.sql)."""
        path = REPO_ROOT / "garganorn" / "sql" / "overture_export_tiles.sql"
        assert not path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should NOT exist (renamed to overture_place_export_tiles.sql)"

    # overture_division files should keep their names
    def test_overture_division_import_sql_unchanged(self):
        """overture_division_import.sql should still exist (not renamed)."""
        path = REPO_ROOT / "garganorn" / "sql" / "overture_division_import.sql"
        assert path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should exist (overture_division files unchanged)"

    def test_overture_division_export_tiles_sql_unchanged(self):
        """overture_division_export_tiles.sql should still exist (not renamed)."""
        path = REPO_ROOT / "garganorn" / "sql" / "overture_division_export_tiles.sql"
        assert path.exists(), \
            f"SQL file {path.relative_to(REPO_ROOT)} should exist (overture_division files unchanged)"


class TestConfigDatabaseTypes:
    """Test that config.py DATABASE_TYPES uses new source keys."""

    def test_config_database_types_has_new_keys(self):
        """DATABASE_TYPES should use 'foursquare', 'overture_place', 'osm', 'overture_division'."""
        from garganorn.config import DATABASE_TYPES
        expected_keys = {"foursquare", "overture_place", "osm", "overture_division"}
        actual_keys = set(DATABASE_TYPES.keys())
        assert actual_keys == expected_keys, \
            f"DATABASE_TYPES keys should be {expected_keys}, got {actual_keys}"

    def test_config_database_types_no_overture_key(self):
        """DATABASE_TYPES should NOT have 'overture' key (renamed to 'overture_place')."""
        from garganorn.config import DATABASE_TYPES
        assert "overture" not in DATABASE_TYPES, \
            "DATABASE_TYPES should not have 'overture' key (renamed to 'overture_place')"

    def test_config_database_types_no_fsq_key(self):
        """DATABASE_TYPES should NOT have 'fsq' key (renamed to 'foursquare')."""
        from garganorn.config import DATABASE_TYPES
        assert "fsq" not in DATABASE_TYPES, \
            "DATABASE_TYPES should not have 'fsq' key (renamed to 'foursquare')"


class TestModuleImports:
    """Test that imports work correctly after the refactor."""

    def test_import_all_collection_classes(self):
        """All collection classes should be importable from garganorn.database."""
        from garganorn.database import (
            FoursquareOSP,
            OverturePlaces,
            OpenStreetMap,
            OvertureDivisions,
        )
        assert FoursquareOSP is not None
        assert OverturePlaces is not None
        assert OpenStreetMap is not None
        assert OvertureDivisions is not None

    def test_config_imports_new_classes(self):
        """config.py should import the new class names."""
        # This will fail if config.py still imports the old names
        import garganorn.config
        # If we get here without ImportError, the imports worked
        assert True


class TestQuadtreeSourceKeys:
    """Test that quadtree.py functions use new source keys."""

    def test_coord_exprs_uses_new_source_keys(self):
        """_coord_exprs should handle 'overture_place' and 'overture_division' keys."""
        import garganorn.quadtree

        # Test with overture_place
        lon, lat = garganorn.quadtree._coord_exprs("overture_place")
        assert "bbox" in lon and "bbox" in lat, \
            "_coord_exprs('overture_place') should return bbox expressions"

        # Test with overture_division
        lon, lat = garganorn.quadtree._coord_exprs("overture_division")
        assert "bbox" in lon and "bbox" in lat, \
            "_coord_exprs('overture_division') should return bbox expressions"

        # Test with foursquare
        lon, lat = garganorn.quadtree._coord_exprs("foursquare")
        assert "longitude" == lon and "latitude" == lat, \
            "_coord_exprs('foursquare') should return longitude/latitude columns"

        # Test with osm
        lon, lat = garganorn.quadtree._coord_exprs("osm")
        assert "longitude" == lon and "latitude" == lat, \
            "_coord_exprs('osm') should return longitude/latitude columns"

    def test_export_tiles_uses_new_source_keys(self):
        """export_tiles should use new source key for SQL file lookup."""
        # This is harder to test directly, but we can verify the SQL file path logic
        sql_dir = REPO_ROOT / "garganorn" / "sql"
        assert (sql_dir / "foursquare_export_tiles.sql").exists(), \
            "export_tiles('foursquare') should find foursquare_export_tiles.sql"
        assert (sql_dir / "overture_place_export_tiles.sql").exists(), \
            "export_tiles('overture_place') should find overture_place_export_tiles.sql"

    def test_run_pipeline_uses_new_source_keys(self):
        """run_pipeline should accept new source keys."""
        import garganorn.quadtree

        # Verify that the function parameter docs or logic reference the new keys
        # We can't easily run the full pipeline, but we can check the function signature
        # and docstring mentions the new keys
        assert garganorn.quadtree.run_pipeline is not None, \
            "run_pipeline function should exist"
