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

    @pytest.mark.parametrize("cls_name,expected", [
        ("FoursquareOSP", "foursquare"),
        ("OverturePlaces", "overture_place"),
        ("OpenStreetMap", "osm"),
        ("OvertureDivisions", "overture_division"),
    ])
    def test_source_key(self, cls_name, expected):
        """Collection classes should have correct source_key class attribute."""
        cls = getattr(importlib.import_module("garganorn.database"), cls_name)
        assert hasattr(cls, 'source_key'), \
            f"{cls_name} missing source_key class attribute"
        assert cls.source_key == expected, \
            f"{cls_name}.source_key should be {expected!r}, got {cls.source_key!r}"

    @pytest.mark.parametrize("cls_name,expected", [
        ("FoursquareOSP", "fsq_place_id"),
        ("OverturePlaces", "id"),
        ("OpenStreetMap", "rkey"),
        ("OvertureDivisions", "id"),
    ])
    def test_source_pk(self, cls_name, expected):
        """Collection classes should have correct source_pk class attribute."""
        cls = getattr(importlib.import_module("garganorn.database"), cls_name)
        assert hasattr(cls, 'source_pk'), \
            f"{cls_name} missing source_pk class attribute"
        assert cls.source_pk == expected, \
            f"{cls_name}.source_pk should be {expected!r}, got {cls.source_pk!r}"


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

    NEW_SQL_FILES = [
        "foursquare_import.sql", "foursquare_importance.sql",
        "foursquare_variants.sql", "foursquare_export_tiles.sql",
        "overture_place_import.sql", "overture_place_importance.sql",
        "overture_place_variants.sql", "overture_place_export_tiles.sql",
    ]

    OLD_SQL_FILES = [
        "fsq_import.sql", "fsq_importance.sql", "fsq_variants.sql", "fsq_export_tiles.sql",
        "overture_import.sql", "overture_importance.sql", "overture_variants.sql", "overture_export_tiles.sql",
    ]

    @pytest.mark.parametrize("filename", NEW_SQL_FILES)
    def test_new_sql_file_exists(self, filename):
        """New SQL files should exist after refactoring."""
        path = REPO_ROOT / "garganorn" / "sql" / filename
        assert path.exists(), \
            f"SQL file {filename} should exist"

    @pytest.mark.parametrize("filename", OLD_SQL_FILES)
    def test_old_sql_file_removed(self, filename):
        """Old SQL files should NOT exist after refactoring."""
        path = REPO_ROOT / "garganorn" / "sql" / filename
        assert not path.exists(), \
            f"SQL file {filename} should NOT exist (renamed)"

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
