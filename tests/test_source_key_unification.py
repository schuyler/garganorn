"""Tests for the source-key/collection-class naming scheme in garganorn.database
and garganorn.quadtree.

Live source keys: overture_place, osm, overture_division. Foursquare was
dropped as a data source (not renamed) and has no class or SQL files.
"""
import importlib
import pytest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent


class TestCollectionClassAttributes:
    """Test that collection classes have source_key and source_pk attributes."""

    @pytest.mark.parametrize("cls_name,expected", [
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

    def test_overture_divisions_removed_from_boundaries(self):
        """OvertureDivisions should NOT be importable from garganorn.boundaries.

        garganorn.boundaries no longer exists as a module at all (OvertureDivisions
        lives in database.py), so this raises ModuleNotFoundError rather than an
        ImportError naming the class specifically; both are ImportError.
        """
        with pytest.raises(ImportError):
            from garganorn.boundaries import OvertureDivisions
            # If we get here, the import succeeded - fail the test
            pytest.fail("OvertureDivisions should not exist in boundaries.py (lives in database.py)")


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
        """SOURCES should be keyed by 'overture_place', 'osm', 'overture_division'."""
        import garganorn.quadtree
        expected_keys = {"overture_place", "osm", "overture_division"}
        actual_keys = set(garganorn.quadtree.SOURCES.keys())
        assert actual_keys == expected_keys, \
            f"SOURCES keys should be {expected_keys}, got {actual_keys}"

    def test_quadtree_sources_values_are_classes(self):
        """SOURCES values should be the collection classes."""
        import garganorn.quadtree
        from garganorn.database import OverturePlaces, OpenStreetMap, OvertureDivisions

        expected_classes = {
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
    """Test that SQL files use the current naming scheme."""

    EXPECTED_SQL_FILES = [
        "overture_place_import.sql", "overture_place_export_tiles.sql",
    ]

    REMOVED_SQL_FILES = [
        "fsq_import.sql", "fsq_export_tiles.sql",
        "foursquare_import.sql", "foursquare_export_tiles.sql",
        "overture_import.sql", "overture_export_tiles.sql",
    ]

    @pytest.mark.parametrize("filename", EXPECTED_SQL_FILES)
    def test_sql_file_exists(self, filename):
        """Current SQL files should exist."""
        path = REPO_ROOT / "garganorn" / "sql" / filename
        assert path.exists(), \
            f"SQL file {filename} should exist"

    @pytest.mark.parametrize("filename", REMOVED_SQL_FILES)
    def test_old_sql_file_removed(self, filename):
        """Old-name and dropped-source SQL files should NOT exist."""
        path = REPO_ROOT / "garganorn" / "sql" / filename
        assert not path.exists(), \
            f"SQL file {filename} should NOT exist"

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


class TestModuleImports:
    """Test that imports work correctly."""

    def test_import_all_collection_classes(self):
        """All live collection classes should be importable from garganorn.database."""
        from garganorn.database import (
            OverturePlaces,
            OpenStreetMap,
            OvertureDivisions,
        )
        assert OverturePlaces is not None
        assert OpenStreetMap is not None
        assert OvertureDivisions is not None

    def test_config_module_load_config_works(self, tmp_path):
        """garganorn.config.load_config should parse repo and tiles config."""
        import garganorn.config

        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text("repo: test.example.org\ntiles:\n  overture_place: {}\n")

        repo, tiles = garganorn.config.load_config(str(cfg_path))
        assert repo == "test.example.org"
        assert tiles == {"overture_place": {}}


class TestQuadtreeSourceKeys:
    """Test that quadtree.py functions use the current source keys."""

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

        # Test with osm
        lon, lat = garganorn.quadtree._coord_exprs("osm")
        assert "longitude" == lon and "latitude" == lat, \
            "_coord_exprs('osm') should return longitude/latitude columns"

    def test_export_tiles_uses_new_source_keys(self, division_parquet, tmp_path):
        """export_tiles resolves 'overture_division' to overture_division_export_tiles.sql.

        overture_place and osm are already exercised end-to-end elsewhere
        (test_pipeline.py); overture_division is not, so this runs the real
        pipeline for it and checks it actually finds and executes the right
        SQL file rather than just checking the file exists on disk.
        """
        from garganorn.quadtree import run_pipeline

        div_parquet, div_area_parquet = division_parquet
        output_dir = tmp_path / "division_export_out"
        output_dir.mkdir()

        run_pipeline(
            "overture_division",
            (div_parquet, div_area_parquet),
            (-122.55, 37.60, -122.30, 37.85),
            str(output_dir),
            memory_limit="4GB",
            max_per_tile=100,
        )

        current_dir = output_dir / "overture_division" / "tiles" / "current"
        gz_files = list(current_dir.rglob("*.json.gz")) if current_dir.exists() else []
        assert gz_files, (
            f"run_pipeline('overture_division', ...) must write at least one "
            f".json.gz under {current_dir}; export_tiles must be resolving "
            f"overture_division_export_tiles.sql correctly"
        )

    def test_run_pipeline_uses_new_source_keys(self):
        """run_pipeline dispatches via garganorn.quadtree.SOURCES, which must
        list exactly the current live source keys."""
        import garganorn.quadtree

        assert set(garganorn.quadtree.SOURCES.keys()) == {
            "overture_place", "osm", "overture_division",
        }, (
            f"SOURCES registry should list exactly the current source keys; "
            f"got {sorted(garganorn.quadtree.SOURCES.keys())}"
        )
