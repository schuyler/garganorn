"""Tests for the osmium pre-filter stage of the OSM import pipeline.

Covers scripts/extract-osm-parquet.sh, which osmium-filters a PBF before
osm-pbf-parquet converts it to Hive-partitioned Parquet.
"""

import os

import pytest


class TestOsmiumPreFilter:
    """Verify that extract-osm-parquet.sh includes the osmium tags-filter pre-filtering stage.

    Covers stages 0-1 of the OSM import (osmium filter + osm-pbf-parquet
    conversion).
    """

    @pytest.fixture(scope="class")
    def script_content(self):
        script_path = os.path.join(
            os.path.dirname(__file__), "..", "scripts", "extract-osm-parquet.sh"
        )
        with open(script_path) as f:
            return f.read()

    def test_script_checks_osmium_dependency(self, script_content):
        assert "command -v osmium" in script_content, (
            "extract-osm-parquet.sh should check for osmium with 'command -v osmium'"
        )

    def test_osmium_tags_filter_command_present(self, script_content):
        assert "osmium tags-filter" in script_content, (
            "extract-osm-parquet.sh should invoke 'osmium tags-filter'"
        )

    def test_all_category_tags_in_osmium_filter(self, script_content):
        """Every SQL category key must appear as both n/<key> and w/<key> in the osmium filter."""
        category_keys = [
            "amenity", "shop", "tourism", "leisure", "office", "craft",
            "healthcare", "historic", "natural", "man_made", "aeroway",
            "railway", "public_transport", "place",
            "landuse", "waterway", "power", "boundary", "highway",
            "barrier", "emergency", "telecom",
        ]
        for key in category_keys:
            assert f"n/{key}" in script_content, (
                f"osmium filter missing node prefix for tag key: n/{key}"
            )
            assert f"w/{key}" in script_content, (
                f"osmium filter missing way prefix for tag key: w/{key}"
            )

        # building has no node arm: it arrives through the merge chain's
        # w/building pass for ways and relation_selectors' r/building entry
        # for relations, so a node arm would be machinery nothing reaches.
        assert "w/building" in script_content, (
            "osmium filter missing way prefix for tag key: w/building"
        )
        assert "n/building" not in script_content, (
            "building must not appear as n/building — there is no node arm for it"
        )

    def test_filtered_pbf_cache_variable(self, script_content):
        assert "filtered.osm.pbf" in script_content, (
            "extract-osm-parquet.sh should reference a cached filtered PBF file"
        )

    def test_osm_pbf_parquet_uses_filtered_input(self, script_content):
        """osm-pbf-parquet should use the filtered PBF, not the original."""
        assert 'osm-pbf-parquet --input "$pbf_path"' not in script_content, (
            "osm-pbf-parquet should use $filtered_pbf (filtered), not $pbf_path (original)"
        )
        assert 'osm-pbf-parquet --input "$filtered_pbf"' in script_content, (
            "osm-pbf-parquet must reference $filtered_pbf variable"
        )

    def test_parquet_cache_sentinel(self, script_content):
        assert "/.complete" in script_content, (
            "extract-osm-parquet.sh should use a .complete sentinel for parquet cache validation"
        )
