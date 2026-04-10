"""Tests for garganorn.boundaries — BoundaryLookup and OvertureDivision."""
import pytest

from garganorn.boundaries import BoundaryLookup, OvertureDivision


# ---------------------------------------------------------------------------
# BoundaryLookup tests
# ---------------------------------------------------------------------------

class TestBoundaryLookupContainment:
    """BoundaryLookup.containment() point-in-polygon tests."""

    def test_returns_all_containing_boundaries(self, boundary_lookup):
        """Point in SF returns continent, country, region, and locality."""
        result = boundary_lookup.containment(37.7749, -122.4194)
        rkeys = [r["rkey"] for r in result]
        assert any("div_continent_na" in rk for rk in rkeys)
        assert any("div_country_us" in rk for rk in rkeys)
        assert any("div_region_ca" in rk for rk in rkeys)
        assert any("div_locality_sf" in rk for rk in rkeys)
        assert not any("div_borough_manhattan" in rk for rk in rkeys)

    def test_ordered_by_admin_level_ascending(self, boundary_lookup):
        """Results are ordered continent-first, most-specific-last.

        Verified by checking rkey sequence matches known admin_level order
        from the division test data.
        """
        result = boundary_lookup.containment(37.7749, -122.4194)
        rkeys = [r["rkey"] for r in result]
        # Division test data admin_level order: continent(0), country(1), region(2), locality(3)
        expected_order = ["div_continent_na", "div_country_us", "div_region_ca", "div_locality_sf"]
        actual_ids = [rk.split(":")[-1] for rk in rkeys]
        assert actual_ids == expected_order

    def test_rkeys_are_collection_qualified(self, boundary_lookup):
        """Each rkey is prefixed with org.atgeo.places.overture.division:"""
        result = boundary_lookup.containment(37.7749, -122.4194)
        for entry in result:
            assert entry["rkey"].startswith("org.atgeo.places.overture.division:")

    def test_containment_returns_rkey_only(self, boundary_lookup):
        """containment() dicts must have 'rkey' only -- no 'name', no 'level'."""
        result = boundary_lookup.containment(37.7749, -122.4194)
        for entry in result:
            assert "rkey" in entry
            assert "name" not in entry, f"'name' key must not appear: {entry}"
            assert "level" not in entry, f"'level' key must not appear: {entry}"

    def test_empty_for_point_outside_all_boundaries(self, boundary_lookup):
        """Point in the middle of the ocean returns empty list."""
        result = boundary_lookup.containment(0.0, 0.0)
        assert result == []

    def test_partial_containment(self, boundary_lookup):
        """Point in Manhattan returns continent+country+borough but not CA/SF."""
        result = boundary_lookup.containment(40.7831, -73.9712)
        rkeys = [r["rkey"] for r in result]
        assert any("div_continent_na" in rk for rk in rkeys)
        assert any("div_country_us" in rk for rk in rkeys)
        assert any("div_borough_manhattan" in rk for rk in rkeys)
        assert not any("div_region_ca" in rk for rk in rkeys)
        assert not any("div_locality_sf" in rk for rk in rkeys)



# ---------------------------------------------------------------------------
# OvertureDivision (Database subclass) tests
# ---------------------------------------------------------------------------

class TestOvertureDivisionGetRecord:
    """OvertureDivision.get_record() tests."""

    def test_returns_correct_record_structure(self, division_db):
        """get_record returns a lexicon-compliant dict with expected top-level keys."""
        record = division_db.get_record("places.atgeo.org", "org.atgeo.places.overture.division", "div_locality_sf")
        assert record is not None
        assert record["$type"] == "org.atgeo.place"
        assert record["collection"] == "org.atgeo.places.overture.division"
        assert record["rkey"] == "div_locality_sf"
        assert "locations" in record
        assert "name" in record
        assert "variants" in record
        assert "attributes" in record

    def test_bbox_location_values(self, division_db):
        """Bbox location has float north/south/east/west values matching test data extent."""
        record = division_db.get_record("places.atgeo.org", "org.atgeo.places.overture.division", "div_locality_sf")
        assert record is not None
        bbox = next(
            loc for loc in record["locations"]
            if loc["$type"] == "community.lexicon.location.bbox"
        )
        assert float(bbox["north"]) == pytest.approx(37.85, abs=0.01)
        assert float(bbox["south"]) == pytest.approx(37.6, abs=0.01)
        assert float(bbox["east"]) == pytest.approx(-122.3, abs=0.01)
        assert float(bbox["west"]) == pytest.approx(-122.55, abs=0.01)

    def test_no_geo_location(self, division_db):
        """Divisions have bbox-only locations — no geo point location."""
        record = division_db.get_record("places.atgeo.org", "org.atgeo.places.overture.division", "div_locality_sf")
        assert record is not None
        geo_locs = [
            loc for loc in record["locations"]
            if loc["$type"] == "community.lexicon.location.geo"
        ]
        assert geo_locs == []

    def test_variants_from_names_struct(self, division_db):
        """Names struct common map is parsed into a non-empty variants list with specific known entries."""
        record = division_db.get_record("places.atgeo.org", "org.atgeo.places.overture.division", "div_locality_sf")
        assert record is not None
        assert isinstance(record["variants"], list)
        assert len(record["variants"]) >= 1
        variant_names = [v["name"] for v in record["variants"]]
        # div_locality_sf has common: {"es": "San Francisco", "zh": "\u65e7\u91d1\u5c71"}
        # The primary name is also "San Francisco", so the es entry may be deduplicated;
        # the Chinese name must always appear as a variant.
        assert "\u65e7\u91d1\u5c71" in variant_names

    def test_attributes(self, division_db):
        """Attributes dict contains expected keys with correct values for div_locality_sf."""
        record = division_db.get_record("places.atgeo.org", "org.atgeo.places.overture.division", "div_locality_sf")
        assert record is not None
        attrs = record["attributes"]
        assert attrs["admin_level"] == 3
        assert attrs["country"] == "US"
        assert attrs["subtype"] == "locality"
        assert "region" in attrs
        assert "wikidata" in attrs
        assert "population" in attrs

    def test_null_names_returns_empty_name_and_variants(self, division_db):
        """Record with names=NULL yields name='' and variants=[]."""
        record = division_db.get_record("places.atgeo.org", "org.atgeo.places.overture.division", "div_continent_na")
        assert record is not None
        assert record["name"] == ""
        assert record["variants"] == []

    def test_not_found_returns_none(self, division_db):
        """Missing rkey returns None."""
        record = division_db.get_record("places.atgeo.org", "org.atgeo.places.overture.division", "nonexistent-id")
        assert record is None

    def test_importance_is_zero(self, division_db):
        """get_record() pops importance then re-adds it if not None; 0 is not None so it appears."""
        record = division_db.get_record("places.atgeo.org", "org.atgeo.places.overture.division", "div_locality_sf")
        assert record is not None
        assert record.get("importance") == 0

    def test_query_nearest_raises(self, division_db):
        """OvertureDivision does not support search."""
        with pytest.raises(NotImplementedError):
            division_db.query_nearest({})
