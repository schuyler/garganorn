"""Tests for garganorn.boundaries — BoundaryLookup and WhosOnFirst."""
import pytest

from garganorn.boundaries import BoundaryLookup


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


from garganorn.boundaries import WhosOnFirst


# ---------------------------------------------------------------------------
# WhosOnFirst (Database subclass) tests
# ---------------------------------------------------------------------------

class TestWhosOnFirstGetRecord:
    """WhosOnFirst.get_record() tests."""

    def test_returns_correct_record_structure(self, wof_db):
        """get_record returns lexicon-compliant dict with geo+bbox locations."""
        record = wof_db.get_record("places.atgeo.org", "org.atgeo.places.wof", "85922583")
        assert record is not None
        assert record["$type"] == "org.atgeo.place"
        assert record["collection"] == "org.atgeo.places.wof"
        assert record["rkey"] == "85922583"
        assert record["name"] == "San Francisco"

        # Should have both geo and bbox locations
        types = [loc["$type"] for loc in record["locations"]]
        assert "community.lexicon.location.geo" in types
        assert "community.lexicon.location.bbox" in types

    def test_bbox_location_values(self, wof_db):
        """Bbox location has correct north/south/east/west strings."""
        record = wof_db.get_record("places.atgeo.org", "org.atgeo.places.wof", "85922583")
        bbox = next(
            loc for loc in record["locations"]
            if loc["$type"] == "community.lexicon.location.bbox"
        )
        assert float(bbox["north"]) == pytest.approx(37.85, abs=0.01)
        assert float(bbox["south"]) == pytest.approx(37.6, abs=0.01)

    def test_variants_from_names_json(self, wof_db):
        """WoF names_json is parsed into variants array."""
        record = wof_db.get_record("places.atgeo.org", "org.atgeo.places.wof", "85922583")
        assert len(record["variants"]) >= 2
        names = [v["name"] for v in record["variants"]]
        assert "San Francisco" in names
        assert "\u65e7\u91d1\u5c71" in names

    def test_concordances_in_attributes(self, wof_db):
        """Concordances JSON is parsed into attributes dict."""
        record = wof_db.get_record("places.atgeo.org", "org.atgeo.places.wof", "85922583")
        attrs = record["attributes"]
        assert attrs["placetype"] == "locality"
        assert attrs["level"] == 50
        assert attrs["country"] == "US"
        assert attrs["concordances"]["wk:id"] == "Q62"
        assert attrs["concordances"]["gn:id"] == "5391959"

    def test_record_without_names_or_concordances(self, wof_db):
        """Record with NULL names_json/concordances has empty variants and no concordances key."""
        record = wof_db.get_record("places.atgeo.org", "org.atgeo.places.wof", "102191575")
        assert record is not None
        assert record["name"] == "North America"
        assert record["variants"] == []
        assert "concordances" not in record["attributes"]

    def test_not_found_returns_none(self, wof_db):
        """Missing rkey returns None."""
        record = wof_db.get_record("places.atgeo.org", "org.atgeo.places.wof", "999999999")
        assert record is None

    def test_importance_is_zero(self, wof_db):
        """WoF records have importance 0 (unranked)."""
        record = wof_db.get_record("places.atgeo.org", "org.atgeo.places.wof", "85922583")
        assert record["importance"] == 0

    def test_query_nearest_raises(self, wof_db):
        """WhosOnFirst does not support search."""
        with pytest.raises(NotImplementedError):
            wof_db.query_nearest({})
