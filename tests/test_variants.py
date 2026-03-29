"""Tests for name variants feature (TDD red phase).

All tests in this file MUST fail against the current code because:
- process_record() always returns "variants": []
- places tables have no variants column
- name_index tables have no is_variant column
- The variant type description in place.json does not include "historical"
"""
import json
from pathlib import Path

import pytest

from garganorn.database import FoursquareOSP, OvertureMaps, OpenStreetMap


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fsq():
    return FoursquareOSP(":memory:")


def _make_ovr():
    return OvertureMaps(":memory:")


def _make_osm():
    return OpenStreetMap(":memory:")


# ---------------------------------------------------------------------------
# A. process_record returns variants (unit tests, no DB)
# ---------------------------------------------------------------------------

class TestProcessRecordVariantsFSQ:
    """FoursquareOSP.process_record with variants in result dict."""

    def test_fsq_empty_variants_stay_empty(self):
        """FSQ process_record with variants=[] returns empty variants list."""
        db = _make_fsq()
        result = {
            "rkey": "fsq001",
            "name": "Blue Bottle Coffee",
            "latitude": "37.774900",
            "longitude": "-122.419400",
            "address": "66 Mint St",
            "locality": "San Francisco",
            "postcode": "94103",
            "region": "CA",
            "country": "US",
            "variants": [],
        }
        record = db.process_record(result)
        assert record["variants"] == []

    def test_fsq_variants_not_in_attributes(self):
        """variants key must be consumed from result and not appear in attributes."""
        db = _make_fsq()
        result = {
            "rkey": "fsq001",
            "name": "Blue Bottle Coffee",
            "latitude": "37.774900",
            "longitude": "-122.419400",
            "address": None,
            "locality": None,
            "postcode": None,
            "region": None,
            "country": None,
            "variants": [],
        }
        record = db.process_record(result)
        assert "variants" not in record.get("attributes", {})


class TestProcessRecordVariantsOSM:
    """OpenStreetMap.process_record with variants in result dict."""

    def test_osm_variants_populated(self):
        """process_record with a non-empty variants list returns populated variants."""
        db = _make_osm()
        result = {
            "rkey": "n240109189",
            "name": "Tartine Manufactory",
            "latitude": "37.761200",
            "longitude": "-122.419500",
            "primary_category": "amenity=cafe",
            "tags": {},
            "variants": [
                {"name": "Tartine Manufactory SF", "type": "alternate", "language": "en"},
                {"name": "Old Tartine", "type": "historical", "language": None},
                {"name": "Tartine MFY", "type": "short", "language": None},
                {"name": "Tartine Alt", "type": "alternate", "language": None},
            ],
        }
        record = db.process_record(result)
        assert len(record["variants"]) == 4

    def test_osm_variant_name_field(self):
        """Each variant entry has a 'name' field."""
        db = _make_osm()
        result = {
            "rkey": "n240109189",
            "name": "Tartine Manufactory",
            "latitude": "37.761200",
            "longitude": "-122.419500",
            "primary_category": None,
            "tags": {},
            "variants": [
                {"name": "Tartine Manufactory SF", "type": "alternate", "language": "en"},
            ],
        }
        record = db.process_record(result)
        assert record["variants"][0]["name"] == "Tartine Manufactory SF"

    def test_osm_variant_type_field(self):
        """Variant entry with type produces a 'type' field in output."""
        db = _make_osm()
        result = {
            "rkey": "n240109189",
            "name": "Tartine Manufactory",
            "latitude": "37.761200",
            "longitude": "-122.419500",
            "primary_category": None,
            "tags": {},
            "variants": [
                {"name": "Old Tartine", "type": "historical", "language": None},
            ],
        }
        record = db.process_record(result)
        assert record["variants"][0]["type"] == "historical"

    def test_osm_variant_language_field(self):
        """Variant with language produces a 'language' field in output."""
        db = _make_osm()
        result = {
            "rkey": "n240109189",
            "name": "Tartine Manufactory",
            "latitude": "37.761200",
            "longitude": "-122.419500",
            "primary_category": None,
            "tags": {},
            "variants": [
                {"name": "Tartine Manufactory SF", "type": "alternate", "language": "en"},
            ],
        }
        record = db.process_record(result)
        assert record["variants"][0]["language"] == "en"

    def test_osm_variant_null_language_omitted(self):
        """Variant with null language should not produce a 'language' key."""
        db = _make_osm()
        result = {
            "rkey": "n240109189",
            "name": "Tartine Manufactory",
            "latitude": "37.761200",
            "longitude": "-122.419500",
            "primary_category": None,
            "tags": {},
            "variants": [
                {"name": "Old Tartine", "type": "historical", "language": None},
            ],
        }
        record = db.process_record(result)
        assert "language" not in record["variants"][0]

    def test_osm_variants_not_in_attributes(self):
        """variants must be consumed from result dict before attributes is set.

        The OSM process_record builds attributes from tag_dict, not result, so
        variants won't appear in attributes. But we also verify that the variants
        list in the output is actually populated (not empty), which is the real
        failure against current code.
        """
        db = _make_osm()
        result = {
            "rkey": "n240109189",
            "name": "Tartine Manufactory",
            "latitude": "37.761200",
            "longitude": "-122.419500",
            "primary_category": "amenity=cafe",
            "tags": {},
            "variants": [
                {"name": "Tartine Alt", "type": "alternate", "language": None},
            ],
        }
        record = db.process_record(result)
        # Variants must be in the top-level variants field, not in attributes
        assert "variants" not in record.get("attributes", {})
        # And must actually be populated (not silently dropped)
        assert len(record["variants"]) == 1


class TestProcessRecordVariantsOverture:
    """OvertureMaps.process_record with variants in result dict."""

    def test_ovr_variants_populated(self):
        """process_record with variants list returns populated variants."""
        db = _make_ovr()
        result = {
            "rkey": "ovr003",
            "name": "Coit Tower",
            "latitude": "37.802400",
            "longitude": "-122.405800",
            "addresses": None,
            "variants": [
                {"name": "Tour de Coit", "type": "alternate", "language": "fr"},
            ],
        }
        record = db.process_record(result)
        assert len(record["variants"]) == 1

    def test_ovr_variant_fields(self):
        """Variant entry has name, type, language fields."""
        db = _make_ovr()
        result = {
            "rkey": "ovr003",
            "name": "Coit Tower",
            "latitude": "37.802400",
            "longitude": "-122.405800",
            "addresses": None,
            "variants": [
                {"name": "Tour de Coit", "type": "alternate", "language": "fr"},
            ],
        }
        record = db.process_record(result)
        v = record["variants"][0]
        assert v["name"] == "Tour de Coit"
        assert v["type"] == "alternate"
        assert v["language"] == "fr"

    def test_ovr_variants_not_in_attributes(self):
        """variants must not appear in attributes."""
        db = _make_ovr()
        result = {
            "rkey": "ovr003",
            "name": "Coit Tower",
            "latitude": "37.802400",
            "longitude": "-122.405800",
            "addresses": None,
            "variants": [
                {"name": "Tour de Coit", "type": "alternate", "language": "fr"},
            ],
        }
        record = db.process_record(result)
        assert "variants" not in record.get("attributes", {})


# ---------------------------------------------------------------------------
# B. Search finds variant names (integration tests, using DB fixtures)
# ---------------------------------------------------------------------------

class TestSearchFindsVariantNames:
    """Search via name_index variant rows finds places by variant name."""

    def test_osm_search_finds_english_variant(self, osm_db):
        """Search for 'Tartine Manufactory SF' (name:en variant) finds the place."""
        results = osm_db.nearest(q="Tartine Manufactory SF")
        rkeys = [r["rkey"] for r in results]
        # The variant is indexed so the search should return Tartine Manufactory
        assert any("240109189" in rk for rk in rkeys), (
            f"Expected node:240109189 in results, got: {rkeys}"
        )

    def test_osm_search_variant_returns_primary_name(self, osm_db):
        """When matched via variant, result still shows the primary name."""
        results = osm_db.nearest(q="Tartine Manufactory SF")
        for r in results:
            if "240109189" in r["rkey"]:
                assert r["name"] == "Tartine Manufactory"
                break
        else:
            pytest.fail("node:240109189 not found in results")

    def test_osm_search_historical_variant(self, osm_db):
        """Search for 'Old Tartine' (old_name/historical variant) finds the place."""
        results = osm_db.nearest(q="Old Tartine")
        rkeys = [r["rkey"] for r in results]
        assert any("240109189" in rk for rk in rkeys), (
            f"Expected node:240109189 in results via historical variant, got: {rkeys}"
        )

    def test_overture_search_finds_language_variant(self, overture_db):
        """Search for 'Tour de Coit' (French variant) finds Coit Tower (ovr003)."""
        results = overture_db.nearest(q="Tour de Coit")
        rkeys = [r["rkey"] for r in results]
        assert "ovr003" in rkeys, (
            f"Expected ovr003 in results via French variant, got: {rkeys}"
        )

    def test_overture_search_variant_returns_primary_name(self, overture_db):
        """When matched via French variant, result still shows primary name 'Coit Tower'."""
        results = overture_db.nearest(q="Tour de Coit")
        for r in results:
            if r["rkey"] == "ovr003":
                assert r["name"] == "Coit Tower"
                break
        else:
            pytest.fail("ovr003 not found in results")


# ---------------------------------------------------------------------------
# C. Schema: places table has variants column
# ---------------------------------------------------------------------------

class TestPlacesTableVariantsColumn:
    """The places table must have a variants column in all three databases."""

    def test_osm_places_has_variants_column(self, osm_db):
        """OSM places table exposes a variants column."""
        rows = osm_db.conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'places' AND column_name = 'variants'"
        ).fetchall()
        assert len(rows) == 1, "OSM places table missing 'variants' column"

    def test_overture_places_has_variants_column(self, overture_db):
        """Overture places table exposes a variants column."""
        rows = overture_db.conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'places' AND column_name = 'variants'"
        ).fetchall()
        assert len(rows) == 1, "Overture places table missing 'variants' column"

    def test_fsq_places_has_variants_column(self, fsq_db):
        """FSQ places table exposes a variants column."""
        rows = fsq_db.conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'places' AND column_name = 'variants'"
        ).fetchall()
        assert len(rows) == 1, "FSQ places table missing 'variants' column"

    def test_fsq_variants_are_empty_list(self, fsq_db):
        """FSQ places have empty variants lists (no source data)."""
        rows = fsq_db.conn.execute(
            "SELECT variants FROM places WHERE fsq_place_id = 'fsq001'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == [], f"Expected [], got {rows[0][0]}"

    def test_osm_place_has_variant_data(self, osm_db):
        """Tartine Manufactory (n240109189) has non-empty variants list."""
        rows = osm_db.conn.execute(
            "SELECT variants FROM places WHERE rkey = 'n240109189'"
        ).fetchall()
        assert len(rows) == 1
        variants = rows[0][0]
        assert variants is not None and len(variants) > 0, (
            f"Expected non-empty variants for n240109189, got: {variants}"
        )

    def test_overture_place_has_variant_data(self, overture_db):
        """Coit Tower (ovr003) has non-empty variants list."""
        rows = overture_db.conn.execute(
            "SELECT variants FROM places WHERE id = 'ovr003'"
        ).fetchall()
        assert len(rows) == 1
        variants = rows[0][0]
        assert variants is not None and len(variants) > 0, (
            f"Expected non-empty variants for ovr003, got: {variants}"
        )


# ---------------------------------------------------------------------------
# D. name_index has is_variant column
# ---------------------------------------------------------------------------

class TestNameIndexIsVariantColumn:
    """name_index must have an is_variant column in all three databases."""

    def test_osm_name_index_has_is_variant(self, osm_db):
        rows = osm_db.conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'name_index' AND column_name = 'is_variant'"
        ).fetchall()
        assert len(rows) == 1, "OSM name_index missing 'is_variant' column"

    def test_overture_name_index_has_is_variant(self, overture_db):
        rows = overture_db.conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'name_index' AND column_name = 'is_variant'"
        ).fetchall()
        assert len(rows) == 1, "Overture name_index missing 'is_variant' column"

    def test_fsq_name_index_has_is_variant(self, fsq_db):
        rows = fsq_db.conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'name_index' AND column_name = 'is_variant'"
        ).fetchall()
        assert len(rows) == 1, "FSQ name_index missing 'is_variant' column"

    def test_osm_primary_entries_have_is_variant_false(self, osm_db):
        """Primary name entries in OSM name_index have is_variant = FALSE."""
        rows = osm_db.conn.execute(
            "SELECT is_variant FROM name_index WHERE rkey = 'n240109189' AND name = 'Tartine Manufactory' LIMIT 1"
        ).fetchall()
        assert len(rows) > 0, "No primary name_index entries for n240109189"
        assert rows[0][0] is False, f"Expected is_variant=FALSE for primary name, got {rows[0][0]}"

    def test_osm_variant_entries_have_is_variant_true(self, osm_db):
        """Variant name entries in OSM name_index have is_variant = TRUE."""
        rows = osm_db.conn.execute(
            "SELECT DISTINCT is_variant FROM name_index "
            "WHERE rkey = 'n240109189' AND name = 'Old Tartine'"
        ).fetchall()
        assert len(rows) > 0, "No variant name_index entries for 'Old Tartine'"
        assert rows[0][0] is True, f"Expected is_variant=TRUE for variant name, got {rows[0][0]}"

    def test_overture_variant_entries_have_is_variant_true(self, overture_db):
        """Variant name entries in Overture name_index have is_variant = TRUE."""
        rows = overture_db.conn.execute(
            "SELECT DISTINCT is_variant FROM name_index "
            "WHERE id = 'ovr003' AND name = 'Tour de Coit'"
        ).fetchall()
        assert len(rows) > 0, "No variant name_index entries for 'Tour de Coit'"
        assert rows[0][0] is True, f"Expected is_variant=TRUE for Tour de Coit, got {rows[0][0]}"


# ---------------------------------------------------------------------------
# E. Lexicon includes "historical" in variant type description
# ---------------------------------------------------------------------------

class TestLexiconVariantType:
    """place.json variant type description must include 'historical'."""

    def _load_lexicon(self):
        lexicon_path = Path(__file__).parent.parent / "garganorn" / "lexicon" / "place.json"
        with open(lexicon_path) as f:
            return json.load(f)

    def test_variant_type_description_includes_historical(self):
        """The variant.type.description in place.json includes the word 'historical'."""
        lexicon = self._load_lexicon()
        variant_def = lexicon["defs"]["variant"]
        type_description = variant_def["properties"]["type"]["description"]
        assert "historical" in type_description, (
            f"Expected 'historical' in variant type description, got: {type_description!r}"
        )

    def test_variant_type_description_includes_official(self):
        """The variant.type.description retains 'official'."""
        lexicon = self._load_lexicon()
        variant_def = lexicon["defs"]["variant"]
        type_description = variant_def["properties"]["type"]["description"]
        assert "official" in type_description

    def test_variant_type_description_includes_alternate(self):
        """The variant.type.description retains 'alternate'."""
        lexicon = self._load_lexicon()
        variant_def = lexicon["defs"]["variant"]
        type_description = variant_def["properties"]["type"]["description"]
        assert "alternate" in type_description


# ---------------------------------------------------------------------------
# F. get_record returns variants from DB (integration, end-to-end)
# ---------------------------------------------------------------------------

class TestGetRecordVariants:
    """get_record must return populated variants when the DB has variant data."""

    def test_osm_get_record_returns_variants(self, osm_db):
        """get_record for Tartine Manufactory returns non-empty variants."""
        record = osm_db.get_record("", "org.atgeo.places.osm", "node:240109189")
        assert record is not None
        assert "variants" in record
        assert len(record["variants"]) > 0, (
            f"Expected non-empty variants, got: {record['variants']}"
        )

    def test_osm_get_record_variant_has_name(self, osm_db):
        """Each variant in get_record result has a 'name' field.

        Requires non-empty variants (enforced by preceding test), then checks
        structure. This test will pass trivially if variants is [] — pair it
        with test_osm_get_record_returns_variants to catch that case.
        """
        record = osm_db.get_record("", "org.atgeo.places.osm", "node:240109189")
        assert len(record["variants"]) > 0, "variants must be non-empty for this test to be meaningful"
        for v in record["variants"]:
            assert "name" in v, f"Variant missing 'name': {v}"

    def test_osm_get_record_historical_variant(self, osm_db):
        """Tartine Manufactory get_record result includes the historical variant."""
        record = osm_db.get_record("", "org.atgeo.places.osm", "node:240109189")
        names = [v["name"] for v in record["variants"]]
        assert "Old Tartine" in names, f"Expected 'Old Tartine' in variants, got: {names}"

    def test_overture_get_record_returns_variants(self, overture_db):
        """get_record for Coit Tower returns non-empty variants."""
        record = overture_db.get_record("", "org.atgeo.places.overture", "ovr003")
        assert record is not None
        assert "variants" in record
        assert len(record["variants"]) > 0, (
            f"Expected non-empty variants for ovr003, got: {record['variants']}"
        )

    def test_fsq_get_record_returns_empty_variants(self, fsq_db):
        """FSQ get_record always returns empty variants."""
        record = fsq_db.get_record("", "org.atgeo.places.foursquare", "fsq001")
        assert record is not None
        assert record["variants"] == []
