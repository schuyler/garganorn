"""Tests for the `variants` field: schema presence and derivation from
Overture's names.common/names.rules (garganorn/sql/overture_place_import.sql)
and from OSM name tags (garganorn/sql/osm_import.sql).

FSQ is dropped entirely, so that case is covered only at the schema level.
"""
import json
from pathlib import Path

import duckdb
import pytest

from tests.quadtree_helpers import run_osm_import, run_overture_import


# ---------------------------------------------------------------------------
# C. Schema: places table has variants column
# ---------------------------------------------------------------------------

class TestPlacesTableVariantsColumn:
    """The places table must have a variants column for both live sources."""

    def test_osm_places_has_variants_column(self, osm_parquet, tmp_path):
        """OSM places table exposes a variants column."""
        conn = duckdb.connect(str(tmp_path / "osm_variants_col.duckdb"))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'places' AND column_name = 'variants'"
        ).fetchall()
        conn.close()
        assert len(rows) == 1, "OSM places table missing 'variants' column"

    def test_overture_places_has_variants_column(self, overture_parquet, tmp_path):
        """Overture places table exposes a variants column."""
        conn = duckdb.connect(str(tmp_path / "overture_variants_col.duckdb"))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'places' AND column_name = 'variants'"
        ).fetchall()
        conn.close()
        assert len(rows) == 1, "Overture places table missing 'variants' column"

    def test_overture_place_has_variant_data(self, overture_parquet, tmp_path):
        """ov010 (names.rules has one 'official' entry) has non-empty variants.

        See tests/test_regressions.py::TestOvertureVariantsCharacterization for
        exhaustive coverage of the names.common/names.rules -> variants mapping;
        this just pins that the column is populated, not empty.
        """
        conn = duckdb.connect(str(tmp_path / "overture_variants_data.duckdb"))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_overture_import(conn, overture_parquet)
        rows = conn.execute(
            "SELECT variants FROM places WHERE id = 'ov010'"
        ).fetchall()
        conn.close()
        assert len(rows) == 1
        variants = rows[0][0]
        assert variants is not None and len(variants) > 0, (
            f"Expected non-empty variants for ov010, got: {variants}"
        )


# ---------------------------------------------------------------------------
# D. OSM variant derivation: tag -> type/language mapping, drop-list,
#    semicolon split, dedup vs. primary name
# ---------------------------------------------------------------------------

class TestOsmVariantsCharacterization:
    """OSM tag -> variants derivation, per the mapping table, drop-list,
    semicolon split, and dedup rule implemented by osm_import.sql's
    osm_variant_type_lang/osm_dropped_suffix macros.

    Fixture records (tests/conftest.py's osm_parquet): n1005/n1007 cover the
    seven mapped base types; n1008-n1011 and w2002 cover the drop-list,
    semicolon split, dedup-vs-primary, the three name:*-only overrides, and
    the suffix extension to the five other name-family tags.
    """

    def _variants_for(self, conn, rkey):
        row = conn.execute(
            "SELECT variants FROM places WHERE rkey = ?", [rkey]
        ).fetchone()
        assert row is not None, f"rkey {rkey!r} not found in places"
        return row[0]

    def _run(self, osm_parquet, tmp_path, name):
        conn = duckdb.connect(str(tmp_path / f"{name}.duckdb"))
        conn.execute("INSTALL spatial; LOAD spatial;")
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"], osm_parquet["relation"])
        return conn

    def test_seven_base_types(self, osm_parquet, tmp_path):
        """n1005 (alt_name, name:fr) and n1007 (old_name/official_name/
        short_name/loc_name/int_name) together cover all seven mapped types."""
        conn = self._run(osm_parquet, tmp_path, "osm_variants_base_types")
        v1005 = self._variants_for(conn, "n1005")
        v1007 = self._variants_for(conn, "n1007")
        conn.close()
        assert v1005 == [
            {"name": "Café Alt", "type": "alternate", "language": "fr"},
            {"name": "The Old Spot", "type": "alternate", "language": None},
        ], v1005
        assert v1007 == [
            {"name": "Former Name", "type": "historical", "language": None},
            {"name": "International Name", "type": "alternate", "language": None},
            {"name": "Local Spot", "type": "colloquial", "language": None},
            {"name": "MVP", "type": "short", "language": None},
            {"name": "Official Title", "type": "official", "language": None},
        ], v1007

    def test_drop_list_suffix_both_forms(self, osm_parquet, tmp_path):
        """n1008: name:prefix (exact) and name:prefix:ru (begins 'prefix:')
        are both dropped; alt_name is not drop-listed and survives -- pins
        that the drop-list excludes specific tags rather than everything
        being empty because nothing is implemented."""
        conn = self._run(osm_parquet, tmp_path, "osm_variants_drop_list")
        variants = self._variants_for(conn, "n1008")
        conn.close()
        assert variants == [
            {"name": "Kept Name", "type": "alternate", "language": None},
        ], variants

    def test_semicolon_split(self, osm_parquet, tmp_path):
        """n1009: alt_name='Foo;Bar' splits into two alternate variants."""
        conn = self._run(osm_parquet, tmp_path, "osm_variants_semicolon")
        variants = self._variants_for(conn, "n1009")
        conn.close()
        assert variants == [
            {"name": "Bar", "type": "alternate", "language": None},
            {"name": "Foo", "type": "alternate", "language": None},
        ], variants

    def test_semicolon_split_and_dedup_combined(self, osm_parquet, tmp_path):
        """n1010: name:zh='Duplicate Case;真名' -- the piece byte-equal to
        the primary name is dropped, the other survives as alternate/zh."""
        conn = self._run(osm_parquet, tmp_path, "osm_variants_dedup")
        variants = self._variants_for(conn, "n1010")
        conn.close()
        assert variants == [
            {"name": "真名", "type": "alternate", "language": "zh"},
        ], variants

    def test_name_star_overrides_and_extended_suffix(self, osm_parquet, tmp_path):
        """n1011: old_name:en (extended-family suffix -> historical/en),
        name:abbr -> short, name:-2024 -> historical, name:carnaval ->
        colloquial."""
        conn = self._run(osm_parquet, tmp_path, "osm_variants_overrides")
        variants = self._variants_for(conn, "n1011")
        conn.close()
        assert variants == [
            {"name": "Carnival Name", "type": "colloquial", "language": None},
            {"name": "EAOP", "type": "short", "language": None},
            {"name": "Historic English Name", "type": "historical", "language": "en"},
            {"name": "Renamed 2024", "type": "historical", "language": None},
        ], variants

    def test_way_variants_derived(self, osm_parquet, tmp_path):
        """w2002: alt_name and old_name:en both survive on the way path,
        proving variant derivation isn't node-only."""
        conn = self._run(osm_parquet, tmp_path, "osm_variants_way")
        variants = self._variants_for(conn, "w2002")
        conn.close()
        assert variants == [
            {"name": "Historic Bridge Name", "type": "historical", "language": "en"},
            {"name": "Old Bridge Name", "type": "alternate", "language": None},
        ], variants


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
