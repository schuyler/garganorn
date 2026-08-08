"""Tests for the still-live `variants` field: schema presence and derivation
from Overture's names.common/names.rules (garganorn/sql/overture_place_import.sql).

OSM's import SQL always emits an empty variants array (no tag-based
derivation survives in the current pipeline), and FSQ is dropped entirely,
so those cases are covered only at the schema level.
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
        run_osm_import(conn, osm_parquet["node"], osm_parquet["way"])
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
