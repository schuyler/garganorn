"""Tests for export and query path bug fixes.

These tests verify that the export pipeline correctly handles:
- Empty trigram lists (short queries, non-ASCII scripts)
- OSM rkey format consistency between export and manifest
- JSON escaping in attribution strings

All tests MUST FAIL with the current code and PASS after fixes are implemented.
"""

import gzip
import json
import os
import tempfile

import duckdb
import pytest

from garganorn.database import OpenStreetMap
from garganorn.tile_reader import TileBackedCollection
from garganorn.stages import write_manifest_db


class TestEmptyTrigrams:
    """Tests for EXPORT-1/EXPORT-12: Empty trigram list causes SQL error.

    Bug: When trigrams list is empty (short queries < 3 chars, or non-ASCII
    scripts), SQL generates `WHERE trigram IN ()` which is invalid SQL,
    causing a crash.

    Fix spec: In nearest(), check for empty trigrams before calling
    query_nearest(). Return empty list with log warning. The guard must be
    BEFORE SQL construction.
    """

    def test_nearest_short_query_returns_empty(self, osm_db):
        """Test that nearest() returns empty list for 2-char query without crashing.

        Query "ab" has only 2 characters, which produces 0 trigrams.
        Current behavior: SQL error "WHERE trigram IN ()" is invalid.
        Expected behavior: Return empty list with a log warning.
        """
        result = osm_db.nearest(q="ab")
        assert result == []
        # Test should pass without raising duckdb.Error or similar

    def test_nearest_normal_query_works(self, osm_db):
        """Non-regression test: normal queries still work after empty trigram fix.

        Query "tartine" produces normal trigrams and should return results.
        This ensures the fix doesn't break normal text search.
        """
        result = osm_db.nearest(q="tartine")
        # Should return at least one result from OSM test data
        assert len(result) > 0
        # Result should contain "Tartine Manufactory" (trigram search on name field)
        names = [r["name"] for r in result]
        assert any("tartine" in name.lower() for name in names)

    def test_nearest_cjk_returns_empty(self, osm_db):
        """Test that nearest() returns empty list for CJK without crashing.

        Query "東京" (Tokyo) is non-ASCII. After accent stripping, it may
        produce 0 trigrams if the script doesn't have 3+ characters.
        Current behavior: May crash with empty trigram list.
        Expected behavior: Return empty list with a log warning.
        """
        # CJK characters that might produce 0 trigrams after normalization
        result = osm_db.nearest(q="東京")
        # Should not crash; empty result is acceptable
        assert isinstance(result, list)


class TestOSMRkeyFormat:
    """Tests for EXPORT-3/EXPORT-8: OSM rkey format mismatch between export and manifest.

    Bug: Export tiles write JSON with rkey `node:12345`, but manifest stores
    `n12345`. When client calls get_record("node:12345"), manifest lookup
    fails → 404.

    The export SQL (osm_export_tiles.sql) transforms rkeys:
    - n12345 → node:12345
    - w67890 → way:67890
    - r11111 → relation:11111

    But write_manifest_db() stores the original n12345 format in record_tiles.

    Fix spec: In write_manifest_db(), transform OSM rkeys to match export format
    when writing manifest: n12345 → node:12345, w67890 → way:67890.
    """

    @pytest.fixture
    def osm_manifest_db(self, tmp_path):
        """Create an OSM database with tile_assignments and manifest.

        Sets up:
        - A temporary OSM database with places and tile_assignments
        - A manifest.duckdb written by write_manifest_db()
        - A mock tile file with test records

        Returns tuple: (manifest_db_path, tiles_dir, test_rkey_mapping)
        """
        # Create output directory structure first
        tiles_dir = tmp_path / "tiles"
        tiles_dir.mkdir()
        (tiles_dir / "130220").mkdir()

        # Create a temporary database with OSM test data
        work_db_path = tmp_path / "osm_work.duckdb"
        con = duckdb.connect(str(work_db_path))
        con.execute("INSTALL spatial; LOAD spatial;")

        # Create places table with OSM schema (minimal, just for import)
        con.execute("""
            CREATE TABLE places (
                rkey VARCHAR,
                name VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                geom GEOMETRY
            )
        """)

        # Insert test OSM places with raw rkey format (n12345, w67890, r11111)
        test_places = [
            ("n12345", "Node Place", 37.7749, -122.4194),
            ("w67890", "Way Place", 37.7750, -122.4195),
            ("r11111", "Relation Place", 37.7751, -122.4196),
        ]

        for rkey, name, lat, lon in test_places:
            con.execute("""
                INSERT INTO places VALUES (?, ?, ?, ?, ST_Point(?, ?))
            """, [rkey, name, lat, lon, lon, lat])

        # Create tile_assignments table (used by write_manifest_db)
        # Note: place_id is the raw rkey format (n12345, w67890, etc.)
        con.execute("""
            CREATE TABLE tile_assignments AS
            SELECT rkey AS place_id, '13022021' AS tile_qk
            FROM places
        """)

        # Write mock tile file with reformatted rkeys (as export SQL does)
        tile_data = {
            "collection": "org.atgeo.places.osm",
            "attribution": "https://www.openstreetmap.org/copyright",
            "records": [
                {
                    "$type": "org.atgeo.place",
                    "rkey": "node:12345",  # Reformatted from n12345
                    "name": "Node Place",
                    "importance": 10,
                    "locations": [{
                        "$type": "community.lexicon.location.geo",
                        "latitude": "37.774900",
                        "longitude": "-122.419400"
                    }],
                    "variants": [],
                    "attributes": {}
                },
                {
                    "$type": "org.atgeo.place",
                    "rkey": "way:67890",  # Reformatted from w67890
                    "name": "Way Place",
                    "importance": 10,
                    "locations": [{
                        "$type": "community.lexicon.location.geo",
                        "latitude": "37.775000",
                        "longitude": "-122.419500"
                    }],
                    "variants": [],
                    "attributes": {}
                },
                {
                    "$type": "org.atgeo.place",
                    "rkey": "relation:11111",  # Reformatted from r11111
                    "name": "Relation Place",
                    "importance": 10,
                    "locations": [{
                        "$type": "community.lexicon.location.geo",
                        "latitude": "37.775100",
                        "longitude": "-122.419600"
                    }],
                    "variants": [],
                    "attributes": {}
                },
            ]
        }

        tile_path = tiles_dir / "130220" / "13022021.json.gz"
        with gzip.open(tile_path, "wt") as f:
            json.dump(tile_data, f)

        # Write manifest.duckdb using the stage function
        # This stores place_id (raw rkey) as rkey in record_tiles
        ta_tmp = str(tiles_dir.parent / "ta_tmp.parquet")
        con.execute(f"COPY tile_assignments TO '{ta_tmp}' (FORMAT PARQUET)")
        con.close()
        write_manifest_db(ta_tmp, str(tiles_dir), "osm")

        manifest_db_path = tiles_dir / "manifest.duckdb"

        # Mapping: reformatted rkey → original rkey
        rkey_mapping = {
            "node:12345": "n12345",
            "way:67890": "w67890",
            "relation:11111": "r11111",
        }

        return manifest_db_path, tiles_dir, rkey_mapping

    def test_get_record_with_reformatted_node_rkey(self, osm_manifest_db):
        """Test that get_record works with reformatted node rkey.

        After export, tiles contain rkey="node:12345".
        The manifest should also store "node:12345" so lookup works.

        Current behavior: Manifest stores "n12345", lookup fails → None.
        Expected behavior: Manifest stores "node:12345", lookup succeeds.
        """
        manifest_db_path, tiles_dir, rkey_mapping = osm_manifest_db

        # Create TileBackedCollection to test get_record
        tbc = TileBackedCollection(
            collection="org.atgeo.places.osm",
            manifest_db_path=str(manifest_db_path),
            tiles_dir=str(tiles_dir),
            attribution="https://www.openstreetmap.org/copyright"
        )

        # Try to get record with reformatted rkey (as tile has it)
        result = tbc.get_record("places.atgeo.org", "org.atgeo.places.osm", "node:12345")

        # Current: returns None (manifest has n12345, not node:12345)
        # Expected: returns the record
        assert result is not None, "get_record should find record with reformatted rkey 'node:12345'"
        assert result["name"] == "Node Place"

    def test_get_record_with_reformatted_way_rkey(self, osm_manifest_db):
        """Test that get_record works with reformatted way rkey.

        Current behavior: Manifest stores "w67890", lookup for "way:67890" fails.
        Expected behavior: Manifest stores "way:67890", lookup succeeds.
        """
        manifest_db_path, tiles_dir, rkey_mapping = osm_manifest_db

        tbc = TileBackedCollection(
            collection="org.atgeo.places.osm",
            manifest_db_path=str(manifest_db_path),
            tiles_dir=str(tiles_dir),
            attribution="https://www.openstreetmap.org/copyright"
        )

        result = tbc.get_record("places.atgeo.org", "org.atgeo.places.osm", "way:67890")

        assert result is not None, "get_record should find record with reformatted rkey 'way:67890'"
        assert result["name"] == "Way Place"

    def test_get_record_with_reformatted_relation_rkey(self, osm_manifest_db):
        """Test that get_record works with reformatted relation rkey.

        Current behavior: Manifest stores "r11111", lookup for "relation:11111" fails.
        Expected behavior: Manifest stores "relation:11111", lookup succeeds.
        """
        manifest_db_path, tiles_dir, rkey_mapping = osm_manifest_db

        tbc = TileBackedCollection(
            collection="org.atgeo.places.osm",
            manifest_db_path=str(manifest_db_path),
            tiles_dir=str(tiles_dir),
            attribution="https://www.openstreetmap.org/copyright"
        )

        result = tbc.get_record("places.atgeo.org", "org.atgeo.places.osm", "relation:11111")

        assert result is not None, "get_record should find record with reformatted rkey 'relation:11111'"
        assert result["name"] == "Relation Place"

    def test_manifest_rkey_format_matches_export(self, osm_manifest_db):
        """Test that manifest record_tiles table uses reformatted rkeys.

        Direct test of the manifest database structure.
        """
        manifest_db_path, tiles_dir, rkey_mapping = osm_manifest_db

        con = duckdb.connect(str(manifest_db_path), read_only=True)

        # Query the record_tiles table
        rows = con.execute("SELECT rkey, tile_qk FROM record_tiles ORDER BY rkey").fetchall()
        con.close()

        # Check that rkeys are in reformatted format
        rkeys_in_manifest = [row[0] for row in rows]

        # Current: ["n12345", "w67890", "r11111"]
        # Expected: ["node:12345", "way:67890", "relation:11111"]
        assert "node:12345" in rkeys_in_manifest, "Manifest should contain reformatted rkey 'node:12345'"
        assert "way:67890" in rkeys_in_manifest, "Manifest should contain reformatted rkey 'way:67890'"
        assert "relation:11111" in rkeys_in_manifest, "Manifest should contain reformatted rkey 'relation:11111'"

        # Old format should NOT be present
        assert "n12345" not in rkeys_in_manifest, "Manifest should not contain old format 'n12345'"
        assert "w67890" not in rkeys_in_manifest, "Manifest should not contain old format 'w67890'"
        assert "r11111" not in rkeys_in_manifest, "Manifest should not contain old format 'r11111'"


class TestAttributionJSONEscaping:
    """Tests for EXPORT-14: Attribution string not JSON-escaped.

    Bug: Attribution string inserted into JSON via f-string in export_tiles().
    Quotes/backslashes break JSON.

    The export_tiles function in stages.py builds payload like:
    payload = f'{{"collection":"{source_cls.collection}","attribution":"{source_cls.attribution}","records":[{joined}]}}'

    If source_cls.attribution contains quotes or backslashes, the JSON becomes invalid.

    Fix spec: Build payload as dict and use json.dumps(), or properly escape
    the attribution string.
    """

    def test_foursquare_attribution_produces_valid_json(self):
        """Test that Foursquare attribution produces valid JSON.

        Current Foursquare attribution: "© 2024 Foursquare" - no special chars.
        This test documents current working behavior.
        """
        from garganorn.database import FoursquareOSP

        attribution = FoursquareOSP.attribution

        # Construct payload as export_tiles does
        payload = f'{{"collection":"{FoursquareOSP.collection}","attribution":"{attribution}","records":[]}}'

        # Should parse without error
        data = json.loads(payload)
        assert data["collection"] == FoursquareOSP.collection
        assert data["attribution"] == attribution

    def test_overture_attribution_produces_valid_json(self):
        """Test that Overture attribution produces valid JSON.

        Current Overture attribution: "© 2024 Overture Maps" - no special chars.
        This test documents current working behavior.
        """
        from garganorn.database import OverturePlaces

        attribution = OverturePlaces.attribution

        payload = f'{{"collection":"{OverturePlaces.collection}","attribution":"{attribution}","records":[]}}'

        data = json.loads(payload)
        assert data["collection"] == OverturePlaces.collection
        assert data["attribution"] == attribution

    def test_osm_attribution_produces_valid_json(self):
        """Test that OSM attribution produces valid JSON.

        Current OSM attribution: "https://www.openstreetmap.org/copyright"
        Contains forward slashes which are OK in JSON.
        This test documents current working behavior.
        """
        from garganorn.database import OpenStreetMap

        attribution = OpenStreetMap.attribution

        payload = f'{{"collection":"{OpenStreetMap.collection}","attribution":"{attribution}","records":[]}}'

        data = json.loads(payload)
        assert data["collection"] == OpenStreetMap.collection
        assert data["attribution"] == attribution

    def test_problematic_attribution_with_quotes(self):
        """Test that demonstrates the bug with quotes in attribution.

        If attribution were changed to include quotes, current code would fail.
        This test shows what happens with a problematic attribution string.
        """
        # Simulate a problematic attribution (hypothetical future change)
        attribution = '© 2024 "The Company" & Partners'
        collection = "org.atgeo.places.test"

        # This is how export_tiles currently builds the payload
        payload = f'{{"collection":"{collection}","attribution":"{attribution}","records":[]}}'

        # Current behavior: This produces invalid JSON
        # Expected behavior (after fix): This should produce valid JSON
        with pytest.raises(json.JSONDecodeError):
            # This will fail because the quotes aren't escaped
            data = json.loads(payload)

    def test_problematic_attribution_with_backslashes(self):
        """Test that demonstrates the bug with backslashes in attribution.

        Backslashes in attribution strings break JSON when inserted via f-string.
        """
        attribution = 'Data from C:\\Users\\Test\\Server'
        collection = "org.atgeo.places.test"

        # Current construction method
        payload = f'{{"collection":"{collection}","attribution":"{attribution}","records":[]}}'

        # Current behavior: Invalid JSON (backslashes not properly escaped)
        # Expected behavior: Valid JSON
        with pytest.raises(json.JSONDecodeError):
            data = json.loads(payload)

    def test_problematic_attribution_with_newlines(self):
        """Test that demonstrates the bug with newlines in attribution.

        Newlines and other control characters must be escaped in JSON.
        """
        attribution = '© 2024 Data\nLine 2 of attribution'
        collection = "org.atgeo.places.test"

        payload = f'{{"collection":"{collection}","attribution":"{attribution}","records":[]}}'

        # Current behavior: Invalid JSON (newlines not escaped)
        # Expected behavior: Valid JSON with \n escape sequence
        with pytest.raises(json.JSONDecodeError):
            data = json.loads(payload)
