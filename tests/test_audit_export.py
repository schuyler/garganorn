"""Tests for export and query path bug fixes.

These tests verify that the export pipeline correctly handles:
- OSM rkey format consistency between export and manifest
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


class TestOSMRkeyFormat:
    """manifest.record_tiles.rkey uses the same node:/way:/relation: prefix
    as the export tiles' JSON rkey, so get_record lookups by the exported
    rkey succeed.
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

        # Write mock tile file with reformatted rkeys (as export SQL does),
        # wrapped in the current {uri, cid, value} envelope shape.
        def _envelope(rkey, name, lat, lon):
            return {
                "uri": f"https://places.atgeo.org/org.atgeo.places.osm/{rkey}",
                "cid": None,
                "value": {
                    "$type": "org.atgeo.place",
                    "rkey": rkey,
                    "name": name,
                    "importance": 10,
                    "locations": [{
                        "$type": "community.lexicon.location.geo",
                        "latitude": lat,
                        "longitude": lon
                    }],
                    "variants": [],
                    "attributes": {}
                },
            }

        tile_data = {
            "collection": "org.atgeo.places.osm",
            "source": "https://www.openstreetmap.org/",
            "license": "https://opendatacommons.org/licenses/odbl/1-0/",
            "records": [
                _envelope("node:12345", "Node Place", "37.774900", "-122.419400"),  # Reformatted from n12345
                _envelope("way:67890", "Way Place", "37.775000", "-122.419500"),  # Reformatted from w67890
                _envelope("relation:11111", "Relation Place", "37.775100", "-122.419600"),  # Reformatted from r11111
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
        """get_record finds a place by its reformatted node rkey "node:12345"."""
        manifest_db_path, tiles_dir, rkey_mapping = osm_manifest_db

        # Create TileBackedCollection to test get_record
        tbc = TileBackedCollection(
            collection="org.atgeo.places.osm",
            manifest_db_path=str(manifest_db_path),
            tiles_dir=str(tiles_dir),
            source_url="https://www.openstreetmap.org/",
            license_url="https://opendatacommons.org/licenses/odbl/1-0/"
        )

        # Try to get record with reformatted rkey (as tile has it)
        result = tbc.get_record("places.atgeo.org", "org.atgeo.places.osm", "node:12345")

        assert result is not None, "get_record should find record with reformatted rkey 'node:12345'"
        assert result["name"] == "Node Place"

    def test_get_record_with_reformatted_way_rkey(self, osm_manifest_db):
        """get_record finds a place by its reformatted way rkey "way:67890"."""
        manifest_db_path, tiles_dir, rkey_mapping = osm_manifest_db

        tbc = TileBackedCollection(
            collection="org.atgeo.places.osm",
            manifest_db_path=str(manifest_db_path),
            tiles_dir=str(tiles_dir),
            source_url="https://www.openstreetmap.org/",
            license_url="https://opendatacommons.org/licenses/odbl/1-0/"
        )

        result = tbc.get_record("places.atgeo.org", "org.atgeo.places.osm", "way:67890")

        assert result is not None, "get_record should find record with reformatted rkey 'way:67890'"
        assert result["name"] == "Way Place"

    def test_get_record_with_reformatted_relation_rkey(self, osm_manifest_db):
        """get_record finds a place by its reformatted relation rkey "relation:11111"."""
        manifest_db_path, tiles_dir, rkey_mapping = osm_manifest_db

        tbc = TileBackedCollection(
            collection="org.atgeo.places.osm",
            manifest_db_path=str(manifest_db_path),
            tiles_dir=str(tiles_dir),
            source_url="https://www.openstreetmap.org/",
            license_url="https://opendatacommons.org/licenses/odbl/1-0/"
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

        assert "node:12345" in rkeys_in_manifest, "Manifest should contain reformatted rkey 'node:12345'"
        assert "way:67890" in rkeys_in_manifest, "Manifest should contain reformatted rkey 'way:67890'"
        assert "relation:11111" in rkeys_in_manifest, "Manifest should contain reformatted rkey 'relation:11111'"

        # Raw place_id prefixes must not leak into the manifest rkey
        assert "n12345" not in rkeys_in_manifest, "Manifest should not contain raw place_id 'n12345'"
        assert "w67890" not in rkeys_in_manifest, "Manifest should not contain raw place_id 'w67890'"
        assert "r11111" not in rkeys_in_manifest, "Manifest should not contain raw place_id 'r11111'"
