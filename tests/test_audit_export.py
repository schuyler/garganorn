"""Tests for export and query path bug fixes.

These tests verify that the export pipeline correctly handles:
- OSM manifest key format consistency between export and manifest
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


class TestOSMManifestKeyFormat:
    """manifest.record_tiles.key is {dataset}:{collection}:{id}, the origin URN
    without its urn:atgeo: prefix, so get_place lookups by the exported
    origin succeed.
    """

    @pytest.fixture
    def osm_manifest_db(self, tmp_path):
        """Create an OSM database with tile_assignments and manifest.

        Sets up:
        - A temporary OSM database with places and tile_assignments
        - A manifest.duckdb written by write_manifest_db()
        - A mock tile file with test records

        Returns tuple: (manifest_db_path, tiles_dir)
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

        def _record(key, name, lat, lon):
            return {
                "value": {
                    "$type": "org.atgeo.place",
                    "origin": f"urn:atgeo:{key}",
                    "name": name,
                    "locations": [{
                        "$type": "community.lexicon.location.geo",
                        "latitude": lat,
                        "longitude": lon
                    }],
                    "variants": [],
                    "attributes": {}
                },
                "importance": 10,
            }

        tile_data = {
            "publishedAt": "2026-10-01T00:00:00Z",
            "attribution": [],
            "records": [
                _record("openstreetmap.org:node:12345", "Node Place", "37.774900", "-122.419400"),
                _record("openstreetmap.org:way:67890", "Way Place", "37.775000", "-122.419500"),
                _record("openstreetmap.org:relation:11111", "Relation Place", "37.775100", "-122.419600"),
            ]
        }

        tile_path = tiles_dir / "130220" / "13022021.json.gz"
        with gzip.open(tile_path, "wt") as f:
            json.dump(tile_data, f)

        # Write manifest.duckdb using the stage function
        # This stores the manifest key derived from place_id in record_tiles
        ta_tmp = str(tiles_dir.parent / "ta_tmp.parquet")
        con.execute(f"COPY tile_assignments TO '{ta_tmp}' (FORMAT PARQUET)")
        con.close()
        write_manifest_db(ta_tmp, str(tiles_dir), "osm")

        manifest_db_path = tiles_dir / "manifest.duckdb"

        return manifest_db_path, tiles_dir

    def test_get_place_with_node_key(self, osm_manifest_db):
        """get_place finds a place by its key "openstreetmap.org:node:12345"."""
        manifest_db_path, tiles_dir = osm_manifest_db

        # Create TileBackedCollection to test get_place
        tbc = TileBackedCollection(
            manifest_db_path=str(manifest_db_path),
            tiles_dir=str(tiles_dir),
        )

        # Look up by the manifest key
        result = tbc.get_place("openstreetmap.org:node:12345")[0]

        assert result is not None, "get_place should find record with key 'openstreetmap.org:node:12345'"
        assert result["value"]["name"] == "Node Place"

    def test_get_place_with_way_key(self, osm_manifest_db):
        """get_place finds a place by its key "openstreetmap.org:way:67890"."""
        manifest_db_path, tiles_dir = osm_manifest_db

        tbc = TileBackedCollection(
            manifest_db_path=str(manifest_db_path),
            tiles_dir=str(tiles_dir),
        )

        result = tbc.get_place("openstreetmap.org:way:67890")[0]

        assert result is not None, "get_place should find record with key 'openstreetmap.org:way:67890'"
        assert result["value"]["name"] == "Way Place"

    def test_get_place_with_relation_key(self, osm_manifest_db):
        """get_place finds a place by its key "openstreetmap.org:relation:11111"."""
        manifest_db_path, tiles_dir = osm_manifest_db

        tbc = TileBackedCollection(
            manifest_db_path=str(manifest_db_path),
            tiles_dir=str(tiles_dir),
        )

        result = tbc.get_place("openstreetmap.org:relation:11111")[0]

        assert result is not None, "get_place should find record with key 'openstreetmap.org:relation:11111'"
        assert result["value"]["name"] == "Relation Place"

    def test_manifest_key_format_matches_export(self, osm_manifest_db):
        """Manifest record_tiles holds {dataset}:{collection}:{id} keys, not raw place_ids."""
        manifest_db_path, tiles_dir = osm_manifest_db

        con = duckdb.connect(str(manifest_db_path), read_only=True)
        keys = [r[0] for r in con.execute(
            "SELECT key FROM record_tiles ORDER BY key").fetchall()]
        con.close()

        assert keys == [
            "openstreetmap.org:node:12345",
            "openstreetmap.org:relation:11111",
            "openstreetmap.org:way:67890",
        ]
