"""Tests for garganorn.tile_reader.TileBackedCollection."""
import gzip
import json
import os
from unittest.mock import patch

import duckdb
import pytest

from garganorn.tile_reader import TileBackedCollection

COLLECTION = "org.atgeo.places.test"
ATTRIBUTION = "https://example.com/tile-attribution"


def _make_manifest_db(tmp_path, entries):
    """Create a manifest.duckdb with the given (rkey, tile_qk) entries."""
    p = tmp_path / "manifest.duckdb"
    con = duckdb.connect(str(p))
    con.execute("CREATE TABLE record_tiles (rkey VARCHAR, tile_qk VARCHAR)")
    for rkey, tile_qk in entries:
        con.execute("INSERT INTO record_tiles VALUES (?, ?)", [rkey, tile_qk])
    con.execute("CREATE TABLE metadata (source VARCHAR, generated_at VARCHAR)")
    con.execute("INSERT INTO metadata VALUES ('test', '2026-01-01T00:00:00+00:00')")
    con.close()
    return p


def _write_tile(tiles_dir, tile_qk, records):
    """Write a gzipped JSON tile file at the expected path."""
    subdir = os.path.join(str(tiles_dir), tile_qk[:6])
    os.makedirs(subdir, exist_ok=True)
    path = os.path.join(subdir, f"{tile_qk}.json.gz")
    with gzip.open(path, "wt") as f:
        json.dump({"records": records}, f)
    return path


class TestTileBackedCollection:
    def setup_method(self):
        TileBackedCollection._cached_read_tile.cache_clear()

    def test_get_record_returns_correct_value(self, tmp_path):
        """rkey in manifest and tile → correct value dict returned."""
        tile_qk = "023010"
        rkey = "place001"
        manifest_db = _make_manifest_db(tmp_path, [(rkey, tile_qk)])
        _write_tile(tmp_path, tile_qk, [
            {
                "uri": f"https://example.com/{COLLECTION}/{rkey}",
                "value": {"rkey": rkey, "name": "Test Place"},
            }
        ])

        col = TileBackedCollection(
            collection=COLLECTION,
            manifest_db_path=str(manifest_db),
            tiles_dir=str(tmp_path),
            attribution=ATTRIBUTION,
        )
        result = col.get_record("repo", COLLECTION, rkey)

        assert result is not None
        assert result["rkey"] == rkey
        assert result["name"] == "Test Place"

    def test_get_record_missing_rkey_returns_none(self, tmp_path):
        """rkey not in manifest → None."""
        tile_qk = "023010"
        manifest_db = _make_manifest_db(tmp_path, [("place001", tile_qk)])
        _write_tile(tmp_path, tile_qk, [
            {
                "uri": f"https://example.com/{COLLECTION}/place001",
                "value": {"rkey": "place001", "name": "Test Place"},
            }
        ])

        col = TileBackedCollection(
            collection=COLLECTION,
            manifest_db_path=str(manifest_db),
            tiles_dir=str(tmp_path),
            attribution=ATTRIBUTION,
        )
        result = col.get_record("repo", COLLECTION, "nonexistent")

        assert result is None

    def test_get_record_missing_tile_file_returns_none(self, tmp_path):
        """rkey in manifest but tile file missing → None (no FileNotFoundError propagated)."""
        tile_qk = "023010"
        rkey = "place001"
        manifest_db = _make_manifest_db(tmp_path, [(rkey, tile_qk)])
        # Intentionally do NOT write the tile file

        col = TileBackedCollection(
            collection=COLLECTION,
            manifest_db_path=str(manifest_db),
            tiles_dir=str(tmp_path),
            attribution=ATTRIBUTION,
        )
        result = col.get_record("repo", COLLECTION, rkey)

        assert result is None

    def test_tile_caching(self, tmp_path):
        """Two get_record calls on the same tile → gzip.open called exactly once."""
        tile_qk = "023010"
        manifest_db = _make_manifest_db(tmp_path, [
            ("place001", tile_qk),
            ("place002", tile_qk),
        ])
        _write_tile(tmp_path, tile_qk, [
            {
                "uri": f"https://example.com/{COLLECTION}/place001",
                "value": {"rkey": "place001", "name": "First Place"},
            },
            {
                "uri": f"https://example.com/{COLLECTION}/place002",
                "value": {"rkey": "place002", "name": "Second Place"},
            },
        ])

        col = TileBackedCollection(
            collection=COLLECTION,
            manifest_db_path=str(manifest_db),
            tiles_dir=str(tmp_path),
            attribution=ATTRIBUTION,
        )

        with patch("garganorn.tile_reader.gzip.open", wraps=gzip.open) as mock_gzip:
            col.get_record("repo", COLLECTION, "place001")
            col.get_record("repo", COLLECTION, "place002")

        assert mock_gzip.call_count == 1
