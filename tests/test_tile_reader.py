"""Tests for garganorn.tile_reader.TileBackedCollection.

Tiles are atgeo v1 {uri, cid, value}-wrapped records. get_record() matches
on `record["value"]["rkey"]` and returns `copy.copy(record["value"])` — the
value sub-object, not the wrapper.
"""
import gzip
import json
import os
from unittest.mock import patch

import duckdb
import pytest

from garganorn.tile_reader import TileBackedCollection

COLLECTION = "org.atgeo.places.test"
SOURCE_URL = "https://example.com/tile-source"
LICENSE_URL = "https://example.com/tile-license"


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


def _write_envelope_tile(tiles_dir, tile_qk, values, generated_at="2026-07-09T18:00:00Z"):
    """Write a gzipped JSON tile file in the NEW atgeo v1 {uri, cid, value} shape.

    `values` is a list of record value dicts (e.g. {"rkey": ..., "name": ...});
    each is wrapped as {"uri": ..., "cid": None, "value": <value>}.
    """
    subdir = os.path.join(str(tiles_dir), tile_qk[:6])
    os.makedirs(subdir, exist_ok=True)
    path = os.path.join(subdir, f"{tile_qk}.json.gz")
    records = [
        {"uri": f"https://places.atgeo.org/{COLLECTION}/{v['rkey']}", "cid": None, "value": v}
        for v in values
    ]
    with gzip.open(path, "wt") as f:
        json.dump({
            "collection": COLLECTION, "source": SOURCE_URL, "license": LICENSE_URL,
            "generated_at": generated_at, "records": records,
        }, f)
    return path


class TestTileBackedCollectionNewEnvelope:
    """get_record() against {uri, cid, value}-wrapped tiles.

    Verifies get_record() matches `record["value"]["rkey"]` and returns
    `copy.copy(record["value"])` — the value sub-object, not the wrapper.
    """

    def setup_method(self):
        TileBackedCollection._cached_read_tile.cache_clear()

    def test_get_record_returns_value_not_wrapper(self, tmp_path):
        """get_record on a new-shape tile returns the value dict, not {uri, cid, value}."""
        tile_qk = "023010"
        rkey = "place001"
        manifest_db = _make_manifest_db(tmp_path, [(rkey, tile_qk)])
        _write_envelope_tile(tmp_path, tile_qk, [
            {"rkey": rkey, "name": "Test Place", "importance": 75}
        ])

        col = TileBackedCollection(
            collection=COLLECTION,
            manifest_db_path=str(manifest_db),
            tiles_dir=str(tmp_path),
            source_url=SOURCE_URL,
            license_url=LICENSE_URL,
        )
        result = col.get_record("repo", COLLECTION, rkey)

        assert result is not None
        assert "uri" not in result, (
            f"get_record must return record['value'], not the {{uri,cid,value}} wrapper; "
            f"got {result}"
        )
        assert "cid" not in result
        assert "value" not in result
        assert result["rkey"] == rkey
        assert result["name"] == "Test Place"
        assert result["importance"] == 75

    def test_get_record_missing_rkey_returns_none_new_shape(self, tmp_path):
        """rkey not in manifest → None, against a new-shape tile."""
        tile_qk = "023010"
        manifest_db = _make_manifest_db(tmp_path, [("place001", tile_qk)])
        _write_envelope_tile(tmp_path, tile_qk, [
            {"rkey": "place001", "name": "Test Place"}
        ])

        col = TileBackedCollection(
            collection=COLLECTION,
            manifest_db_path=str(manifest_db),
            tiles_dir=str(tmp_path),
            source_url=SOURCE_URL,
            license_url=LICENSE_URL,
        )
        result = col.get_record("repo", COLLECTION, "nonexistent")

        assert result is None

    def test_get_record_finds_correct_record_among_several(self, tmp_path):
        """Multiple wrapped records in one tile → correct value.rkey match returned."""
        tile_qk = "023010"
        manifest_db = _make_manifest_db(tmp_path, [
            ("place001", tile_qk),
            ("place002", tile_qk),
        ])
        _write_envelope_tile(tmp_path, tile_qk, [
            {"rkey": "place001", "name": "First Place"},
            {"rkey": "place002", "name": "Second Place"},
        ])

        col = TileBackedCollection(
            collection=COLLECTION,
            manifest_db_path=str(manifest_db),
            tiles_dir=str(tmp_path),
            source_url=SOURCE_URL,
            license_url=LICENSE_URL,
        )
        result = col.get_record("repo", COLLECTION, "place002")

        assert result is not None
        assert result["rkey"] == "place002"
        assert result["name"] == "Second Place"

    def test_get_record_mutation_does_not_corrupt_cache(self, tmp_path):
        """Popping a key from the returned value (as the server layer does with
        'importance') must not corrupt the lru_cache-held tile dict — the
        shallow-copy contract (per the envelope decisions above) must hold for the
        value sub-object too."""
        tile_qk = "023010"
        rkey = "place001"
        manifest_db = _make_manifest_db(tmp_path, [(rkey, tile_qk)])
        _write_envelope_tile(tmp_path, tile_qk, [
            {"rkey": rkey, "name": "Test Place", "importance": 75}
        ])

        col = TileBackedCollection(
            collection=COLLECTION,
            manifest_db_path=str(manifest_db),
            tiles_dir=str(tmp_path),
            source_url=SOURCE_URL,
            license_url=LICENSE_URL,
        )
        first = col.get_record("repo", COLLECTION, rkey)
        first.pop("importance", None)

        second = col.get_record("repo", COLLECTION, rkey)
        assert "importance" in second, (
            "mutating one get_record() result must not affect a subsequent call "
            "(cache corruption via shared reference)"
        )

    def test_get_record_missing_tile_file_returns_none(self, tmp_path):
        """rkey is in the manifest but the tile file itself is missing on disk
        (e.g. deleted after manifest generation) → get_record() catches
        FileNotFoundError and returns None instead of raising."""
        tile_qk = "023010"
        rkey = "place001"
        manifest_db = _make_manifest_db(tmp_path, [(rkey, tile_qk)])
        # Deliberately do not write the tile file.

        col = TileBackedCollection(
            collection=COLLECTION,
            manifest_db_path=str(manifest_db),
            tiles_dir=str(tmp_path),
            source_url=SOURCE_URL,
            license_url=LICENSE_URL,
        )
        result = col.get_record("repo", COLLECTION, rkey)

        assert result is None

    def test_tile_caching(self, tmp_path):
        """Two get_record() calls against the same tile only read the tile
        file from disk once (_cached_read_tile is lru_cache-backed)."""
        tile_qk = "023010"
        rkey = "place001"
        manifest_db = _make_manifest_db(tmp_path, [(rkey, tile_qk)])
        _write_envelope_tile(tmp_path, tile_qk, [
            {"rkey": rkey, "name": "Test Place", "importance": 75}
        ])

        col = TileBackedCollection(
            collection=COLLECTION,
            manifest_db_path=str(manifest_db),
            tiles_dir=str(tmp_path),
            source_url=SOURCE_URL,
            license_url=LICENSE_URL,
        )

        with patch("garganorn.tile_reader.gzip.open", wraps=gzip.open) as mock_open:
            col.get_record("repo", COLLECTION, rkey)
            col.get_record("repo", COLLECTION, rkey)

        assert mock_open.call_count == 1, (
            "second get_record() for the same tile should hit the cache, "
            "not reopen the tile file"
        )
