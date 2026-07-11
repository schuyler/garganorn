"""Tests for garganorn.tile_reader.TileBackedCollection.

New-envelope coverage (phase2b-design.md §6 item 10, Part B / OQ-P2-1):
TestTileBackedCollectionNewEnvelope below asserts get_record() works against
atgeo v1 {uri, cid, value}-wrapped tiles, returning the `value` sub-object
(not the wrapper) with rkey/importance handling intact. TestTileBackedCollection
(pre-existing, below) exercises the OLD flat-record shape and doubles as the
§B.8 deployment-window tolerance-path fixture (`record.get("value", record)`)
while old-shape tiles may still exist on disk during the deploy window.
"""
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
    """Write a gzipped JSON tile file at the expected path (OLD flat-record shape)."""
    subdir = os.path.join(str(tiles_dir), tile_qk[:6])
    os.makedirs(subdir, exist_ok=True)
    path = os.path.join(subdir, f"{tile_qk}.json.gz")
    with gzip.open(path, "wt") as f:
        json.dump({"collection": COLLECTION, "attribution": ATTRIBUTION, "records": records}, f)
    return path


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
            "atgeo": 1, "collection": COLLECTION, "attribution": ATTRIBUTION,
            "generated_at": generated_at, "records": records,
        }, f)
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
            {"rkey": rkey, "name": "Test Place"}
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
            {"rkey": "place001", "name": "Test Place"}
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
            {"rkey": "place001", "name": "First Place"},
            {"rkey": "place002", "name": "Second Place"},
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


class TestTileBackedCollectionNewEnvelope:
    """§6 item 10 — get_record() against new-shape ({uri, cid, value}) tiles.

    get_record() must match `record["value"]["rkey"]` (not top-level
    `record["rkey"]`, which does not exist in wrapped records) and return
    `copy.copy(record["value"])` — the value sub-object, not the wrapper
    (§B.7.5). These FAIL against the current implementation, which does
    `record["rkey"]` directly and would KeyError/never-match against
    {uri, cid, value}-wrapped records.
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
            attribution=ATTRIBUTION,
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
            attribution=ATTRIBUTION,
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
            attribution=ATTRIBUTION,
        )
        result = col.get_record("repo", COLLECTION, "place002")

        assert result is not None
        assert result["rkey"] == "place002"
        assert result["name"] == "Second Place"

    def test_get_record_mutation_does_not_corrupt_cache(self, tmp_path):
        """Popping a key from the returned value (as the server layer does with
        'importance') must not corrupt the lru_cache-held tile dict — the
        shallow-copy contract (§B.7.5) must hold for the value sub-object too."""
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
            attribution=ATTRIBUTION,
        )
        first = col.get_record("repo", COLLECTION, rkey)
        first.pop("importance", None)

        second = col.get_record("repo", COLLECTION, rkey)
        assert "importance" in second, (
            "mutating one get_record() result must not affect a subsequent call "
            "(cache corruption via shared reference)"
        )
