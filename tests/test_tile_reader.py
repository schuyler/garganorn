"""Tests for garganorn.tile_reader.TileBackedCollection.

Tiles hold {value, importance} records. get_record() matches on
`record["value"]["origin"] == "urn:atgeo:" + key` and returns
`copy.copy(record)` -- the tile record, with its importance.
"""
import gzip
import json
import os
from unittest.mock import patch

import duckdb

from garganorn.tile_reader import TileBackedCollection

PREFIX = "overturemaps.org:place:"


def _make_manifest_db(tmp_path, entries):
    """Create a manifest.duckdb with the given (key, tile_qk) entries."""
    p = tmp_path / "manifest.duckdb"
    con = duckdb.connect(str(p))
    con.execute("CREATE TABLE record_tiles (key VARCHAR, tile_qk VARCHAR)")
    for key, tile_qk in entries:
        con.execute("INSERT INTO record_tiles VALUES (?, ?)", [key, tile_qk])
    con.close()
    return p


def _record(id, name="Test Place", importance=75, **value):
    return {
        "value": {"origin": f"urn:atgeo:{PREFIX}{id}", "name": name, **value},
        "importance": importance,
    }


def _write_tile(tiles_dir, tile_qk, records):
    """Write a gzipped JSON tile file in the {publishedAt, attribution, records} shape."""
    subdir = os.path.join(str(tiles_dir), tile_qk[:6])
    os.makedirs(subdir, exist_ok=True)
    path = os.path.join(subdir, f"{tile_qk}.json.gz")
    with gzip.open(path, "wt") as f:
        json.dump({
            "publishedAt": "2026-07-09T18:00:00Z", "attribution": [], "records": records,
        }, f)
    return path


def _collection(tmp_path, manifest_db):
    return TileBackedCollection(
        manifest_db_path=str(manifest_db), tiles_dir=str(tmp_path),
    )


class TestTileBackedCollection:
    """get_record() against {value, importance} tile records."""

    def setup_method(self):
        TileBackedCollection._cached_read_tile.cache_clear()

    def test_get_record_returns_the_tile_record(self, tmp_path):
        """get_record returns {value, importance}, importance outside the value."""
        manifest_db = _make_manifest_db(tmp_path, [(f"{PREFIX}place001", "023010")])
        _write_tile(tmp_path, "023010", [_record("place001")])

        result = _collection(tmp_path, manifest_db).get_record(f"{PREFIX}place001")

        assert set(result) == {"value", "importance"}
        assert result["importance"] == 75
        assert result["value"]["origin"] == f"urn:atgeo:{PREFIX}place001"
        assert result["value"]["name"] == "Test Place"
        assert "importance" not in result["value"]

    def test_get_record_returns_record_unmodified(self, tmp_path):
        """locations, variants, attributes and precomputed relations pass through unchanged."""
        record = _record(
            "place001",
            locations=[{"$type": "community.lexicon.location.geo",
                        "latitude": "37.774900", "longitude": "-122.419400"}],
            variants=[{"name": "Lugar de Prueba", "language": "es"}],
            attributes={"category": "cafe"},
            relations={"within": [{"uri": "urn:atgeo:overturemaps.org:division:85922583",
                                   "name": "Somewhere", "type": "region"}]},
        )
        manifest_db = _make_manifest_db(tmp_path, [(f"{PREFIX}place001", "023010")])
        _write_tile(tmp_path, "023010", [record])

        result = _collection(tmp_path, manifest_db).get_record(f"{PREFIX}place001")

        assert result == record

    def test_get_record_without_relations_has_no_relations_key(self, tmp_path):
        """get_record does not invent a relations key when the value has none."""
        manifest_db = _make_manifest_db(tmp_path, [(f"{PREFIX}place001", "023010")])
        _write_tile(tmp_path, "023010", [_record("place001")])

        result = _collection(tmp_path, manifest_db).get_record(f"{PREFIX}place001")

        assert "relations" not in result["value"]

    def test_get_record_missing_key_returns_none(self, tmp_path):
        """key not in manifest -> None."""
        manifest_db = _make_manifest_db(tmp_path, [(f"{PREFIX}place001", "023010")])
        _write_tile(tmp_path, "023010", [_record("place001")])

        assert _collection(tmp_path, manifest_db).get_record(f"{PREFIX}nonexistent") is None

    def test_get_record_finds_correct_record_among_several(self, tmp_path):
        """Multiple records in one tile -> the one whose value.origin matches the key."""
        manifest_db = _make_manifest_db(tmp_path, [
            (f"{PREFIX}place001", "023010"),
            (f"{PREFIX}place002", "023010"),
        ])
        _write_tile(tmp_path, "023010", [
            _record("place001", name="First Place", importance=10),
            _record("place002", name="Second Place", importance=20),
        ])

        result = _collection(tmp_path, manifest_db).get_record(f"{PREFIX}place002")

        assert result["value"]["name"] == "Second Place"
        assert result["importance"] == 20

    def test_get_record_mutation_does_not_corrupt_cache(self, tmp_path):
        """Popping a key from the returned record must not corrupt the
        lru_cache-held tile dict -- the shallow-copy contract."""
        manifest_db = _make_manifest_db(tmp_path, [(f"{PREFIX}place001", "023010")])
        _write_tile(tmp_path, "023010", [_record("place001")])
        col = _collection(tmp_path, manifest_db)

        first = col.get_record(f"{PREFIX}place001")
        first.pop("importance")

        second = col.get_record(f"{PREFIX}place001")
        assert "importance" in second, (
            "mutating one get_record() result must not affect a subsequent call "
            "(cache corruption via shared reference)"
        )

    def test_get_record_missing_tile_file_returns_none(self, tmp_path):
        """key is in the manifest but the tile file itself is missing on disk
        (e.g. deleted after manifest generation) -> None instead of raising."""
        manifest_db = _make_manifest_db(tmp_path, [(f"{PREFIX}place001", "023010")])
        # Deliberately do not write the tile file.

        assert _collection(tmp_path, manifest_db).get_record(f"{PREFIX}place001") is None

    def test_tile_caching(self, tmp_path):
        """Two get_record() calls against the same tile only read the tile
        file from disk once (_cached_read_tile is lru_cache-backed)."""
        manifest_db = _make_manifest_db(tmp_path, [(f"{PREFIX}place001", "023010")])
        _write_tile(tmp_path, "023010", [_record("place001")])
        col = _collection(tmp_path, manifest_db)

        with patch("garganorn.tile_reader.gzip.open", wraps=gzip.open) as mock_open:
            col.get_record(f"{PREFIX}place001")
            col.get_record(f"{PREFIX}place001")

        assert mock_open.call_count == 1, (
            "second get_record() for the same tile should hit the cache, "
            "not reopen the tile file"
        )
