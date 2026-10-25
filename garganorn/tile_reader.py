import copy
import duckdb
import gzip
import json
import os
import threading
from functools import lru_cache


class TileBackedCollection:
    """Reads a tile record by manifest key from static tile files + manifest.duckdb."""

    def __init__(self, manifest_db_path: str, tiles_dir: str):
        self.tiles_dir = tiles_dir
        self._db_path = manifest_db_path
        self._local = threading.local()

    @property
    def _con(self):
        """Per-thread DuckDB connection (DuckDB connections are not thread-safe)."""
        if not hasattr(self._local, "con"):
            self._local.con = duckdb.connect(self._db_path, read_only=True)
        return self._local.con

    def get_place(self, key: str):
        """Look up which tile contains this key, read the tile, and return
        (record, attribution): the record and the tile's attribution array.
        Returns None when the key is not found.

        fetchone() picks one row arbitrarily when a division key has N
        record_tiles rows (one per overlapping tile) -- correct only because
        all copies of a division's record are byte-identical.
        """
        result = self._con.execute(
            "SELECT tile_qk FROM record_tiles WHERE key = ?", [key]
        ).fetchone()
        if result is None:
            return None
        try:
            tile_data = self._read_tile(result[0])
        except FileNotFoundError:
            return None
        origin = "urn:atgeo:" + key
        for record in tile_data["records"]:
            if record["value"]["origin"] == origin:
                # Copies: a caller may replace top-level keys of the record or
                # edit the list, but `value` and the tile are lru_cache-held.
                return copy.copy(record), list(tile_data.get("attribution", []))
        return None

    def _read_tile(self, tile_qk: str) -> dict:
        """Read and decompress a tile file. Uses LRU cache to amortize repeated access."""
        # tile_qk[:6] is the 6-char subdirectory prefix. The export pipeline produces
        # zoom 6-17 keys, plus the summary band's short keys, where the slice is the
        # whole key.
        tile_path = os.path.join(self.tiles_dir, tile_qk[:6], f"{tile_qk}.json.gz")
        return self._cached_read_tile(tile_path)

    @staticmethod
    @lru_cache(maxsize=256)
    def _cached_read_tile(tile_path: str) -> dict:
        # Process-global cache keyed on tile_path. Tiles are immutable once written;
        # if tiles are regenerated at the same paths, a process restart is required
        # to clear stale cache entries.
        with gzip.open(tile_path, "rt") as f:
            return json.load(f)
