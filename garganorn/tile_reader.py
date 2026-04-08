import copy
import duckdb
import gzip
import json
import os
import threading
from functools import lru_cache


class TileBackedCollection:
    """Serves getRecord from static tile files + manifest.duckdb."""

    def __init__(self, collection: str, manifest_db_path: str,
                 tiles_dir: str, attribution: str):
        self.collection = collection
        self.attribution = attribution
        self.tiles_dir = tiles_dir
        self._db_path = manifest_db_path
        self._local = threading.local()

    @property
    def _con(self):
        """Per-thread DuckDB connection (DuckDB connections are not thread-safe)."""
        if not hasattr(self._local, "con"):
            self._local.con = duckdb.connect(self._db_path, read_only=True)
        return self._local.con

    def get_record(self, _repo: str, _collection: str, rkey: str):
        """Look up which tile contains this rkey, read the tile, find the record."""
        result = self._con.execute(
            "SELECT tile_qk FROM record_tiles WHERE rkey = ?", [rkey]
        ).fetchone()
        if result is None:
            return None
        tile_qk = result[0]
        try:
            tile_data = self._read_tile(tile_qk)
        except FileNotFoundError:
            return None
        # ATProto rkeys are ASCII alphanumeric + hyphen + dot (no slashes), so
        # endswith on "/{collection}/{rkey}" is unambiguous — no false-positive risk.
        target_uri_suffix = f"/{self.collection}/{rkey}"
        for record in tile_data["records"]:
            if record["uri"].endswith(target_uri_suffix):
                # Shallow copy prevents mutations by the server layer (e.g., popping
                # "importance") from corrupting the lru_cache-held tile dict.
                return copy.copy(record["value"])
        return None

    def _read_tile(self, tile_qk: str) -> dict:
        """Read and decompress a tile file. Uses LRU cache to amortize repeated access."""
        # tile_qk[:6] is the 6-char subdirectory prefix. The export pipeline produces
        # zoom-6+ keys (always >= 6 chars), so the slice is always a full 6 chars.
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
