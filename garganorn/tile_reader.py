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
        """Return (record, attribution) for this key, or None when not found."""
        return self.get_places([key]).get(key)

    def get_places(self, keys) -> dict:
        """Map each found key to (record, attribution): the record and the
        tile's attribution array. Keys that are not found are absent.

        any_value() picks one tile when a division key has N record_tiles
        rows (one per overlapping tile) -- correct only because all copies of
        a division's record are byte-identical.
        """
        # list_contains(?, key) bypasses the key index and scans the 73M-row Overture manifest.
        rows = self._con.execute(
            "SELECT key, any_value(tile_qk) FROM record_tiles "
            "WHERE key IN (SELECT unnest(?)) GROUP BY key", [keys]
        ).fetchall()
        found = {}
        for key, tile_qk in rows:
            try:
                tile_data = self._read_tile(tile_qk)
            except FileNotFoundError:
                continue
            origin = "urn:atgeo:" + key
            for record in tile_data["records"]:
                if record["value"]["origin"] == origin:
                    # Copies: a caller may replace top-level keys of the record or
                    # edit the list, but `value` and the tile are lru_cache-held.
                    found[key] = (copy.copy(record), list(tile_data.get("attribution", [])))
                    break
        return found

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


_SEARCH_SQL = """
SELECT rid FROM search_blocks
WHERE block = $block
  AND ($scope IS NULL OR scope = $scope)
  AND ($rw IS NULL OR (s <= $rn AND n >= $rs AND (
        CASE WHEN $rw <= $re
             THEN CASE WHEN w - e > 1 THEN w <= $re OR e >= $rw
                       ELSE (w <= $re AND e >= $rw)
                            OR (e = 180 AND $rw = -180) OR (w = -180 AND $re = 180) END
             ELSE w - e > 1 OR e >= $rw OR w <= $re END)))
  AND len(list_filter(names,
          nm -> nm IS NOT NULL
               AND len(list_filter($tokens, t -> NOT contains(nm, ' ' || t))) = 0)) > 0
ORDER BY rid
LIMIT $limit
"""


class TileSearchIndex:
    """Searches a run's search.duckdb: names by word prefix, in rank order."""

    def __init__(self, search_db_path: str):
        self._db_path = search_db_path
        self._local = threading.local()

    @property
    def _con(self):
        """Per-thread DuckDB connection (DuckDB connections are not thread-safe)."""
        if not hasattr(self._local, "con"):
            self._local.con = duckdb.connect(self._db_path, read_only=True)
        return self._local.con

    def tokens(self, q: str) -> list[str]:
        return self._con.execute("SELECT name_tokens(?)", [q]).fetchone()[0]

    def search(self, tokens: list[str], bbox: tuple | None, scope: str | None,
               limit: int) -> list[tuple[str, int]]:
        """(key, importance) of up to `limit` records, best first, with some
        name having every token as a word prefix. bbox is whole-degree
        (west, south, east, north); scope is '{dataset}:{collection}'."""
        rw, rs, re, rn = (int(v) for v in bbox) if bbox else (None,) * 4
        params = {
            "block": max(tokens, key=len)[:2], "scope": scope, "tokens": tokens,
            "rw": rw, "rs": rs, "re": re, "rn": rn, "limit": limit,
        }
        rids = [row[0] for row in self._con.execute(_SEARCH_SQL, params).fetchall()]
        return [
            self._con.execute(
                "SELECT key, importance FROM search_records WHERE rid = ?", [rid]
            ).fetchone()
            for rid in rids
        ]
