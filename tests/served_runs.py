"""Hand-built tileset runs and Flask apps over them, for server tests.

A run is written the way the export writes it: manifest.duckdb, tileset.json,
gzip tiles, and manifest.json last. Descriptors and tiles are checked against
the frozen lexicons so a fixture cannot drift from them.
"""
import gzip
import json
from unittest.mock import patch

import duckdb

from garganorn.__main__ import create_app
from tests.lexicon_helpers import goat_validate, validate_def

OSM = "openstreetmap.org"
OVERTURE = "overturemaps.org"
STAMP = "20260101T000000"
PUBLIC_BASE = "http://localhost:8000"
TILE_BASE = "https://tiles.example.net/t"
PUBLISHED_AT = "2026-01-01T00:00:00Z"
EXTENT = {"north": "37.80", "west": "-122.42", "south": "37.77", "east": "-122.41"}


def place(dataset, collection, id, name="Blue Bottle Coffee", importance=42):
    """A tile record {value, importance} for one place; importance=None omits
    the key (the tilePayload lexicon makes it optional)."""
    value = {
        "$type": "org.atgeo.place",
        "origin": f"urn:atgeo:{dataset}:{collection}:{id}",
        "name": name,
        "locations": [{
            "$type": "community.lexicon.location.geo",
            "latitude": "37.774900", "longitude": "-122.419400",
        }],
    }
    goat_validate(value)
    record = {"value": value}
    if importance is not None:
        record["importance"] = importance
    return record


def make_run(tiles_root, stamp, slug, collections, tiles=None, complete=True,
             descriptor=True):
    """Write tiles_root/<stamp>/ and return it.

    collections: (dataset, collection) pairs the tileset carries.
    tiles: {quadkey: [tile record, ...]}. A quadkey with no records still gets
    a record_tiles row, so the manifest knows the tile.
    complete=False omits manifest.json; descriptor=False omits tileset.json.
    """
    tiles = tiles or {}
    run_dir = tiles_root / stamp
    run_dir.mkdir(parents=True)

    entries = []
    for qk, records in tiles.items():
        keys = [r["value"]["origin"][len("urn:atgeo:"):] for r in records]
        entries += [(key, qk) for key in keys or [f"filler:filler:{qk}"]]
        tile = {
            "publishedAt": PUBLISHED_AT,
            "attribution": [{"dataset": d, "collection": c} for d, c in collections],
            "records": records,
        }
        validate_def("org.atgeo.tilePayload", tile)
        (run_dir / qk[:6]).mkdir(exist_ok=True)
        with gzip.open(run_dir / qk[:6] / f"{qk}.json.gz", "wt") as f:
            json.dump(tile, f)

    con = duckdb.connect(str(run_dir / "manifest.duckdb"))
    try:
        con.execute("CREATE TABLE record_tiles (key VARCHAR, tile_qk VARCHAR)")
        for key, qk in entries:
            con.execute("INSERT INTO record_tiles VALUES (?, ?)", [key, qk])
    finally:
        con.close()

    if descriptor:
        tileset = {
            "slug": slug,
            "publishedAt": PUBLISHED_AT,
            "collections": [
                {"dataset": d, "collection": c,
                 "recordCount": sum(1 for key, _ in entries if key.startswith(f"{d}:{c}:"))}
                for d, c in collections
            ],
            "references": [],
        }
        if entries:
            tileset["extent"] = EXTENT
        # Mirror the producer: the descriptor counts distinct manifest keys in
        # the overview band (tiles shorter than 6), fillers included.
        band_keys = {key for key, qk in entries if len(qk) < 6}
        if band_keys:
            tileset["overview"] = {"records": len(band_keys)}
        validate_def("org.atgeo.defs#tileset", tileset)
        (run_dir / "tileset.json").write_text(json.dumps(tileset))
    if complete:
        (run_dir / "manifest.json").write_text(json.dumps({"complete": True}))
    return run_dir


def point_current(tiles_root, run_dir):
    """(Re)point tiles_root/current at run_dir as a symlink, so the stamp a
    server resolves is never the literal string 'current'."""
    current = tiles_root / "current"
    if current.exists():
        current.unlink()
    current.symlink_to(run_dir, target_is_directory=True)
    return current


def tileset_config(current, slug):
    return {"run_dir": str(current), "base_url": f"{TILE_BASE}/{slug}"}


def serve_run(tmp_path, slug="overture-place", collections=((OVERTURE, "place"),),
              tiles=None, subdir=None, **kwargs):
    """Write one run under tmp_path, point 'current' at it, and return its
    tiles config entry."""
    tiles_root = tmp_path / (subdir or slug) / "tiles"
    run_dir = make_run(tiles_root, STAMP, slug, collections, tiles, **kwargs)
    return tileset_config(point_current(tiles_root, run_dir), slug)


def tile_url(slug, qk):
    return f"{TILE_BASE}/{slug}/{STAMP}/{qk[:6]}/{qk}.json"


def build_app(*tilesets, max_tiles=None):
    tiles_config = {"tilesets": list(tilesets)}
    if max_tiles is not None:
        tiles_config["max_tiles"] = max_tiles
    with patch("garganorn.__main__.load_config") as mock_load:
        mock_load.return_value = ("places.atgeo.org", tiles_config, PUBLIC_BASE)
        app = create_app()
    app.config["TESTING"] = True
    return app
