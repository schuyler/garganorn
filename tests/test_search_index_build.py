"""The chunked search index builder (#70) writes exactly what the frozen
pre-#70 builder wrote: both tables, row for row, in physical order."""

import gzip
import json
import random
from pathlib import Path

import duckdb

import garganorn.stages as stages
from garganorn.stages import write_search_index

REFERENCE_SQL = Path(__file__).parent / "fixtures" / "search_index_reference.sql"
CHUNK_ROWS = 40

SYLLABLES = [c + v for c in "bdfgklmnprstvz" for v in "aeiou"]


def _place(key, name, importance, *, variants=(), locations=None):
    return {
        "importance": importance,
        "value": {
            "$type": "org.atgeo.place",
            "origin": f"urn:atgeo:{key}",
            "name": name,
            "variants": [{"name": v} for v in variants],
            "locations": locations if locations is not None else [],
        },
    }


def _geo(lon, lat):
    return [{"$type": "community.lexicon.location.geo",
             "latitude": str(lat), "longitude": str(lon)}]


def _write_tiles(run_dir):
    rng = random.Random(70)
    tiles = {}  # quadkey -> records
    qks = [f"{n:06d}{n % 4}" for n in range(0, 600, 97)]  # several tile files

    special = [
        # bbox-only place: no geo location at all
        _place("osm:place:bbox1", "Bboxville", 5, locations=[{
            "$type": "community.lexicon.location.bbox",
            "west": "-10.5", "south": "20.25", "east": "-9.5", "north": "21.75"}]),
        _place("osm:place:acc1", "Café Zürich Östra", 3, variants=["Cafe Zurich"],
               locations=_geo(8.54, 47.37)),
        _place("osm:place:cyr1", "Москва Красная", 9, variants=["Moskva"],
               locations=_geo(37.61, 55.75)),
        _place("osm:place:grk1", "ΟΔΟΣ Ὀδυσσεύς", 9, locations=_geo(23.72, 37.98)),
        _place("osm:place:cjk1", "東京タワー 東京", 4, variants=["Tokyo Tower"],
               locations=_geo(139.74, 35.66)),
        _place("osm:place:lrm1", "‎Leftmark", 2, locations=_geo(1.5, 2.5)),
        _place("osm:place:shy1", "­Softhyphen", 2, variants=["­Soft"],
               locations=_geo(-1.5, -2.5)),
        _place("osm:place:dot1", ".", 1, locations=_geo(0.5, 0.5)),
        _place("osm:place:apo1", "O'Brien’s Place", 1, locations=_geo(-6.26, 53.35)),
    ]

    bulk = []
    for i in range(260):
        n = rng.choice([1, 2, 3])
        name = " ".join("".join(rng.choice(SYLLABLES) for _ in range(rng.choice([2, 3])))
                        for _ in range(n))
        variants = [name.upper() + " x"] if i % 5 == 0 else []
        bulk.append(_place(f"osm:place:r{i:04d}", name.title(), rng.choice([0, 1, 1, 2, 5, 5, 8]),
                           variants=variants,
                           locations=_geo(round(rng.uniform(-179, 179), 4),
                                          round(rng.uniform(-80, 80), 4))))

    records = special + bulk
    for i, rec in enumerate(records):
        tiles.setdefault(qks[i % len(qks)], []).append(rec)
    # a few records repeated byte-identically in a second tile
    for i in (0, 2, 5, 20, 41):
        other = qks[(i + 1) % len(qks)]
        tiles[other].append(json.loads(json.dumps(records[i])))

    for qk, recs in tiles.items():
        (run_dir / qk[:6]).mkdir(exist_ok=True)
        with gzip.open(run_dir / qk[:6] / f"{qk}.json.gz", "wt") as f:
            json.dump({"publishedAt": "2026-01-01T00:00:00Z",
                       "attribution": [], "records": recs}, f, ensure_ascii=False)


def _build_reference(run_dir, out_path):
    import glob
    import os
    tiles = sorted(glob.glob(os.path.join(run_dir, "*", "*.json.gz")))
    schema, fill = REFERENCE_SQL.read_text().split("-- fill\n")
    con = duckdb.connect(str(out_path))
    try:
        con.execute(schema)
        con.execute(fill.replace("${tiles}", repr(tiles)))
    finally:
        con.close()


def _dump(path):
    con = duckdb.connect(str(path), read_only=True)
    try:
        return (con.execute("SELECT * FROM search_records").fetchall(),
                con.execute("SELECT * FROM search_blocks").fetchall())
    finally:
        con.close()


def test_chunked_build_matches_reference(tmp_path, monkeypatch):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _write_tiles(run_dir)

    ref_path = tmp_path / "reference.duckdb"
    _build_reference(run_dir, ref_path)
    ref_records, ref_blocks = _dump(ref_path)

    # Precondition: the walk must cut at least three chunks, and no block is
    # bigger than a chunk (an oversized block would be a chunk of its own).
    per_block = {}
    for row in ref_blocks:
        per_block[row[0]] = per_block.get(row[0], 0) + 1
    assert len(ref_blocks) > 3 * CHUNK_ROWS
    assert max(per_block.values()) <= CHUNK_ROWS

    monkeypatch.setattr(stages, "SEARCH_CHUNK_ROWS", CHUNK_ROWS)
    write_search_index(str(run_dir))
    new_records, new_blocks = _dump(run_dir / "search.duckdb")

    assert new_records == ref_records
    assert new_blocks == ref_blocks
