# Garganorn

Garganorn is intended to be a test bed for experimenting with adding location data to the ATmosphere.

Currently, the project implements an ATProtocol XRPC server designed to serve static location datasets ("gazetteers").

**WARNING: This code has not been formally released and interfaces WILL change without warning. YMMV. Patches welcome.** There are no users yet — nothing depends on this in production.

The project is named after the earliest recorded [mammoth goose](https://en.wikipedia.org/wiki/Garganornis).

![Garganornis ballmanni](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c5/Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg/374px-Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg)

## What it serves

Three collections today: [Overture Maps](https://overturemaps.org/) places (`org.atgeo.places.overture.place`), [OpenStreetMap](https://www.openstreetmap.org/) (`org.atgeo.places.osm`), and Overture administrative divisions (`org.atgeo.places.overture.division`). Tiles are pre-built and stored gzip-compressed on disk, but served as plain JSON — wire compression is negotiated separately via `Accept-Encoding`. There's no live search or query-by-name; you discover tiles for a bounding box and fetch them.

## Configuration

Garganorn loads its data sources from a YAML config file. By default it looks for `config.yaml` in the current directory, or you can set the `GARGANORN_CONFIG` environment variable to point elsewhere. Copy [`config.yaml.example`](config.yaml.example) to `config.yaml` and edit it for your environment — `config.yaml` is gitignored since it's deployment-specific.

```yaml
repo: places.atgeo.org
tiles:
  collections:
    org.atgeo.places.overture.place:
      slug: overture-place
      manifest: data/overture_place/tiles/current/manifest.duckdb
      tiles_dir: data/overture_place/tiles/current
      base_url: https://places.atgeo.org/tiles/overture-place
      source: https://overturemaps.org/
      license: https://docs.overturemaps.org/attribution/
  max_coverage_tiles: 50
```

`source` and `license` aren't validated — `load_config` just parses the YAML, and omitting either yields an empty string, no error. They matter for `getRecord`: `TileBackedCollection` puts these exact config values in every record's envelope. Tile file headers carry a different source/license, hardcoded per source class in `garganorn/database.py`, independent of this config.

## Getting source data

Garganorn builds tiles from local parquet, so you need to fetch that first.

Overture Maps (places + administrative divisions, auto-discovers the latest release):

```
scripts/download-overture.sh --cache-dir db/cache/overture
```

OpenStreetMap comes from Geofabrik as a `.osm.pbf` extract, then gets filtered and converted to parquet (requires [`osmium`](https://osmcode.org/osmium-tool/) and [`osm-pbf-parquet`](https://github.com/OvertureMaps/osm-pbf-parquet) on your `PATH`):

```
scripts/download-osm.sh --region north-america/us-northeast --cache-dir db/cache/osm
scripts/extract-osm-parquet.sh db/cache/osm/us-northeast-latest.osm.pbf --cache-dir db/cache/osm
```

Use a smaller Geofabrik region for local testing — the default `north-america` extract is large.

## Tile export pipeline

`python -m garganorn.quadtree` builds quadtree tile exports from parquet data. It takes a subcommand: `run` builds one source end to end, `all` builds every source named in a config file, and `density`, `idf`, and `covering` build the shared artifacts the sources depend on. Invoking it with no subcommand is an error — use `run`.

Each source produces a timestamped directory of gzipped JSON tile files under `<output>/<source>/tiles/<timestamp>/`, with a `<output>/<source>/tiles/current` symlink pointing to the latest run. Each stage also writes parquet artifacts alongside — `places.parquet`, `tile_assignments.parquet`, `tile_references.parquet` (division only, expanding the grid to every tile a division's geometry overlaps), `summary_tile_assignments.parquet` and `summary_tile_references.parquet` (division only) for a z1–z5 summary tile band of top-ranked places, `tile_assignments_combined.parquet` (the union that feeds containment, export, and the manifest), `containment/` — and skips itself when that artifact is still newer than its inputs, so re-running only rebuilds what changed. Pass `--force` to rebuild regardless.

Supported sources (for `run --source`):

| `--source` | Input | Collection |
|---|---|---|
| `overture_place` | `--parquet <glob>` | `org.atgeo.places.overture.place` |
| `osm` | `--parquet-dir <dir>` | `org.atgeo.places.osm` |
| `overture_division` | `--division-parquet <path> --division-area-parquet <path>` | `org.atgeo.places.overture.division` |

### overture_division

Imports Overture Maps administrative boundaries from the `division` and `division_area` parquet themes. Produces two outputs:

- **Tile files** under `<output>/overture_division/tiles/current/` — one gzipped JSON file per quadtree tile, each record carrying a `community.lexicon.location.bbox` location and attributes (subtype, country, region, level, wikidata, population).
- **`boundaries.duckdb`** at `<output>/overture_division/boundaries.duckdb` — a DuckDB file with an R-tree spatial index for point-in-polygon containment queries, alongside a `covering/` directory of quadtree-to-boundary index parquet. Used by other sources' tile pipelines via `--boundaries`.

```
python -m garganorn.quadtree run \
  --source overture_division \
  --division-parquet db/cache/overture/*/division/*.parquet \
  --division-area-parquet db/cache/overture/*/division_area/*.parquet \
  --output data
```

To enrich another source's tiles with division containment (adds `relations.within` to each record), the `overture_division` run above must have already produced `covering/` next to `boundaries.duckdb` — `--boundaries` fails loud if it's missing or stale:

```
python -m garganorn.quadtree run \
  --source overture_place \
  --parquet 'db/cache/overture/*/part-*.parquet' \
  --boundaries data/overture_division/boundaries.duckdb \
  --output data
```

Optional arguments (`run`, all sources):

| Argument | Default | Description |
|---|---|---|
| `--bbox XMIN YMIN XMAX YMAX` | none | Restrict import to a bounding box |
| `--memory-limit` | `48GB` | DuckDB memory limit |
| `--max-per-tile` | `1000` | Maximum records per tile in the assignment grid, every source. It does not bound a division tile's exported record count, which follows geometry |
| `--temp-directory` | DuckDB default | Volume DuckDB spills to when a stage exceeds `--memory-limit` |
| `--max-temp-directory-size` | `250GB` | Ceiling on that spill; a runaway query fails with "temp directory full" instead of filling the volume |
| `--export-workers` | unset (`ThreadPoolExecutor` default: `min(32, cpu_count + 4)`) | Threads for tile gzip compression |
| `--force` | off | Rebuild every stage, ignoring artifact freshness |
| `--config` | none | YAML config file; `run` reads `memory_limit`, `max_per_tile`, `temp_directory`, and `max_temp_directory_size` from the `pipeline:` section (`all` reads the rest) |

Point `--temp-directory` at a volume with room to spare: a global import spills
tens of gigabytes, and left unset DuckDB spills wherever its default lands —
which may be the root filesystem. `--max-temp-directory-size` bounds the damage
when a query spills more than expected.

To build every configured source in one shot:

```
python -m garganorn.quadtree all --config config.yaml
```

`all` runs, in order: the shared density extract, per-source category IDF, `overture_division`, then the remaining sources. It reads the `pipeline:` section of the config (paths, `memory_limit`, `max_per_tile`, `temp_directory`, `max_temp_directory_size`, `bbox`, and the per-source inputs); the `tiles:` section is server-side config only.

## Running the server

Install and start a Flask dev server on `localhost:8000`:

```
pip install -e .
python -m garganorn
```

For production, use gunicorn:

```
gunicorn "garganorn.__main__:create_app()" --bind 0.0.0.0:8000 --workers 2
```

## Querying the XRPC service

There's no search — you ask for tile coverage over a bounding box, then fetch the tiles directly. Bounding boxes must be snapped to a 0.01° grid (that's the privacy model: coarse enough that the server can't infer a client's precise location from requests).

### getCoverage

```
$ curl 'http://127.0.0.1:8000/xrpc/org.atgeo.getCoverage?collection=org.atgeo.places.osm&bbox=-71.55,43.20,-71.50,43.25'
{"tiles":["https://places.atgeo.org/tiles/osm/20260808T061621/030233/03023303220.json.gz","https://places.atgeo.org/tiles/osm/20260808T061621/030233/03023303221.json.gz"]}
```

Fetch one of those URLs directly — despite the `.json.gz` name, the server always decompresses it and returns plain JSON (wire compression, if any, is negotiated separately via `Accept-Encoding` and handled transparently by your HTTP client); source/license are included in the header so you don't need a separate lookup. OSM rkeys carry one of three prefixes — `node:`, `way:`, `relation:` — naming the source OSM element type:

```
$ curl -s 'https://places.atgeo.org/tiles/osm/20260808T061621/030233/03023303220.json.gz'
{
  "collection": "org.atgeo.places.osm",
  "source": "https://www.openstreetmap.org/",
  "license": "https://opendatacommons.org/licenses/odbl/1-0/",
  "generated_at": "2026-08-08T06:16:21Z",
  "records": [
    {
      "uri": "https://places.atgeo.org/org.atgeo.places.osm/node:10080395917",
      "cid": null,
      "value": {
        "$type": "org.atgeo.place",
        "rkey": "node:10080395917",
        "name": "HomeGoods",
        "importance": 0,
        "locations": [
          {"$type": "community.lexicon.location.geo", "latitude": "43.288557", "longitude": "-71.575664"}
        ],
        "variants": [],
        "attributes": {
          "addr:city": "Concord",
          "addr:street": "Merchants Way",
          "shop": "houseware"
        },
        "relations": {}
      }
    }
    // ... more records ...
  ]
}
```

### getRecord

Look up a single record by collection + rkey instead:

```
$ curl 'http://127.0.0.1:8000/xrpc/com.atproto.repo.getRecord?repo=places.atgeo.org&collection=org.atgeo.places.osm&rkey=node:10080395917'
{
  "uri": "https://places.atgeo.org/org.atgeo.places.osm/node:10080395917",
  "source": "https://www.openstreetmap.org/",
  "license": "https://opendatacommons.org/licenses/odbl/1-0/",
  "importance": 0,
  "value": {
    "$type": "org.atgeo.place",
    "rkey": "node:10080395917",
    "name": "HomeGoods",
    "locations": [
      {"$type": "community.lexicon.location.geo", "latitude": "43.288557", "longitude": "-71.575664"}
    ],
    "variants": [],
    "attributes": {"addr:city": "Concord", "addr:street": "Merchants Way", "shop": "houseware"},
    "relations": {}
  },
  "_query": {"parameters": {"repo": "places.atgeo.org", "collection": "org.atgeo.places.osm", "rkey": "node:10080395917"}, "elapsed_ms": 6}
}
```

## Proposed Lexicon schemas

* [`org.atgeo.place`](garganorn/lexicon/place.json)
* [`community.lexicon.location.geo`](garganorn/lexicon/geo.json)
* [`community.lexicon.location.address`](garganorn/lexicon/address.json)

**NOTE**: These schemas are only *proposed*, and have not been adopted by the Lexicon community.

## Lexicon dependencies

* `com.atproto.repo.getRecord`

## Development

As aforementioned, this project is under development and should not be used for production purposes. I intend to try to track the work of the lexicon.community ATGeo working group as it evolves.

Patches are extremely welcome.

Come find us in the BlueSky API Touchers Discord.

## License etc.

It's MIT licensed, yo. See [LICENSE](LICENSE) for details. If it breaks, you get to keep the pieces.
