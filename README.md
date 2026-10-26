# Garganorn

Garganorn is intended to be a test bed for experimenting with adding location data to the ATmosphere.

Currently, the project implements an ATProtocol XRPC server designed to serve static location datasets ("gazetteers").

**WARNING: This code has not been formally released and interfaces WILL change without warning. YMMV. Patches welcome.** There are no users yet — nothing depends on this in production.

The project is named after the earliest recorded [mammoth goose](https://en.wikipedia.org/wiki/Garganornis).

![Garganornis ballmanni](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c5/Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg/374px-Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg)

## What it serves

Three tilesets today, one per source: [Overture Maps](https://overturemaps.org/) places (slug `overture-place`, collection `(overturemaps.org, place)`), [OpenStreetMap](https://www.openstreetmap.org/) (slug `osm`, collections `(openstreetmap.org, node)`, `way` and `relation`), and Overture administrative divisions (slug `overture-division`, collection `(overturemaps.org, division)`). A collection is one sub-division of one dataset; it names the places' URNs (`urn:atgeo:openstreetmap.org:way:{id}`) and is the unit of licensing. Tiles are pre-built and stored gzip-compressed on disk, but served as plain JSON — wire compression is negotiated separately via `Accept-Encoding`. There's no live search or query-by-name; you discover tiles for a bounding box and fetch them.

## Configuration

Garganorn loads its data sources from a YAML config file. By default it looks for `config.yaml` in the current directory, or you can set the `GARGANORN_CONFIG` environment variable to point elsewhere. Copy [`config.yaml.example`](config.yaml.example) to `config.yaml` and edit it for your environment — `config.yaml` is gitignored since it's deployment-specific.

```yaml
repo: places.atgeo.org
tiles:
  tilesets:
    - run_dir: data/overture_place/tiles/current
      base_url: https://places.atgeo.org/tiles/overture-place
  max_tiles: 50
```

`repo` is the server's hostname; it determines the `did:web` identity. `run_dir` is a tile run directory (or the `current` symlink to one); the server reads `manifest.duckdb`, `manifest.json` and `tileset.json` from it and resolves the symlink once, at startup. `max_tiles` caps the tiles in a `getTiles` answer before it falls back to overview tiles; `describeService` advertises it as `maxTiles`.

A tileset's slug is fixed when it is built, stored in its `tileset.json`, and the tileset is served under it: tiles at `/tiles/{slug}/...`, the descriptor at `/tilesets/{slug}`. `base_url` must end in `/{slug}`, or the tile URLs `getTiles` emits would not resolve. Two tilesets with the same slug fail startup.

The optional top-level `public_base` is the server's public base URL, defaulting to `https://{repo}`. It is the `serviceEndpoint` in `did.json` and the prefix of `getPlace`'s `url`, where a place's URN dereferences. A non-https base is accepted only for development hosts (`localhost`, `*.localhost`, or a loopback, private, or link-local IP address); any other `http` base, and any other scheme, fails at startup. To run a local server:

```yaml
repo: places.atgeo.org
public_base: http://localhost:8000
```

Tiles contain no gazetteer host: a tileset can be served from any host, under its slug.

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

The extract script also writes `release` into the parquet directory: the PBF's replication timestamp, which becomes the OSM release in the tileset's attribution. It fails if the PBF header has none.

Use a smaller Geofabrik region for local testing — the default `north-america` extract is large.

## Tile export pipeline

`python -m garganorn.quadtree` builds quadtree tile exports from parquet data. It takes a subcommand: `run` builds one source end to end, `all` builds every source named in a config file, and `density`, `idf`, and `covering` build the shared artifacts the sources depend on. Invoking it with no subcommand is an error — use `run`.

Each source produces a timestamped directory of gzipped JSON tile files under `<output>/<source>/tiles/<timestamp>/`, with a `<output>/<source>/tiles/current` symlink pointing to the latest run. Each stage also writes parquet artifacts alongside — `places.parquet`, `tile_assignments.parquet`, `tile_references.parquet` (division only, expanding the grid to every tile a division's geometry overlaps), `summary_tile_assignments.parquet` and `summary_tile_references.parquet` (division only) for a z1–z5 summary tile band of top-ranked places, `tile_assignments_combined.parquet` (the union that feeds containment, export, and the manifest), `containment/` — and skips itself when that artifact is still newer than its inputs, so re-running only rebuilds what changed. Pass `--force` to rebuild regardless. A run also writes `tileset.json`, the tileset's descriptor, and `manifest.duckdb` beside the tiles; see [`docs/pipeline-artifacts.md`](docs/pipeline-artifacts.md).

Supported sources (for `run --source`):

| `--source` | Input | Collection |
|---|---|---|
| `overture_place` | `--parquet <glob>` | `(overturemaps.org, place)` |
| `osm` | `--parquet-dir <dir>` | `(openstreetmap.org, node)`, `(openstreetmap.org, way)`, `(openstreetmap.org, relation)` |
| `overture_division` | `--division-parquet <path> --division-area-parquet <path>` | `(overturemaps.org, division)` |

### overture_division

Imports Overture Maps administrative boundaries from the `division` and `division_area` parquet themes. Produces two outputs:

- **Tile files** under `<output>/overture_division/tiles/current/` — one gzipped JSON file per quadtree tile, each record carrying a `community.lexicon.location.bbox` location and attributes (subtype, country, region, wikidata, population).
- **`boundaries.duckdb`** at `<output>/overture_division/boundaries.duckdb` — a DuckDB file with an R-tree spatial index for point-in-polygon containment queries, alongside a `covering/` directory of quadtree-to-boundary index parquet. Used by other sources' tile pipelines via `--boundaries`.

```
python -m garganorn.quadtree run \
  --source overture_division \
  --division-parquet db/cache/overture/*/division/*.parquet \
  --division-area-parquet db/cache/overture/*/division_area/*.parquet \
  --release overturemaps.org=2026-09-17.0 \
  --output data
```

To enrich another source's tiles with division containment (adds `relations.within` to each record), the `overture_division` run above must have already produced `covering/` next to `boundaries.duckdb` — `--boundaries` fails loud if it's missing or stale:

```
python -m garganorn.quadtree run \
  --source overture_place \
  --parquet 'db/cache/overture/*/part-*.parquet' \
  --boundaries data/overture_division/boundaries.duckdb \
  --release overturemaps.org=2026-09-17.0 \
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
| `--release DATASET=VALUE` | none | Release of a dataset the tileset carries or references, e.g. `overturemaps.org=2026-09-17.0`; repeatable. Export fails, naming the first dataset left without one |
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

`all` supplies the releases itself. The Overture release is the one the configured globs' `*` release segment resolves to (newest); the OSM release is read from the `release` file `scripts/extract-osm-parquet.sh` writes into the parquet directory. `all` stops with a message if a configured source's release is missing. It runs, in order: the shared density extract, per-source category IDF, `overture_division`, then the remaining sources. It reads the `pipeline:` section of the config (paths, `memory_limit`, `max_per_tile`, `temp_directory`, `max_temp_directory_size`, `bbox`, and the per-source inputs); the `tiles:` section is server-side config only.

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

## Querying the service

There's no search — you ask for tile coverage over a bounding box, then fetch the tiles directly. Bounding boxes must be snapped to a 0.01° grid (that's the privacy model: coarse enough that the server can't infer a client's precise location from requests).

| Endpoint | Returns |
|---|---|
| `GET /xrpc/org.atgeo.getTiles?dataset=&collection=&bbox=` | Tile URLs covering the bbox, given as `west,south,east,north`; `collection` is optional and picks tilesets, not records (an OSM `way` search still downloads the area's nodes and relations; see [the spec](docs/atgeo-spec.md#orgatgeogettiles)) |
| `GET /xrpc/org.atgeo.getPlace?uri=` | One place by its `urn:atgeo:` URN, with its importance and its tileset's attribution |
| `GET /xrpc/org.atgeo.describeTileset?slug=` | A tileset's descriptor: slug, extent, carried and referenced collections with release, licences and attribution |
| `GET /xrpc/org.atgeo.describeService` | The service's DID, its tile budget (`maxTiles`), and the slug and collections of each tileset |
| `GET /tilesets/{slug}` | The same document as `describeTileset` |
| `GET /{dataset}/{collection}/{id}` | The same place as `getPlace` for `urn:atgeo:{dataset}:{collection}:{id}`; this is what `getPlace`'s `url` points at |
| `GET /tiles/{slug}/.../{quadkey}.json` | Tile files, as plain JSON |
| `GET /.well-known/did.json` | The `did:web` document, advertising the tile service as `#atgeo_tiles` (`AtgeoTileService`) at `public_base` |

### Errors

Every error is JSON, `{"error": "<Name>", "message": "<text>"}`, and every response (errors included) carries `Access-Control-Allow-Origin: *`.

**XRPC methods answer errors with HTTP 400.** That's the atproto XRPC convention (and what the SDKs expect), so `PlaceNotFound` is a 400 here, not a 404. Branch on the `error` name, not on the status. These are the errors each method raises:

| Method | Errors (HTTP 400) |
|---|---|
| `getTiles` | `InvalidBbox`, `BboxTooPrecise`, `DatasetNotFound`, `CollectionNotFound`, `BboxTooLarge` |
| `getPlace` | `PlaceNotFound` |
| `describeTileset` | `TilesetNotFound` |
| `describeService` | none |

The XRPC layer (lexrpc, and Flask beneath it) adds errors no lexicon lists: `InvalidRequest` (HTTP 400) for a missing or malformed parameter, a bad NSID, or a request body that isn't valid JSON; `UnsupportedMediaType` (HTTP 415) for a body with a non-JSON `Content-Type`; `MethodNotAllowed` (HTTP 405, with an `Allow` header) for an HTTP method the route doesn't take; and `MethodNotImplemented` (HTTP 501) for an XRPC method the service doesn't have. A 5xx otherwise means the server broke, not that it refused you.

**The plain-HTTP routes answer a miss with HTTP 404**, in the same JSON shape, never an HTML page:

| Route | Miss | `error` |
|---|---|---|
| `GET /{dataset}/{collection}/{id}` | place not served, including a malformed path like a dataset with no `.` in it | `PlaceNotFound` |
| `GET /tilesets/{slug}` | unknown slug | `TilesetNotFound` |
| `GET /tiles/{slug}/...` | unknown slug | `TilesetNotFound` |
| `GET /tiles/{slug}/...` | no such tile; a path not ending in `.json` (including the on-disk `.json.gz` name) or one that tries to escape the tiles directory counts as no such tile | `TileNotFound` |
| `GET /{nsid}` | an NSID-shaped name that isn't a served lexicon | `LexiconNotFound` |

Everything else is JSON too, on `/xrpc/` as well as the plain routes: a path that matches no route (including a single segment that isn't an NSID, like `/favicon.ico`) is a 404 `NotFound`, a wrong method on a route is a 405 `MethodNotAllowed` (with an `Allow` header), and a crash or other 5xx is `InternalServerError` (or `ServiceUnavailable` etc.) with a generic message; details go to the server log, not the response. Note the alias and XRPC differ on a malformed URN: `/{dataset}/{collection}/{id}` gives a 404 `PlaceNotFound`, while `getPlace?uri=` gives a 400 `InvalidRequest` for a string that isn't a URI at all (and a 400 `PlaceNotFound` for a URI that isn't a served place).

### getTiles

```
$ curl 'http://127.0.0.1:8000/xrpc/org.atgeo.getTiles?dataset=openstreetmap.org&collection=way&bbox=-71.55,43.20,-71.50,43.25'
{"tiles":["https://places.atgeo.org/tiles/osm/20260808T061621/030233/03023303220.json","https://places.atgeo.org/tiles/osm/20260808T061621/030233/03023303221.json"]}
```

A bbox covering more than `max_tiles` tiles is answered with coarse overview tiles holding only the most important places, and `"overview": true`; if a selected tileset has no overview tiles, the answer is `BboxTooLarge`.

Fetch one of those URLs directly. Tiles are stored gzipped as `.json.gz`, but the server decompresses them and returns plain JSON at `.json` (wire compression, if any, is negotiated separately via `Accept-Encoding` and handled transparently by your HTTP client). The header carries the attribution of every collection the tileset carries or references, so you don't need a separate lookup. A place's `origin` is its URN; for OSM the collection is `node`, `way` or `relation`, naming the source element type. `importance` sits beside the place, not in it:

```
$ curl -s 'https://places.atgeo.org/tiles/osm/20260808T061621/030233/03023303220.json'
{
  "publishedAt": "2026-08-08T06:16:21Z",
  "attribution": [
    {
      "dataset": "openstreetmap.org",
      "collection": "node",
      "release": "2026-08-07T20:21:15Z",
      "source": "https://www.openstreetmap.org/",
      "licenses": ["https://opendatacommons.org/licenses/odbl/1-0/"],
      "attribution": "https://www.openstreetmap.org/copyright"
    }
    // ... way, relation, and any referenced collection ...
  ],
  "records": [
    {
      "value": {
        "$type": "org.atgeo.place",
        "origin": "urn:atgeo:openstreetmap.org:node:10080395917",
        "name": "HomeGoods",
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
      },
      "importance": 0
    }
    // ... more records ...
  ]
}
```

### getPlace

`getPlace` takes the URN from a record's `origin`; no discovery call is needed. The answer echoes it as `uri`, gives the place's own address on this server as `url`, and carries the tile's `value` with `importance` beside it, plus the tileset's `attribution`, the same list its tiles carry:

```
$ curl 'http://127.0.0.1:8000/xrpc/org.atgeo.getPlace?uri=urn:atgeo:openstreetmap.org:node:10080395917'
{"uri":"urn:atgeo:openstreetmap.org:node:10080395917","url":"https://places.atgeo.org/openstreetmap.org/node/10080395917","importance":0,"value":{...},"attribution":[...]}
```

An unknown or unserved URN is a `PlaceNotFound` error (HTTP 400 over XRPC; 404 at the `/{dataset}/{collection}/{id}` alias).

## Lexicon schemas

* [`org.atgeo.place`](garganorn/lexicon/place.json)
* [`org.atgeo.defs`](garganorn/lexicon/defs.json)
* [`org.atgeo.tilePayload`](garganorn/lexicon/tilePayload.json)
* [`org.atgeo.getTiles`](garganorn/lexicon/getTiles.json)
* [`org.atgeo.getPlace`](garganorn/lexicon/getPlace.json)
* [`org.atgeo.describeTileset`](garganorn/lexicon/describeTileset.json)
* [`org.atgeo.describeService`](garganorn/lexicon/describeService.json)
* [`community.lexicon.location.geo`](garganorn/lexicon/geo.json)
* [`community.lexicon.location.address`](garganorn/lexicon/address.json)
* [`community.lexicon.location.bbox`](garganorn/lexicon/bbox.json)

**NOTE**: These schemas are only *proposed*, and have not been adopted by the Lexicon community. [`docs/lexicon-publication.md`](docs/lexicon-publication.md) is the runbook for publishing the `org.atgeo.*` ones.

## Development

As aforementioned, this project is under development and should not be used for production purposes. I intend to try to track the work of the lexicon.community ATGeo working group as it evolves.

Patches are extremely welcome.

Come find us in the BlueSky API Touchers Discord.

## License etc.

It's MIT licensed, yo. See [LICENSE](LICENSE) for details. If it breaks, you get to keep the pieces.
