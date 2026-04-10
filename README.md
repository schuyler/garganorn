# Garganorn

Garganorn is intended to be a test bed for experimenting with adding location data to the ATmosphere.

Currently, the project implements an ATProtocol XRPC server designed to serve static location datasets ("gazetteers").

**WARNING: This code has not been formally released and interfaces WILL change without warning. YMMV. Patches welcome.**

The project is named after the earliest recorded [mammoth goose](https://en.wikipedia.org/wiki/Garganornis).

![Garganornis ballmanni](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c5/Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg/374px-Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg)

## Configuration

Garganorn loads its data sources from a YAML config file. By default it looks for `config.yaml` in the current directory, or you can set the `GARGANORN_CONFIG` environment variable to point elsewhere.

```yaml
repo: places.atgeo.org
databases:
  - type: foursquare
    path: db/fsq-osp.duckdb
  - type: overture
    path: db/overture-maps.duckdb
```

Supported database types are `foursquare` ([Foursquare Open Source Places](https://docs.foursquare.com/data-products/docs/fsq-places-open-source)) and `overture` ([Overture Maps](https://overturemaps.org/)). You can configure one or both. Paths are relative to the working directory.

## Data import

Look in [`scripts/import-fsq-extract.sh`](scripts/import-fsq-extract.sh) and [`scripts/import-overture-extract.sh`](scripts/import-overture-extract.sh) for examples of how to import data. Example:

```
$ scripts/import-fsq-extract.sh -122.5137 37.7099 -122.3785 37.8101
```

Building one of these databases takes a few minutes for a reasonable bounding box on a reasonable machine with a reasonable Internet connection. You must build at least one database locally for the service to have data to serve.

The import scripts also build a `name_index` table used for text search. If `db/density.parquet` exists at import time, places are assigned density-based importance scores for ranking. Category IDF is computed inline during import from the places table itself. If the density file is absent, importance defaults to 0 and text search still works.

The density table is an optional artifact built separately from a global places dataset. To build it, pass either `fsq` or `overture`:

```
$ scripts/build-density.sh fsq
```

This produces a versioned parquet file in `db/` with a symlink (`density.parquet`). Rebuilding is rarely needed — global density patterns change slowly.

See [`docs/s2_duckdb_design.md`](docs/s2_duckdb_design.md) for design details.

## Tile export pipeline

`python -m garganorn.quadtree` builds quadtree tile exports from parquet data. Each source produces a timestamped directory of gzipped JSON tile files under `<output>/<source>/`, with a `current` symlink pointing to the latest run.

Supported sources:

| `--source` | Input | Collection |
|---|---|---|
| `fsq` | `--parquet <glob>` | `org.atgeo.places.foursquare` |
| `overture` | `--parquet <glob>` | `org.atgeo.places.overture.place` |
| `osm` | `--parquet-dir <dir>` | `org.atgeo.places.osm` |
| `overture_division` | `--division-parquet <path> --division-area-parquet <path>` | `org.atgeo.places.overture.division` |

### overture_division

Imports Overture Maps administrative boundaries from the `division` and `division_area` parquet themes. Produces two outputs:

- **Tile files** under `<output>/overture_division/current/` — one gzipped JSON file per quadtree tile, each record carrying a `community.lexicon.location.bbox` location and attributes (subtype, country, region, admin_level, wikidata, population).
- **`boundaries.duckdb`** at `<output>/overture_division/boundaries.duckdb` — a DuckDB file with an R-tree spatial index for point-in-polygon containment queries. Used by the venue tile pipeline via `--boundaries`.

```
python -m garganorn.quadtree \
  --source overture_division \
  --division-parquet /data/overture/division.parquet \
  --division-area-parquet /data/overture/division_area.parquet \
  --output /srv/tiles
```

To enrich another source's tiles with division containment (adds `relations.within` to each record):

```
python -m garganorn.quadtree \
  --source overture \
  --parquet '/data/overture/places/*.parquet' \
  --boundaries /srv/tiles/overture_division/boundaries.duckdb \
  --output /srv/tiles
```

Optional arguments (all sources):

| Argument | Default | Description |
|---|---|---|
| `--bbox XMIN YMIN XMAX YMAX` | none | Restrict import to a bounding box |
| `--memory-limit` | `48GB` | DuckDB memory limit |
| `--max-per-tile` | `1000` | Maximum records per tile |
| `--export-workers` | CPU count | Threads for tile gzip compression |
| `--config` | none | YAML config file (can set `tiles.memory_limit`, `tiles.max_per_tile`, `tiles.boundaries`) |

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

The collection name for each data source is set by the database class. For Foursquare OSP it's `org.atgeo.places.foursquare`; for Overture Maps it's `org.atgeo.places.overture.place`.

### searchRecords

Search by location:
```
$ curl 'http://127.0.0.1:8000/xrpc/org.atgeo.searchRecords?collection=org.atgeo.places.foursquare&latitude=37.776145&longitude=-122.433898&limit=1'
```

Or by name:
```
$ curl 'http://127.0.0.1:8000/xrpc/org.atgeo.searchRecords?collection=org.atgeo.places.foursquare&q=Alamo+Square&limit=1'
```

Result:
```json
{
  "records": [
    {
      "$type": "org.atgeo.searchRecords#record",
      "distance_m": 0,
      "uri": "https://places.atgeo.org/org.atgeo.places.foursquare/4460d38bf964a5200a331fe3",
      "value": {
        "$type": "org.atgeo.place",
        "collection": "org.atgeo.places.foursquare",
        "rkey": "4460d38bf964a5200a331fe3",
        "names": [
          {"text": "Alamo Square", "priority": 0}
        ],
        "locations": [
          {
            "$type": "community.lexicon.location.geo",
            "latitude": "37.776146",
            "longitude": "-122.433898"
          },
          {
            "$type": "community.lexicon.location.address",
            "country": "US",
            "region": "CA",
            "locality": "San Francisco",
            "postalCode": "94117",
            "street": "Steiner St"
          }
        ],
        "attributes": {
          "fsq_place_id": "4460d38bf964a5200a331fe3",
          "fsq_category_labels": [
            "Landmarks and Outdoors > Park",
            "Landmarks and Outdoors > Park > Playground",
            "Landmarks and Outdoors > Park > Dog Park"
          ],
          "tel": "(415) 831-2700",
          "website": "http://sfrecpark.org/alamo-square"
        }
      }
    }
  ],
  "_query": {
    "parameters": {
      "collection": "org.atgeo.places.foursquare",
      "latitude": "37.776145",
      "longitude": "-122.433898",
      "limit": 1,
      "repo": "places.atgeo.org"
    },
    "elapsed_ms": 161
  }
}
```

### getRecord

```
$ curl 'http://127.0.0.1:8000/xrpc/com.atproto.repo.getRecord?repo=places.atgeo.org&collection=org.atgeo.places.foursquare&rkey=4460d38bf964a5200a331fe3'
```

Result:
```json
{
  "uri": "https://places.atgeo.org/org.atgeo.places.foursquare/4460d38bf964a5200a331fe3",
  "value": {
    "$type": "org.atgeo.place",
    "collection": "org.atgeo.places.foursquare",
    "rkey": "4460d38bf964a5200a331fe3",
    "names": [
      {"text": "Alamo Square", "priority": 0}
    ],
    "locations": [
      {
        "$type": "community.lexicon.location.geo",
        "latitude": "37.776146",
        "longitude": "-122.433898"
      },
      {
        "$type": "community.lexicon.location.address",
        "country": "US",
        "region": "CA",
        "locality": "San Francisco",
        "postalCode": "94117",
        "street": "Steiner St"
      }
    ],
    "attributes": {
      "fsq_place_id": "4460d38bf964a5200a331fe3",
      "fsq_category_labels": [
        "Landmarks and Outdoors > Park",
        "Landmarks and Outdoors > Park > Playground",
        "Landmarks and Outdoors > Park > Dog Park"
      ],
      "tel": "(415) 831-2700",
      "website": "http://sfrecpark.org/alamo-square"
    }
  },
  "_query": {
    "parameters": {
      "collection": "org.atgeo.places.foursquare",
      "repo": "places.atgeo.org",
      "rkey": "4460d38bf964a5200a331fe3"
    },
    "elapsed_ms": 5
  }
}
```

## Proposed Lexicon schemas

* [`org.atgeo.place`](garganorn/lexicon/place.json)
* [`org.atgeo.searchRecords`](garganorn/lexicon/searchRecords.json)
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
