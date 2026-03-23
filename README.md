# Garganorn

Garganorn is intended to be a test bed for experimenting with adding location data to the ATmosphere.

Currently, the project implements an ATProtocol XRPC server designed to serve static location datasets ("gazetteers").

**WARNING: This code has not been formally released and interfaces WILL change without warning. YMMV. Patches welcome.**

The project is named after the earliest recorded [mammoth goose](https://en.wikipedia.org/wiki/Garganornis).

![Garganornis ballmanni](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c5/Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg/374px-Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg)

## Configuration

Garganorn loads its data sources from a YAML config file. By default it looks for `config.yaml` in the current directory, or you can set the `GARGANORN_CONFIG` environment variable to point elsewhere.

```yaml
repo: gazetteer.social
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

The import scripts also build a `name_index` table used for text search. If `db/density.parquet` and/or `db/category_idf.parquet` exist at import time, places are assigned importance scores for ranking. If absent, importance defaults to 0 and text search still works.

## Building the density and IDF tables

The density and category IDF tables are optional artifacts used to rank text search results. Neither is required — if absent, text search works with all results at equal importance.

To build the density table, pass either `fsq` or `overture` to indicate which global dataset to use:

```
$ scripts/build-density.sh fsq
```

To build the category IDF table:

```
$ scripts/build-idf.sh all
```

The `all` option processes both Foursquare and Overture categories. You can also pass `fsq` or `overture` individually.

Both scripts produce versioned parquet files in `db/` with symlinks (`density.parquet`, `category_idf.parquet`). Rebuilding is rarely needed — global density patterns and category distributions change slowly.

See [`docs/s2_duckdb_design.md`](docs/s2_duckdb_design.md) for design details.

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

The collection name for each data source is set by the database class. For Foursquare OSP it's `community.lexicon.location.com.foursquare.places`; for Overture Maps it's `community.lexicon.location.org.overturemaps.places`.

### searchRecords

Search by location:
```
$ curl 'http://127.0.0.1:8000/xrpc/community.lexicon.location.searchRecords?collection=community.lexicon.location.com.foursquare.places&latitude=37.776145&longitude=-122.433898&limit=1'
```

Or by name:
```
$ curl 'http://127.0.0.1:8000/xrpc/community.lexicon.location.searchRecords?collection=community.lexicon.location.com.foursquare.places&q=Alamo+Square&limit=1'
```

Result:
```json
{
  "records": [
    {
      "$type": "community.lexicon.location.searchRecords#record",
      "distance_m": 0,
      "uri": "https://gazetteer.social/community.lexicon.location.com.foursquare.places/4460d38bf964a5200a331fe3",
      "value": {
        "$type": "community.lexicon.location.place",
        "collection": "community.lexicon.location.com.foursquare.places",
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
      "collection": "community.lexicon.location.com.foursquare.places",
      "latitude": "37.776145",
      "longitude": "-122.433898",
      "limit": 1,
      "repo": "gazetteer.social"
    },
    "elapsed_ms": 161
  }
}
```

### getRecord

```
$ curl 'http://127.0.0.1:8000/xrpc/com.atproto.repo.getRecord?repo=gazetteer.social&collection=community.lexicon.location.com.foursquare.places&rkey=4460d38bf964a5200a331fe3'
```

Result:
```json
{
  "uri": "https://gazetteer.social/community.lexicon.location.com.foursquare.places/4460d38bf964a5200a331fe3",
  "value": {
    "$type": "community.lexicon.location.place",
    "collection": "community.lexicon.location.com.foursquare.places",
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
      "collection": "community.lexicon.location.com.foursquare.places",
      "repo": "gazetteer.social",
      "rkey": "4460d38bf964a5200a331fe3"
    },
    "elapsed_ms": 5
  }
}
```

## Proposed Lexicon schemas

* [`community.lexicon.location.place`](garganorn/lexicon/place.json)
* [`community.lexicon.location.searchRecords`](garganorn/lexicon/searchRecords.json)
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
