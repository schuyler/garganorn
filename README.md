# Garganorn

Garganorn is intended to be a test bed for experimenting with adding location data to the ATmosphere.

Currently, the project implements an ATProtocol XRPC server designed to serve static location datasets ("gazetteers"). 

**WARNING: This code has not been formally released and interfaces WILL change without warning. YMMV. Patches welcome.**

The project is named after the earliest recorded [mammoth goose](https://en.wikipedia.org/wiki/Garganornis).

![Garganornis ballmanni](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c5/Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg/374px-Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg)

## Data sources

Right now, Garganorn supports either [Foursquare Open Source Places](https://docs.foursquare.com/data-products/docs/fsq-places-open-source) or [Overture Maps](https://overturemaps.org/) as a data source. You will need to modify `garganorn/__main__.py` if you want to use Foursquare OSP instead of Overture.

Look in [`scripts/import-fsq-extract.sh`](scripts/import-fsq-extract.sh) and [`scripts/import-overture-extract.sh`](scripts/import-overture-extract.sh) for examples of how to import data. Example:

```
$ scripts/import-overture-extract.sh -122.5137 37.7099 -122.3785 37.8101
```

Building one of these databases takes a few minutes for a reasonable bounding box on a reasonable machine with a reasonable Internet connection. You must build one of these databases locally for the service to have data to serve.

## Running the server

Install and start a Flask server on `localhost:5000`:

```
pip install .
python garganorn 
```

## Querying the XRPC service

### getRecord

Query:
```
curl 'http://127.0.0.1:5000/xrpc/com.atproto.repo.getRecord?repo=repo.local&collection=org.overturemaps.id&rkey=08f2830829d8c099036c7f5f8bba30ec'
```

Result:
```
{
  "uri": "at://geo.schuyler.info/org.overturemaps.id/08f2830829d8c099036c7f5f8bba30ec",
  "value": {
    "$type": "info.schuyler.geo.place",
    "name": "Full House Picnic Site",
    "location": {
      "$type": "info.schuyler.geo.place#location",
      "latitude": "37.776077",
      "longitude": "-122.433400"
    },
    "attributes": {
      "addresses": [
        {
          "country": "US",
          "freeform": null,
          "locality": "San Francisco",
          "postcode": null,
          "region": "CA"
        }
      ],
      "categories": {
        "alternate": [
          "attractions_and_activities",
          "public_plaza"
        ],
        "primary": "park"
      },
      "id": "08f2830829d8c099036c7f5f8bba30ec",
      "names": {
        "common": null,
        "primary": "Full House Picnic Site",
        "rules": null
      }
    }
  },
  "_query": {
    "elapsed_ms": 3,
    "parameters": {
      "collection": "org.overturemaps.id",
      "repo": "repo.local",
      "rkey": "08f2830829d8c099036c7f5f8bba30ec"
    }
  }
}
```

### listNearestRecords

Query:
```
$ curl 'http://127.0.0.1:5000/xrpc/info.schuyler.geo.listNearestRecords?latitude=37.776145&longitude=-122.433898&limit=1'
```

Result:
```
{
  "records": [
    {
      "$type": "info.schuyler.geo.listNearestRecords#record",
      "distance_m": 56,
      "uri": "at://geo.schuyler.info/org.overturemaps.id/08f2830829d8c099036c7f5f8bba30ec",
      "value": {
        "$type": "info.schuyler.geo.place",
        "name": "Full House Picnic Site",
        "location": {
          "$type": "info.schuyler.geo.place#location",
          "latitude": "37.776077",
          "longitude": "-122.433400"
        },
        "attributes": {
          "addresses": [
            {
              "country": "US",
              "freeform": null,
              "locality": "San Francisco",
              "postcode": null,
              "region": "CA"
            }
          ],
          "categories": {
            "alternate": [
              "attractions_and_activities",
              "public_plaza"
            ],
            "primary": "park"
          },
          "id": "08f2830829d8c099036c7f5f8bba30ec",
          "names": {
            "common": null,
            "primary": "Full House Picnic Site",
            "rules": null
          }
        }
      }
    }
  ],
  "_query": {
    "elapsed_ms": 10,
    "parameters": {
      "collection": "org.overturemaps.id",
      "latitude": "37.776145",
      "limit": 1,
      "longitude": "-122.433898",
      "repo": "geo.schuyler.info"
    }
  }
}
```

## Lexicon schemas

* [`info.schuyler.geo.place`](garganorn/lexicon/place.json)
* [`info.schuyler.geo.listNearestRecords`](garganorn/lexicon/listNearestRecords.json)

## Lexicon dependencies
* `com.atproto.repo.getRecord`

## Development

As aforementioned, this project is under development and should not be used for production purposes. I intend to try to track the work of the lexicon.community ATGeo working group as it evolves.

Patches are extremely welcome.

Come find us in the BlueSky API Touchers Discord.

## License etc.

It's MIT licensed, yo. See [LICENSE](LICENSE) for details. If it breaks, you get to keep the pieces.