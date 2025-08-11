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

Install in development mode and start a Flask server on `localhost:8000`:

```
pip install -e .
flask --app garganorn run --debug --host 0.0.0.0 --port 8000
```

Or use the traditional Python module approach:
```
python -m garganorn
```

## Querying the XRPC service

### getRecord

Query:
```
curl 'http://127.0.0.1:8000/xrpc/com.atproto.repo.getRecord?repo=gazetteer.social&collection=org.overturemaps.place&rkey=08f2830829d8c099036c7f5f8bba30ec'
```

Result:
```
{
  "uri": "at://gazetteer.social/org.overturemaps.id/08f2830829d8c099036c7f5f8bba30ec",
  "value": {
    "$type": "social.gazetteer.place",
    "name": "Full House Picnic Site",
    "location": {
      "$type": "social.gazetteer.place#location",
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
      "collection": "org.overturemaps.place",
      "repo": "repo.local",
      "rkey": "08f2830829d8c099036c7f5f8bba30ec"
    }
  }
}
```

### searchRecords

Query:
```
$ curl 'http://127.0.0.1:8000/xrpc/community.lexicon.location.searchRecords?collection=org.overturemaps.places&latitude=37.776145&longitude=-122.433898&limit=1'
```

Result:
```
{
  "_query": {
    "elapsed_ms": 81,
    "parameters": {
      "collection": "org.overturemaps.places",
      "latitude": "37.776145",
      "limit": 1,
      "longitude": "-122.433898",
      "repo": "gazetteer.social"
    }
  },
  "records": [
    {
      "$type": "community.lexicon.location.searchRecords#record",
      "distance_m": 56,
      "uri": "at://gazetteer.social/org.overturemaps.places/08f2830829d8c099036c7f5f8bba30ec",
      "value": {
        "$type": "community.lexicon.location.place",
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
          "brand": null,
          "categories": {
            "alternate": [
              "attractions_and_activities",
              "public_plaza"
            ],
            "primary": "park"
          },
          "confidence": "0.485",
          "id": "08f2830829d8c099036c7f5f8bba30ec",
          "names": {
            "common": null,
            "primary": "Full House Picnic Site",
            "rules": null
          },
          "phones": null,
          "socials": [
            "https://www.facebook.com/149829208542296"
          ],
          "websites": null
        },
        "locations": [
          {
            "$type": "community.lexicon.location.geo",
            "latitude": "37.776077",
            "longitude": "-122.433400"
          }
        ],
        "names": [
          {
            "priority": 0,
            "text": "Full House Picnic Site"
          }
        ],
        "rkey": "08f2830829d8c099036c7f5f8bba30ec"
      }
    }
  ]
}
```

## Proposed Lexicon schemas

* [`community.lexicon.location.place`](garganorn/lexicon/place.json)
* [`community.lexicon.location.searchRecords`](garganorn/lexicon/searchRecords.json)

**NOTE**: These schemas are only *proposed*, and have not been adopted by the Lexicon community.

## Lexicon dependencies

* `com.atproto.repo.getRecord`

## Development

As aforementioned, this project is under development and should not be used for production purposes. I intend to try to track the work of the lexicon.community ATGeo working group as it evolves.

Patches are extremely welcome.

Come find us in the BlueSky API Touchers Discord.

## License etc.

It's MIT licensed, yo. See [LICENSE](LICENSE) for details. If it breaks, you get to keep the pieces.