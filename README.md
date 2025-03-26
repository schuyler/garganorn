# Garganorn

Garganorn is intended to be an ATProtocol XRPC server for the evolving lexicon.community gazetteer specification.

The project is named after the earliest recorded [mammoth goose](https://en.wikipedia.org/wiki/Garganornis).

![Garganornis ballmanni](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c5/Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg/374px-Garganornis_ballmanni_%28reconstruction_by_Stefano_Maugeri%29.jpg)

## Running

Install and start a Flask server on `localhost:5000`:

```
pip install .
python garganorn 
```

You must have a DuckDB spatial containing Foursquare OSP in `db/fsq-osp.duckdb` for this to work. See [scripts/import-fsq-osp.sql](scripts/import-fsq-osp.sql).

## Querying

### Nearest places

Query:
```
$ curl 'http://127.0.0.1:5000/xrpc/info.schuyler.gazetteer.nearest?latitude=37.776145&longitude=-122.433898&limit=5'
```

Result:
```
{
  "elapsed_ms": 9,
  "locations": [
    {
      "distance_m": 0,
      "location": {
        "latitude": "37.776146",
        "longitude": "-122.433898",
        "name": "Alamo Square"
      },
      "properties": {
        "address": "Steiner St",
        "country": "US",
        "date_created": "2006-05-09",
        "date_refreshed": "2025-03-03",
        "fsq_category_labels": [
          "Landmarks and Outdoors > Park",
          "Landmarks and Outdoors > Park > Playground",
          "Landmarks and Outdoors > Park > Dog Park"
        ],
        "locality": "San Francisco",
        "postcode": "94117",
        "region": "CA"
      },
      "uri": "https://www.foursquare.com/v/4460d38bf964a5200a331fe3"
    },
    {
      "distance_m": 16,
      "location": {
        "latitude": "37.776099",
        "longitude": "-122.434036",
        "name": "Lady Falcon Coffee Club"
      },
      "properties": {
        "address": "1396 La Playa St",
        "country": "US",
        "date_created": "2016-12-11",
        "date_refreshed": "2025-02-23",
        "fsq_category_labels": [
          "Dining and Drinking > Food Truck",
          "Dining and Drinking > Cafe, Coffee, and Tea House > Coffee Shop"
        ],
        "locality": "San Francisco",
        "postcode": "94122",
        "region": "CA"
      },
      "uri": "https://www.foursquare.com/v/584dbf7f6431e51a66133458"
    },
    {
      "distance_m": 22,
      "location": {
        "latitude": "37.776358",
        "longitude": "-122.434064",
        "name": "Alamo Square Tennis Courts"
      },
      "properties": {
        "address": "Fulton",
        "country": "US",
        "date_created": "2010-04-10",
        "date_refreshed": "2024-10-27",
        "fsq_category_labels": [
          "Sports and Recreation > Racquet Sports > Tennis > Tennis Court"
        ],
        "locality": "San Francisco",
        "postcode": "94117",
        "region": "CA"
      },
      "uri": "https://www.foursquare.com/v/4bc0b7074cdfc9b6b64f9321"
    },
    {
      "distance_m": 25,
      "location": {
        "latitude": "37.776471",
        "longitude": "-122.433752",
        "name": "Alamo Square Playground"
      },
      "properties": {
        "address": "",
        "country": "US",
        "date_created": "2021-01-17",
        "date_refreshed": "2025-03-02",
        "fsq_category_labels": [
          "Landmarks and Outdoors > Park > Playground"
        ],
        "locality": "San Francisco",
        "postcode": "94117",
        "region": "CA"
      },
      "uri": "https://www.foursquare.com/v/6004caf48c7d053336cf545a"
    },
    {
      "distance_m": 50,
      "location": {
        "latitude": "37.776278",
        "longitude": "-122.434338",
        "name": "Alamo Square Shoe Garden"
      },
      "properties": {
        "address": "Grove St.",
        "country": "US",
        "date_created": "2010-10-10",
        "date_refreshed": "2025-02-23",
        "fsq_category_labels": [
          "Landmarks and Outdoors > Garden"
        ],
        "locality": "San Francisco",
        "postcode": "94117",
        "region": "CA"
      },
      "uri": "https://www.foursquare.com/v/4cb22d73c5e6a1cd159ce3f6"
    }
  ],
  "parameters": {
    "catalog": "default",
    "latitude": "37.776145",
    "limit": 5,
    "longitude": "-122.433898"
  }
}
```
