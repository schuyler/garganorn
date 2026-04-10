# Proposed changes to the place lexicon

This document summarizes the changes we're contemplating for
`org.atgeo.place` and related schemas, based on
discussions in the [ATGeo working group](https://discourse.lexicon.community/tag/wg-atgeo)
through late 2025.

The intent is to bring the lexicon closer to something the WG could
actually ratify, without over-engineering for hypothetical future needs.

## 1. Add the missing geometry types

`place.json` declares a `locations` union that references
`community.lexicon.location.wkt` and `community.lexicon.location.geojson`,
but neither file exists in `garganorn/lexicon/`. They do exist in the stale
`build/` directory. This is just broken.

**Change:** Copy `wkt.json` (WKT as a string) and `geojson.json` (GeoJSON
as bytes, to avoid JSON-within-JSON) into `garganorn/lexicon/`.

GeoJSON is encoded as `bytes` rather than `object` because GeoJSON
contains raw floats, which ATProtocol's data model doesn't support. Storing
it as an opaque byte array sidesteps the problem. WKT is already text, so
it's just a string.

(The question of whether arbitrarily large geometries should use a blob
`strongRef` instead remains open, but it's not blocking.)

## 2. Rethink place names

The current `#name` type is a flat object with `text`, `lang`, and
`priority`. It has no way to say "this is the primary name" except by
convention — which, as the WG discussion made clear, is not sufficient in
a decentralized environment where you can't assume anyone read the spec
before stuffing names in arbitrary order.

There's also no way to distinguish an official name from an alternate,
a colloquial name, or a transliteration.

The [Overture Maps naming model](https://docs.overturemaps.org/schema/reference/places/place/)
was identified in the WG discussion as having most of the desirable
properties: a `primary` name for simple display, a `common` map for
language-keyed lookups, and a `rules` array for variants (official,
alternate, short, colloquial).

**Proposed change:** Replace the flat `#name` array with a structured
naming object. Something like:

```json
{
  "primary": "San Francisco",
  "names": [
    {"text": "San Francisco", "lang": "en", "variant": "primary"},
    {"text": "San Francisco", "lang": "es", "variant": "primary"},
    {"text": "City of San Francisco", "lang": "en", "variant": "official"},
    {"text": "Frisco", "lang": "en", "variant": "colloquial"},
    {"text": "SF", "lang": "en", "variant": "short"},
    {"text": "サンフランシスコ", "lang": "ja", "variant": "primary"}
  ]
}
```

The `primary` field gives app developers a single string they can display
without iterating over anything. The `names` array carries the full
linguistic detail for geocoding, search, and multilingual UIs. The
`variant` field disambiguates meaning without relying on array position.

`lang` should use the `language` format (BCP 47), and we should consider
whether `langs` (an array) is the right field name for names that span
multiple language contexts — but that's a refinement, not a blocker.

## 3. Give `same_as` some structure

Currently `same_as` is an array of bare `record-key` strings. This means
a consumer receiving a place record has no idea what dataset those IDs
refer to without parsing or guessing.

The WG discussed several approaches. The one that fits best with our
existing architecture is to encode both the dataset (as an NSID/collection)
and the record ID.

**Proposed change:** Replace the flat string array with an array of
structured source references:

```json
{
  "sources": [
    {
      "collection": "org.atgeo.places.foursquare",
      "id": "40982e80f964a520ecf21ee3"
    },
    {
      "collection": "org.atgeo.places.overture.place",
      "id": "08f2830829d8c099036c7f5f8bba30ec"
    }
  ]
}
```

This makes the reference self-describing. A gazetteer server receiving
these can resolve them without ambient knowledge of the ID format.

The `collection` + `id` pair is intentionally the same vocabulary used
elsewhere in Garganorn — it's the `{dataset}/{rkey}` path that we've said
should be portable across gazetteer instances.

An `update_time` field per source (as in Overture's model) is worth
considering but not essential for a first pass.

## 4. Add `collection` to `#ref`

The `#ref` type (for embedding a place reference in another record) has an
`id` field but no indication of which dataset or collection the ID belongs
to. Same problem as `same_as`.

**Proposed change:** Add a required `collection` field (format: `nsid`) to
`#ref`.

## 5. Add `bbox` to the locations union

`community.lexicon.location.bbox` is defined and has the right shape
(named `north`/`south`/`east`/`west` edges, as agreed in the WG), but
`place.json` doesn't include it in the `locations` union. Some places —
administrative regions, parks, bodies of water — are better described by
a bounding box than a point.

**Proposed change:** Add `community.lexicon.location.bbox` to the
`locations` union in both the `main` record and `#ref`.

## 6. Retire `fsq.json`

`community.lexicon.location.fsq` is a vestige of an earlier approach
where each data source got its own location type with dataset-specific
fields (`fsq_place_id`, lat, lon, name). The current architecture handles
Foursquare data through the generic `place` model, with FSQ-specific data
in `attributes`.

`fsq.json` isn't referenced by any union or by the server code's record
processing. It's dead weight that will confuse anyone reading the lexicon
directory.

**Proposed change:** Remove `fsq.json`.

## What we're NOT doing

A few things came up in WG discussions that we're deliberately leaving out:

- **Status / superseded_by / supersedes** — Aaron's WoF proposal included
  lifecycle tracking. On-protocol ATProto data is immutable, making these
  fields semantically awkward. More metadata than the median app developer
  needs. If someone builds a gazetteer that tracks place lifecycles, they
  can put it in `attributes`.

- **Placetype / category as a top-level field** — WoF uses a minimal
  placetype taxonomy (locality, venue, campus). This is useful for
  gazetteers but not clearly necessary in the common data model. Categories
  from different datasets don't share a vocabulary, and inventing a
  universal one is a tar pit. Categories stay in `attributes` for now.

- **The `atgeo:` URI scheme** — The WG discussed `atgeo:{collection}:{rkey}`
  as a durable identifier for off-protocol places. It's a good idea, but
  it's a convention, not a schema change. We can adopt it without modifying
  the lexicon.

- **STAC integration** — Interesting but orthogonal. STAC is well-suited
  for raster data catalogs; its fit for vector place data is less obvious.
  Worth revisiting when there's a concrete proposal.
