# The ATGeo gazetteer interface

What a client of an ATGeo gazetteer sends and receives: how places are named, what a tile holds, and the HTTP routes and XRPC methods that serve them. This is the contract garganorn ships today. The `org.atgeo.*` lexicons in [`garganorn/lexicon/`](../garganorn/lexicon/) are the normative schemas, and the gazetteer serves them at `GET /{nsid}`; this document is the narrative around them, plus the things a schema can't say: status codes, headers, limits, and why.

The server is a set of static, pre-built tiles and a few lookups over them. If you are writing an app, you probably want the [client SDK](atgeo-client-sdk.md), which does everything below for you. If you are writing a client or a gazetteer, read on.

---

## The safety principle

The server must never receive information precise enough to locate a person.

Everything unusual about this interface follows from that one rule. There is no server-side search, so search terms, which reveal intent, never leave the client. There is no way to list or enumerate places. The only spatial question a client can ask is coarse: which tiles cover this region, at a precision floor of roughly a kilometre. The only other question names a place the client already knows the identity of. Neither shape lets a query describe a point, so no query can resolve to a device's position.

The threat model and the full reasoning are in [tile-privacy-design.md](tile-privacy-design.md). The sections below refer back to this principle rather than re-deriving it.

## How queries work

1. Call `org.atgeo.getTiles` with a dataset (and optionally a collection) and a bounding box. The server returns a list of tile URLs.
2. Fetch every URL returned and concatenate the `records` arrays.
3. Deduplicate by `origin`, drop records of collections you didn't ask for, and match, rank and filter locally.

That merged set is the query result. The server never sees what you were looking for. There is no pagination, no server-side ranking, and no partial-result protocol.

For a region too large to cover with the regular tiles, the answer is a sample: see [The overview band](#the-overview-band).

To fetch a single place whose identity you already hold, call `org.atgeo.getPlace` with its record key (its `origin`). Prefer tiles you already have: `getPlace` tells the server exactly which place you are looking at, which is a sharper signal than the box `getTiles` sees. It exists for resolving a link someone shared, where you have an identity and no region.

## Identity

A place is named by an ATGeo URN:

    urn:atgeo:{dataset}:{collection}:{id}
    urn:atgeo:openstreetmap.org:way:123456

- **dataset** is the upstream publisher's domain: `openstreetmap.org`, `overturemaps.org`. No colons.
- **collection** is a sub-division the dataset defines: OSM has `node`, `way` and `relation`; Overture has `place` and `division`. No colons. A `(dataset, collection)` pair is the unit of licensing and of attribution.
- **id** is the upstream's own identifier within that collection: an OSM numeric id, an Overture GERS id. It is everything after the fourth colon.

Every gazetteer place carries its URN as `value.origin`. Clients build URNs from parts they hold and compare them as exact strings, including by prefix: every OSM way starts with `urn:atgeo:openstreetmap.org:way:`. Only a gazetteer serving the dataset takes a URN apart, to resolve it.

A URN is a name, not a location. It does not say which server holds the place, and it does not carry the upstream release; two gazetteers serving different extracts of one dataset use the same URNs, and their [tileset descriptors](#describetileset) say which extracts they are. Upstream ids are only as stable as the upstream makes them: Overture replaced every GERS id in release `2025-06-25.0`, so an id captured before that does not resolve against current data.

The `atgeo` URN namespace is not registered with IANA, and this document doesn't claim RFC 8141 conformance.

Gazetteer places are not repository records. There is no `at://` URI for one, and no `com.atproto.*` method serves them. A place copied into a user's repo keeps the gazetteer's URN as its `origin`; the copy gets its own `at://` URI. A record with no `origin` is its own origin and is identified by its `at://` URI.

## Tilesets

A **tileset** is one build of one source: a directory of tiles, a manifest, and a descriptor. A tileset carries one or more collections, and is served under a **slug** fixed when it was built (`osm`, `overture-place`, `overture-division`). Every gazetteer serving the tileset serves it under that slug. Garganorn builds three:

| slug | carries | source |
|---|---|---|
| `overture-place` | `(overturemaps.org, place)` | [Overture Maps](https://overturemaps.org/) places |
| `osm` | `(openstreetmap.org, node)`, `way`, `relation` | [OpenStreetMap](https://www.openstreetmap.org/) |
| `overture-division` | `(overturemaps.org, division)` | Overture administrative divisions |

A deployment may serve any subset, and another producer may serve different ones. Ask `describeService` which.

A tileset can also **reference** a collection it does not carry. When a build's places include containment, each place's `relations.within` names divisions, so the tileset lists `(overturemaps.org, division)` as a reference: the division's attribution applies to the data the tiles include, though the divisions themselves are served by `overture-division`.

Because a tile can hold records of a collection you didn't ask for (all three OSM element types share the OSM tiles, for one), `getTiles` returns tiles, not records of one collection. See [Tiles](#tiles).

## Tiles

A tile is a set of place records associated with one quadkey cell.

**Quadkey scheme.** Web-Mercator (Bing) quadkeys over WGS84 longitude and latitude. Digits encode `0=NW, 1=NE, 2=SW, 3=SE`, with y increasing southward. The outermost row at every zoom reaches the pole (±90°), so every latitude is covered. Zoom is the length of the quadkey string. A tile URL garganorn hands out ends in `{quadkey}.json`, but this document does not define the URL as part of the interface; see below.

**Tile URLs are opaque.** A client never constructs one; it receives whole URLs from `getTiles` and fetches them as given. A tile URL is immutable: it never serves two different byte streams. When the gazetteer publishes a new build, the URLs change completely, which costs a client nothing, because it always asks `getTiles` again rather than remembering URLs. Old builds are eventually retired, so a hoarded URL will in time return 404; that is the penalty for remembering. The tiles themselves contain no gazetteer host, so a tileset can be served from any host under its slug.

**Zoom and assignment.** The regular tiles exist at zooms 6 through 17. Every record gets a home tile by an adaptive rule (a division by its interior point): it goes in the coarsest tile holding no more than a per-build cap of records (1,000 by default), falling back to zoom 17 when no coarser tile qualifies. A zoom-17 tile can therefore hold more than the cap. Divisions are polygons and are also referenced from every tile their geometry overlaps; a division's per-tile count is not bounded by the cap. A tileset may also carry an [overview band](#the-overview-band) at zooms 1 through 5. Tiles at different zooms coexist in one tileset, so no fixed zoom may be assumed. Records within a tile are in no meaningful order.

**Deduplicate by `origin`.** A client concatenating `records` arrays from more than one tile MUST deduplicate them by `value.origin`, falling back to the record's `uri` for a record with no origin. This holds for every collection, unconditionally. Today a point record is in exactly one regular tile, plus possibly one overview tile, and a single `getTiles` answer never mixes the two bands, so duplicates are rare in practice; but a division is referenced from every tile it overlaps, so there they are the norm. A client can't tell from the wire which collections avoid duplicates, and a server-side change shouldn't require a client change to stay correct. Every copy of a record is byte-identical, so it doesn't matter which one you keep.

**Transport.** Tiles are stored gzip-compressed, as `{quadkey}.json.gz` on disk, and served as plain JSON with `Content-Type: application/json` at `{quadkey}.json`. The server maps the one to the other. Wire compression is negotiated by the ordinary `Accept-Encoding` exchange; rely on your HTTP library rather than assuming an encoding.

Tile URLs used to end in `.json.gz`, which said gzip and delivered plain JSON. Those older URLs still resolve, to the same bytes with the same headers, until the last run a pre-`.json` server advertised is retired: two builds under the retention policy, plus the week a tile may sit in a cache. After that they're ordinary 404s, the same as any other URL from a retired build.

**Caching.** Because tile URLs are immutable, tiles are served with `Cache-Control: public, max-age=604800, immutable`: cache them for a week and never revalidate. See [Headers](#headers) for the rest.

### The overview band

A tileset may carry a coarse band of tiles at zooms 1 through 5 (quadkeys shorter than six characters) alongside the regular tiles. It holds the N records of the tileset with the highest `importance`, ties broken by id; garganorn builds with N = 10,000, which is a producer parameter, not protocol. It's one cut, the same for every tileset: the division formula's [administrative floors](#importance) are what keep countries, dependencies and regions in it. A tileset with a band says so in its descriptor's `overview`, whose `records` is how many records the band actually holds, which can be fewer than N. A record in the band is byte-identical to its copy in the regular tiles, so the deduplication rule covers it. Zoom 5 is the finest the band goes, so an overview tile can hold more records than the per-tile cap.

`getTiles` uses the band as a fallback. It answers from the regular tiles first. If that answer would cover more than the deployment's tile budget (garganorn's `max_tiles`, 50 by default, advertised as `describeService`'s `maxTiles`) and every selected tileset has an overview band, it returns the overview tiles that intersect the box instead, with no budget applied, and sets `"overview": true`. An overview answer is not subject to `max_tiles`, and can exceed it. It is bounded in practice: no more than 1,364 tiles exist per tileset at zooms 1 through 5, and a band of 10,000 records usually has a few dozen. The result is deliberately a sample of the region, not everything in it.

The response says which kind of answer it is. An overview answer carries `"overview": true` alongside `tiles`; a regular answer omits the key, so absent means regular. A client that needs to know reads the flag. It does not look at the length of the quadkey in a tile URL.

The budget counts tiles across every tileset the request selects. A request for `dataset=overturemaps.org` with no collection selects both `overture-place` and `overture-division`, and the combined count is what's compared with the budget. If any selected tileset has no overview band (its descriptor has no `overview`), the answer is `BboxTooLarge` instead.

## The tile payload

A tile is one JSON object (`org.atgeo.tilePayload`):

| key | meaning |
|---|---|
| `publishedAt` | RFC 3339 UTC timestamp, `Z`-suffixed, seconds precision. When the tileset was built |
| `attribution` | array of one entry per collection the tileset carries or references |
| `records` | array of record objects |

`publishedAt` and `attribution` are identical in every tile of a tileset, and `publishedAt` is the tileset descriptor's value too. They are build-scoped, not per-tile, so a difference between tiles from different builds says nothing about the freshness of the records inside.

Each `attribution` entry is a `collectionAttribution`: `dataset`, `collection`, and where known `release` (the upstream's own version string: an Overture release id, or an OSM replication timestamp), `source` (the upstream's URL), `licenses` (URLs of the text of every licence the collection is under), and `attribution` (the upstream's own attribution or copyright page). The attribution is in every tile so a consumer holding one tile has the notices for what's in it, with no separate lookup.

A tile record has up to three keys:

- **`value`** — the place, an `org.atgeo.place` value, exactly as described [below](#place-records).
- **`importance`** — an integer 0 to 100; see [Importance](#importance). It sits beside the place, not in it: it is the producer's build parameter, not a property of the place, so a place copied out of a tile doesn't carry it.
- **`uri`** — present only for a record that has no `origin`, such as a copy of a repo record. It is the record's own identity, never the gazetteer's URL for the record. The records garganorn serves all have an origin and omit it.

There is no version field. Lexicon schemas evolve upgrade-only, so the rule that governs the records inside a tile governs the tile too. The general form of that rule, which applies to every response shape in this document: **ignore keys you don't recognize.** New keys may appear anywhere; existing keys keep their meaning.

A trimmed OSM tile:

```json
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
        "attributes": {"addr:city": "Concord", "shop": "houseware"},
        "relations": {}
      },
      "importance": 0
    }
  ]
}
```

(A real tile has an `attribution` entry for every collection of the tileset: here, `way` and `relation` too.)

## Place records

The normative schema is the `org.atgeo.place` lexicon; this section is the narrative. A place's `value` carries:

| field | type | notes |
|---|---|---|
| `$type` | string | `org.atgeo.place` |
| `origin` | string | the place's URN; see [Identity](#identity) |
| `name` | string | primary display name |
| `locations` | array | union members below |
| `variants` | array | `{name, type?, language?}` |
| `attributes` | object | source-specific; shape differs by collection |
| `relations` | object | `{within: [...]}` |

Coordinate values inside `locations` are decimal strings with six decimal places, not JSON numbers (the AT Protocol data model has no floats). Parse them. The same goes for any other fractional value in `attributes`: garganorn emits no JSON floats.

`locations` is declared as a four-member union: `community.lexicon.location.geo`, `.hthree`, `.address`, and `.bbox`. What the producer currently emits: OSM nodes, ways and relations and Overture places carry a `geo` point (a way or relation is a point too, not a geometry); Overture place records may add `address` entries; division records carry a `bbox` of their extent instead. `hthree` is declared but not yet produced, and its lexicon is not among those the server serves at `GET /{nsid}`. Handle the union, not the current subset. A `bbox` whose `west` is greater than its `east` crosses the antimeridian.

`variants` holds alternate names. For Overture places and divisions they come from the upstream's multilingual and rule-based names; for OSM, from the name tags (`name:{lang}`, `alt_name`, `official_name`, and related). `type` is a hint, with known values `official`, `alternate`, `short`, `colloquial` and `historical`; `language` is present when the upstream's language tag is valid.

`attributes` is the source's own vocabulary, passed through rather than homogenized. OSM records carry a filtered tag map, including the tag that gives the element its primary category; other classifying tags are dropped. Overture place records carry the upstream fields (`names`, `categories`, `websites`, `brand`, `sources`, and so on). Division records carry `subtype`, `country`, `region`, `wikidata` and `population`: `population` is always present, 0 meaning the source recorded none, and the others appear when the source has them. A tileset's descriptor lists the keys its records may carry.

`relations.within` lists the divisions containing the record, as `{uri, name, type}` objects: `uri` is the division's URN (`urn:atgeo:overturemaps.org:division:{id}`), `name` its display name, and `type` its Overture `subtype` (`country`, `region`, `locality`, and so on). The list is ordered broadest first; see [Containment levels](#containment-levels). Containment reaches every level in the vocabulary, country through microhood, wherever the source data supports it. Compare `uri`s as exact strings.

Two declared fields are not yet produced: `updatedAt` and `relations.sameAs`. A client must tolerate their absence now and their presence later.

## Importance

`importance` is an integer from 0 to 100 expressing how much a place matters relative to other places. It blends two signals: how busy the place's surroundings are, and how distinctive its category is (for a division, how many people live in it).

Scores are comparable across the tilesets one producer builds from one dataset, and meaningless across producers. So garganorn's `overture-place` and `overture-division` scores can be ranked against each other, and a dataset-wide `overturemaps.org` search can rank places and divisions together. Another producer's 62 tells you nothing about garganorn's 62: the formula, its constants and its corpora are all the producer's own. Nor does the promise stretch across datasets: an Overture place scoring 62 and an OSM place scoring 62 are scored against different corpora with different category vocabularies.

The reference producer computes it as follows. For place records (Overture places, and OSM nodes, ways and relations):

    importance = round(60 · min(density / 10, 1) + 40 · min(idf / 18, 1))

where `density` is ln(1 + n) for n the count of Overture places in the enclosing zoom-15 cell (Overture-derived even when scoring OSM records, so both share one notion of "busy"), and `idf` is ln(N / n) over the source's own corpus of named places, n being the places in the record's primary category: rare categories score high, ubiquitous ones low.

For division records, the administrative level sets a floor and a ceiling, and a 0-to-1 signal places the division between them:

    importance = round(floor + (ceiling − floor) · s)

    s = min(ln(1 + population) / 20, 1)                                       every subtype but locality
    s = 0.6 · min(density / 10, 1) + 0.4 · min(ln(1 + population) / 20, 1)   locality

with `density` averaged over the zoom-15 cells intersecting the locality's bounding box. A division with no recorded population (and, for a locality, no density) sits at its floor.

| subtype | floor | ceiling |
|---|--:|--:|
| `country` | 90 | 100 |
| `dependency` | 85 | 100 |
| `region` | 80 | 100 |
| `county` | 40 | 79 |
| `localadmin` | 35 | 79 |
| `locality` | 30 | 79 |
| `borough` | 20 | 79 |
| `macrohood` | 15 | 79 |
| `neighborhood` | 10 | 79 |
| `microhood` | 5 | 79 |

The floors fall in [containment](#containment-levels) order, country highest. Countries, dependencies and regions share the 80-to-100 range and everything below them tops out at 79, so the weakest region still outranks the densest city. That's what lets the [overview band](#the-overview-band) be a plain importance cut. Within the upper range the bands overlap on purpose: a floor is a minimum, so a populous dependency can outrank a tiny country.

The normalization constants (10, 18, 20) and the floors and ceilings are producer parameters, not protocol. Another producer could choose differently and still be an ATGeo gazetteer.

## Containment levels

The wire carries a division's `subtype` as the `type` of each `within` entry, and defines no vocabulary of its own: administrative names and their nesting aren't universal. The order of the list is what carries the nesting. Garganorn orders it, broadest first, by an internal level it derives from the subtype alone, ties broken by id. Overture's own `admin_level` is deliberately not used: in practice it is 96% NULL and ambiguous even within a single subtype. The level is not emitted.

| level | subtype |
|------:|--------------|
| 10 | `country` |
| 15 | `dependency` |
| 25 | `region` |
| 35 | `county` |
| 45 | `localadmin` |
| 50 | `locality` |
| 55 | `borough` |
| 60 | `macrohood` |
| 65 | `neighborhood` |
| 70 | `microhood` |

The stride-5 numbering follows the descent locality → borough → macrohood → neighborhood → microhood, so a macrohood sorts above its neighborhoods and a microhood nests inside its neighborhood. The gaps are deliberate insertion room. There is no level 0: continents are never emitted as divisions. `borough` is mapped but currently unpopulated, because present Overture division data contains no borough subtype.

A subtype outside this table fails the producer's build before any artifact is written, so no record ever carries an out-of-vocabulary type. Each collection's descriptor lists the types its records actually use as `containmentTypes`, broadest first. Another producer is free to use a different vocabulary and order; read `containmentTypes` rather than assuming this table.

## Tilesets on the wire: `describeService` and `describeTileset`

### describeService

`GET /xrpc/org.atgeo.describeService` takes no parameters, and ignores any query parameters it is sent (a cache-buster, say). It returns `{did, maxTiles, tilesets}`. `did` is the `did:web` the deployment identifies as, the same identifier `/.well-known/did.json` carries. It's informational: no discovery path resolves a DID to a gazetteer yet, so a client that has this answer already knew the server's URL. `maxTiles` is the tile budget: the most regular tiles a `getTiles` answer may list before it falls back to [the overview band](#the-overview-band) or `BboxTooLarge`. It's informational too, good for a README or a log line. A client can't use it to plan requests, because tile zooms are adaptive and it does no quadkey math, so it can't predict how many tiles a box will need. `maxTiles` is optional in the lexicon, so another producer may leave it out; garganorn always sends it. Each element of `tilesets` is `{slug, collections}`, with `collections` a list of `{dataset, collection}`.

The list is authoritative. A dataset absent from it returns `DatasetNotFound` from `getTiles`.

```json
{
  "did": "did:web:places.atgeo.org",
  "maxTiles": 50,
  "tilesets": [
    {"slug": "osm", "collections": [
      {"dataset": "openstreetmap.org", "collection": "node"},
      {"dataset": "openstreetmap.org", "collection": "way"},
      {"dataset": "openstreetmap.org", "collection": "relation"}
    ]},
    {"slug": "overture-division", "collections": [
      {"dataset": "overturemaps.org", "collection": "division"}
    ]}
  ]
}
```

### describeTileset

`GET /xrpc/org.atgeo.describeTileset?slug=osm`, or `GET /tilesets/osm`, returns the tileset's descriptor (`org.atgeo.defs#tileset`). The two return the same document. The descriptor describes one build, not a promise about future ones.

- `slug`, and `publishedAt`, which equals `publishedAt` in every tile.
- `extent`: a `community.lexicon.location.bbox` bounding the regular tiles.
- `overview`: present when the tileset has an [overview band](#the-overview-band), as `{records}`, the number of records actually in the band. Absent means no band, so a box over the tile budget is `BboxTooLarge`. It says nothing about the band's zooms; tile URLs are opaque.
- `collections`: one `collectionInfo` for each collection the tileset carries. It has the collection's attribution fields (as in the [tile payload](#the-tile-payload)) plus `recordCount`, `locationFormats` (the `$type`s of the locations its records carry), `containmentTypes`, `categories` (values of the records' primary category, with counts, descending) and `attributes` (the keys its records may carry). The vocabularies are observed, not closed.
- `references`: the collections the tileset includes data from without carrying them, as `collectionAttribution`s.

```json
{
  "slug": "osm",
  "publishedAt": "2026-08-08T06:16:21Z",
  "extent": {"north": "45.089036", "south": "40.979898", "east": "-66.513260", "west": "-73.125000"},
  "overview": {"records": 10000},
  "collections": [
    {
      "dataset": "openstreetmap.org",
      "collection": "node",
      "release": "2026-08-07T20:21:15Z",
      "source": "https://www.openstreetmap.org/",
      "licenses": ["https://opendatacommons.org/licenses/odbl/1-0/"],
      "attribution": "https://www.openstreetmap.org/copyright",
      "recordCount": 1204332,
      "locationFormats": ["community.lexicon.location.geo"],
      "containmentTypes": ["country", "region", "county", "locality"],
      "categories": [{"value": "amenity=cafe", "count": 80211}],
      "attributes": ["addr:city", "amenity", "name"]
    }
  ],
  "references": [
    {
      "dataset": "overturemaps.org",
      "collection": "division",
      "release": "2026-09-17.0",
      "source": "https://overturemaps.org/",
      "licenses": [
        "https://opendatacommons.org/licenses/odbl/1-0/",
        "https://creativecommons.org/licenses/by/4.0/legalcode"
      ],
      "attribution": "https://docs.overturemaps.org/attribution/"
    }
  ]
}
```

(An abridged descriptor: a real one has an entry per collection and a far longer `categories` list. The numbers are illustrative.)

## XRPC methods

Four queries, all `GET /xrpc/{nsid}`, all described by lexicons. Responses may include keys not documented here; per the forward-compatibility rule above, ignore them.

### org.atgeo.getTiles

Which tiles cover a bounding box.

Parameters: `dataset` (required, a domain like `openstreetmap.org`), `collection` (optional, like `way`), `bbox` (required, comma-separated `west,south,east,north` in WGS84 decimal degrees).

The four values use the same words and mean the same things as a tileset's `extent` and `community.lexicon.location.bbox`. `bbox` is positional where those have named keys because it's a query parameter: one string, four numbers, in that order.

Without `collection`, the answer covers every tileset carrying the dataset. With one, it covers every tileset carrying that collection. Either way the tiles may hold records of other collections: a caller that asked for a collection keeps the records whose `origin` starts with `urn:atgeo:{dataset}:{collection}:`, and a record with no `origin` matches no collection.

Returns `{"tiles": [...]}`: fully-formed, opaque tile URLs, in ascending string order. Fetch them all and merge; filtering and ordering are yours, applied after the merge. When the answer is the [overview band](#the-overview-band) rather than the regular tiles, the response also carries `"overview": true`.

A bbox may cross the antimeridian: `west > 0 > east` means the box wraps across ±180°. Tiles that merely touch the box at an edge or corner count as covering it.

```
GET /xrpc/org.atgeo.getTiles?dataset=openstreetmap.org&collection=way&bbox=-71.55,43.20,-71.50,43.25
```
```json
{"tiles": [
  "https://places.atgeo.org/tiles/osm/20260808T061621/030233/03023303220.json",
  "https://places.atgeo.org/tiles/osm/20260808T061621/030233/03023303221.json"
]}
```

Errors, in evaluation order:

- `BboxTooPrecise` — any coordinate carrying more than two decimal places. See [The precision floor](#the-precision-floor).
- `InvalidBbox` — not four comma-separated finite numbers, a longitude outside [-180, 180] or a latitude outside [-90, 90] (both inclusive), `south ≥ north`, or `west ≥ east` without the antimeridian signature above.
- `DatasetNotFound` — no tileset carries the dataset.
- `CollectionNotFound` — the dataset is served but no tileset carries the collection.
- `BboxTooLarge` — the box covers more tiles than the deployment's budget, and a selected tileset has no [overview band](#the-overview-band). Ask for a smaller region. A tileset with an overview band never raises this; it answers with the overview tiles, and the response says so with `"overview": true`.

### org.atgeo.getPlace

One place by identity.

Parameter: `uri` (required), the record's key: its `origin`, or its own `uri` when it has none. For every place garganorn serves that's the origin, such as `urn:atgeo:openstreetmap.org:way:123456`. Pass it whole; the server takes it apart.

Returns `{uri, url, importance?, value, attribution}`, the same `{uri, value}` shape as AT Protocol's `getRecord`:

- `uri` echoes the record key you asked for.
- `url` is the plain-HTTP address of this same view, `{public_base}/{dataset}/{collection}/{id}`, which returns this same document; `public_base` is the deployment's configured public URL, not the host the request arrived on.
- `importance` sits beside the place, as in a tile, and is absent when the tile record has none.
- `value` is the place's `org.atgeo.place` value, unmodified, exactly what the tile holds.
- `attribution` is the place's tileset's `collectionAttribution` entries, carried and referenced: the same build-scoped array every tile of that tileset carries. A place fetched by link has nothing else to credit from.

If more than one tileset carries the collection, the first (in the deployment's configuration order) that holds the place answers.

```
GET /xrpc/org.atgeo.getPlace?uri=urn:atgeo:openstreetmap.org:node:10080395917
```
```json
{
  "uri": "urn:atgeo:openstreetmap.org:node:10080395917",
  "url": "https://places.atgeo.org/openstreetmap.org/node/10080395917",
  "importance": 0,
  "value": {
    "$type": "org.atgeo.place",
    "origin": "urn:atgeo:openstreetmap.org:node:10080395917",
    "name": "HomeGoods",
    "locations": [
      {"$type": "community.lexicon.location.geo", "latitude": "43.288557", "longitude": "-71.575664"}
    ],
    "variants": [],
    "attributes": {"addr:city": "Concord", "shop": "houseware"},
    "relations": {}
  },
  "attribution": [
    {
      "dataset": "openstreetmap.org",
      "collection": "node",
      "release": "2026-08-07T20:21:15Z",
      "source": "https://www.openstreetmap.org/",
      "licenses": ["https://opendatacommons.org/licenses/odbl/1-0/"],
      "attribution": "https://www.openstreetmap.org/copyright"
    },
    {
      "dataset": "openstreetmap.org",
      "collection": "way",
      "release": "2026-08-07T20:21:15Z",
      "source": "https://www.openstreetmap.org/",
      "licenses": ["https://opendatacommons.org/licenses/odbl/1-0/"],
      "attribution": "https://www.openstreetmap.org/copyright"
    },
    {
      "dataset": "openstreetmap.org",
      "collection": "relation",
      "release": "2026-08-07T20:21:15Z",
      "source": "https://www.openstreetmap.org/",
      "licenses": ["https://opendatacommons.org/licenses/odbl/1-0/"],
      "attribution": "https://www.openstreetmap.org/copyright"
    },
    {
      "dataset": "overturemaps.org",
      "collection": "division",
      "release": "2026-09-17.0",
      "source": "https://overturemaps.org/",
      "licenses": [
        "https://opendatacommons.org/licenses/odbl/1-0/",
        "https://creativecommons.org/licenses/by/4.0/legalcode"
      ],
      "attribution": "https://docs.overturemaps.org/attribution/"
    }
  ]
}
```

Errors: `PlaceNotFound`, for a URN the server doesn't serve, a collection it doesn't carry, a malformed `atgeo` URN, or a URI that isn't a URN at all. A string that isn't a valid URI is `InvalidRequest`.

### org.atgeo.describeService and org.atgeo.describeTileset

See [above](#tilesets-on-the-wire-describeservice-and-describetileset). `describeTileset` takes `slug` and has one error, `TilesetNotFound`. `describeService` has none.

### Errors over XRPC

Every error the methods raise returns **HTTP 400** with a body of `{"error": name, "message": text}`, per the AT Protocol convention. A client branches on the `error` name, not on the status: `PlaceNotFound` is a 400 here, not a 404. The XRPC layer (lexrpc, and Flask beneath it) adds a few errors no lexicon lists:

| error | status | when |
|---|---|---|
| `InvalidRequest` | 400 | a required parameter is missing, a parameter doesn't fit its lexicon, the NSID is malformed, or a request body isn't valid JSON |
| `UnsupportedMediaType` | 415 | a request body with a non-JSON `Content-Type` |
| `MethodNotAllowed` | 405 | an HTTP method the route doesn't take, with an `Allow` header |
| `MethodNotImplemented` | 501 | the NSID isn't one of the four methods the server implements, including every `com.atproto.*` method and a served record lexicon such as `org.atgeo.place` |

A 5xx means the server broke, not that it refused you; see [Errors on plain-HTTP routes](#plain-http).

### The precision floor

Bounding-box coordinates in `getTiles` carry at most two decimal places, a 0.01° grid about 1.1 km across at the equator. This is the safety principle made concrete. Search terms are already gone, but a box centred on a device's raw GPS fix still locates the person to about ten metres however large the box is, and a minimum-area check doesn't catch that. Holding every coordinate to 0.01° limits what the server can extract to "which grid cell", not "which street corner".

The floor is enforced, not advised. The check is textual, on the string you sent: `37.770` fails on its trailing zero, and scientific notation (`1e2`) is rejected outright. The server rejects rather than truncating, so a client that leaks precision hears about it instead of being silently covered for. Clients snap before sending; the server refuses regardless, so a client built without that logic can't leak systematically. Snap outward, so the grid box contains your region.

Refusal doesn't protect the coordinates in the refused request: the server has to receive a value to judge it. The guarantee is against sustained exposure. A leaking client is caught on its first exchange instead of running unnoticed.

## Plain HTTP

| route | returns |
|---|---|
| `GET /tiles/{slug}/...` | a tile, as plain JSON. The URLs `getTiles` returns point here only when the tileset's configured `base_url` does; this route is what serves the files |
| `GET /tilesets/{slug}` | the same document as `describeTileset` |
| `GET /{dataset}/{collection}/{id}` | the same document as `getPlace` for `urn:atgeo:{dataset}:{collection}:{id}`. This is what `getPlace`'s `url` points at. The first segment must contain a dot, since a dataset is a domain |
| `GET /{nsid}` | the lexicon document for that NSID |
| `GET /.well-known/did.json` | the server's DID document |
| `GET /health` | `{"status": "ok", "service": "garganorn"}` |

The DID document's `id` is the same `did:web` as `describeService`'s `did`. It advertises the tile service, with the deployment's public URL as the endpoint, and nothing else:

```json
{
  "id": "did:web:places.atgeo.org",
  "service": [
    {"id": "#atgeo_tiles", "type": "AtgeoTileService", "serviceEndpoint": "https://places.atgeo.org"}
  ]
}
```

It doesn't advertise a personal data server, because there isn't one.

**Every error body is JSON, `{"error", "message"}`, on every route, never an HTML page.** That includes XRPC and the plain routes alike. A miss on a plain-HTTP route is HTTP 404:

| route | miss | error |
|---|---|---|
| `GET /{dataset}/{collection}/{id}` | place not served, or a first segment with no dot | `PlaceNotFound` |
| `GET /tilesets/{slug}` | unknown slug | `TilesetNotFound` |
| `GET /tiles/{slug}/...` | unknown slug | `TilesetNotFound` |
| `GET /tiles/{slug}/...` | no such tile; a path not ending in `.json` (or the older `.json.gz`), or one that tries to leave the tiles directory, counts as no such tile. A `.json` path is always looked up as its `.json.gz` tile, so the run's own `manifest.json` and `tileset.json` aren't reachable here | `TileNotFound` |
| `GET /{nsid}` | an NSID-shaped name that isn't a served lexicon | `LexiconNotFound` |
| any other path | no route matches, including a single segment that isn't an NSID (`/favicon.ico`, `/xrpc`, `/tiles`) | `NotFound` |

Everything else follows the same shape. A wrong method on any route, XRPC included, is 405 `MethodNotAllowed` with an `Allow` header. Any other 4xx carries the name of the HTTP status without spaces (`BadRequest`, anywhere but the XRPC endpoint, where a 400 is always `InvalidRequest`). Any 5xx is `InternalServerError`, `ServiceUnavailable` or the like, with the generic message "The server hit an unexpected error."; the details go to the server's log, not the response. A redirect (`/tilesets//x` gets a 308, say) is not turned into an error.

The status differs from XRPC on purpose: XRPC follows the AT Protocol convention of 400 for every error; a plain GET at an address that holds nothing is the ordinary web 404. The two also differ on a malformed identity: the alias `/{dataset}/{collection}/{id}` answers a malformed URN with 404 `PlaceNotFound`, while `getPlace` answers a `uri` that isn't a URI at all with 400 `InvalidRequest`, and a URI that isn't a served place with 400 `PlaceNotFound`.

## Headers

| response | `Cache-Control` |
|---|---|
| a tile (`/tiles/{slug}/...`) | `public, max-age=604800, immutable` |
| `getTiles`, 200 only | `public, max-age=3600` |
| `describeService`, 200 only | `public, max-age=3600` |
| anything else | none set<sup>1</sup> |

<sup>1</sup> Except a CORS preflight: an `OPTIONS` request to `getTiles` or `describeService` is a 200 and carries `public, max-age=3600` too.

Tile URLs are immutable, so tiles are cached for a week and never revalidated. `getTiles` and `describeService` change when the gazetteer publishes a new build, so they carry an hour; a client learns of a new build within an hour of asking. The deployment's build cadence has to respect the week: a retired build whose URLs clients still hold as fresh is a 404.

**CORS.** Every response from the server, including errors and tiles, carries `Access-Control-Allow-Origin: *`, so browser clients on any origin can read it. A server MUST send it. The header appears exactly once.

**Content sniffing.** Every response also carries `X-Content-Type-Options: nosniff`. Error messages can echo the request path, so a browser must not be allowed to guess that one is HTML.

## Licensing

Each collection's licences and attribution page are in its `attribution` entry, in every tile and in its descriptor. Garganorn publishes:

| collection | licences | attribution page |
|---|---|---|
| `(openstreetmap.org, node \| way \| relation)` | [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/) | [openstreetmap.org/copyright](https://www.openstreetmap.org/copyright) |
| `(overturemaps.org, place)` | [CDLA Permissive 2.0](https://cdla.dev/permissive-2-0/), [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0), [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/legalcode) | [Overture attribution](https://docs.overturemaps.org/attribution/) |
| `(overturemaps.org, division)` | [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/), [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/legalcode) | [Overture attribution](https://docs.overturemaps.org/attribution/) |

The tile payload's `attribution` array is where the notices live, and a licence's duties attach to conveying the dataset, not to a lone record. A place copied out of a tile keeps its `origin`, whose dataset segment names the source, and carries no licence link; the person who copies it is the one conveying it. That is a design position, not legal advice. The reasoning is in [place-identity-redesign.md](place-identity-redesign.md#attribution-and-licensing-in-tiles).
