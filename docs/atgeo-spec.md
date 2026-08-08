# The ATGeo gazetteer interface

What a client of an ATGeo gazetteer sends and receives: the tile format, the
record shapes, and the XRPC methods. If you are writing a client, this
document plus the lexicon documents the server publishes (`GET /{nsid}`,
described under [Plain HTTP](#plain-http)) are the contract; you should not
need to read the server's source.

This document describes the behavior and intent of the interface as it is
served today. Implementation guidance for spatial AppViews will eventually
live here too; for now, the gazetteer interface is the whole story.

---

## The safety principle

The server must never receive information precise enough to locate a person.

Everything unusual about this interface follows from that one rule. There is
no server-side search, so search terms — which reveal intent — never leave
the client. There is no record enumeration. The only spatial question a
client can ask is coarse: which tiles cover this region, at a precision floor
of roughly a kilometre. A caller can name a region no finer than that, or
name a record it already knows the key for — neither shape lets a query
describe a point, so no query can resolve to a device's position.

The threat model and the full reasoning are in
[tile-privacy-design.md](tile-privacy-design.md). Sections below reference
this principle rather than re-deriving it.

## How queries work

Two steps, and only two:

1. Call `org.atgeo.getCoverage` with a collection and a bounding box. The
   server returns a list of tile URLs.
2. Fetch every URL returned and concatenate the `records` arrays into one
   result set.

That merged set is the query result. Matching, ranking, and filtering are
the client's job, performed locally — the server never sees what you were
looking for. There is no pagination, no server-side ranking, and no
partial-result protocol.

## Tiles

A tile is a set of place records whose locations fall inside one quadkey
cell, stored as a gzipped JSON file.

**Quadkey scheme.** Web-Mercator (Bing) quadkeys over WGS84 longitude and
latitude. Digits encode `0=NW, 1=NE, 2=SW, 3=SE`, with y increasing
southward. Latitude is clamped to ±85.05112878. Zoom is the length of the
quadkey string.

**Tile URLs are opaque.** A client never constructs one; it receives whole
URLs from `getCoverage` and fetches them. A tile URL is permanently
immutable — it never serves two different byte streams. When the gazetteer
publishes a new data release, the URLs change completely, which costs a
client nothing because a client always asks `getCoverage` again rather than
remembering URLs. Old releases are eventually retired, so a hoarded URL will
in time return 404; that is the penalty for remembering.

**Zoom and assignment.** Tiles exist at zooms 6 through 17. Assignment is
adaptive: a record goes in the coarsest tile holding no more than a
per-deployment cap of records (1,000 by default), falling back to zoom 17
when no coarser tile qualifies. Consequences a client must handle: tiles at
different zooms coexist in one collection, and no fixed zoom may be
assumed.

**One record, one tile — with one declared exception.** Within a
collection, each record belongs to exactly one tile, so any two tiles
hold disjoint record sets and concatenation needs no deduplication. The
division collection (`org.atgeo.places.overture.division`) is the
exception: a division can be referenced from more than one tile, so a
client concatenating `records` arrays from more than one division tile
MUST deduplicate by `rkey`. This is declared ahead of the work that will
produce it — division tile assignment today still puts each division in
exactly one tile — so a client written against this spec is correct
before and after that lands.

**Transport.** Tiles are served with `Content-Encoding: gzip` and
`Content-Type: application/json`, unconditionally. The server does not
negotiate: gzip is sent regardless of the request's `Accept-Encoding`, so a
client must be prepared to decompress.

**Caching.** Because tile URLs are immutable, tiles are served with
`Cache-Control: public, max-age=604800, immutable` — cache them for a week
and never revalidate. `getCoverage` responses carry `max-age=3600`, so a
client learns about a new release within an hour of asking.

## The tile payload

A tile payload is one JSON object with these top-level keys, in order:

| key | meaning |
|---|---|
| `collection` | NSID of the collection carried in this tile |
| `source` | URL of the upstream dataset |
| `license` | URL of the upstream dataset's license |
| `generated_at` | RFC 3339 UTC timestamp, `Z`-suffixed, seconds precision |
| `records` | array of record objects |

`source` and `license` together are the attribution: a link to where the
data came from and a link to the terms it came under.

`generated_at` is release-scoped: every tile of a release carries the
identical value. It is not a per-tile time, so differences between tiles
from different releases never indicate relative freshness of the records
inside.

There is no version field. Lexicon schemas evolve upgrade-only, so the rule
that governs the records inside a tile governs the tile too. The general
form of that rule, which applies to every response shape in this document:
**ignore keys you don't recognize.** New keys may appear anywhere; existing
keys keep their meaning.

Each element of `records` has exactly three keys:

- **`uri`** — `https://{host}/{collection}/{rkey}`, a dereferenceable URL
  where a GET returns the bare record value. The rkey is served raw:
  characters legal in a URI path segment, colons included, are not
  percent-encoded. This is the same form `getRecord` reports, so tile URIs
  and XRPC-resolved URIs agree by construction.

  Gazetteer records never use `at://`. That scheme addresses ATProto
  repository data, and a gazetteer record is not repository-backed: no
  signed commit includes it, so an `at://` URI would promise a verifiability
  the record cannot deliver. The URI scheme is a provenance signal —
  `https://` means "not repository data, not commit-verifiable."

- **`cid`** — currently always the literal `null`, present rather than
  omitted. The reason is cost, not principle. A CID is a content hash and
  carries no claim of repository membership, so a gazetteer could compute one
  honestly. What it could not do is make that hash mean what a CID means in a
  PDS response, where it resolves through a signed commit and so attests
  authorship. Here it would attest only that the bytes match a hash served
  alongside them — enough to tell whether a record changed between releases,
  worth nothing against a hostile server. That is a real use with no consumer
  yet, set against canonically re-encoding every record on every build, so
  the field stays null for now. Read `null` as "no hash available," not as a
  permanent property of gazetteer records.

- **`value`** — the place record.

## Place records

The normative schema is the `org.atgeo.place` lexicon; this section is the
narrative. A place record's `value` carries:

| field | type | notes |
|---|---|---|
| `$type` | string | `org.atgeo.place` |
| `rkey` | string | record key |
| `name` | string | primary name |
| `importance` | integer | 0–100; see [Importance](#importance) |
| `locations` | array | union members below |
| `variants` | array | `{name, type?, language?}` |
| `attributes` | object | source-specific; shape differs by collection |
| `relations` | object | `{within: [...]}` |

Coordinate values inside `locations` are decimal strings with six decimal
places, not JSON numbers. Parse them.

`locations` is declared as a four-member union:
`community.lexicon.location.geo`, `.hthree`, `.address`, and `.bbox`. What
producers currently emit: point records carry a `geo`; division (boundary)
records carry a `bbox` of their extent; Overture place records may add
`address` entries. `hthree` is declared but not yet produced. Handle the
union, not the current subset.

`variants` is populated only for `org.atgeo.places.overture.place`, from the
upstream multilingual and rule-based names. The other two collections always
carry an empty array — the field is part of the record shape everywhere, but
no producer fills it for OSM or divisions today. Don't build multilingual
lookup against a collection that has none.

`attributes` is the source's own vocabulary, passed through rather than
homogenized: OSM records carry a filtered tag map including the primary
category tag; Overture place records carry the upstream Overture fields
(names, categories, websites, and so on); division records carry `subtype`,
`country`, `level`, and `population` — the last always present, 0 meaning
the source recorded none — plus `region` and `wikidata` when present.

`relations.within` lists the divisions containing the record, as `{rkey,
name, level}` objects whose `rkey`s are collection-qualified —
`org.atgeo.places.overture.division:{id}` — ordered broadest first
(containment level ascending, then by id). Containment reaches every level
in the vocabulary, country through microhood, not just locality and
coarser, wherever the source data supports it.

Two declared fields are not yet produced: `published_at` and `same_as`.
They are declared ahead of the work that will emit them (dataset
conflation, for the latter). A client must tolerate their absence now and
their presence later.

## Importance

`importance` is an integer from 0 to 100 expressing how much a place
matters relative to other places in the same collection. It blends two
signals: how busy the place's surroundings are, and how distinctive its
category is. It is comparable within a collection and meaningless across
collections.

The reference producer computes it as follows. For place records (Overture
places and OSM alike):

    importance = round(60 · min(density / 10, 1) + 40 · min(idf / 18, 1))

where `density` is ln(1 + n) for n the count of Overture places in the
enclosing zoom-15 cell — Overture-derived even when scoring OSM records,
so both collections share one notion of "busy" — and `idf` is ln(N / n)
over the source's own corpus of named, categorized places: rare categories
score high, ubiquitous ones score low.

For division records:

    importance = round(40 · min(ln(1 + population) / 20, 1))

except localities, which add the density term the way places do:

    importance = round(60 · min(density / 10, 1) + 40 · min(ln(1 + population) / 20, 1))

with `density` averaged over the zoom-15 cells intersecting the locality's
bounding box. A consequence worth knowing: divisions other than localities
top out at 40.

The normalization constants (10, 18, 20) are producer parameters, not
protocol. Another producer could choose differently and still be an ATGeo
gazetteer.

## Containment levels

Division containment uses this vocabulary rather than any source dataset's
native admin-level field. Overture's `admin_level` is deliberately excluded:
in practice it is 96% NULL and ambiguous even within a single subtype.

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

The stride-5 numbering follows the descent locality → borough → macrohood →
neighborhood → microhood, so a macrohood sorts above its neighborhoods and a
microhood nests inside its neighborhood. The gaps are deliberate insertion
room. There is no level 0: continents are never emitted as divisions.
`borough` is mapped but currently unpopulated — present Overture division
data contains no borough subtype.

A subtype outside this table fails the producer's build before any artifact
is written, so the guarantee a client gets is simple: no record ever carries
an out-of-vocabulary level.

## XRPC interface

Two methods, both at `/xrpc/{nsid}`. Every error returns HTTP 400 with a
body of `{"error": name, "message": text}`. Responses may include keys not
documented here; per the forward-compatibility rule above, ignore them.

### org.atgeo.getCoverage

Which tiles cover a bounding box.

Parameters: `collection` (required, NSID), `bbox` (required, comma-separated
`minLon,minLat,maxLon,maxLat` in WGS84 decimal degrees).

Returns `{"tiles": [...]}` — a sorted list of fully-formed, opaque tile
URLs. Fetch them all and merge; filtering and ordering are yours, applied
after the merge.

A bbox may cross the antimeridian: `minLon > 0 > maxLon` means the box
wraps across ±180°. Tiles that merely touch the box at an edge or corner
count as covering it.

Errors, in evaluation order:

- `BboxTooPrecise` — any coordinate carrying more than two decimal places.
  This is the precision floor from [the safety principle](#the-safety-principle):
  requests snap to a 0.01° grid, about 1.1 km at the equator. The check is
  textual — `37.770` fails on its trailing zero, and scientific notation is
  rejected outright. The server rejects rather than truncating, so a client
  that leaks precision hears about it instead of being silently covered
  for. Snap your coordinates before sending.
- `InvalidBbox` — not four comma-separated finite numbers, `minLat ≥
  maxLat`, or `minLon ≥ maxLon` without the antimeridian signature above.
- `CollectionNotFound` — the collection is unknown or not currently
  servable.
- `BboxTooLarge` — the box covers more tiles than the deployment's budget,
  50 by default. Ask for a smaller region.

### com.atproto.repo.getRecord

One record by key.

Parameters: `repo`, `collection`, `rkey` (all required). `repo` is the
gazetteer's own hostname. A `cid` parameter is declared for future version
selection and is not yet honored.

Returns `{"uri", "source", "license", "importance"?, "value"}`. `source`
and `license` mean what they mean in the tile payload. `importance` appears
when the record has one, hoisted out of `value` into the response wrapper.
There is no `cid` key in the response — not even a null one.

Errors: `CollectionNotFound`, `RecordNotFound`.

One special case: with `collection=com.atproto.lexicon.schema`, the method
returns the server's own lexicon documents as `{uri, value}` with an
`at://` URI and no source, license, or importance. Fetching `GET /{nsid}`
(below) is the simpler way to get the same documents.

### Plain HTTP

- `GET /{collection}/{rkey}` returns the bare record value, no envelope.
  This is what makes the `uri` in every tile record dereferenceable.
- `GET /{nsid}` returns the lexicon document for that NSID.
- `GET /.well-known/did.json` identifies the server as a `did:web`.

## Collections

The producer currently builds three collections:

| collection | source | license |
|---|---|---|
| `org.atgeo.places.overture.place` | [Overture Maps](https://overturemaps.org/) places | [Overture attribution](https://docs.overturemaps.org/attribution/) |
| `org.atgeo.places.osm` | [OpenStreetMap](https://www.openstreetmap.org/) | [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/) |
| `org.atgeo.places.overture.division` | [Overture Maps](https://overturemaps.org/) divisions | [Overture attribution](https://docs.overturemaps.org/attribution/) |

A deployment may serve any subset. Ask `getCoverage`; a collection it
doesn't recognize is a collection the deployment doesn't serve.
