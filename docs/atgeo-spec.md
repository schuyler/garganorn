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

For a region too large to cover with the regular tiles, the result is a
sample: see [The summary band](#the-summary-band).

## Tiles

A tile is a set of place records associated with one quadkey cell.

**Quadkey scheme.** Web-Mercator (Bing) quadkeys over WGS84 longitude and
latitude. Digits encode `0=NW, 1=NE, 2=SW, 3=SE`, with y increasing
southward. The outermost row at every zoom reaches the pole (±90°), so
every latitude is covered. Zoom is the length of the quadkey string.

**Tile URLs are opaque.** A client never constructs one; it receives whole
URLs from `getCoverage` and fetches them. A tile URL is permanently
immutable — it never serves two different byte streams. When the gazetteer
publishes a new data release, the URLs change completely, which costs a
client nothing because a client always asks `getCoverage` again rather than
remembering URLs. Old releases are eventually retired, so a hoarded URL will
in time return 404; that is the penalty for remembering.

**Zoom and assignment.** The regular tiles exist at zooms 6 through 17. For
point records, assignment is adaptive: a record goes in the coarsest tile
holding no more than a per-deployment cap of records (1,000 by default),
falling back to zoom 17 when no coarser tile qualifies. Administrative
divisions are polygons and instead appear in every tile their geometry
overlaps; a division's per-tile count is not bounded by that cap. A
collection may also carry a [summary band](#the-summary-band) at zooms 1
through 5. Consequences a client must handle: tiles at different zooms
coexist in one collection, and no fixed zoom may be assumed.

**Deduplicate by `rkey`.** A client concatenating `records` arrays from
more than one tile in the same collection MUST deduplicate by `rkey`.
This holds for every collection, unconditionally — a client can't tell
from the wire alone whether a given collection's current placement
behavior happens to avoid duplicates, and a future server-side change
shouldn't require a corresponding client-side change to stay correct.

Whether duplication actually occurs today varies by collection, as a
performance characteristic rather than a client-facing guarantee:
non-division collections currently place each record in exactly one
regular tile, plus one summary tile for the most important records, and a
single `getCoverage` answer never mixes the two bands, so concatenating
its tiles produces near-zero redundant bytes in practice. The division collection
(`org.atgeo.places.overture.division`) is the current exception: a
division can be referenced from more than one tile, so real duplication
should be expected there.

**Transport.** Tiles are served with `Content-Type: application/json`.
Wire compression is negotiated via standard HTTP content negotiation
against the request's `Accept-Encoding` — clients should rely on their
HTTP library's usual behavior rather than assuming any particular
encoding. Every response, tiles included, carries
`Access-Control-Allow-Origin: *` so browser clients on any origin can read
it; servers MUST send it.

**Caching.** Because tile URLs are immutable, tiles are served with
`Cache-Control: public, max-age=604800, immutable` — cache them for a week
and never revalidate. `getCoverage` and `describeService` responses carry
`max-age=3600`, so a client learns about a new release within an hour of
asking.

### The summary band

A collection may carry a coarse band of tiles at zooms 1 through 5 (quadkeys
shorter than six characters), alongside the regular tiles. It holds the
10,000 records with the highest `importance` in the collection, plus, for the
division collection, every division of subtype `country`, `region` or
`dependency`. A record in the summary band is byte-identical to its copy in
the regular tiles, so the deduplication rule above covers it. A summary tile
can hold more records than the per-tile cap.

`getCoverage` uses the band as a fallback. It answers from the regular tiles
first; if that answer would exceed the deployment's tile budget and the
collection has a summary band, it returns the summary tiles that intersect the
box instead, with no budget applied. The band is bounded by construction, a
few dozen tiles per collection. The result is deliberately a sample of the
region, not everything in it.

Nothing in the response says which kind of answer it is. `getCoverage`
returns only `tiles`, and a summary tile looks like any other. A client that
needs to know can see it only by the quadkey length in the tile URL, which
this document does not define as part of the interface; tile URLs are
opaque.

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

- **`uri`** — `{base}/{collection}/{rkey}`, a dereferenceable URL where a
  GET returns the bare record value. `{base}` is the public base URL of the
  deployment serving the data; any host may serve the data under its own
  base. The base is `https` for any deployment reachable from the open
  internet. It may be `http` only for a development server on a development
  host: `localhost`, a `*.localhost` name, or a loopback, private, or
  link-local IP address. The rkey is served raw: characters legal in a URI
  path segment, colons included, are not percent-encoded.

  `getRecord` reports the same form, so tile URIs and XRPC-resolved URIs
  agree when the tiles were exported for the serving deployment's base. A
  server serving tiles exported for a different base (a development server
  using production-built tiles, say) reports its own base from `getRecord`,
  while the tile URIs keep the exporting deployment's base. Clients must not
  assume the two match in that case.

  Gazetteer records never use `at://`. That scheme addresses ATProto
  repository data, and a gazetteer record is not repository-backed: no
  signed commit includes it, so an `at://` URI would promise a verifiability
  the record cannot deliver. The URI scheme is a provenance signal — an
  HTTP(S) URL means "not repository data, not commit-verifiable."

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

`variants` is populated for all three collections: `org.atgeo.places.overture.place`
and `org.atgeo.places.overture.division` from upstream multilingual and
rule-based names, `org.atgeo.places.osm` from OSM name tags — `name:{lang}`,
`alt_name`, `official_name`, and related tags.

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

Three methods, all at `/xrpc/{nsid}`. Every XRPC error returns HTTP 400 with
a body of `{"error": name, "message": text}`. Responses may include keys not
documented here; per the forward-compatibility rule above, ignore them.

### org.atgeo.describeService

Which collections this deployment serves.

Takes no parameters. Returns `{"did", "collections": [...]}`. `did` is the
`did:web` the deployment identifies as, the same identifier
`/.well-known/did.json` carries. Each element of `collections` is
`{"collection", "metadata"?}`: the collection's NSID and, when the serving
build produced one, the URL of its [metadata record](#collection-metadata).

The list is authoritative. A collection absent from it returns
`CollectionNotFound` from `getCoverage`, and the other way around.

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
  50 by default, and the collection has no [summary band](#the-summary-band).
  Ask for a smaller region. A collection with a summary band never raises
  this; it answers with the summary tiles instead, and the response does not
  say that it did.

### com.atproto.repo.getRecord

One record by key.

Parameters: `repo`, `collection`, `rkey` (all required). Clients MUST send
the `did` from `describeService` as `repo`. The lexicon types `repo` as an
AT Protocol identifier, and the server rejects any value that is not a valid
handle or DID. Today it does nothing else with the value, so any valid
identifier gets the same record; a client that depends on that is not
conformant. A `cid` parameter is declared for future version selection and
is not yet honored.

Returns `{"uri", "source", "license", "importance"?, "value"}`. `source`
and `license` mean what they mean in the tile payload. `importance` appears
when the record has one, hoisted out of `value` into the response wrapper.
There is no `cid` key in the response — not even a null one.

Errors: `CollectionNotFound`, `RecordNotFound`.

One special case: with `collection=com.atproto.lexicon.schema`, the method
returns the server's own lexicon documents as `{uri, value}` with an
`at://` URI and no source, license, or importance. Fetching `GET /{nsid}`
(below) is the simpler way to get the same documents.

The other special case is `collection=org.atgeo.collection` with the
described collection's NSID as `rkey`, which returns that collection's
[metadata record](#collection-metadata) as `{uri, source, license, value}`,
with an HTTP(S) `uri` and no `importance`.

### Plain HTTP

- `GET /{collection}/{rkey}` returns the bare record value, no envelope.
  This is what makes the `uri` in every tile record dereferenceable. It
  answers HTTP 404 with an `{"error", "message"}` body for an unknown
  collection or record, where XRPC answers 400.
- `GET /{nsid}` returns the lexicon document for that NSID.
- `GET /.well-known/did.json` is the server's DID document: `id` is the
  `did:web` also reported by `describeService`.

## Collection metadata

Each served collection may have an `org.atgeo.collection` record describing
the build being served, keyed by the collection's NSID and reachable as
`getRecord` or as `GET /org.atgeo.collection/{nsid}`. `describeService` gives
its URL. Its fields: `collection`, `source`, `license`, `generatedAt` (the
release's `generated_at`), `recordCount`, `extent` (a `bbox` aligned to tile
boundaries), and `locationTypes` are always present; `attribution`,
`containmentLevels`, `categories` (observed values with counts) and
`attributes` (keys records may carry) are optional. The vocabularies are
observed, not closed. The normative schema is the `org.atgeo.collection`
lexicon.

## Collections

The producer currently builds three collections:

| collection | source | license |
|---|---|---|
| `org.atgeo.places.overture.place` | [Overture Maps](https://overturemaps.org/) places | [Overture attribution](https://docs.overturemaps.org/attribution/) |
| `org.atgeo.places.osm` | [OpenStreetMap](https://www.openstreetmap.org/) | [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/) |
| `org.atgeo.places.overture.division` | [Overture Maps](https://overturemaps.org/) divisions | [Overture attribution](https://docs.overturemaps.org/attribution/) |

A deployment may serve any subset. Ask `describeService`; a collection absent
from its list is a collection the deployment doesn't serve.
