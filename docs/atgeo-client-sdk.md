# The ATGeo client SDK

Two methods. `searchPlaces` finds places in a region; `getPlace` fetches one
by key. Everything else — coverage lookup, tile fetching, decompression,
matching, ranking, caching — happens inside those two calls and is not the
developer's problem.

Nothing here is built yet. This document is the design the first
implementation should follow. It replaces an earlier SDK design that assumed
clients hold the tile manifest and compute coverage locally; the gazetteer
does not distribute one, which invalidated most of that surface.

The wire format this describes is in [atgeo-spec.md](atgeo-spec.md), which is
normative for anything on the network. Where the two disagree, the spec wins.

## Why two methods

The gazetteer serves exactly two things a client can call: `getCoverage` and
`getRecord`. A larger SDK surface would be inventing capabilities the server
doesn't have. The earlier design had six read methods, three of which
(`coverage`, `prefetch`, `discover`) described machinery that doesn't exist,
and two of which (`nearby`, `search`) were the same call with different
arguments.

The mapping is one to one:

| SDK | Server |
|---|---|
| `searchPlaces` | `org.atgeo.getCoverage`, then N tile fetches |
| `getPlace` | `com.atproto.repo.getRecord` |

The client does not implement quadkeys. Tile URLs are opaque, coverage is
computed server-side, and the antimeridian is the server's problem. That
deletes the hardest half of what a client would otherwise have to get right,
and it's the main reason this shape is worth having rather than merely
smaller.

## searchPlaces

```ts
const places = await geo.searchPlaces({
  region: { lat: 37.776, lon: -122.434, radius: 1000 },
  text: "coffee",
  sort: "distance",
});
```

`region` accepts either a centre and radius or a bounding box; both are
ordinary WGS84 degrees at whatever precision the caller has. `text` is
optional — omit it and you get everything in the region, which is the
"what's around me" case. `sort` is `relevance` (default), `importance`, or
`distance`.

Internally: snap the region outward, ask `getCoverage`, fetch the tiles it
names, decompress, parse, filter, rank, and hand back places. One network
round trip for coverage, then one HTTP/2 connection's worth of tile fetches.

### One collection per call

`searchPlaces` searches exactly one collection, defaulting to
`org.atgeo.places.overture.place`. This is an invariant, not a limitation to
route around later: it matches `getCoverage`, it matches `getRecord`, and it
matches how AT Protocol addresses records generally. Applications that want
Overture places and OSM places make two calls and decide for themselves how
to interleave the results.

There is no merged ranking across collections, because there is no defined
way to do one. `importance` is comparable within a collection and meaningless
across collections — an Overture place scoring 62 and an OSM place scoring 62
are not equivalently important, they are scored against different corpora
with different category vocabularies. An SDK that merged them would be
inventing a comparison the data does not support.

The same gap decides it at the application layer regardless of what the SDK
allows. `attributes` is the source's own vocabulary passed through rather
than homogenized, so an OSM record's category lives in a tag map and an
Overture record's lives in Overture's category tree, and nothing translates
between them. An app that renders a category icon, filters to restaurants, or
does anything at all with attributes has to write against one shape. Picking
a collection and staying there is what apps will do in practice; the
invariant just declines to pretend otherwise.

### Division results need deduplication

Every other collection guarantees one record, one tile. The division
collection (`org.atgeo.places.overture.division`) does not: a division
can be referenced from more than one tile it overlaps, so a `searchPlaces`
call against that collection must dedupe its concatenated results by
`rkey` before matching and ranking. [atgeo-spec.md](atgeo-spec.md) is
normative here; this note exists so the concatenation step doesn't get
written once per collection and get this one wrong.

### Precision is the SDK's job, not the developer's

`getCoverage` rejects any coordinate with more than two decimal places
(`BboxTooPrecise`), because a bbox centred on raw GPS locates a person to ten
metres no matter how large the box is. The reasoning is in
[tile-privacy-design.md](tile-privacy-design.md).

The SDK snaps the region outward to the 0.01° grid before sending it, and
keeps the caller's original unsnapped region for local work. A developer
never sees the grid, never reads about it, and cannot leak past it —
`BboxTooPrecise` is unreachable through the SDK by construction.

This is also where distance sorting gets its origin. The caller's region
already carries the point they care about: its centre. If they built the
region around a device position, the centre is that position; if they built
it from a map viewport, the centre is what they're looking at. Both are the
right thing to measure from, and the precise value stays on the device
because it is only ever used for local ranking. No second parameter, no
privacy boundary for the developer to reason about.

Results are filtered back to the caller's original region before they are
returned. The snapped-outward region is what gets *fetched* — that's where
the privacy benefit lives, in the fetch pattern not centring on the user —
so filtering the results costs nothing and keeps the API predictable. A tight
region in a sparse area can filter down to nothing while the SDK holds
perfectly good places a couple of kilometres away; predictability is worth
more than the near-miss, but it is a thing to watch in testing rather than a
thing to be surprised by in production.

### Partial results

Tiles are individually optional. A tile that fails to fetch degrades the
result set rather than failing the call, which means a caller showing "three
results" may be showing three results out of forty-one tiles that loaded and
nine that didn't. The return value says so. This is the one piece of
protocol-shaped behaviour the two-method surface has to expose that the
server does not model.

### How much this costs

Unbounded by design, bounded in practice.

The protocol places no limit on how much a client fetches and filters. There
is no pagination, no partial-result protocol, no server-side ranking to cut
the set down — that is the whole point of client-side search, and building a
limit into the SDK would be reintroducing the thing the architecture removed.

In practice the ceiling comes from `getCoverage`'s tile budget, 50 by
default. Against the 2026-08-08 global build, Overture places averaged about
62 KB gzipped per tile, so a maximal query is roughly 3 MB and up to 50,000
records filtered locally. Comfortable on a laptop, noticeable on a phone.

The budget bites soonest where the SDK is most useful. In Manhattan or
Shibuya, tiles subdivide to keep up with density, so a region that returns
eight tiles in Lisbon returns fifty in midtown and fails with
`BboxTooLarge`. What usable search radius that leaves in a dense city is an
open question that wants measuring before the first implementation freezes
its defaults.

`BboxTooLarge` surfaces to the caller. The SDK does not silently subdivide an
over-large region into several `getCoverage` calls; the budget exists to
protect the origin, and quietly issuing four requests where the server asked
for one defeats it.

### Matching and ranking

Normalize with `N(s)`: Unicode NFKD, strip combining marks (category Mn),
case fold, collapse whitespace. Every platform ships this natively —
`String.normalize` plus `\p{Mn}` in JavaScript, Foundation folding in Swift,
`java.text.Normalizer` on Android — which is exactly why it drifts between
implementations and needs fixtures.

Match `N(query)` against `N(name)` and each `N(variant)`, in tiers: exact,
prefix, token-prefix (every query token prefixes some name token), substring,
no match. Rank by tier ascending, then `importance` descending, then distance
ascending, then `uri` ascending as the total-order tiebreak.

No fuzziness. Jaro-Winkler and trigram matching died with `searchRecords` and
are not being reimplemented client-side; typo tolerance is a later decision,
made once, not an individual implementation's private improvement.

## getPlace

```ts
const place = await geo.getPlace({
  collection: "org.atgeo.places.overture.place",
  rkey: "08f2830828b0d3c0043f4f2e5b6a1c9d",
});
```

Cache first, network second. If the record's tile is already in the cache —
the common case, because the user just tapped a search result — the record
comes from memory, which is both faster and tells the server nothing. If it
isn't, the SDK calls `getRecord`.

The fallback is worth being clear-eyed about: a `getRecord` call tells the
server exactly which place a user is looking at, which is a sharper signal
than the kilometre-square box `getCoverage` sees. That is an acceptable cost
for its real use — resolving a link someone shared, where the client has a
key and no region — and a bad one for hydrating search results, which is why
the cache is checked first rather than as an optimisation.

Without a manifest the client cannot work out which tile holds an arbitrary
rkey, so "check the cache" means the tiles already fetched, not a lookup.

## Caching

Tile URLs are permanently immutable and change wholesale when the gazetteer
publishes a release, so tiles cache forever and never revalidate. Coverage
responses carry `max-age=3600`, so a client picks up a new release within an
hour of asking. That combination gets most of an offline mode for free: once
a region has been searched, searching it again works with no network at all
until the coverage response expires.

There is no `prefetch` method. A `searchPlaces` call with no text and a large
region is already a warm-up, and adding a method whose only job is to be a
different name for that would be surface without capability. If deliberate
offline support turns out to be a real requirement rather than a nice
property, it earns its own design.

Budget 50 MB by default, LRU, configurable.

## Errors

| Error | Meaning |
|---|---|
| `BboxTooLarge` | Region covers more tiles than the deployment allows. Ask for less. |
| `CollectionNotFound` | This deployment doesn't serve that collection. |
| `RecordNotFound` | No such record. |
| `InvalidBbox` | Malformed region. Should be unreachable through the SDK. |
| `BboxTooPrecise` | Coordinates finer than 0.01°. Unreachable through the SDK. |

The last two are listed because the server can return them and a debugging
developer will want to recognise them, not because the SDK should ever
provoke one. If either shows up in the wild, the SDK has a bug.

## What is deliberately absent

**Region-less search.** You cannot search for "Kyoto" without knowing roughly
where Kyoto is. Tiles killed couch-geocoding: fetching tiles for a place
requires locating it first. The earlier design proposed recovering this by
wholesale-prefetching the division tileset and running a locality tier over
it, which needs a manifest the gazetteer does not publish. The real answer is
summary tiles, sketched in
[planned-features.md](planned-features.md#summary-tiles-for-region-less-search).
Until those exist, `region` is required.

**The write path.** `composeLocation`, `checkin`, and anything else that
writes to a repo. Blocked on a check-in lexicon that nobody has drafted, and
out of scope for the gazetteer besides. When there is a lexicon there can be
a separate package; the read SDK should not carry a write path shaped around
a guess.

**Discovery.** `Atgeo.discover(didOrHandle)` needs the DID service entry and
`org.atgeo.tiles.service` announcement records described under Discovery in
[atgeo-appview-design.md](atgeo-appview-design.md). Neither exists — the
served `did.json` advertises only `AtprotoPersonalDataServer`. The
constructor takes a host.

**Version negotiation.** There is no version field in the tile envelope and
no version to reject. Ignore keys you don't recognise; that is the whole
forward-compatibility story.

**Anything that describes a collection.** There is no way to ask what a
collection contains, what its attributes look like, or which collections a
gazetteer serves — the only discovery mechanism is guessing an NSID and
seeing whether `getCoverage` returns `CollectionNotFound`. The SDK will want
to surface both once they exist; see
[planned-features.md](planned-features.md#collection-and-service-metadata).

## Conformance fixtures

The earlier design called for a versioned corpus in its own repo, consumed by
TypeScript, Swift, and Kotlin implementations in CI. That was the right answer
to "three SDKs, no shared code," and it is premature for one.

Two of the six proposed fixture files are also gone: `quadkey.json` and
`coverage.json` tested client-side quadkey math and bbox-to-tile-set
intersection, neither of which a client does any more. `envelope.json` is
mostly covered by `org.atgeo.tilePayload`, the lexicon the tile format now
has.

What actually needs fixtures is `normalize.json` and `ranking.json` — the two
things that drift between implementations and cannot be validated against a
schema. Write the first SDK, keep the fixtures it generates, and promote them
to a shared corpus when there is a second implementation to hold to them.

## Open questions

- **Usable search radius in dense cities.** The 50-tile budget is reached in
  Manhattan-density areas. Measure before picking defaults.
- **Default host.** The zero-configuration constructor is most of the
  adoption argument, and it needs a deployment behind it with someone paying
  for the bandwidth. This is a governance question wearing a technical hat.
- **Platforms after the first.** TypeScript is the obvious first
  implementation. Whether Swift and Kotlin follow determines whether the
  fixture corpus needs to become a real versioned artifact.
