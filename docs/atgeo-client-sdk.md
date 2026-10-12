# The ATGeo client SDK

Two methods do the work. `searchPlaces` finds places in a region; `getPlace`
fetches one by key. Coverage lookup, tile fetching, matching, ranking, and
caching all happen inside those two calls and are not the developer's problem.

The TypeScript SDK ([atgeo-sdk-js](https://github.com/schuyler/atgeo-sdk-js),
`@atgeo/client`) is the first implementation. Its README is the developer
reference: signatures, options, result shapes, and error codes. This document
is the design rationale, and the behavior a port to another language has to
make a decision about.

The wire format is in [atgeo-spec.md](atgeo-spec.md), which is normative for
anything on the network. Where the two disagree, the spec wins.

## Why two methods

The gazetteer serves two things a client can call to read places:
`getCoverage` and `getRecord`. A larger search surface would be inventing
capabilities the server doesn't have.

| SDK | Server |
|---|---|
| `searchPlaces` | `org.atgeo.getCoverage`, then N tile fetches |
| `getPlace` | `com.atproto.repo.getRecord` |

The client does not implement quadkeys. Tile URLs are opaque and coverage is
computed server-side. That deletes the hardest half of what a client would
otherwise have to get right, and it's the main reason this shape is worth
having rather than merely smaller.

Two metadata calls sit beside them, `describeService` and `getCollectionInfo`.
They exist because `getPlace` needs the server's DID and callers want to know
what a collection holds; they take no part in search.

## searchPlaces

Internally: snap the region outward, ask `getCoverage`, fetch the tiles it
names, parse, dedupe, filter to the region, match, rank, and hand back places.
One coverage round trip, then tile fetches, eight at a time.

`region` is a circle (centre and radius in meters) or a bounding box in WGS84
degrees. A box with `minLon > maxLon` crosses the antimeridian. `text` is
optional; without it you get everything in the region, which is the "what's
around me" case.

### One collection per call

`searchPlaces` searches exactly one collection, defaulting to
`org.atgeo.places.overture.place`. This is an invariant, not a limitation to
route around later: it matches `getCoverage`, it matches `getRecord`, and it
matches how AT Protocol addresses records generally. Applications that want
Overture places and OSM places make two calls and decide for themselves how
to interleave the results.

There is no merged ranking across collections, because there is no defined
way to do one. `importance` is comparable within a collection and meaningless
across collections: an Overture place scoring 62 and an OSM place scoring 62
are not equivalently important, they are scored against different corpora
with different category vocabularies. An SDK that merged them would be
inventing a comparison the data does not support.

The same gap decides it at the application layer regardless of what the SDK
allows. `attributes` is the source's own vocabulary passed through rather
than homogenized, so an OSM record's category lives in a tag map and an
Overture record's lives in Overture's category tree, and nothing translates
between them. An app that renders a category icon, filters to restaurants, or
does anything at all with attributes has to write against one shape.

### Deduplicate concatenated results by `rkey`

Every search that concatenates `records` arrays from more than one tile must
dedupe the result by `rkey` before matching and ranking, for every collection
and not just ones that currently produce duplicates. [atgeo-spec.md](atgeo-spec.md)
is normative here; this note exists so the concatenation step doesn't get
written once per collection and skip dedup because a given collection looks
clean today.

In practice only the division collection
(`org.atgeo.places.overture.division`) produces real duplicates today: a
division can be referenced from more than one tile it overlaps. Every other
collection places each record in exactly one tile. The SDK keeps the first
copy of each `rkey` in tile order and does not assume that stays true.

### Precision is the SDK's job, not the developer's

`getCoverage` rejects any coordinate with more than two decimal places
(`BboxTooPrecise`), because a bbox centred on raw GPS locates a person to ten
metres no matter how large the box is. The reasoning is in
[tile-privacy-design.md](tile-privacy-design.md).

The SDK snaps the region outward to the 0.01 degree grid before sending it,
and keeps the caller's original region for local work. A developer never sees
the grid and cannot leak past it. A region crossing the antimeridian is split
into two `getCoverage` calls, one on each side of ±180°, and unions the tile
sets, rather than sending the wrapped box that the server also accepts. The
client still does no quadkey math.

Distance sorting gets its origin the same way. The caller's region already
carries the point they care about: its centre. If they built the region
around a device position, the centre is that position; if they built it from
a map viewport, the centre is what they're looking at. The precise value is
only ever used for local ranking and never leaves the device. No second
parameter, no privacy boundary for the developer to reason about.

Results are filtered back to the caller's original region before they are
returned. A point location is kept if it falls inside the region; a place
with only a bounding box is kept if that box touches the region. The
snapped-outward region is what gets *fetched*, and that's where the privacy
benefit lives: in a fetch pattern that doesn't centre on the user. Filtering
the results costs nothing and keeps the API predictable. A tight region in a
sparse area can filter down to nothing while the SDK holds perfectly good
places a couple of kilometres away. Predictability is worth more than the
near-miss, but it is a thing to watch in testing rather than be surprised by
in production.

### Partial results

Tiles are individually optional. A tile that fails to fetch is skipped and
counted rather than failing the call, so a caller showing "three results" may
be showing three results out of forty-one tiles that loaded and nine that
didn't. The result carries `complete` and a fetched/failed count so the
application can say so. This is the one piece of protocol-shaped behaviour
the two-method surface has to expose that the server does not model.

A failed coverage request is different: with no tile list there is nothing to
degrade, so the search rejects.

### How much this costs

Unbounded by design, bounded in practice.

The protocol places no limit on how much a client fetches and filters. There
is no pagination, no partial-result protocol, no server-side ranking to cut
the set down. That is the whole point of client-side search, and building a
limit into the SDK would reintroduce the thing the architecture removed.

In practice the ceiling is `getCoverage`'s tile budget, 50 by default. The
budget bites soonest where the SDK is most useful: in Manhattan or Shibuya,
tiles subdivide to keep up with density, so a region that costs eight tiles
in Lisbon costs fifty in midtown.

When the regular answer would exceed the budget, a server whose collection
has a summary band does not fail. It answers with the coarse z1-z5 summary
tiles that intersect the region, which hold only the top records by
`importance` (see
[design-constraints.md](design-constraints.md#the-summary-band-is-a-coarse-tile-tier-for-region-less-resolution)).
The server marks such an answer with `summary: true` in the `getCoverage`
response, and the SDK exposes it as `SearchResult.summary`, always present and
`false` for a regular answer. The SDK follows tile URLs without interpreting
them, so it never infers the flag from their shape. It fetches the tiles,
filters them to the region, and ranks them like any others. The result is a
deliberately incomplete sample of the region, and `summary` is how a caller
tells "everything here" from "the important things here".

`summary` is independent of `complete`, which means only that no tile fetch
failed. A summary answer with every tile loaded is `summary: true,
complete: true`. If any bbox of an antimeridian-split search comes back as a
summary, the result is `summary: true`. A server that predates the flag
never sends it, so its answers read as `summary: false`.

`BboxTooLarge` reaches the caller only from a collection with no summary
band. The SDK does not subdivide an over-large region into several
`getCoverage` calls; the budget exists to protect the origin, and quietly
issuing four requests where the server asked for one defeats it.

### Matching and ranking

The SDK's matching and ranking are one implementation's choices, made so
results pass a legibility check by eye: typos find the right chain, a
nearby unimportant place doesn't beat a famous one, nothing is shuffled
between runs. Other implementations may choose differently. The wire format
doesn't depend on any of it.

Normalize with `N(s)`: Unicode NFKD, strip combining marks (category Mn),
case fold, collapse whitespace. Every platform ships this natively
(`String.normalize` plus `\p{Mn}` in JavaScript, Foundation folding in Swift,
`java.text.Normalizer` on Android), which is exactly why it drifts between
implementations and needs fixtures.

Matching is fuzzy. For each of a place's `name` and `variants[].name`, take
the best of:

- Jaro-Winkler between the whole normalized query and the normalized name,
  blended 0.5/0.5 with
- the mean over query tokens of each token's best Jaro-Winkler against any
  name token.

Only the first 12 words of the query count. Names scoring below 0.6 are
dropped; the 0.5 weight and the 0.6 threshold are this implementation's
constants. The token blend applies to single-token queries too. A
whole-string-only score would drop "coffee" against "Blue Bottle Coffee",
because the full-string similarity is low even though the token matches
exactly.

Three sort modes:

- `fused` (default): score rounded to three decimals, descending; then
  `importance` descending; then distance ascending; then `uri`. Rounding makes
  near-ties on score fall through to importance rather than letting a
  0.001 difference override it. With no text every score is equal, so this
  is importance, then distance.
- `distance`: distance ascending, then importance, then score, then `uri`.
  Text still filters poor matches.
- `similarity`: score (rounded), then distance, then `uri`. It requires
  text.

A missing `importance` counts as 0, and a place with no point location sorts
after those with one. The `uri` tiebreak makes the order total.

## getPlace

Cache first, network second. If the record's tile is already in the cache
(the common case, because the user just tapped a search result) the record
comes from memory, which is both faster and tells the server nothing.
Otherwise the SDK calls `getRecord`. Its `repo` parameter is validated as an
AT Protocol identifier, so the SDK first needs the server's DID, which it
gets from `describeService` and caches. `getRecord` returns `importance`
beside the record rather than inside it; the SDK merges it into the returned
place.

The fallback is worth being clear-eyed about: a `getRecord` call tells the
server exactly which place a user is looking at, which is a sharper signal
than the kilometre-square box `getCoverage` sees. That is an acceptable cost
for its real use, resolving a link someone shared where the client has a key
and no region, and a bad one for hydrating search results, which is why the
cache is checked first.

Without a manifest the client cannot work out which tile holds an arbitrary
rkey, so "check the cache" means scanning tiles already fetched, not a lookup.

## Caching

Tile URLs are immutable and change wholesale when the gazetteer publishes a
release, so tiles cache until evicted and never revalidate. Coverage
responses carry `max-age=3600`, and the SDK holds coverage and the service
description for the same hour, so a client picks up a new release within an
hour. The cache is in memory only, least-recently-used out first, 50 MiB by
default; a new client starts cold. Once a region has been searched, searching
it again makes no requests until the coverage entry expires.

There is no `prefetch` method. A `searchPlaces` call with no text and a large
region is already a warm-up, and a method whose only job is to be a different
name for that would be surface without capability. If deliberate offline
support turns out to be a real requirement, it earns its own design.

## Errors

The server's error codes pass through as the `code` of the SDK's error. What
can actually reach a caller:

| Error | Source | Meaning |
|---|---|---|
| `BboxTooLarge` | `getCoverage` | Budget exceeded and the collection has no summary band. Ask for less. |
| `CollectionNotFound` | server, or SDK | The server doesn't serve that collection. `getCollectionInfo` raises it itself when `describeService` doesn't list the collection. |
| `RecordNotFound` | `getRecord` | No such record. |
| `HttpError` | either | Non-2xx without a parseable error body. |

`InvalidBbox` and `BboxTooPrecise` are unreachable by construction: the SDK
validates the region and snaps it before sending. If either shows up in the
wild, the SDK has a bug. An invalid region (non-finite or out-of-range
coordinates, `minLat > maxLat`, negative radius) and `similarity` sort
without text throw a synchronous `TypeError` before any request. A failed
coverage request rejects the search; a failed tile is skipped with
`complete: false`.

## What is deliberately absent

**Region-less search.** You cannot search for "Kyoto" without knowing roughly
where Kyoto is. Tiles killed couch-geocoding: fetching tiles for a place
requires locating it first. `region` is required. The summary band makes a
very large region (the whole world, say) answerable, and the SDK will filter
and rank whatever comes back, so a name search over a huge box can find
places important enough to be in the summary band. It is a property of what
the server returns, not a feature of the SDK, and carries the caveat above.

**The write path.** `composeLocation`, `checkin`, and anything else that
writes to a repo. Blocked on a check-in lexicon that nobody has drafted, and
out of scope for the gazetteer besides. When there is a lexicon there can be
a separate package; the read SDK should not carry a write path shaped around
a guess.

**Discovery by DID or handle.** `Atgeo.discover(didOrHandle)` would need the
DID service entry and `org.atgeo.tiles.service` announcement records
described under Discovery in
[atgeo-appview-design.md](atgeo-appview-design.md). Neither exists; the
served `did.json` advertises only `AtprotoPersonalDataServer`. The
constructor takes a server URL, default `https://places.atgeo.org`.
`describeService` and `getCollectionInfo` do cover asking a known server what
it serves.

**Version negotiation.** There is no version field in the tile envelope and
no version to reject. Ignore keys you don't recognise; that is the whole
forward-compatibility story.

## Conformance fixtures

The SDK's `fixtures/` directory holds language-neutral reference behavior for
ports:

- `normalize.json`: input/output pairs for `N(s)`.
- `jaro-winkler.json`: input/output pairs for the similarity function.
- `ranking-scenarios.json`: regions, text, and records, asserting which places
  land in the top K. It checks membership rather than exact order, because
  the weights are expected to change.

These are normative in the sense that they show what the reference
implementation does and a port should aim to match them. They are not a hard
requirement. A language may not be able to reproduce the search exactly
(string folding differs, floating point differs), and equal scores or equal
order across implementations is not a goal. A port that gives sensible fuzzy
results over the same wire format is conformant.

## Open questions

- **`region` stays required.** Whether the SDK should offer an explicit
  world-scale mode on top of the summary band, rather than leaving callers to
  pass a huge region, is undecided. The `summary` flag in the `getCoverage`
  response tells such a mode when it got a sample.
- **Usable search radius in dense cities.** Unmeasured. The 50-tile budget is
  reached in Manhattan-density areas; no one has measured what radius that
  leaves, or what a maximal query costs in bytes on a phone.
- **Default host.** `https://places.atgeo.org` is the constructor default.
  The zero-configuration constructor is most of the adoption argument, and it
  needs a deployment behind it with someone paying for the bandwidth. This is
  a governance question wearing a technical hat.
- **Platforms after the first.** TypeScript is the first implementation.
  Whether Swift and Kotlin follow determines whether the fixtures need to
  become a versioned shared artifact.
