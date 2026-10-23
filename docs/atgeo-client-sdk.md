# The ATGeo client SDK

Two calls do the work. `searchPlaces` finds places in a region; `getPlace` fetches one by its `origin`. Tile lookup, tile fetching, matching, ranking, and caching all happen inside them and are not the developer's problem.

The TypeScript SDK ([atgeo-sdk-js](https://github.com/schuyler/atgeo-sdk-js), `@atgeo/client`) is the first implementation. Its README is the developer reference: signatures, options, result shapes, and error codes. This document is the design rationale, and the behavior a port to another language has to make a decision about. The exact, testable statement of that behavior is the SDK repo's [port contract](https://github.com/schuyler/atgeo-sdk-js/blob/main/docs/port-contract.md), written for porters; where it and this document disagree about what the client does, it wins.

The wire format is in [atgeo-spec.md](atgeo-spec.md), which is normative for anything on the network. Where the two disagree, the spec wins.

## Why two calls

The gazetteer serves a few things a client can call to read places: `getTiles` and `getPlace` for places, `describeService` and `describeTileset` for what a server holds. A larger search surface would be inventing capabilities the server doesn't have.

| SDK | Server |
|---|---|
| `searchPlaces` | `org.atgeo.getTiles`, then N tile fetches |
| `getPlace` | the cached tiles, else `org.atgeo.getPlace` |
| `describeService` | `org.atgeo.describeService` |
| `describeTileset` | `org.atgeo.describeTileset` |

The client does not implement quadkeys. Tile URLs are opaque and the server works out which tiles cover a box. That deletes the hardest half of what a client would otherwise have to get right, and it's the main reason this shape is worth having rather than merely smaller.

The two describe calls are plain pass-throughs for callers who want to know what a server holds. They take no part in search; `describeService` is cached for an hour, `describeTileset` is not cached.

## searchPlaces

Internally: snap the region outward, ask `getTiles`, fetch the tiles it names, parse, dedupe, filter to the collection, filter to the region, match, rank, and hand back places. One `getTiles` round trip per bounding box (two across the antimeridian), then tile fetches, eight at a time.

`region` is a circle (centre and radius in meters) or a bounding box in WGS84 degrees. A box with `minLon > maxLon` crosses the antimeridian. `text` is optional; without it you get everything in the region, which is the "what's around me" case.

### One dataset per call

A search covers one dataset, optionally narrowed to one of its collections:

| `dataset` | `collection` | searched |
|---|---|---|
| absent | absent | `overturemaps.org` / `place` |
| absent | given | `overturemaps.org` / that collection |
| given | absent | the whole dataset |
| given | given | that collection |

So the default is Overture places, and `dataset: "openstreetmap.org"` alone searches nodes, ways and relations together, which is usually what you want from OSM, since the element type is an accident of how the mapper drew it. The `collection` parameter is sent to the server only when the search has one.

Tiles can hold records of collections you didn't ask for. All three OSM element types share the OSM tiles, for one. When the search has a collection, the SDK keeps the records whose `origin` starts with `urn:atgeo:{dataset}:{collection}:`, colon included, so `way` doesn't match `wayside`. A record with no `origin` matches no collection. Without a collection, nothing is filtered.

A dataset-wide search of `overturemaps.org` would mix places and divisions, built as separate tilesets whose `importance` isn't comparable (see below), which is one reason the default is `place` and divisions are on request.

There is no merged ranking across datasets, because there is no defined way to do one. `importance` is comparable within a tileset and meaningless across tilesets: an Overture place scoring 62 and an OSM place scoring 62 are not equivalently important, they are scored against different corpora with different category vocabularies. An SDK that merged them would be inventing a comparison the data does not support. Applications that want Overture places and OSM places make two calls and decide for themselves how to interleave the results.

The same gap decides it at the application layer regardless of what the SDK allows. `attributes` is the source's own vocabulary passed through rather than homogenized, so an OSM record's category lives in a tag map and an Overture record's lives in Overture's category tree, and nothing translates between them. An app that renders a category icon, filters to restaurants, or does anything at all with attributes has to write against one shape.

### Deduplicate concatenated results by `origin`

Every search that concatenates `records` arrays from more than one tile must dedupe the result by `origin` before filtering and ranking, for every collection and not just ones that currently produce duplicates. [atgeo-spec.md](atgeo-spec.md) is normative here; this note exists so the concatenation step doesn't get written once per collection and skip dedup because a given collection looks clean today.

In practice only divisions produce real duplicates today: a division can be referenced from more than one tile it overlaps. Every point collection places each record in one regular tile. The SDK sorts the tile URLs, keeps the first copy of each `origin` in that order, and does not assume that stays true. A record with no `origin` falls back to its `uri`; a record with neither can't be told apart from another and is never merged.

Dedupe happens before the region filter, so if two copies of one place differ in location and the winner lies outside the region, the place is dropped even though another copy is inside. Garganorn's copies of a place are byte-identical, so this can't happen against garganorn. It's a documented pothole for other producers' tilesets, not something to work around.

### Precision is the SDK's job, not the developer's

`getTiles` rejects any coordinate with more than two decimal places (`BboxTooPrecise`), because a bbox centred on raw GPS locates a person to ten metres no matter how large the box is. The reasoning is in [tile-privacy-design.md](tile-privacy-design.md).

The SDK snaps the region outward to the 0.01 degree grid before sending it, and keeps the caller's original region for local work. A developer never sees the grid and cannot leak past it. Two details matter to a port. Snapping tolerates floating-point error, so `37.77`, which is `3777.0000000000005` hundredths in binary, stays on its grid line instead of widening to `37.78`. And a region with no extent, a point or a line, is widened by one grid step, so the request is still a box. A region crossing the antimeridian is split into two `getTiles` calls, one on each side of ±180°, and the tile sets are unioned, rather than sending the wrapped box that the server also accepts. The client still does no quadkey math.

A circle becomes the box that encloses it, using the sphere's true longitude half-width rather than a flat `1/cos(lat)` approximation, which misses the circle's east and west edges at high latitudes. A circle that holds a pole spans every longitude.

Distance sorting gets its origin the same way. The caller's region already carries the point they care about: its centre. If they built the region around a device position, the centre is that position; if they built it from a map viewport, the centre is what they're looking at. The precise value is only ever used for local ranking and never leaves the device. No second parameter, no privacy boundary for the developer to reason about.

Results are filtered back to the caller's original region before they are returned. A point location is kept if it falls inside the region; a place with only a bounding box is kept if that box touches the region. A circle means the circle, not its enclosing square. The snapped-outward region is what gets *fetched*, and that's where the privacy benefit lives: in a fetch pattern that doesn't centre on the user. Filtering the results costs nothing and keeps the API predictable. A tight region in a sparse area can filter down to nothing while the SDK holds perfectly good places a couple of kilometres away. Predictability is worth more than the near-miss, but it is a thing to watch in testing rather than be surprised by in production.

### What goes on the wire

A search sends `getTiles` with the snapped bounding box, the dataset, and the collection if there is one. Then it sends GETs for tile URLs exactly as the server named them. Nothing else: not the search text, in any form, and not the precise region. The server can tell which tiles a client has fetched before, but nothing finer than the tiles themselves. `getPlace` on a cache miss sends the `origin` of the place asked for, and nothing else.

Diagnostics stay in the app's process. The client sends no telemetry and no error report, anywhere. A failed-tile entry carries a tile URL and what fetching it produced, never the search text or the region. The port contract's "Privacy invariants" section lists the tests a port has to pass.

### Partial results

Tiles are individually optional. A tile that fails to fetch is skipped and counted rather than failing the call, so a caller showing "three results" may be showing three results out of forty-one tiles that loaded and nine that didn't. The result carries `complete`, a fetched/failed count, and a `tileFailures` list, so the application can say so. Each failure has the tile URL and a `kind` that says where it went wrong: `refused` (not an absolute http(s) URL, or one carrying credentials, so no request was made), `status` (a non-2xx), `decode` (a 2xx that isn't a tile) or `transport` (the fetch itself threw). The kind comes from which step failed, not from the error's type. This is the one piece of protocol-shaped behaviour the small surface has to expose that the server does not model.

A failed `getTiles` request is different: with no tile list there is nothing to degrade, so the search rejects.

The client doesn't retry a failed tile. Tolerance runs the other way too: it reads only the fields it needs, skips a record or a tile it can't use, and treats a wrong-typed field as absent, so a server change that keeps the wire functional doesn't break apps built on it.

### How much this costs

Unbounded by design, bounded in practice.

The protocol places no limit on how much a client fetches and filters. There is no pagination, no server-side ranking, no partial-result protocol to cut the set down. That is the whole point of client-side search, and building a limit into the SDK would reintroduce the thing the architecture removed.

In practice the ceiling is `getTiles`'s tile budget, 50 by default (a deployment sets it with `max_tiles`). The budget bites soonest where the SDK is most useful: in Manhattan or Shibuya, tiles subdivide to keep up with density, so a region that costs eight tiles in Lisbon costs fifty in midtown.

When the regular answer would exceed the budget, a server whose tilesets all have an overview band does not fail. It answers with the coarse zoom 1 to 5 overview tiles that intersect the region, which hold only the top 10,000 records by `importance` per tileset (see [the overview band](atgeo-spec.md#the-overview-band), and for the design reasoning [design-constraints.md](design-constraints.md#the-summary-band-is-a-coarse-tile-tier-for-region-less-resolution)). The server marks such an answer with `overview: true` in the `getTiles` response, and the SDK exposes it as `SearchResult.overview`, always present and `false` for a regular answer. The SDK follows tile URLs without interpreting them, so it never infers the flag from their shape. It fetches the tiles, filters them to the region, and ranks them like any others. The result is a deliberately incomplete sample of the region, and `overview` is how a caller tells "everything here" from "the important things here".

`overview` is independent of `complete`, which means only that no tile fetch failed. An overview answer with every tile loaded is `overview: true, complete: true`. If either bbox of an antimeridian-split search comes back as an overview, the result is `overview: true`.

`BboxTooLarge` reaches the caller only when a selected tileset has no overview band. The SDK does not subdivide an over-large region into several `getTiles` calls; the budget exists to protect the origin, and quietly issuing four requests where the server asked for one defeats it.

### Matching and ranking

The SDK's matching and ranking are one implementation's choices, made so results pass a legibility check by eye: typos find the right chain, a nearby unimportant place doesn't beat a famous one, nothing is shuffled between runs. Other implementations may choose differently. The wire format doesn't depend on any of it.

Normalize with `N(s)`: lowercase, Unicode NFKD, lowercase again, strip combining marks (general category Mn), collapse whitespace. Every platform ships the pieces natively (`String.normalize` plus `\p{Mn}` in JavaScript, Foundation folding in Swift, `java.text.Normalizer` on Android), which is exactly why it drifts between implementations and needs fixtures. The second lowercase is there because NFKD turns some compatibility characters into capitals that the first pass left alone.

Matching is fuzzy. For each of a place's `name` and `variants[].name`, take the best of:

- Jaro-Winkler between the whole normalized query and the normalized name, blended 0.5/0.5 with
- the mean over query tokens of each token's best Jaro-Winkler against any name token.

Only the first 12 words of the query count in the token half. Names scoring below 0.6 are dropped; the 0.5 weight and the 0.6 threshold are this implementation's constants. The token blend applies to single-token queries too. A whole-string-only score would drop "coffee" against "Blue Bottle Coffee", because the full-string similarity is low even though the token matches exactly.

Three sort modes:

- `fused` (default): score rounded to three decimals, descending; then `importance` descending; then distance ascending; then `origin`. Rounding makes near-ties on score fall through to importance rather than letting a 0.001 difference override it. With no text every score is equal, so this is importance, then distance.
- `distance`: distance ascending, then specificity (the smaller place first, so the most specific of the places containing the centre leads), then importance, then score, then `origin`. Text still filters poor matches.
- `similarity`: score (rounded), then distance, then `origin`. It requires text.

A missing `importance` counts as 0, and a place with no `origin` sorts after those that have one. The `origin` tiebreak makes the order total.

## getPlace

Cache first, network second. If the place's tile is already in the cache (the common case, because the user just tapped a search result) it comes from memory, which is both faster and tells the server nothing. Otherwise the SDK asks the server with `org.atgeo.getPlace`, passing the `origin` as `uri`. No discovery call comes first: the server takes a URN whole, so the client needs nothing but the identity it already holds. `importance` arrives beside the place rather than inside it, and the SDK returns it beside the place too.

The fallback is worth being clear-eyed about: a `getPlace` call tells the server exactly which place a user is looking at, which is a sharper signal than the kilometre-square box `getTiles` sees. That is an acceptable cost for its real use, resolving a link someone shared where the client has an identity and no region, and a bad one for hydrating search results, which is why the cache is checked first.

Without a manifest the client cannot work out which tile holds an arbitrary place, so "check the cache" means scanning tiles already fetched, not a lookup. If several cached tiles hold a copy, the one from the lowest tile URL is returned, the same rule search uses. A network answer is the server's own copy; the two agree when the copies are identical, which for garganorn they are.

## Caching

Tile URLs are immutable and change wholesale when the gazetteer publishes a new build, so tiles cache until evicted and never revalidate. `getTiles` responses and the service description are held for an hour, matching the `max-age=3600` the server sends on them, so a client picks up a new build within an hour. The SDK holds them for a fixed hour rather than reading the header. The tile cache is in memory only, least-recently-used out first, 50 MiB by default (measured as the length of each tile's response); a new client starts cold. Once a region has been searched, searching it again makes no requests while its tile list is fresh and its tiles are still cached, and changing only `text` or `sort` reuses everything.

There is no `prefetch` method. A `searchPlaces` call with no text and a large region is already a warm-up, and a method whose only job is to be a different name for that would be surface without capability. If deliberate offline support turns out to be a real requirement, it earns its own design. Persisting tiles to disk is the same story: the HTTP layer is the seam, and an app that wants a disk cache can supply a transport that has one.

## Errors

The server's error codes pass through as the `code` of the SDK's `AtgeoError`, which also carries the HTTP `status`, so a caller can tell a request the server refused (4xx) from one it failed to answer (5xx) without matching on a code that's an open list. XRPC errors are HTTP 400, whatever their name; see [atgeo-spec.md](atgeo-spec.md#errors-over-xrpc). What can actually reach a caller:

| Error | Source | Meaning |
|---|---|---|
| `BboxTooLarge` | `getTiles` | Budget exceeded and a selected tileset has no overview band. Ask for less. |
| `DatasetNotFound` | `getTiles` | The server serves no tileset carrying that dataset. |
| `CollectionNotFound` | `getTiles` | The server serves the dataset, but no tileset carrying that collection. |
| `PlaceNotFound` | `getPlace` | The server serves no place with that origin. |
| `TilesetNotFound` | `describeTileset` | The server serves no tileset with that slug. |
| `InvalidRequest` | any | A malformed parameter, such as a `getPlace` argument that isn't a URI. |
| `HttpError` | any | Non-2xx without a parseable error body. |

`InvalidBbox` and `BboxTooPrecise` are unreachable by construction: the SDK validates the region and snaps it before sending. If either shows up in the wild, the SDK has a bug. An invalid region (non-finite or out-of-range coordinates, `minLat > maxLat`, negative radius) and `similarity` sort without text throw a synchronous `TypeError` before any request. A failed `getTiles` request rejects the search; a failed tile is skipped with `complete: false`.

## What is deliberately absent

**Region-less search.** You cannot search for "Kyoto" without knowing roughly where Kyoto is. Tiles killed couch-geocoding: fetching tiles for a place requires locating it first. `region` is required. The overview band makes a very large region (the whole world, say) answerable, and the SDK will filter and rank whatever comes back, so a name search over a huge box can find places important enough to be in the overview band. It is a property of what the server returns, not a feature of the SDK, and carries the caveat above.

**The write path.** `composeLocation`, `checkin`, and anything else that writes to a repo. Blocked on a check-in lexicon that nobody has drafted, and out of scope for the gazetteer besides. When there is a lexicon there can be a separate package; the read SDK should not carry a write path shaped around a guess.

**Discovery by DID or handle.** The served `did.json` advertises the tile service (`#atgeo_tiles`, type `AtgeoTileService`, at the deployment's public URL), but the SDK does not resolve DIDs. The constructor takes a server URL, default `https://places.atgeo.org`. The `org.atgeo.tiles.service` announcement records described under Discovery in [atgeo-appview-design.md](atgeo-appview-design.md) don't exist yet either. `describeService` and `describeTileset` do cover asking a known server what it serves.

**Version negotiation.** There is no version field in the tile envelope and no version to reject. Ignore keys you don't recognise; that is the whole forward-compatibility story.

**Restricting tile URLs.** The server names the tile URLs and the client fetches them as given, so a compromised server can send a client anywhere. The client refuses a URL that isn't absolute http(s) or that carries credentials, and no more. Refusing URLs that are off the server's host, or not https, is a decision for all the SDKs together; none does it yet.

## Conformance fixtures

The SDK's `fixtures/` directory holds language-neutral reference behavior for ports:

- `normalize.json`: input/output pairs for `N(s)`, which a port must match exactly.
- `jaro-winkler.json`: input/output pairs for the similarity function, to within the rounding of the fixture.
- `ranking-scenarios.json`: regions, text, and records, asserting which places land in the top K. It checks membership rather than exact order, because the weights are expected to change.

The SDK's `test/lexicon/` directory holds copies of this repo's lexicons, taken from a recorded garganorn commit, which its fake server validates every request and response against, so a test can't pass on a response the real server would never send.

The fixtures show what the reference implementation does and a port should aim to match them. A language may not be able to reproduce the search exactly at the margins (string folding differs, floating point differs), and equal scores across implementations are not a goal; the port contract says which margins are permitted and which are not. A port that gives sensible fuzzy results over the same wire format is conformant.

## Open questions

- **`region` stays required.** Whether the SDK should offer an explicit world-scale mode on top of the overview band, rather than leaving callers to pass a huge region, is undecided. The `overview` flag in the `getTiles` response tells such a mode when it got a sample.
- **Usable search radius in dense cities.** Unmeasured. The 50-tile budget is reached in Manhattan-density areas; no one has measured what radius that leaves, or what a maximal query costs in bytes on a phone.
- **Default host.** `https://places.atgeo.org` is the constructor default. The zero-configuration constructor is most of the adoption argument, and it needs a deployment behind it with someone paying for the bandwidth. This is a governance question wearing a technical hat.
- **Platforms after the first.** TypeScript is the first implementation. Swift, Kotlin and Python ports have specs in the SDK repo, and a Rust one is proposed. They share the fixtures and the port contract; whether the fixtures need to become a versioned shared artifact is undecided.
