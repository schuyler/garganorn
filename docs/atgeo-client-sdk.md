# The ATGeo client SDK

Two kinds of call do the work. Four search methods find places: `searchNearby` in a region, `searchByKeyword` by name anywhere in a dataset, `searchNearbyThenByKeyword` nearby first and by name if that finds nothing solid, and `autocompleteByKeyword` for a search box. `getPlace` fetches one place by its `origin`. Tile lookup, tile fetching, matching, ranking, and caching all happen inside them and are not the developer's problem.

This is the search the SDK is meant to have. The four methods, what each one sends, and the approach to matching and ranking are settled design, not a first cut awaiting a different search. The weights and thresholds are still being tuned.

The TypeScript SDK ([atgeo-sdk-js](https://github.com/schuyler/atgeo-sdk-js), `@atgeo/client`) is the first implementation. Its README is the developer reference: signatures, options, result shapes, and error codes. This document is the design rationale, and the behavior a port to another language has to make a decision about. The exact, testable statement of that behavior is the SDK repo's [port contract](https://github.com/schuyler/atgeo-sdk-js/blob/main/docs/port-contract.md), written for porters; where it and this document disagree about what the client does, it wins.

The wire format is in [atgeo-spec.md](atgeo-spec.md), which is normative for anything on the network. Where the two disagree, the spec wins.

## Which server calls the SDK makes

The gazetteer serves a few things a client can call to read places: `getTiles`, `getPlace` and `org.atgeo.searchPlaces` for places, `describeService` and `describeTileset` for what a server holds. The two searches differ on purpose. The tile search sends the server no text at all and finds places in a region; the server's `org.atgeo.searchPlaces` ([atgeo-spec.md](atgeo-spec.md#orgatgeosearchplaces)) is a keyword search over a whole dataset, which has to be told the keyword. The SDK exposes both, under names that say which is which.

| SDK | Server |
|---|---|
| `searchNearby` | `org.atgeo.getTiles`, then N tile fetches |
| `searchByKeyword` | `org.atgeo.searchPlaces` |
| `searchNearbyThenByKeyword` | what `searchNearby` calls, then `org.atgeo.searchPlaces` only when the nearby results have no solid hit |
| `autocompleteByKeyword` | its `searchByKeyword` calls `org.atgeo.searchPlaces`, designed to send only a keyword the server would accept (the margins are in the port contract's section 17: compatibility characters, half-width kana) |
| `getPlace` | the cached tiles, else `org.atgeo.getPlace` |
| `describeService` | `org.atgeo.describeService` |
| `describeTileset` | `org.atgeo.describeTileset` |

### The naming rule

Every method whose name contains `Keyword` may send search text to the server. No other method does. In an options object, the field that carries text is called `text` where it never leaves the device (`searchNearby`) and `keyword` where it may. A developer can tell from the method name what leaves the device, and the SDK never decides that for them: there is no method that quietly falls back to a global query. `searchNearbyThenByKeyword` is the one that sends text only sometimes, and its name says so.

The client does not implement quadkeys. Tile URLs are opaque and the server works out which tiles cover a box. That deletes the hardest half of what a client would otherwise have to get right, and it's the main reason this shape is worth having rather than merely smaller.

The two describe calls are plain pass-throughs for callers who want to know what a server holds. They take no part in search; `describeService` is cached for an hour, `describeTileset` is not cached.

## searchNearby

Internally: snap the region outward, ask `getTiles`, fetch the tiles it names, parse, dedupe, filter to the collection, filter to the region, match, rank, and hand back places. One `getTiles` round trip per bounding box (two across the antimeridian), then tile fetches, eight at a time.

`region` is a circle (centre and radius in meters) or a bounding box in WGS84 degrees. A box with `west > east` crosses the antimeridian. `text` is optional; without it you get everything in the region, which is the "what's around me" case.

### One dataset per call

A search covers one dataset, optionally narrowed to one of its collections. `dataset` is required:

| `dataset` | `collection` | searched |
|---|---|---|
| given | absent | the whole dataset |
| given | given | that collection |

A search without a dataset is an argument error, raised before any request is made. The port-contract revision ([atgeo-sdk-js#43](https://github.com/schuyler/atgeo-sdk-js/issues/43)) brings the SDKs to this; until a port has shipped it, that port still defaults to `overturemaps.org` / `place`. Each port uses its own idiom for that: a required parameter, a non-optional type, or a thrown error. There's no default because any default would be a guess. Categories and attributes aren't normalised across datasets, so an app is always written for the dataset it uses, and a default would only hide that choice. A client library has no business hard-coding a publisher's name, either.

`dataset: "openstreetmap.org"` alone searches nodes, ways and relations together, which is usually what you want from OSM, since the element type is an accident of how the mapper drew it. The `collection` parameter is sent to the server only when the search has one.

Tiles can hold records of collections you didn't ask for. All three OSM element types share the OSM tiles, for one. When the search has a collection, the SDK keeps the records whose `origin` starts with `urn:atgeo:{dataset}:{collection}:`, colon included, so `way` doesn't match `wayside`. A record with no `origin` matches no collection. Without a collection, nothing is filtered.

A dataset-wide search of `overturemaps.org` mixes places and divisions, built as separate tilesets. That ranks soundly: `importance` is comparable across the tilesets one producer builds from one dataset ([Importance](atgeo-spec.md#importance)). It does reach the tile budget sooner, since the budget counts tiles across both tilesets.

There is no merged ranking across datasets, because there is no defined way to do one. The comparability promise stops at the dataset, and `importance` is meaningless across producers: an Overture place scoring 62 and an OSM place scoring 62 are not equivalently important, they are scored against different corpora with different category vocabularies. An SDK that merged them would be inventing a comparison the data does not support. Applications that want Overture places and OSM places make two calls and decide for themselves how to interleave the results.

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

`searchNearby` sends `getTiles` with the snapped bounding box, the dataset, and the collection if there is one. Then it sends GETs for tile URLs exactly as the server named them. Nothing else: not the search text, in any form, and not the precise region. The server can tell which tiles a client has fetched before, but nothing finer than the tiles themselves. `getPlace` on a cache miss sends the `origin` of the place asked for, and nothing else.

The keyword methods are the exception, and say so in their names. `searchByKeyword` sends `org.atgeo.searchPlaces` with the dataset, the collection if there is one, `q` (the keyword exactly as passed), and `bbox` if the caller gave a region. The server learns the keyword and, with a region, the whole-degree box that covers it; see [Keyword search](#searchbykeyword).

Diagnostics stay in the app's process. The client sends no telemetry and no error report, anywhere. A failed-tile entry carries a tile URL and what fetching it produced, never the search text or the region. The port contract's "Privacy invariants" section lists the tests a port has to pass.

### What the server learned: `disclosed`

Every search result carries `disclosed`, a record of what the server learned during that call: `keyword` is true if the keyword was sent, and `region` is the grid, in degrees, of the finest region sent (`0.01` for tile requests, `1` for a keyword region), or null if none was. The point is that a developer can show it to users, or log it in their own tests. A `searchNearby` answered entirely from cache reports `region: null`, since nothing was sent; a combined search that sent both reports the finer grid.

### Partial results

Tiles are individually optional. A tile that fails to fetch is skipped and counted rather than failing the call, so a caller showing "three results" may be showing three results out of forty-one tiles that loaded and nine that didn't. The result carries `complete`, a fetched/failed count, and a `tileFailures` list, so the application can say so. Each failure has the tile URL and a `kind` that says where it went wrong: `refused` (not an absolute http(s) URL, or one carrying credentials, so no request was made), `status` (a non-2xx), `decode` (a 2xx that isn't a tile) or `transport` (the fetch itself threw). The kind comes from which step failed, not from the error's type. This is the one piece of protocol-shaped behaviour the small surface has to expose that the server does not model.

A failed `getTiles` request is different: with no tile list there is nothing to degrade, so the search rejects.

The client doesn't retry a failed tile. Tolerance runs the other way too: it reads only the fields it needs, skips a record or a tile it can't use, and treats a wrong-typed field as absent, so a server change that keeps the wire functional doesn't break apps built on it.

### How much this costs

Unbounded by design, bounded in practice.

The protocol places no limit on how much a client fetches and filters. `getTiles` has no pagination, no server-side ranking, no partial-result protocol to cut the set down. That is the whole point of client-side search, and building a limit into the SDK would reintroduce the thing the architecture removed.

In practice the ceiling is `getTiles`'s tile budget, 75 by default on garganorn (a deployment sets it with `max_tiles`, and advertises it as `maxTiles` in its `describeService` answer). That number is for humans: tile zooms are adaptive and the client does no quadkey math, so it can't tell in advance whether a region will exceed it. The budget bites soonest where the SDK is most useful: in Manhattan or Shibuya, tiles subdivide to keep up with density, so the same map view costs 25 Overture tiles in Berlin and 88 in midtown.

Measured on places.atgeo.org's 2026-10-26 builds, for a 506 by 470 pixel map view: an Overture place tile is about 85 KB on the wire and 1 MB of JSON, holding about 500 records, so a search that fills a 75-tile budget downloads about 6 MB and parses about 70 MB (in Node, a 47-tile search added about 170 MB of heap). An OSM tile is about 30 KB on the wire and 0.4 MB of JSON, and OSM views need a third to a half as many tiles. Across 36 cities, the widest Overture view that fits the budget:

| Widest zoom | At 50 tiles | At 75 tiles |
|---|---|---|
| 12 or wider | Beijing, Shanghai, Lagos | Beijing, Shanghai, Lagos, Amsterdam, Johannesburg, Moscow |
| 13 | 7 cities, among them Delhi, Mumbai, San Francisco | 15 cities, among them Berlin, Istanbul, Los Angeles, Seoul, Sydney |
| 14 | 15 cities, among them Paris, Bangkok, Mexico City | 13 cities, among them London, Paris, Hong Kong, Singapore, Chicago |
| 15 | 7 cities, among them Hong Kong, Singapore, Taipei | none |
| closer | Chicago, London, Midtown Manhattan, Shibuya | Midtown Manhattan, Shibuya |

A z13 view is about 3.5 km across at mid latitudes; z14 is half that. Beijing and Shanghai are cheap because both sources are sparse there, not because the cities are. The budget went from 50 to 75 on these numbers: z14 works in nearly every big city, at about 1.5 times the bytes.

When the regular answer would exceed the budget, a server whose tilesets all have an overview band does not fail. It answers with the coarse zoom 1 to 5 overview tiles that intersect the region, which hold only each tileset's most important records, as many as its descriptor's `overview.records` (see [the overview band](atgeo-spec.md#the-overview-band), and for the design reasoning [design-constraints.md](design-constraints.md#the-summary-band-is-a-coarse-tile-tier-for-region-less-resolution)). The server marks such an answer with `overview: true` in the `getTiles` response, and the SDK exposes it as `NearbySearchResult.overview`, always present and `false` for a regular answer. The SDK follows tile URLs without interpreting them, so it never infers the flag from their shape. It fetches the tiles, filters them to the region, and ranks them like any others. The result is a deliberately incomplete sample of the region, and `overview` is how a caller tells "everything here" from "the important things here".

`overview` is independent of `complete`, which means only that no tile fetch failed. An overview answer with every tile loaded is `overview: true, complete: true`. If either bbox of an antimeridian-split search comes back as an overview, the result is `overview: true`.

`BboxTooLarge` reaches the caller only when a selected tileset has no overview band. The SDK does not subdivide an over-large region into several `getTiles` calls; the budget exists to protect the origin, and quietly issuing four requests where the server asked for one defeats it.

### Attribution on results

A search result carries the notices for what it shows. `NearbySearchResult.attribution` is the union of the `attribution` entries of every tile the search loaded, deduplicated by `(dataset, collection)` and sorted by `(dataset, collection)` in ordinal string order, not tile order, so every SDK agrees. It includes referenced collections, not just the ones that produced a hit: an OSM hit's `relations.within` carries Overture division names, so showing it conveys Overture data, and the tile's reference entry is how the server says so. A keyword result's `attribution` is the union of its places' own `attribution` entries, sorted the same way.

Each hit also gets `collection`, a `{dataset, collection}` pair or null. It is one field rather than two because the pair is the unit of licensing and the halves are never useful apart. The SDK fills it by matching the hit's `origin` against `urn:atgeo:{dataset}:{collection}:` for each entry in its tile's `attribution` (a keyword hit: the place's own `attribution`), the same prefix test as the collection filter, rather than parsing the URN. A record whose origin matches no entry gets null.

The tile cache keeps each tile's `attribution` beside its records, so a warm search has it too. These fields come with the port-contract revision ([atgeo-sdk-js#43](https://github.com/schuyler/atgeo-sdk-js/issues/43)); ports pick them up from there.

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

## searchByKeyword

`searchByKeyword` finds a place by name without knowing where it is. It is one `org.atgeo.searchPlaces` request: a keyword, a dataset, optionally a collection and a region. It needs no region, so "Kyoto" works from anywhere. The server answers with at most 20 places, most notable first; the SDK keeps the server's order and does no ranking, filtering or deduplication of its own, since the server returns each place once. Each hit carries `via: "keyword"` and has no `distance` or `score`: the server ranks by neither, and an app that wants distance computes it from the hit's `location`.

The match rule is the server's: every word of the keyword must begin some word of one of the place's names, and at least one word must be two or more characters long. A keyword the server refuses for that reason is not caught on the client; the call rejects with `KeywordRequired`. The keyword goes out exactly as passed, up to 256 bytes of UTF-8 (`InvalidRequest` past that).

The optional `region` is a bounding box only, never a circle: an API that takes a circle invites a device's GPS position as its centre. The SDK snaps it outward to whole degrees, the grid the server accepts for this call, and never sends anything finer. See [The precision floor](atgeo-spec.md#the-precision-floor) for why the grid is that coarse here and 0.01 degrees for tiles. A box that crosses the antimeridian, or whose snapped edges would meet or pass each other, goes as the smallest box the server accepts that covers it, up to the full longitude band.

Keyword calls are not cached; every call is one request. `getPlace` on a keyword hit's `origin` consults the tile cache and then the network, as for any `origin`.

## searchNearbyThenByKeyword

The combined search is for the search box that should work whether the place is next door or across the world, without sending a global query for "Starbucks" when six are within five miles. It runs `searchNearby` with the keyword as `text`, and looks at the result. If any nearby hit is a tier-1 match, one of the place's names having every query word as the start of one of its words (the server's own match rule), the nearby result is a solid answer and the call returns it. No keyword request is made, and `disclosed.keyword` is false. If no hit is solid, the call makes the `searchByKeyword` request and merges.

Two decisions are the caller's rather than the SDK's:

- `keywordRegion` is required: `"none"` sends the keyword alone, `"degree"` sends it with the nearby region converted to a box and snapped to whole degrees. With `"none"`, the server learns only the keyword, which finds "Kyoto" but returns the globally most notable "Starbucks", useless as a fallback. With `"degree"` it learns the keyword together with a degree cell. The SDK does not choose, because the disclosure is the developer's choice every time.
- The `keyword` is required, since the fallback needs text.

The merged list has the keyword hits first, in the server's order, then the nearby hits in their order, dropping any nearby hit whose `origin` equals a keyword hit's. Keyword hits lead because they pass the tier-1 rule and every nearby hit here is a fuzzy (tier-2) match; tier 1 precedes tier 2 everywhere in the SDK. Each hit's `via` says which request produced it. `disclosed.region` is the finest region sent during the whole call, so a cold search with `"degree"` reports `0.01` even though the keyword travelled with a one-degree box.

A keyword the server would refuse for being too short is designed not to be sent (with the margins of contract section 17): the nearby result is returned as it is. A nearby search with failed tiles and no solid hit still falls back, though the missing tiles might have held one. A typo such as `Starbux` has no tier-1 match, so the fallback fires and discloses the keyword for an answer that is almost certainly empty, because the server matches by prefix. A failed keyword request rejects the call, as a failed `getTiles` does.

## autocompleteByKeyword

`autocompleteByKeyword` returns an object with a `searchByKeyword(keyword)` method, for a text field that searches as the user types. It is a separate object rather than a client method because "the previous call" is per text field: two fields on one page sharing a client must not cancel each other.

Every keyword that qualifies is sent to the server as typed, prefixes included. The server keeps no log of it ([tile-privacy-design.md](tile-privacy-design.md)), but a hostile operator can record what it receives. Debouncing reduces how many prefixes go out and is the app's to choose. A keyword that cannot qualify (one character, say) resolves empty with no request, and `disclosed` says `keyword: false`.

Each call cancels the previous call on the same autocompleter. How the superseded call ends is the port's cancellation idiom: an `AbortError` in TypeScript, `CancellationError` in Swift, `CancellationException` in Kotlin, `asyncio.CancelledError` in Python's async client, and `Error::Superseded` in Rust's async client, which has no cancellation error because dropping a future cancels it and the autocompleter does not own the caller's future. In each port that superseded outcome means "superseded" and is for the app to ignore; the caller's own cancellation still reaches the call. The blocking clients (Python's and Rust's) cannot cancel a call in flight and offer no autocompleter.

## getPlace

Cache first, network second. If the place's tile is already in the cache (the common case, because the user just tapped a search result) it comes from memory, which is both faster and tells the server nothing. Otherwise the SDK asks the server with `org.atgeo.getPlace`, passing the `origin` as `uri`. No discovery call comes first: the server takes a URN whole, so the client needs nothing but the identity it already holds.

The wire answer is `{uri, url, importance?, value, attribution}`. Its `uri` is the record key the SDK sent, which the SDK calls `origin`; `value` is what the SDK calls `place`. The public API keeps those names, and only a port's transport layer knows the wire's. `importance` arrives beside the place rather than inside it, and the SDK returns it beside the place too. `url` is the server's plain-HTTP address for the same document. `attribution` is the place's tileset's attribution entries, the same list its tiles carry, and the SDK exposes it as `PlaceRecord.attribution`, so a place resolved from a shared link has something to credit. Reading this shape, and `PlaceRecord.attribution`, also arrive with the port-contract revision ([atgeo-sdk-js#43](https://github.com/schuyler/atgeo-sdk-js/issues/43)); an SDK released before it reads the earlier `{uri, importance?, record}` answer.

The fallback is worth being clear-eyed about: a `getPlace` call tells the server exactly which place a user is looking at, which is a sharper signal than the kilometre-square box `getTiles` sees. That is an acceptable cost for its real use, resolving a link someone shared where the client has an identity and no region, and a bad one for hydrating search results, which is why the cache is checked first.

Without a manifest the client cannot work out which tile holds an arbitrary place, so "check the cache" means scanning tiles already fetched, not a lookup. If several cached tiles hold a copy, the one from the lowest tile URL is returned, the same rule search uses. A network answer is the server's own copy; the two agree when the copies are identical, which for garganorn they are.

## Caching

Tile URLs are immutable and change wholesale when the gazetteer publishes a new build, so tiles cache until evicted and never revalidate. `getTiles` responses and the service description are held for an hour, matching the `max-age=3600` the server sends on them, so a client picks up a new build within an hour. The SDK holds them for a fixed hour rather than reading the header. The tile cache is in memory only, least-recently-used out first, 50 MiB by default (measured as the length of each tile's response); a new client starts cold. Once a region has been searched, searching it again makes no requests while its tile list is fresh and its tiles are still cached, and changing only `text` or `sort` reuses everything. Keyword calls are not cached.

There is no `prefetch` method. A `searchNearby` call with no text and a large region is already a warm-up, and a method whose only job is to be a different name for that would be surface without capability. If deliberate offline support turns out to be a real requirement, it earns its own design. Persisting tiles to disk is the same story: the HTTP layer is the seam, and an app that wants a disk cache can supply a transport that has one.

## Errors

The server's error codes pass through as the `code` of the SDK's `AtgeoError`, which also carries the HTTP `status`, so a caller can tell a request the server refused (4xx) from one it failed to answer (5xx) without matching on a code that's an open list. The errors an XRPC method raises are HTTP 400, whatever their name; see [atgeo-spec.md](atgeo-spec.md#errors-over-xrpc). What can actually reach a caller:

| Error | Source | Meaning |
|---|---|---|
| `BboxTooLarge` | `getTiles` | Budget exceeded and a selected tileset has no overview band. Ask for less. |
| `DatasetNotFound` | `getTiles` | The server serves no tileset carrying that dataset. |
| `CollectionNotFound` | `getTiles` | The server serves the dataset, but no tileset carrying that collection. |
| `KeywordRequired` | `searchPlaces` | No word among the keyword's first 12 distinct words is two or more characters long. |
| `PlaceNotFound` | `getPlace` | The server serves no place with that key (its origin). |
| `TilesetNotFound` | `describeTileset` | The server serves no tileset with that slug. |
| `InvalidRequest` | any | A malformed parameter, such as a `getPlace` argument that isn't a URI, or a keyword over 256 bytes of UTF-8. |
| `InternalServerError`, `ServiceUnavailable`, … | any | The server failed (5xx); garganorn answers these as JSON too, with a generic message. |
| `HttpError` | any | Non-2xx without a parseable error body, which from garganorn means something in between, such as a proxy or CDN. |

`InvalidBbox` and `BboxTooPrecise` are unreachable by construction: the SDK validates the region and snaps it before sending, to 0.01 degrees for tiles and to whole degrees for a keyword region. If either shows up in the wild, the SDK has a bug. An invalid region (non-finite or out-of-range coordinates, `south > north`, negative radius), a keyword region that is not a bounding box, and `similarity` sort without text throw a synchronous `TypeError` before any request, as does a non-string keyword (except in the autocompleter's `searchByKeyword`, which is async and rejects with `TypeError`). `KeywordRequired` reaches a caller of `searchByKeyword`; the combined search and the autocompleter are designed not to send a keyword that would draw it, with the margins of the port contract's section 17 (compatibility characters, half-width kana). A failed `getTiles` request rejects the search; a failed tile is skipped with `complete: false`.

## What is deliberately absent

**The write path.** `composeLocation`, `checkin`, and anything else that writes to a repo. Blocked on a check-in lexicon that nobody has drafted, and out of scope for the gazetteer besides. When there is a lexicon there can be a separate package; the read SDK should not carry a write path shaped around a guess.

**Discovery by DID or handle.** The SDK does not resolve DIDs, and nothing on the network would make it worth doing yet. The served `did.json` advertises the tile service (`#atgeo_tiles`, type `AtgeoTileService`, at the deployment's public URL), and `describeService` returns the same `did`, but the spec describes that `did` as informational: no discovery path resolves a DID to a gazetteer. It's kept because a `did:web` identity costs nothing and is the hook any future discovery design would hang from. The constructor takes a server URL. An earlier draft of an `org.atgeo.tiles.service` announcement record, which predated the place-identity redesign, was discarded ([explored-and-discarded.md](explored-and-discarded.md)); discovery itself stays open in [planned-features.md](planned-features.md#collection-and-service-metadata). `describeService` and `describeTileset` do cover asking a known server what it serves.

**A world-scale mode.** `searchNearby` requires a region, and no mode searches the whole world over tiles. Finding a place anywhere is `searchByKeyword`'s job: one `org.atgeo.searchPlaces` request, which has to be told the keyword, under a name that says so. A huge region passed to `searchNearby` gets the overview band, flagged `overview` as a sample, or `BboxTooLarge` where a tileset has no band (see [How much this costs](#how-much-this-costs)).

**Version negotiation.** There is no version field in the tile envelope and no version to reject. Ignore keys you don't recognise; that is the whole forward-compatibility story.

**Restricting tile URLs.** The server names the tile URLs and the client fetches them as given, so a compromised server can send a client anywhere. The client refuses a URL that isn't absolute http(s) or that carries credentials, and no more. Refusing URLs that are off the server's host, or not https, is a decision for all the SDKs together; none does it yet.

## Conformance fixtures

The SDK's `fixtures/` directory holds language-neutral reference behavior for ports:

- `normalize.json`: input/output pairs for `N(s)`, which a port must match exactly.
- `jaro-winkler.json`: input/output pairs for the similarity function, to within the rounding of the fixture.
- `ranking-scenarios.json`: regions, text, and records, asserting which places land in the top K. It checks membership rather than exact order, because the weights are expected to change.
- `keyword-fallback.json`: scenarios for `searchNearbyThenByKeyword`: tiles, the server's keyword answer, and the expected requests, merged order and `disclosed`. It pins the solid-hit decision and the merge, and checks exact order.
- `keyword-regions.json`: an input box and the whole-degree box a keyword request carries for it, at every seam (fractional, zero-width, polar, antimeridian-crossing and ±180 boxes).
- `lenient-readers.json`: a response body for each of the six wire readers (`getTiles`, a tile payload, `getPlace`, `searchPlaces`, `describeService`, `describeTileset`) and what a port's reader keeps of it, or that it rejects the body as unusable.

The SDK's `test/lexicon/` directory holds copies of this repo's lexicons, taken from a recorded garganorn commit, which its fake server validates every request and response against, so a test can't pass on a response the real server would never send.

The fixtures show what the reference implementation does and a port should aim to match them. A language may not be able to reproduce the search exactly at the margins (string folding differs, floating point differs), and equal scores across implementations are not a goal; the port contract says which margins are permitted and which are not. A port that gives sensible fuzzy results over the same wire format is conformant.

## Open questions

- **What a maximal query costs on a phone.** The radius and the bytes are measured (see [How much this costs](#how-much-this-costs)); the parse time and memory on a phone browser aren't. A full 75-tile Overture search is about 70 MB of JSON, more than the SDK's default 50 MiB tile cache holds, so panning around one area refetches tiles.
- **Default host.** `https://places.atgeo.org` is the constructor default. Once `dataset` is required (the port-contract revision, [atgeo-sdk-js#43](https://github.com/schuyler/atgeo-sdk-js/issues/43)), that default no longer implies a publisher's data: the host is a default, and which dataset to search is always the app's call. The server still has to serve the dataset asked for, and `DatasetNotFound` says when it doesn't. The zero-configuration constructor is most of the adoption argument, and it needs a deployment behind it with someone paying for the bandwidth. This is a governance question wearing a technical hat.
- **Pre-release ports.** TypeScript is the first implementation. Swift, Kotlin, Python and Rust ports are built ([atgeo-sdk-swift](https://github.com/schuyler/atgeo-sdk-swift), [-kotlin](https://github.com/schuyler/atgeo-sdk-kotlin), [-python](https://github.com/schuyler/atgeo-sdk-python), [-rs](https://github.com/schuyler/atgeo-sdk-rs)), all pre-release and installed from GitHub. Each shares the fixtures and the port contract, and has a `CONFORMANCE.md` listing where it differs at the margins. Each vendors the fixtures and lexicons at a pinned commit, and every port's CI runs a pin check, `scripts/check-fixtures.sh` in atgeo-sdk-js: it fails if a vendored file differs from its pinned commit's, and warns when the pin is behind the reference.
