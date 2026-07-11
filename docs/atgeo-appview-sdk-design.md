---
category: Design
tags: [atgeo, atproto, appview, sdk, tiles, privacy, garganorn]
last_updated: 2026-07-02
confidence: design-complete, protocol details open for working-group review
---

# atgeo: Protocol Contract, Spatial AppView, and Client SDKs

Garganorn's tile scheme, generalized into a protocol with two producer
types and N client implementations. This document specifies three layers:

1. **The protocol contract** — tile envelope, manifest, discovery, and
   coordinate-precision rules. Everything below implements this.
2. **The spatial AppView** — a sidecar that consumes the ATProto firehose
   and materializes live location records into the same tile format the
   batch gazetteer pipeline produces.
3. **Client SDKs** — native TypeScript, Swift, and Kotlin implementations,
   kept interchangeable by a shared conformance-vector corpus rather than
   shared code.

Companion to the pipeline restructure design; assumes its outcomes (static
tiles, static manifest, `searchRecords` removed). The adoption thesis
governing every SDK decision: **the integration must be easier than not
using it** — no API key, no billing, no signup, one call to first result.
The competitor is not another geo library; it is a developer pasting a
Google Places key into their app.

## 0. Goals and non-goals

Goals:

1. One envelope, one manifest, one fetch path, regardless of whether a
   tile came from a gazetteer import or live firehose records. The record
   `uri` scheme deliberately differs by provenance (§1.2) — https for
   server-authored reference data, at:// for repo-verified live data.
   Uniformity is in *processing*; provenance is a signal clients deserve,
   not a leak to smooth over.
2. The AppView holds no canonical state — fully rebuildable from firehose
   replay or repo CARs.
3. SDK conformance is verified by vectors in CI, not by sharing code.
   Three native codebases, one behavioral spec.
4. Read privacy is structural (tiles + local search); write privacy is
   structural in the SDK (precision as a required, defaulted-coarse
   parameter).
5. `npm install` / SPM / Maven to first `nearby()` result in under five
   lines, on all three platforms.

Non-goals:

- Private location data. ATProto repos are public; published locations are
  public. The SDK mitigates precision, not publicity. Stated plainly in
  every SDK README.
- Routing, geocoding of freeform addresses, map rendering. This is places
  and containment, not a maps stack.
- Moderation/labeling of location records (compose with existing ATProto
  labeling; nothing here conflicts with it, nothing here implements it).
- A Rust/WASM shared core. Considered and rejected: the shareable pure
  logic (quadkey math, manifest intersection, ranking) is a few hundred
  lines, while the bulk of each SDK is platform I/O (fetch/URLSession/
  OkHttp, disk caches) that *should* be native. Conformance vectors police
  the seams more cheaply than binding toolchains do.

## 1. Protocol contract

This section is the normative spec. Producers (pipeline, AppView) and
consumers (SDKs) implement it; the conformance corpus (§1.6) tests it.

### 1.1 Tile scheme

- Web-Mercator quadkeys (Bing tile system), WGS84 lon/lat.
- Tile files at `{base}/{qk6}/{qk}.json.gz` where `qk6 = qk[:6]`; tiles
  may exist at any zoom present in the manifest (the producer's adaptive
  assignment decides; consumers must not assume a fixed zoom).
- Gzip is part of the format (files are stored gzipped; producers set
  `Content-Encoding` or serve `.json.gz` verbatim — SDKs must handle
  both).
- A record belongs to exactly one tile per (producer, collection).

### 1.2 Record envelope

Tile payload (one JSON object per tile file):

```json
{
  "atgeo": 1,
  "collection": "org.atgeo.places.overture.place",
  "attribution": "https://docs.overturemaps.org/attribution/",
  "generated_at": "2026-07-02T00:00:00Z",
  "records": [
    { "uri": "https://places.atgeo.org/org.atgeo.places.overture.place/<rkey>",
      "cid": null,
      "value": { "$type": "org.atgeo.place", "rkey": "...", "name": "...",
                 "importance": 62, "locations": [...], "variants": [...],
                 "attributes": {...}, "relations": {...} } }
  ]
}
```

Changes from the current pipeline format, to be folded into the pipeline's
export SQL (small diff, do it before SDKs freeze on the format):

- `atgeo: 1` version field.
- Records wrapped in `{uri, cid, value}`, with the URI scheme carrying
  provenance. **Gazetteer records**: canonical
  `https://{host}/{collection}/{rkey}` — dereferenceable (a GET returns
  the bare record, the existing atgeo.org practice) — with `cid: null`.
  These records are *not* atproto repository data: no MST, no signed
  commit, no sync path. The at:// scheme is specified as addressing
  repository data, and "signed by a DID" has no per-record mechanism in
  atproto outside MST inclusion in a signed commit — so gazetteer records
  must not mint at:// URIs they cannot resolve or verify. **AppView
  records**: the genuine `at://` URI plus commit-verified CID, so a
  client can fetch and verify the record at its origin PDS. *Marked
  speculative, not v1*: a pipeline stage could batch-build a real MST and
  signed commit over the gazetteer, earning at:// honestly regardless of
  the storage engine underneath; it is inadvisable today because shared
  relay infrastructure assumes account-sized repos, and a 100M-record
  repo rewriting monthly is a crawl-load citizenship problem, not a
  compliance one. Revisit if the ecosystem grows bulk-repo conventions.
- `generated_at` moves into the tile (already in the manifest; duplicating
  it per tile lets caches be reasoned about per-file). **`generated_at` is
  run-scoped** (`docs/phase2b-design.md` §5 `P1`, §B.4): identical across
  every tile of a run and equal to the manifest's `generated_at`; RFC 3339
  UTC, `Z`-suffixed, seconds precision. It is not a per-flush or per-record
  timestamp — a second producer (e.g. the AppView, §2.5) stamping per-flush
  times would let consumers wrongly infer per-tile freshness ordering.
- `cid` is required and nullable, but never omitted (§1.2 record shape:
  every record is exactly `{uri, cid, value}`). For gazetteer records
  `cid` is always `null` and is explicitly **never computed**
  (`docs/phase2b-design.md` §5 `P4`, §B.3) — there is no signed commit to
  verify it against, so hashing the value would record a number that
  verifies nothing. Only AppView records carry a genuine commit-verified
  CID.
- Record order within a tile is producer-defined; consumers must not rely
  on it (`docs/phase2b-design.md` §5 `P5`, §B.2a). §2.5 has the AppView
  flush tiles ordered by `rkey`; the pipeline orders by its own internal
  sort key. Both are valid producers of this envelope.

`value` schemas are the existing lexicons (`org.atgeo.place`,
`community.lexicon.location.*`); this document does not change them.

### 1.3 Manifest

`{base}/current/manifest.json` (or a live path for the AppView, §2.5):

```json
{
  "atgeo": 1,
  "source": "overture_place",
  "collection": "org.atgeo.places.overture.place",
  "attribution": "https://docs.overturemaps.org/attribution/",
  "generated_at": "2026-07-02T00:00:00Z",
  "tile_url_template": "{base}/{qk6}/{qk}.json.gz",
  "cache": { "max_age": 86400, "immutable": true },
  "quadkeys": ["02301", "023010", "..."]
}
```

- `attribution` (optional) — **[protocol amendment, `docs/phase2b-design.md`
  §5 `P3`]** lets a client render attribution without fetching a tile.
  Unknown-field tolerance (§1.6) makes it safe to add; a manifest lacking
  it is still conformant.
- `cache.immutable: true` for timestamped pipeline outputs (URL changes
  when content changes, via the `current` symlink); `false` with a short
  `max_age` for AppView-served live tiles. This resolves the CDN-vs-
  freshness tension per collection instead of globally. **`immutable:
  true` is permitted only when `tile_url_template` embeds a run-unique
  path segment** (`docs/phase2b-design.md` §5 `P2`, §B.6) — otherwise the
  same tile URL can return different bytes after a later run, and a CDN
  honoring `immutable` would serve stale tiles for the full `max_age`. The
  current pipeline's `tile_url_template` has no run-unique segment (tiles
  are served through a `current` symlink), so it ships `cache.immutable:
  false`; a future run-stamped template (e.g.
  `{base}/<run-timestamp>/{qk6}/{qk}.json.gz`) could legitimately flip it
  to `true`.
- `quadkeys` is the complete sorted coverage list. Size budget: measure in
  pipeline validation; if gzipped manifests exceed ~5 MB the fallback is
  z4-prefix manifest shards (`manifest/{qk4}.json`, same shape). The SDK
  abstraction (`Coverage` type, §3.2) must not leak which layout is in
  use, so the shard decision can be made later without an SDK-breaking
  change.

### 1.4 Discovery

A client should find tile services through ATProto identity, not
configuration. Two mechanisms, both specced:

1. **DID document service entry** on the operator's DID:

```json
{ "id": "#atgeo_tiles",
  "type": "AtgeoTileService",
  "serviceEndpoint": "https://tiles.atgeo.org" }
```

2. **Announcement record** `org.atgeo.tiles.service` in the operator's
   repo, one per served collection:
   `{collection, baseUrl, manifestPath, kind: "gazetteer"|"live"}`.
   Records are enumerable via `com.atproto.repo.listRecords`, so a client
   holding only a DID can enumerate everything the operator serves.

The division of labor keeps the at:// namespace honest (§1.2): the DID
service entry declares a non-PDS service, which a DID document may do
freely without implying repository hosting; the announcement records live
in a real, account-sized repo on a real PDS and therefore legitimately
carry at:// URIs. Identity anchors the service; bulk data stays out of
the at:// namespace.

The service entry answers "where"; the announcement records answer "what."
SDKs implement resolution of both, with a plain base-URL constructor for
developers who want zero ceremony (the five-line integration uses a
default well-known instance and never touches discovery).

New lexicon required: `org.atgeo.tiles.service` — drafted as
`org.atgeo.tiles.service.json` alongside this document.

### 1.5 Coordinate precision rules

- **Read path**: any region a client derives from device location must be
  snapped outward to the 0.01° grid before computing coverage. Since
  coverage requests are now local manifest intersections (no network),
  this rule protects against *tile-fetch* patterns, not request
  parameters: the snap plus prefetch inflation (§3.4) keeps the fetched
  tile set from centering on the user.
- **Write path**: every SDK helper that composes a location object takes a
  required `precision` parameter. Default representation is
  `community.lexicon.location.hthree` at a coarse resolution (proposed
  default: H3 res 8, ~0.7 km² cells — same order as the read-path grid);
  exact `community.lexicon.location.geo` requires an explicit
  `precision: "exact"`. The API shape makes coarse the path of least
  resistance; it cannot make it mandatory, and shouldn't.

### 1.6 Conformance vectors

A versioned corpus of JSON fixtures in its own repo (`atgeo-conformance`),
consumed by all three SDK test suites in CI. Categories:

| File | Tests | Shape |
|---|---|---|
| `quadkey.json` | lonlat→qk (all zooms 1–17), qk→bbox, parent/child | input, expected output |
| `coverage.json` | bbox→tile set against sample manifests, incl. antimeridian bboxes, empty coverage, over-limit | manifest fixture + bbox → sorted qk list or `TooManyTiles` |
| `snap.json` | 0.01° outward snapping, edge cases at grid lines and ±180 | bbox → bbox |
| `normalize.json` | text normalization (§3.3): mixed scripts, combining marks, ligatures, case folding | string → string |
| `ranking.json` | full query→ordered-results over fixture tiles, incl. ties | tiles + query → ordered rkey list |
| `envelope.json` | tile/manifest parsing, unknown-field tolerance, version rejection | file → parsed model or error |

Rules: vectors are generated by the TypeScript reference plus hand-written
adversarial cases; a vector release is immutable; SDKs pin a corpus
version and CI fails on any mismatch. Unknown JSON fields must be
ignored (forward compatibility); unknown `atgeo` major version must be
rejected. This corpus is the substitute for a shared core — treat a vector
failure with the severity of a broken build, because it is one.

### 1.7 Containment level vocabulary

The `within` relation's `level` values are atgeo's own normative
enumeration — the WoF-placetype-flavored numbering already documented on
the atgeo.org Lexicon page, made official here. Overture's `admin_level`
is explicitly *not* the protocol vocabulary: it is OSM-inherited and
semantically inconsistent across countries.

| level | meaning        | Overture `subtype` mapping |
|------:|----------------|----------------------------|
| 0     | continent      | — (not present in divisions)|
| 10    | country        | `country`                  |
| 15    | dependency     | `dependency`               |
| 25    | region         | `region`                   |
| 35    | county         | `county`                   |
| 45    | localadmin     | `localadmin`               |
| 50    | locality       | `locality`                 |
| 55    | borough        | `borough`                  |
| 60    | macrohood      | `macrohood`                |
| 65    | neighborhood   | `neighborhood`             |
| 70    | microhood      | `microhood`                |

**Phase 2b amendment (`docs/phase2b-design.md` §A.3, §5 `§1.7-renumber`):**
the hoods are renumbered on a uniform stride-5. `macrohood`=60 and
`microhood`=70 are new entries (absent from the prior table); `neighborhood`
moves from its prior value of 60 to 65. This is a protocol change, not a
clarification: any consumer holding the old `neighborhood`=60 value must
update. Placement follows the WoF descent locality → borough → macrohood →
neighborhood → microhood, so a macrohood sorts above (contains) its
neighborhoods and a microhood sorts below (nests inside) its neighborhood.

Rules: consumers sort `within` ascending by level; gaps in the numbering
are deliberate insertion room and additions are a minor protocol bump; a
producer encountering a source subtype absent from its mapping must fail
loudly at import, never guess. Implementation note for the pipeline
agent: verify the actual Overture subtype set (`SELECT DISTINCT subtype`
against a current division parquet) before trusting this table — the
mapping above is drawn from documented subtypes and must be confirmed,
and any further extras brought back as a table amendment, not silently
mapped. WoF placetypes, if that source returns, map into the same scale.
The atgeo.org Lexicon page should be updated to reference this section as
normative (site punch list) — including the neighborhood 60→65 move, not
just the macrohood/microhood additions.

## 2. Spatial AppView (the sidecar)

A single small service: consume repo events, maintain a rebuildable index,
serve tiles + manifest in the §1 format.

### 2.1 Upstream and filtering

Config selects the firehose source:

- `pds`: `com.atproto.sync.subscribeRepos` against one PDS (co-op scale;
  the intended first deployment, next to the co-op's PDS).
- `relay`: same endpoint against a relay (network scale). Note recorded,
  not solved: the firehose has no server-side collection filter, so
  relay scale means drinking the full stream to keep a trickle — a
  bandwidth commitment, not a compute one.
- `jetstream`: a Jetstream-style filtered JSON feed with
  `wantedCollections` — cheap, but inserts a trusted filter operator and
  drops the signed-commit verification path. Acceptable explicitly, per
  deployment.

Watched collections are a config list of NSIDs. For each, config names
which lexicon field(s) carry location objects (a JSON-pointer-ish path
list), so new check-in-shaped lexicons are onboarded by config, not code.

### 2.2 Index

**SQLite, not DuckDB.** The sidecar's workload is single-row upserts and
deletes at firehose rate — precisely what DuckDB's columnar engine is
worst at (pipeline constraint D3: every UPDATE is a table rewrite) and
what SQLite is built for. DuckDB stays the batch tool; SQLite is the
streaming tool. Schema:

```sql
CREATE TABLE records (
  did        TEXT NOT NULL,
  collection TEXT NOT NULL,
  rkey       TEXT NOT NULL,
  cid        TEXT NOT NULL,
  qk17       TEXT NOT NULL,
  tile_qk    TEXT NOT NULL,        -- current assignment
  value_json TEXT NOT NULL,
  PRIMARY KEY (did, collection, rkey)
);
CREATE INDEX idx_tile ON records (collection, tile_qk);

CREATE TABLE dirty_tiles (
  collection TEXT NOT NULL, tile_qk TEXT NOT NULL,
  PRIMARY KEY (collection, tile_qk)
);

CREATE TABLE cursor_state (upstream TEXT PRIMARY KEY, seq INTEGER);
```

`qk17` derivation per location type: `geo` → direct; `hthree` → H3 cell
centroid (record retains the cell; the centroid is index-only);
`address`-only records are indexed only if a geo/hthree sibling exists
(no geocoding, non-goal). Records failing lexicon validation are counted
and skipped, never partially indexed.

### 2.3 Event handling

- `#commit` create/update: validate, derive qk17, assign tile (§2.4),
  upsert row, mark old and new tile dirty (they differ only if the record
  moved).
- `#commit` delete: remove row, mark tile dirty. **Tombstones must work**
  — a deleted check-in disappears from the next tile flush; this is the
  structural difference from gazetteer data and gets its own tests.
- `#account` (deactivated/deleted/takendown): delete all rows for the DID,
  mark affected tiles dirty. Takedown state should also gate backfill.
- `#identity`: no-op (index keys on DID; handles never enter the index).
- Cursor: persist `seq` transactionally with the batch that consumed it;
  resume from cursor on restart; a cursor gap beyond the upstream's replay
  window triggers rebuild (§2.6).

### 2.4 Tile assignment, streaming variant

The batch pipeline picks each record's tile zoom globally
(coarsest zoom whose tile holds ≤ `max_per_tile`). Streaming can't do
global assignment, so the AppView uses a simpler monotone rule: a record
lands in its tile at the collection's current zoom for that qk prefix;
when a tile's row count exceeds `max_per_tile`, the flush loop splits it —
reassigns its records one zoom deeper, rewrites the four children, removes
the parent from the manifest. Tiles never merge (accept the one-way
ratchet; live datasets grow). Split is an index-and-flush operation,
~O(max_per_tile) rows, done inside one transaction.

### 2.5 Flush loop and serving

Every `flush_interval` (default 15 s): drain `dirty_tiles`, rewrite each
tile file from the index (`SELECT ... WHERE collection=? AND tile_qk=?
ORDER BY rkey`, envelope per §1.2 with real `uri`/`cid`), tmp+rename, then
rewrite `manifest.json` (tmp+rename) if the coverage set changed. Write
amplification is one ~100 KB gzip per dirty tile per interval —
negligible at any plausible check-in rate.

Serving is the same static layout as the pipeline (nginx/object storage in
front), with `cache.immutable: false` and `max_age` = `flush_interval` in
the manifest. Eventual consistency of one flush interval is the freshness
contract; nothing here needs CDN purge APIs.

The AppView also publishes its §1.4 announcement records and (if it
controls the DID) service entry — deployment includes identity, which is
what makes "run a geo sidecar next to your PDS" discoverable rather than
bilateral.

### 2.6 Rebuild and backfill

`--rebuild` drops the index and either (a) replays the firehose from
cursor 0 where the upstream retains history, or (b) enumerates repos and
fetches CARs via `com.atproto.sync.getRepo`, indexing matching records,
then tails from the current cursor. Rebuildability is an invariant, not a
feature: it is what licenses the "no canonical state" claim and mirrors
the pipeline's reproducibility property. A CI test rebuilds from a fixture
CAR set and byte-compares tiles.

### 2.7 Implementation notes

- Language: TypeScript on Node. Reasons: `@atproto/sync` /
  `@atproto/repo` give firehose consumption, CAR parsing, and commit
  verification off the shelf; and the AppView shares envelope-writing
  fixtures with the TS reference SDK. (Python would match Garganorn but
  reimplements firehose plumbing; not worth it.)
- Footprint target: idles in tens of MB; a Dokploy-deployable single
  container with a volume for SQLite + tiles. This is deliberately a
  thing a co-op member could run.
- Metrics: events consumed, records indexed/skipped-invalid, dirty flushes,
  tombstones applied, cursor lag. Log counters, no APM dependency.

### 2.8 Abuse resistance

The split ratchet (§2.4) is a DoS surface: tiles never merge, so one
account writing thousands of records into a single qk17 forces permanent
subdivision and bloats the tileset for everyone. Mitigations, all config,
enforced at index time:

- **Per-DID record cap** per (collection, tile at the collection's
  current zoom) — default 100. Over-cap creates are dropped and counted,
  not indexed; the counter is a metric so abuse is visible.
- **Watched-collections config doubles as the primary gate** — the
  AppView only ever indexes explicitly configured lexicons, so "spam via
  a new collection" requires operator action to matter.
- **DID denylist**, applied at event time and retroactively (a denylist
  addition deletes the DID's rows and dirties affected tiles, same path
  as `#account` deletion, §2.3).

Deeper moderation — label-based filtering, community allowlists,
reputation — is a policy layer this design deliberately leaves as a knob:
the event handler exposes a single `admit(event) -> bool` hook where such
policies compose, and v1 ships only the three mechanisms above. The
roadmap's user-generated-places moderation problem is real and unsolved;
these caps are the minimum that keeps an unsolved policy problem from
becoming an infrastructure problem.

## 3. Client SDKs

Three repos: `@atgeo/client` (npm), `AtgeoClient` (SwiftPM),
`org.atgeo:client` (Maven). Native code, platform-idiomatic I/O
(`fetch` / `URLSession` / `OkHttp` + coroutines), zero heavyweight
dependencies, all pinned to the same conformance corpus version.

### 3.1 The five-line bar

TypeScript (Swift/Kotlin equivalents in each README, same shape):

```ts
import { Atgeo } from "@atgeo/client";

const geo = new Atgeo();                    // default public instance
const places = await geo.nearby({ lat: 37.776, lon: -122.434 });
console.log(places[0].name);
```

No key, no signup, works offline after first fetch of the area. Every
README leads with this; every repo contains a runnable demo app (web page,
SwiftUI app, Compose app: "what's near me" + a check-in button), because
developers adopt examples. The demo apps are deliverables, in scope.

### 3.2 API surface (language-neutral; idiomatic per platform)

Read:

- `nearby({lat, lon, radius?, collection?, limit?})` → ranked places.
  Internally: snap region → intersect coverage → fetch/cached tiles →
  local filter + rank. The caller never sees a tile.
- `search(query, {region, collection?, limit?})` → ranked places
  (normalization + ranking per §3.3).
- `getPlace(ref)` → place, where `ref` is a strong ref `{uri, cid?}` or
  rkey+collection; resolves via tiles when possible, `getRecord` fallback.
- `prefetch(region, collection?)` → warms the cache for offline use;
  returns byte/tile counts so apps can show progress.
- `coverage(region)` → tile count and availability, for politeness checks
  and UX ("this area isn't covered").
- `Atgeo.discover(didOrHandle)` → constructs a client from §1.4 discovery.

Write (requires the caller's existing ATProto session — the SDK composes
records and calls `putRecord` via `@atproto/api`'s Agent and platform
equivalents; it never manages credentials):

- `composeLocation({lat, lon, precision})` → location union object, hthree
  by default per §1.5.
- `checkin({agent, place, text?, precision?})` → builds a record
  referencing the place via the atgeo ref shape — the place lexicon's
  `#ref` def: collection-qualified `id` (record-key) plus optional `cid`
  — never embedded coordinates, which is also what lets an AppView
  aggregate per-place later; plus an optional coarse location, and writes
  it. `com.atproto.repo.strongRef` is deliberately not used: it requires
  `at-uri` format, which gazetteer records (https URIs, §1.2) cannot
  satisfy. The place lexicon's ref shape predates this design and turns
  out to be the reason it works. Exact record lexicon for
  check-ins is an open item (§5); the helper is designed so the lexicon
  slots in.

**Global search without a region (two-tier).** Tiles killed
couch-geocoding — you cannot fetch tiles for a place you cannot locate.
The division tileset restores it client-side: it is a complete, small
gazetteer of localities, so `search("Kyoto")` with no region runs a
locality tier over wholesale-prefetched division tiles, resolves the top
locality to a region, then optionally runs the place tier within it. Same
ranking spec (§3.3) at both tiers; the locality tier is just `search()`
against a different collection. The SDK prefetches and pins the division
tileset on first region-less search (size to be measured in pipeline
validation; expected single-digit MB gzipped — if it lands much larger,
the locality tier prefetches per-continent z-prefix instead). This is
what replaces the useful part of `searchRecords` without recreating its
surveillance surface.

Errors are typed and small: `NoCoverage`, `TooManyTiles`,
`ManifestUnavailable`, `StaleManifest`. `TooManyTiles` is the old
`BboxTooLarge`, now a client-side politeness limit (default 50, override
allowed with an explicit parameter — the SDK protects the CDN by default
but doesn't nanny).

### 3.3 Matching and ranking — fully specified, deliberately dumb

The Jaro-Winkler/trigram machinery died with `searchRecords` and is not
reincarnated three times. The spec, testable by vectors:

Normalization `N(s)`: Unicode NFKD → remove combining marks (category Mn)
→ case fold → collapse whitespace. Expressed entirely in operations every
platform ships natively (`String.normalize` + `\p{Mn}` in JS, Foundation
folding in Swift, `java.text.Normalizer`/ICU on Android) — this is the
drift-prone area, hence the largest vector file, with CJK/Arabic/Cyrillic
cases that must pass unchanged through mark-stripping.

Match tiers on `N(query)` vs `N(name)` and each `N(variant)`:
1 exact, 2 prefix, 3 token-prefix (every query token prefixes some name
token), 4 substring, else no match. Rank: tier asc, then `importance`
desc, then distance asc (when the query has a region), then `uri` asc as
the total-order tiebreak. No fuzziness in v1; typo tolerance is a future
spec bump gated on the same vector mechanism, not an implementation's
private improvement.

### 3.4 Caching and fetch behavior

- Tile cache keyed by full URL; `immutable` manifests → cache-forever
  (URL rotation invalidates); live manifests → revalidate with
  `If-None-Match` after `max_age`. Manifest cached with its own
  `max_age`, refreshed opportunistically.
- Storage: web `Cache` API (fallback in-memory), iOS `URLCache` +
  file-backed store, Android OkHttp cache. Default budget 50 MB, LRU,
  configurable.
- Fetch: HTTP/2 multiplexed, concurrency cap 8, no retries beyond one
  (tiles are optional individually; partial coverage degrades results,
  doesn't error).
- Privacy behaviors, on by default: region snap (§1.5) and prefetch
  inflation — fetch the snapped region expanded by one tile ring, so the
  fetched set never tightly centers the device. Documented, not hidden;
  disabling requires an explicit named option.

### 3.5 Per-platform notes

- **TypeScript** is the reference: it generates vectors, and the AppView
  reuses its envelope fixtures. Target: browser + Node + React Native
  (RN needs the storage fallback path tested, not assumed). ESM, no
  bundler-hostile deps; bundle-size budget ≤ 15 kB gzipped for the read
  path — bundle size is an adoption criterion on web.
- **Swift**: async/await, `Sendable`-clean, SwiftPM only, iOS 16+/macOS 13+.
  CoreLocation interop helpers (`CLLocationCoordinate2D` in/out).
- **Kotlin**: coroutines + `Flow` for prefetch progress, Maven Central,
  minSdk pragmatic (24). Plain Kotlin/JVM module + small Android artifact
  for the cache wiring; judgment call recorded: no Kotlin Multiplatform —
  its Swift-consumer ergonomics would make the iOS SDK feel translated,
  and "a Swift developer can't tell it wasn't written for them" is a
  requirement.
- Version skew: SDK minor versions may lag the corpus by one release, no
  more; a `corpus_version` constant is exported by each SDK for
  debuggability.

## 4. Sequencing

Ordered for adoption, not architectural purity — the SDK developers touch
first ships first.

1. **Protocol freeze v1**: envelope + manifest changes into the pipeline
   export (small SQL diff), the decided items encoded — https/at:// URI
   split (§1.2) and the level vocabulary (§1.7) — the
   `org.atgeo.tiles.service` lexicon finalized from its draft, and the
   conformance repo scaffolded. Bootstrap note: the quadkey, snap, and
   coverage vectors do not wait for the TS SDK — they are generated from
   the existing Python reference (`quadkey_to_bbox` and friends in
   `stages.py`) plus hand-written adversarial cases, so the TS agent
   builds against fixtures rather than inventing them. Normalize and
   ranking vectors follow from the TS reference as it stabilizes.
2. **TypeScript SDK, read path** + web demo against the existing
   places.atgeo.org data. Generates normalize/ranking vectors as it
   stabilizes. This is the adoption wedge; polish counts.
3. **AppView MVP** against a single PDS, one check-in-shaped collection,
   tombstones + rebuild tested. Deployed on the co-op Dokploy box.
4. **Swift and Kotlin SDKs** against the frozen corpus, with demo apps.
   Parallelizable; each is agent-executable given the corpus and the TS
   reference.
5. **Write path** across all three (blocked on the check-in lexicon
   decision), then discovery helpers.

## 5. Open questions

- **Check-in record lexicon.** Whether to draft `org.atgeo.checkin` or
  adopt/track a lexicon.community proposal. The SDK write helpers are
  shaped to be lexicon-agnostic, but the demo apps need a concrete answer.
  Recommend raising in the ATGeo working group before unilaterally
  minting one.
- **H3 default resolution** for write precision (res 8 proposed here) —
  sanity-check against what existing `hthree` users emit.
- **Relay-scale filtering** (§2.1) — revisit if/when a network-scale
  deployment is actually wanted; until then the PDS and Jetstream modes
  cover real use.
- **Manifest sharding** — inherited from the pipeline doc's validation
  item; the SDK `Coverage` abstraction is designed so either answer fits.
- **Default public instance** for the zero-config constructor: which
  deployment, who operates it, and its bandwidth budget. This is a
  governance question (ATCF-shaped) more than a technical one, but the
  five-line bar depends on the answer existing.
