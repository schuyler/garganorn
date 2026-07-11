# The atgeo Protocol: Normative Specification

This document specifies the atgeo tile protocol (`atgeo: 1`), the behavior
required of a Spatial AppView producer, and the behavior required of client
SDKs, so that independent implementations interoperate without shared code.

Version: **1.0-draft**. Date: **2026-07-11**. Protocol version: **`atgeo: 1`**.

This is a draft. Several values are marked `[PROVISIONAL]` (§11) and several
points where the source design is silent or internally ambiguous are recorded
in Editor's notes (§12) rather than resolved. This document does not invent
or settle open protocol questions; where the sources disagree or are silent,
that is stated, not adjudicated.

Normative source precedence, where this document summarizes rather than
quotes: `docs/atgeo-appview-sdk-design.md` (as amended by
`docs/phase2b-design.md` §5) is the primary protocol source;
`docs/oq-p2-5-serving-path-design.md` governs the deployed serving-path
details; `docs/org.atgeo.tiles.service.json` is the discovery lexicon.
Shipped code (`garganorn/envelope.py`, `garganorn/levels.py`, the lexicon
JSON under `garganorn/lexicon/`) is cross-checked in §12 as a conforming
producer, not as an independent source of protocol decisions.

## Conformance language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in
[RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) and
[RFC 8174](https://www.rfc-editor.org/rfc/rfc8174) when, and only when,
they appear in all capitals, as shown here.

Requirements are identified by a stable ID (e.g. `TILE-1`, `ENV-4`). Each ID
states exactly one requirement and names the actor(s) it binds: **producer**
(anything that writes tiles/manifests — the batch pipeline or the AppView),
**consumer** (anything that reads them — an SDK or a hand-rolled client),
**AppView** (the streaming producer specifically, where a requirement does
not apply to the batch pipeline), or **SDK** (client-library-specific
requirements, a subset of consumer requirements). A few IDs (AV-2, AV-17)
are explicitly labeled `(informative)` and state context rather than a
requirement. IDs are stable across future revisions of this document; do not
renumber or reuse them.

Material outside numbered requirements — rationale, JSON examples, the
SQLite schema, the five-line adoption bar — is **informative** and marked
as such. Rationale notes are kept short; the full rationale record is
`docs/atgeo-appview-sdk-design.md` and `docs/phase2b-design.md`.

---

## 1. Scope and goals (informative)

atgeo is a protocol for publishing and consuming geographic place data as
static, cacheable, gzip-compressed tile files plus a coverage manifest, with
one envelope shape regardless of whether the data came from a batch
gazetteer import or a live ATProto firehose. It layers three things on that
one envelope:

1. **The protocol contract** (§2–§8): tile scheme, record envelope,
   manifest, discovery, coordinate-precision rules, the containment level
   vocabulary, and the conformance-vector corpus that tests all of the
   above.
2. **The Spatial AppView** (§9): a sidecar producer that consumes the
   ATProto firehose and serves live records in the same tile format.
3. **Client SDKs** (§10): native TypeScript, Swift, and Kotlin
   implementations, kept interchangeable by the conformance corpus rather
   than shared code.

Goals: one envelope/manifest/fetch-path regardless of provenance, with
provenance itself carried honestly in the URI scheme (§3.2); an AppView with
no canonical state, fully rebuildable from firehose replay; SDK conformance
verified by vectors in CI, not by sharing code; read privacy structural via
local tile filtering; write privacy structural via a required precision
parameter; a five-line integration path on every platform.

Non-goals: private location data (ATProto repos are public; the protocol
mitigates precision, not publicity); routing, freeform-address geocoding, or
map rendering; moderation/labeling (compose with existing ATProto labeling);
a shared Rust/WASM core (rejected — conformance vectors police the seams
more cheaply than binding toolchains).

---

## 2. Tile scheme

**TILE-1.** Tile coordinates MUST use the Web-Mercator quadkey (Bing tile
system) scheme over WGS84 longitude/latitude. *(producer, consumer)*

**TILE-2.** A tile file's path MUST be `{base}/{qk6}/{qk}.json.gz`, where
`qk6` is the first six characters of the tile's quadkey `qk`. *(producer,
consumer)*

**TILE-3.** Tiles MAY exist at any zoom level present in the manifest's
`quadkeys` list. A consumer MUST NOT assume a fixed zoom level; the
producer's tile-assignment strategy is not part of the wire contract.
*(producer, consumer)*

**TILE-4.** Tile files MUST be gzip-compressed; gzip is part of the format,
not a transport-layer add-on. A producer MAY serve the compressed bytes
either via a `Content-Encoding: gzip` response header or by serving the
literal `.json.gz` bytes uncompressed-at-the-HTTP-layer (i.e. the client
MUST gunzip them itself). A consumer MUST handle both delivery modes.
*(producer, consumer)*

> Informative: the deployed pipeline serves gzip bytes with an explicit
> `Content-Encoding: gzip` header and does not rely on the HTTP layer to
> transcode (`oq-p2-5-serving-path-design.md`, Change A). A consumer that
> only trusts `Content-Encoding` and never falls back to sniffing/gunzipping
> the body itself may fail against a producer that serves the second mode.

**TILE-5.** A given record MUST belong to exactly one tile per (producer,
collection) pair. A record MUST NOT appear in more than one tile file
simultaneously within one producer's output for one collection. *(producer)*

---

## 3. Record envelope

### 3.1 Tile payload shape

**ENV-1.** A tile payload MUST be a single JSON object with exactly these
top-level keys: `atgeo`, `collection`, `attribution`, `generated_at`,
`records`. No other top-level keys are defined by this version; see ENV-2
for forward-compatibility handling of keys a producer adds anyway.
*(producer, consumer)*

**ENV-2.** A consumer MUST ignore unknown top-level or nested JSON fields
rather than rejecting the document. This is forward compatibility, and it
is what makes MAN-2 (optional `attribution`) and similar additive changes
safe. *(consumer)*

**ENV-3.** `atgeo` MUST be present and MUST be the integer protocol major
version (`1` for this document). A consumer MUST reject a document whose
`atgeo` value is an unknown major version. There is no minor-version field
and no negotiation handshake in v1 — a consumer fetches the manifest first
and can reject before fetching any tile. *(producer, consumer)*

**ENV-4.** `collection` MUST be the NSID of the record collection carried
in this tile (e.g. `org.atgeo.places.overture.place`). *(producer)*

**ENV-5.** `attribution` MUST be present in the tile payload as a string.
The specification does not constrain its content beyond "a string" —
producers commonly emit a URL rather than human-readable text; a consumer
MUST NOT assume either form. *(producer, consumer)*

**ENV-6.** `generated_at` MUST be an RFC 3339 UTC timestamp, `Z`-suffixed,
at seconds precision (no sub-second digits, no `+00:00` form). *(producer)*

**ENV-7.** `generated_at` MUST be run-scoped: every tile produced by one
producer run MUST carry the identical `generated_at` value, and that value
MUST equal the manifest's `generated_at` for the same run. `generated_at`
MUST NOT be a per-flush or per-record timestamp. *(producer)* [Amendment
`phase2b-design.md` §5 `P1`.]

> Rationale: without ENV-7, a second producer (e.g. the AppView, which
> flushes on an interval, §9.5) could legitimately stamp per-flush times,
> and a consumer could wrongly infer per-tile freshness ordering from
> `generated_at` differences that only reflect flush timing, not data
> content.

> Warning: what counts as one "run" for a continuously-flushing streaming
> producer (the AppView, AV-14) is not defined by the sources — see
> Editor's note 12 (§12) before implementing ENV-7 against a streaming
> producer.

**ENV-8.** `records` MUST be a JSON array of record objects (§3.2). Its
order is producer-defined; a consumer MUST NOT rely on record order within
a tile for any purpose (correctness, freshness, or ranking). *(producer,
consumer)* [Amendment `phase2b-design.md` §5 `P5`.]

> Informative: the AppView flushes tiles ordered by `rkey` (§9.5); the batch
> pipeline orders by its own internal sort key (`tile_qk, place_id`
> ascending). Both are conformant producers of this envelope; a consumer
> that depended on either order would break against the other.

### 3.2 Record shape

**ENV-9.** Each element of `records` MUST be a JSON object with exactly
three keys: `uri`, `cid`, `value`. All three keys MUST always be present;
`cid` MUST be explicitly `null` rather than omitted when there is no CID
(see ENV-12). *(producer, consumer)*

**ENV-10.** `uri` MUST carry provenance in its scheme, per producer kind:

- A **gazetteer record** (batch-produced, no ATProto repository backing)
  MUST use an `https://` URI of the form `https://{host}/{collection}/{rkey}`
  — a dereferenceable URL where a GET returns the bare record value. It
  MUST NOT use an `at://` URI. *(producer)*
- An **AppView record** (backed by a real, commit-verified ATProto repo)
  MUST use the genuine `at://` URI for the record, addressable and
  verifiable at its origin PDS. *(AppView)*

*(consumer: MUST treat the URI scheme as a provenance signal — https vs.
at:// tells the consumer whether the record is repository-backed and
CID-verifiable — and MUST NOT assume interchangeability of the two schemes
for verification purposes.)*

> Rationale: the `at://` scheme is specified as addressing ATProto
> repository data, and ATProto has no per-record "signed by a DID"
> mechanism outside MST inclusion in a signed commit. A gazetteer record
> minting an `at://` URI it cannot resolve or verify would be dishonest
> about what it is. This is a deliberate design choice, not an oversight —
> see the batch-MST idea recorded as explicitly speculative and out of
> scope for v1 (`atgeo-appview-sdk-design.md` §1.2).

**ENV-11.** For gazetteer records, the canonical `uri` MUST be
`https://{host}/{collection}/{rkey}`, where `rkey` is the record's key
*after* any source-specific transform has been applied (for example, an
OSM-sourced record's rkey is the `node:|way:|relation:`-prefixed form, not
the bare source ID). This is the same form the collection's `getRecord`
XRPC endpoint returns, so tile URIs and XRPC-resolved URIs agree by
construction. A producer MUST NOT percent-encode the `rkey` segment when
composing this URI — colons and other characters legal in a URI path
segment (RFC 3986) are served raw, e.g. an OSM rkey's `node:|way:|relation:`
prefix. *(producer)*

**ENV-12.** `cid` MUST be required-but-nullable in every record, never
omitted. For gazetteer records, `cid` MUST be `null` and MUST NOT be
computed by any means (not a content hash of `value`, not any other
derivation) — there is no signed commit to verify such a hash against, so
computing one would record a number that verifies nothing. Only AppView
records, which carry a genuine commit-verified CID from the repo, MAY carry
a non-null `cid`. *(producer)* [Amendment/clarification `phase2b-design.md`
§5 `P4`.]

> Rationale (`phase2b-design.md` §B.3): a "real" atproto-style CID requires
> canonical DAG-CBOR encoding, which bans floats — a constraint today's
> gazetteer records only meet by accident (raw Overture `attributes`
> structs can contain floats). Computing a hash nobody can verify against a
> signed commit is a caching nicety, not a security property, and is not
> worth the encoding-audit cost. If content-addressing for gazetteer data
> is wanted later (mirror dedup, offline verification), the design
> direction is a batch-built MST and signed commit over the gazetteer —
> explicitly out of scope for v1.

**ENV-13.** `value` MUST be the record's payload using its existing lexicon
schema (e.g. `org.atgeo.place`, `community.lexicon.location.*`). This
document does not define or alter those lexicon schemas; it defines only
the envelope wrapping them. *(producer)*

### 3.3 Informative example — tile payload

```json
{
  "atgeo": 1,
  "collection": "org.atgeo.places.overture.place",
  "attribution": "https://docs.overturemaps.org/attribution/",
  "generated_at": "2026-07-02T00:00:00Z",
  "records": [
    {
      "uri": "https://places.atgeo.org/org.atgeo.places.overture.place/08f2...",
      "cid": null,
      "value": {
        "$type": "org.atgeo.place",
        "rkey": "08f2...",
        "name": "...",
        "importance": 62,
        "locations": ["..."],
        "variants": ["..."],
        "attributes": {"...": "..."},
        "relations": {"...": "..."}
      }
    }
  ]
}
```

---

## 4. Manifest

### 4.1 Field set

**MAN-1.** A manifest document MUST be a single JSON object at
`{base}/current/manifest.json` (or an equivalent live path for a streaming
producer, §9.5) with these fields: `atgeo`, `source`, `collection`,
`generated_at`, `tile_url_template`, `cache`, `quadkeys`. *(producer)*

> Warning: this `{base}/current/manifest.json` path conflicts with the
> deployed OQ-P2-5 serving path, under which `base_url` already resolves
> inside `<source>/tiles/current` — see Editor's note 9 (§12) before relying
> on this default against a real deployment.

**MAN-2.** `attribution` in the manifest is OPTIONAL. A manifest lacking it
is still conformant (ENV-2 unknown/absent-field tolerance applies
symmetrically here — its absence MUST NOT break a consumer). When present,
it lets a consumer render attribution without fetching any tile. *(producer,
consumer)* [Amendment `phase2b-design.md` §5 `P3`.]

**MAN-3.** `source` MUST be a producer-internal string identifying the data
source (e.g. `overture_place`). This document does not constrain its
vocabulary; it exists for producer self-description. *(producer)*

**MAN-4.** `collection` MUST be the NSID of the collection this manifest
covers, matching the tiles' `collection` field (ENV-4). *(producer)*

**MAN-5.** `generated_at` MUST use the same run-scoped RFC 3339 format as
ENV-6/ENV-7 and MUST be identical to the `generated_at` value stamped on
every tile of the same run. *(producer)*

**MAN-6.** `tile_url_template` MUST be a string template of the form
`{base}/{qk6}/{qk}.json.gz` (or a producer-specific variant satisfying
TILE-2/TILE-1) that a consumer can expand to a tile URL given a `qk`.
*(producer, consumer)*

**MAN-7.** `quadkeys` MUST be the complete, sorted list of quadkeys this
manifest covers. A consumer MUST treat this as the full coverage set — a
quadkey absent from the list has no tile. *(producer, consumer)*

**MAN-8.** `quadkeys` MAY be split across shards if the unsharded manifest
exceeds a size budget (`[PROVISIONAL]`, §11): shard files use the path
`manifest/{qk4}.json`, one per z4 quadkey prefix, each shard having the same
document shape as an unsharded manifest restricted to that prefix's
quadkeys. A consumer-facing `Coverage` abstraction (or equivalent) MUST NOT
leak whether a given manifest is sharded or not — sharding MUST be an
implementation detail an SDK's public API hides. *(producer, SDK)*

### 4.2 Cache semantics

**MAN-9.** `cache` MUST be an object with at least `max_age` (integer
seconds) and `immutable` (boolean). *(producer)*

**MAN-10.** A producer MUST NOT set `cache.immutable: true` unless
`tile_url_template` embeds a run-unique path segment (i.e. the URL for a
given tile changes whenever that tile's content could change across runs).
*(producer)* [Amendment `phase2b-design.md` §5 `P2`.]

> Rationale: if the same tile URL can return different bytes after a later
> producer run (as it does under the deployed `current`-symlink serving
> path, `oq-p2-5-serving-path-design.md` Change A), a CDN or client honoring
> `immutable: true` would serve stale bytes for the full `max_age` window.
> `immutable` is a promise about a specific URL's bytes never changing, not
> about the collection's freshness cadence.

**MAN-11.** A consumer MUST treat `cache.immutable: true` as license to
cache the tile forever (until the URL itself changes) and MUST treat
`cache.immutable: false` as requiring revalidation after `max_age` seconds
(see CACHE-2). Per MAN-9, `immutable` is a required field and a conformant
producer MUST always send it; a consumer encountering it absent anyway
MUST treat that absence as `false` — this fallback is this document's own
consumer-robustness addition, not a license for producers to omit the
field. *(consumer)*

**MAN-12.** The current pipeline's `tile_url_template` has no run-unique
path segment (tiles are served through a `current` symlink/route), so it
MUST ship `cache.immutable: false`. A future run-stamped template (e.g.
`{base}/<run-timestamp>/{qk6}/{qk}.json.gz`) MAY legitimately set
`immutable: true` once deployed. *(producer)*

### 4.3 Informative example — manifest

```json
{
  "atgeo": 1,
  "source": "overture_place",
  "collection": "org.atgeo.places.overture.place",
  "attribution": "https://docs.overturemaps.org/attribution/",
  "generated_at": "2026-07-02T00:00:00Z",
  "tile_url_template": "{base}/{qk6}/{qk}.json.gz",
  "cache": { "max_age": 86400, "immutable": false },
  "quadkeys": ["02301", "023010", "..."]
}
```

---

## 5. Discovery

**DISC-1.** An operator's ATProto DID document MAY include a service entry
declaring tile-service presence:

```json
{ "id": "#atgeo_tiles",
  "type": "AtgeoTileService",
  "serviceEndpoint": "https://tiles.atgeo.org" }
```

This entry declares a non-PDS service and MAY be present on any DID
document regardless of whether that DID hosts an ATProto repository.
*(producer/operator)*

**DISC-2.** An operator serving one or more collections MUST publish, for
each served collection, an `org.atgeo.tiles.service` record in its own
ATProto repository, keyed by the collection's NSID. *(producer/operator)*

**DISC-3.** The `org.atgeo.tiles.service` record MUST include `collection`
(NSID, equal to the record key), `baseUrl` (HTTPS base URL), and `kind`
(a string; the lexicon at `docs/org.atgeo.tiles.service.json` documents
`"gazetteer"` and `"live"` as `knownValues` — an open, advisory list, not a
closed enum, so a consumer MUST tolerate an unrecognized `kind` value rather
than rejecting the record). It MAY include `manifestPath`
(defaults to `current/manifest.json` if omitted), `attribution`, and
`createdAt`. *(producer/operator)*

> Warning: this `current/manifest.json` default conflicts with the deployed
> OQ-P2-5 serving path — see Editor's note 9 (§12).

**DISC-4.** A consumer holding only an operator's DID MUST be able to
enumerate every collection that operator serves by calling
`com.atproto.repo.listRecords` against `org.atgeo.tiles.service` on that
DID's repo. *(consumer)*

**DISC-5.** An SDK MUST provide a discovery-resolution path that, given a
DID or handle, resolves the DID document's service entry (DISC-1, "where")
and enumerates the announcement records (DISC-2, "what") to construct a
usable client. An SDK MUST also provide a plain base-URL constructor
requiring no discovery ceremony, for the zero-configuration adoption path
(§10.8), targeting a `[PROVISIONAL]` default public instance (§11) whose
operator, deployment, and bandwidth budget are unresolved governance
questions. *(SDK)*

> Rationale for the DISC-1/DISC-2 split (`atgeo-appview-sdk-design.md`
> §1.4): it keeps the `at://` namespace honest. A DID document may declare a
> non-PDS service freely without implying repository hosting (DISC-1). The
> announcement records (DISC-2) live in a real, account-sized repo on a real
> PDS, so they legitimately carry `at://` URIs. Identity anchors the
> service; bulk tile data stays out of the `at://` namespace, consistent
> with ENV-10/ENV-11.

---

## 6. Coordinate precision rules

**PREC-1.** A consumer deriving a query region from device/user location
MUST snap that region outward to the 0.01° grid before computing tile
coverage. *(consumer)*

> Rationale: since coverage computation is a local manifest intersection
> (no network round trip carries the raw location), this rule protects
> against the *fetched tile set* revealing a tight center on the user, not
> against a request parameter leaking precision to a server — combined with
> prefetch inflation (CACHE-6), it keeps the fetched tile footprint from
> resolving to the user's exact location.

**PREC-2.** Every SDK helper that composes a location object for writing
(e.g. `composeLocation`) MUST take a REQUIRED `precision` parameter — there
MUST NOT be an implicit-precision code path. *(SDK)*

**PREC-3.** The default representation for a composed location MUST be
`community.lexicon.location.hthree` at a coarse resolution
(`[PROVISIONAL]` default: H3 resolution 8, ~0.7 km² cells — see §11).
*(SDK)*

**PREC-4.** Composing an exact `community.lexicon.location.geo` location
MUST require the caller to pass an explicit `precision: "exact"` (or
equivalent unambiguous named value) — exactness MUST NOT be reachable via a
default or an unlabeled numeric/string value that could be mistaken for a
coarse setting. *(SDK)*

> Rationale: PREC-2/PREC-4 make the coarse path the path of least
> resistance without making it mandatory — the SDK cannot stop a developer
> who deliberately asks for exact coordinates, and should not try to.

---

## 7. Containment level vocabulary

**LVL-1.** The `within` relation's `level` field MUST use atgeo's own
normative enumeration below, not any source dataset's native admin-level
field (e.g. Overture's `admin_level` is explicitly excluded — it is
OSM-inherited and semantically inconsistent across countries).
*(producer, consumer)*

**LVL-2.** Normative level table:

| level | meaning | Overture `subtype` mapping |
|------:|----------------|----------------------------|
| 0 | continent | — (not present in current division data) |
| 10 | country | `country` |
| 15 | dependency | `dependency` |
| 25 | region | `region` |
| 35 | county | `county` |
| 45 | localadmin | `localadmin` |
| 50 | locality | `locality` |
| 55 | borough | `borough` (not present in current Overture data; reserved) |
| 60 | macrohood | `macrohood` |
| 65 | neighborhood | `neighborhood` |
| 70 | microhood | `microhood` |

This table reflects the stride-5 renumbering amendment
(`phase2b-design.md` §5 `§1.7-renumber`, §A.3): `macrohood` (60) and
`microhood` (70) are additions over the prior table, and `neighborhood`
moved from a prior value of 60 to 65. **This was a protocol change, not a
clarification** — any consumer holding the old `neighborhood = 60` value
MUST update. *(producer, consumer)*

**LVL-3.** A consumer sorting the `within` array MUST sort ascending by
`level`. *(consumer)*

Informative: gaps in the numbering (e.g. between 15 and 25, or between 0 and
10) are deliberate insertion room, not errors. A future addition to this
table is a minor protocol bump, not a major one.

**LVL-4.** A producer encountering a source subtype/placetype not present
in its `subtype`→`level` mapping MUST fail loudly at import time (raise/abort,
listing the unmapped value(s)) and MUST NOT guess, default, or silently drop
the record. *(producer)*

> Informative: the shipped mapping (`garganorn/levels.py`) implements this
> via a SQL `CASE` expression with **no `ELSE` branch**, so an unmapped
> subtype produces `NULL`, combined with a pre-artifact-write existence check
> and a post-CTAS `count(*) WHERE level IS NULL = 0` assertion —
> belt-and-braces enforcement of LVL-4.

**LVL-5.** A producer adding a new source (including a reintroduction of
WoF placetypes) MUST map that source's native type vocabulary into this
same numeric scale rather than introducing a parallel one. *(producer)*

---

## 8. Conformance vectors

**VEC-1.** A versioned corpus of JSON fixtures MUST exist in a dedicated
repository (`atgeo-conformance`) and MUST be consumed by every SDK's test
suite in CI. *(SDK)*

> Informative: fixtures are generated from a reference implementation (the
> TypeScript SDK for `normalize.json`/`ranking.json`; the existing Python
> reference, e.g. `quadkey_to_bbox` and related functions, for
> `quadkey.json`/`snap.json`/`coverage.json` ahead of the TS SDK's
> availability) plus hand-written adversarial cases the reference generator
> would not think to produce on its own.

**VEC-2.** The corpus MUST include, at minimum, the six categories below.

| File | Tests | Shape | Requirements tested |
|---|---|---|---|
| `quadkey.json` | lonlat→qk (zooms 1–17), qk→bbox, parent/child | input → expected output | TILE-1, TILE-2 |
| `coverage.json` | bbox→tile set against sample manifests, incl. antimeridian bboxes, empty coverage, over-limit | manifest fixture + bbox → sorted qk list or `TooManyTiles` | MAN-7, ERR-2 |
| `snap.json` | 0.01° outward snapping, edge cases at grid lines and ±180 | bbox → bbox | PREC-1 |
| `normalize.json` | text normalization: mixed scripts, combining marks, ligatures, case folding | string → string | NORM-1, NORM-2 |
| `ranking.json` | full query→ordered-results over fixture tiles, incl. ties | tiles + query → ordered rkey list | RANK-1, RANK-2 |
| `envelope.json` | tile/manifest parsing, unknown-field tolerance, version rejection | file → parsed model or error | ENV-1..13, MAN-1..12 |

*(SDK, informative table — the requirement-mapping column is this
document's addition, not verbatim from the source design.)*

**VEC-3.** A released corpus version MUST be immutable — fixtures in a
released version MUST NOT be edited or removed after release; corrections
land in a new version. *(corpus maintainer)*

**VEC-4.** Each SDK MUST pin a specific corpus version, and CI MUST fail on
any mismatch between the pinned version's expected behavior and the SDK's
actual behavior. *(SDK)*

**VEC-5.** A consumer parsing envelope/manifest documents MUST ignore
unknown JSON fields (ENV-2, restated here as a testable vector requirement)
and MUST reject documents with an unknown `atgeo` major version (ENV-3).
*(consumer, tested by `envelope.json`)*

**VEC-6.** A corpus version's vector fixtures MUST NOT be treated with
lower severity than a compiler error — a vector failure MUST be treated as
a build-breaking failure, because the corpus is the sole interoperability
mechanism between independently-written SDKs (there is no shared code to
fall back on). *(SDK, binds CI configuration)*

---

## 9. Spatial AppView

Informative framing: the AppView is a single small sidecar service that
consumes ATProto repo events, maintains a rebuildable local index, and
serves tiles and a manifest in the §2–§4 format. It holds no canonical
state — everything here exists to make that claim actually true.

### 9.1 Upstream and filtering

**AV-1.** An AppView deployment's firehose source MUST be configurable as
one of: `pds` (`com.atproto.sync.subscribeRepos` against one PDS), `relay`
(the same protocol against a relay), or `jetstream` (a filtered JSON feed
using `wantedCollections`). *(AppView)*

**AV-2.** In `jetstream` mode, the operator explicitly trusts the Jetstream
operator as a filter — this drops the signed-commit verification path
present in `pds`/`relay` mode. This trade-off is an explicit, informed
per-deployment choice, not a silent default. *(AppView operator, informative
trust-tradeoff note)*

**AV-3.** Watched collections MUST be operator-configured as a list of
NSIDs; for each, config MUST name which lexicon field(s) carry location
objects (a JSON-pointer-ish path list), so a new check-in-shaped lexicon is
onboarded by configuration, not by a code change. *(AppView)*

### 9.2 Index (informative reference schema)

The following SQLite schema is **informative** — it documents one correct
implementation's shape, not a wire contract other implementations must
match field-for-field. SQLite (not DuckDB) is the correct engine class
here: the workload is single-row upserts/deletes at firehose rate, which is
precisely what a columnar engine (every `UPDATE` a table rewrite) is worst
at.

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

**AV-4.** An AppView MUST derive an indexable `qk17` per location record as
follows: `community.lexicon.location.geo` → direct quadkey-at-zoom-17
derivation from the coordinate; `community.lexicon.location.hthree` → the
H3 cell's centroid (the record itself retains the H3 cell; the centroid is
index-only, not re-emitted as the record's location); an `address`-only
record MUST NOT be indexed unless a `geo`/`hthree` sibling exists among the
record's configured location fields (AV-3) — there is no geocoding step
(non-goal). *(AppView)*

**AV-5.** A record that fails lexicon validation MUST be counted (as a
metric) and skipped in full — an AppView MUST NOT partially index an
invalid record. *(AppView)*

### 9.3 Event handling

**AV-6.** On a `#commit` create or update event for a watched collection,
the AppView MUST: validate the record, derive `qk17` (AV-4), assign a tile
(§9.4), upsert the index row, and mark both the record's old tile and new
tile dirty (they differ only if the record's location changed enough to
change tile assignment). *(AppView)*

**AV-7.** On a `#commit` delete event, the AppView MUST remove the index
row and mark the affected tile dirty. A deletion MUST be reflected — the
record MUST be absent from that tile's *next* flush. Tombstone handling is
mandatory, not best-effort, and is the structural difference from batch
gazetteer data. *(AppView)*

**AV-8.** On an `#account` event indicating deactivation, deletion, or
takedown, the AppView MUST delete all index rows for that DID and mark all
affected tiles dirty. A takedown state SHOULD also gate backfill (§9.6) —
i.e. a takedown discovered during backfill SHOULD prevent that DID's records
from being (re-)indexed. *(AppView)*

**AV-9.** An `#identity` event MUST be a no-op with respect to the index
(the index keys on DID; handles never enter the index). *(AppView)*

**AV-10.** The AppView MUST persist its upstream cursor (`seq`)
transactionally with the batch of events that advanced it, MUST resume from
the persisted cursor on restart, and MUST trigger a full rebuild (§9.6) if
a cursor gap exceeds the upstream's replay window. *(AppView)*

### 9.4 Streaming tile assignment

**AV-11.** The AppView MUST use a monotone tile-assignment rule distinct
from the batch pipeline's global assignment: a record is placed into the
tile at the collection's *current* zoom for its qk prefix. When a tile's
record count exceeds `max_per_tile` — an operator-configured per-tile record
threshold shared with the batch pipeline's tile-assignment strategy, default
`[PROVISIONAL]` 1000 (§11) — the AppView MUST split it — reassign
its records one zoom level deeper, write the (up to) four child tiles,
remove the parent tile from the manifest's coverage set. *(AppView)*

**AV-12.** Tiles MUST NOT merge under any circumstance in the AppView's
lifetime. The split ratchet is one-directional. *(AppView)*

**AV-13.** A tile split MUST be atomic: the reassignment of that tile's
records, the write of its children, and the removal of the parent from the
coverage set MUST happen within a single transaction (no external observer
can see a partially-split state). *(AppView)*

### 9.5 Flush loop and serving

**AV-14.** The AppView MUST periodically (default interval
`[PROVISIONAL]` 15 s, §11) drain its dirty-tile set and, for each dirty
tile, rewrite that tile file from the current index state, ordered
`ORDER BY rkey` (the AppView's producer-defined order per ENV-8), using the
§3 envelope with genuine `uri`/`cid` values (ENV-10 AppView branch, ENV-12
AppView branch). Tile writes MUST use a write-to-temp-then-rename pattern
(no reader ever observes a partially-written tile file). *(AppView)*

> Warning: AV-14 rewrites only DIRTY tiles per flush interval, which is in
> tension with ENV-7's run-scoped `generated_at` requirement for
> un-rewritten tiles of the same run — see Editor's note 12 (§12).

**AV-15.** After a flush, if the coverage set changed (e.g. a split
occurred), the AppView MUST rewrite `manifest.json` using the same
temp-then-rename pattern. *(AppView)*

**AV-16.** The AppView MUST serve tiles from the same static layout as a
batch producer (§2–§4), with `cache.immutable: false` (per MAN-10 — the
AppView's tile URLs are not run-stamped) and `cache.max_age` set to the
flush interval. *(AppView)*

**AV-17.** The AppView's freshness contract is exactly one flush interval
of eventual consistency; this document does not require or expect any CDN
purge/invalidation mechanism beyond `max_age` expiry. *(informative)*

**AV-18.** An AppView instance MUST publish its DISC-2 announcement records
as part of normal operation, unconditionally. If the AppView instance
controls its own operator DID, it MUST also publish its DISC-1 service
entry. Deployment includes identity publication, not just serving.
*(AppView operator)*

### 9.6 Rebuild and backfill

**AV-19.** An AppView MUST support a rebuild operation that drops the local
index and reconstructs it either (a) by replaying the firehose from cursor
zero where the upstream retains sufficient history, or (b) by enumerating
repos and fetching CARs via `com.atproto.sync.getRepo`, indexing matching
records, then tailing from the then-current cursor. *(AppView)*

**AV-20.** A CI verification MUST exist that rebuilds an AppView index from a
fixture CAR set (AV-19 path (b)) under a controlled/injected `generated_at`
timestamp and byte-compares the resulting tiles against the expected fixture
output. This is a CI conformance check on the replay/CAR-rebuild path, not a
claim that every rebuild of live production state reproduces byte-identical
output (see Editor's note 8, §12). *(AppView, CI verification requirement)*

### 9.7 Abuse resistance

**AV-21.** The AppView MUST enforce a per-DID record cap per (collection,
tile-at-current-zoom) — default `[PROVISIONAL]` 100 (§11). Creates over the
cap MUST be dropped (not indexed) and MUST be counted in a visible metric.
*(AppView)*

> Informative: the cap-exceeded metric (above) and the invalid-record metric
> (AV-5) are two entries in a broader set of counters design §2.7 expects an
> AppView deployment to expose: events consumed, records indexed, records
> skipped as invalid, dirty-tile flushes, tombstones applied, and upstream
> cursor lag. The source frames these as log counters with no APM
> dependency, not a specific metrics-system integration.

**AV-22.** The watched-collections configuration (AV-3) is itself an abuse
gate: the AppView MUST NOT index any collection not explicitly configured,
so an attacker cannot introduce a new collection to bypass caps without
operator action. *(AppView)*

**AV-23.** The AppView MUST support a DID denylist, applied both at event
time (future events from a denylisted DID are dropped) and retroactively
(adding a DID to the denylist MUST delete that DID's existing rows and
dirty the affected tiles, via the same code path as AV-8's `#account`
deletion handling). *(AppView)*

**AV-24.** The AppView MUST expose a single `admit(event) -> bool` hook in
the event-handling path where deployment-specific policy (label-based
filtering, allowlists, reputation systems) can compose without modifying
AV-21–AV-23. v1 ships only the three mechanisms above; deeper moderation is
explicitly left to this hook and is not otherwise specified here.
*(AppView)*

---

## 10. Client SDKs

### 10.1 API surface

**SDK-1.** An SDK MUST provide a `nearby({lat, lon, radius?, collection?,
limit?})` read call that internally: snaps the region (PREC-1), intersects
local coverage, fetches/uses-cached tiles, and applies local filtering and
ranking (§10.4) — the caller MUST NOT be exposed to tile-level concepts
through this call's return type. *(SDK)*

**SDK-2.** An SDK MUST provide a `search(query, {region, collection?,
limit?})` read call using the normalization and ranking rules of §10.4.
*(SDK)*

**SDK-3.** An SDK MUST provide a `getPlace(ref)` call, where `ref` is
either a strong-ref-shaped `{uri, cid?}` or an `{rkey, collection}` pair;
it MUST attempt resolution via already-fetched/cached tiles first and MUST
fall back to `com.atproto.repo.getRecord`-equivalent resolution when the
record is not locally available. *(SDK)*

**SDK-4.** An SDK MUST provide a `prefetch(region, collection?)` call that
warms the local cache for offline use and returns a byte/tile-count
progress signal. *(SDK)*

**SDK-5.** An SDK MUST provide a `coverage(region)` call returning tile
count and availability for the given region, for UX purposes ("this area
isn't covered") and for politeness checks before a large fetch. *(SDK)*

**SDK-6.** An SDK MUST provide `discover(didOrHandle)`, constructing a
usable client via the §5 discovery mechanisms. *(SDK)*

**SDK-7.** An SDK MUST provide `composeLocation({lat, lon, precision})`
satisfying PREC-2–PREC-4. *(SDK)*

**SDK-8.** An SDK MUST provide `checkin({agent, place, text?, precision?})`
that composes and writes (via the caller's existing ATProto session/agent —
the SDK MUST NOT manage credentials itself) a check-in-shaped record, using
a `[PROVISIONAL]` check-in record lexicon (§11 register), referencing
`place` via the atgeo place-reference shape (`org.atgeo.place`
lexicon's `#ref` def: a collection-qualified `id` — a record key — plus an
optional `cid`), never via embedded coordinates. The optional `precision`
parameter governs an optional coarse location attached to the check-in
record itself (composed per PREC-2–PREC-4 via SDK-7), distinct from the
place reference. *(SDK)*

**SDK-9.** An SDK's write path MUST NOT use `com.atproto.repo.strongRef` to
reference a place, because `strongRef` requires `at-uri` format, which a
gazetteer record's `https://` URI (ENV-10) cannot satisfy. The
`org.atgeo.place#ref` shape MUST be used instead. *(SDK)*

> Rationale: this is a case where an existing lexicon shape (predating this
> design) turns out to be exactly the mechanism the https/at:// URI split
> requires — a strong-ref-only write path would have made every gazetteer
> place uncheckin-able.

**SDK-10.** The exact check-in record lexicon is an open item (§11 register)
— an SDK's write helpers MUST be structured so that the concrete lexicon
can be substituted without changing the helper's call shape. *(SDK,
forward-looking constraint)*

### 10.2 Region-less ("global") search — two-tier

**SDK-11.** An SDK MUST support text search with no region parameter by
running a two-tier resolution: first a locality-tier search (SDK-2's
ranking machinery) against a wholesale-prefetched division/locality
tileset, resolving the top match to a region; then, optionally, a
place-tier search within that resolved region. *(SDK)*

**SDK-12.** The division/locality tileset MUST be prefetched and pinned
locally on first region-less search use (not fetched per-query), with a
`[PROVISIONAL]` expected size of single-digit MB gzipped (§11 register).
*(SDK)*

> Informative: expected size is single-digit MB gzipped, measured in
> pipeline validation; if it lands significantly larger, the intended
> fallback is prefetching by per-continent z-prefix instead of the whole
> tileset at once. This is a sizing contingency, not a second normative code
> path — §10 does not mandate which one an SDK uses at what size threshold.

### 10.3 Typed errors

**ERR-1.** An SDK MUST expose distinct, catchable error types (not generic
exceptions/string codes) for at least: `NoCoverage`, `TooManyTiles`,
`ManifestUnavailable`, `StaleManifest`. *(SDK)*

**ERR-2.** `TooManyTiles` MUST be a client-side politeness limit (not a
server-enforced rejection reflected back), with a default of
`[PROVISIONAL]` (§11) 50 tiles, overridable via an explicit parameter on the
calling API. *(SDK)*

### 10.4 Normalization and ranking

**NORM-1.** Text normalization `N(s)` MUST be exactly: Unicode NFKD
decomposition → remove all codepoints of Unicode general category `Mn`
(combining marks) → case-fold → collapse consecutive whitespace to a single
space. An SDK MUST express this using its platform's native Unicode
facilities (e.g. `String.normalize` + a `\p{Mn}`-matching regex in
JavaScript/TypeScript, Foundation string folding in Swift,
`java.text.Normalizer`/ICU on the JVM/Android) rather than a hand-rolled
table. *(SDK)*

**NORM-2.** `N(s)` MUST pass CJK, Arabic, and Cyrillic text through
unchanged in the sense that mark-stripping MUST NOT corrupt base characters
in those scripts — this is tested by the `normalize.json` vector file and
is the correctness bar for NORM-1's implementation, not a separate rule.
*(SDK)*

**RANK-1.** A query string and a candidate name/variant are compared as
`N(query)` vs. `N(name)` (and independently vs. each `N(variant)`) and
MUST be assigned a match tier:

1. exact match
2. prefix match
3. token-prefix match (every whitespace-separated token of the query is a
   prefix of some token of the name)
4. substring match
5. else: no match (candidate excluded)

*(SDK)*

**RANK-2.** The total ranking order over matched candidates MUST be: tier
ascending (1 before 2 before 3 before 4), then `importance` descending,
then distance ascending (only when the query carries a region/centroid),
then `uri` ascending as the final deterministic tiebreak. *(SDK)*

**RANK-3.** v1 MUST NOT implement fuzzy/typo-tolerant matching. Any future
typo tolerance is a protocol version bump gated through the same
conformance-vector mechanism (§8), not an individual SDK's private
enhancement. *(SDK)*

### 10.5 Caching and fetch behavior

**CACHE-1.** Tile cache entries MUST be keyed by the full tile URL.
*(SDK)*

**CACHE-2.** A tile fetched from a manifest with `cache.immutable: true`
(MAN-10/MAN-11) MUST be cached indefinitely (invalidated only by the URL
itself changing, e.g. a new manifest generation). A tile fetched from a
`cache.immutable: false` manifest MUST be revalidated (e.g. via
`If-None-Match`) after `max_age` seconds have elapsed since it was cached.
*(SDK)*

**CACHE-3.** The manifest itself MUST be cached with its own `max_age` and
refreshed opportunistically (not necessarily synchronously on every call).
*(SDK)*

**CACHE-4.** An SDK MUST use the platform-appropriate persistent cache
mechanism (web: `Cache` API with an in-memory fallback; iOS: `URLCache`
plus a file-backed store; Android: OkHttp's disk cache) with a default
budget of `[PROVISIONAL]` (§11) 50 MB, evicted LRU, and MUST make the budget
configurable. *(SDK)*

**CACHE-5.** Tile fetches MUST use HTTP/2 multiplexing where available,
capped at `[PROVISIONAL]` (§11) 8 concurrent requests, with no more than one
retry per tile. A single tile's fetch failure MUST degrade the result set
(partial coverage) rather than raising an error for the whole call — tiles
are individually optional. *(SDK)*

**CACHE-6.** Region-snap (PREC-1) and prefetch inflation (fetching the
snapped region expanded by one tile ring beyond the minimum needed) MUST
be enabled by default. These are privacy defaults, and MUST be documented,
not hidden. Disabling either behavior MUST require an explicit, named
option. *(SDK)*

### 10.6 Per-platform requirements

**SDK-13.** The TypeScript implementation MUST be the reference
implementation: it generates the `normalize.json` and `ranking.json`
conformance vectors. *(TS SDK)*

> Informative: design §2.7 notes the AppView reuses the TypeScript SDK's
> envelope-writing fixtures — a shared-fixtures point, not an assertion that
> this document normatively requires the AppView's implementation language
> to be TypeScript (that is an implementation note, not a §9 decision).

**SDK-14.** The TypeScript SDK's read path MUST target a bundle-size budget
of ≤15 kB gzipped, MUST support browser and Node.js runtimes, MUST support
React Native with its storage-fallback path under test (not merely
assumed to work), and MUST use ESM with no bundler-hostile dependencies.
*(TS SDK)*

**SDK-15.** The Swift SDK MUST use async/await, MUST be `Sendable`-clean,
MUST be distributed via SwiftPM only, and MUST target iOS 16+/macOS 13+. It
MUST provide CoreLocation interop helpers
(`CLLocationCoordinate2D` in and out). *(Swift SDK)*

**SDK-16.** The Kotlin SDK MUST use coroutines and `Flow` for prefetch
progress reporting, MUST be distributed via Maven Central, and MUST target
a pragmatic minSdk of 24. It MUST be structured as a plain Kotlin/JVM
module plus a small Android artifact for platform cache wiring. The Kotlin
SDK MUST NOT be built as a Kotlin Multiplatform module targeting iOS — a
KMP-shared iOS consumer would read as "translated" rather than
platform-native, which is a stated requirement, not a style preference.
*(Kotlin SDK)*

**SDK-17.** Every SDK repository MUST ship a runnable demo application
("what's near me" plus a check-in button, appropriate to the platform: web
page, SwiftUI app, Compose app) as an in-scope deliverable, not an
afterthought. *(SDK)*

**SDK-18.** Every SDK README MUST lead with the five-line (or
platform-equivalent minimal) example achieving a first `nearby()` result
with no API key, no signup, and no billing setup. *(SDK)*

### 10.7 Corpus version-skew rule

**SDK-19.** An SDK's minor version MAY lag the conformance corpus by at
most one corpus release; it MUST NOT lag by more. *(SDK)*

**SDK-20.** Every SDK MUST export a `corpus_version` constant (or
platform-idiomatic equivalent) identifying which corpus version it was
validated against, for debuggability. *(SDK)*

### 10.8 Informative — the five-line bar

```ts
import { Atgeo } from "@atgeo/client";

const geo = new Atgeo();                    // default public instance
const places = await geo.nearby({ lat: 37.776, lon: -122.434 });
console.log(places[0].name);
```

No key, no signup, works offline after the first fetch of the area. The
"default public instance" this constructs against is itself an open item
(§11 register) — which deployment, who operates it, and its bandwidth
budget are unresolved governance questions the five-line bar depends on.

---

## 11. Open items and provisional defaults

Every value below is tagged `[PROVISIONAL]` in its PRIMARY defining
requirement — the row's first-listed requirement in the "Defining
requirement(s)" column. Where a row lists secondary requirements alongside
the primary one, those secondary requirements may be untagged; the register
row itself is what ties the value to the whole set. This register collects
each value with what would need to happen to settle it.

| Value | Default | Defining requirement(s) | What would settle it |
|---|---|---|---|
| H3 write-precision default | resolution 8 (~0.7 km² cells) | PREC-3 | Sanity-check against what existing `community.lexicon.location.hthree` producers/consumers actually emit in the wild (source design flags this explicitly as unverified) |
| `TooManyTiles` politeness limit | 50 tiles | ERR-2 | No blocking dependency identified in sources; could be settled by measuring real query patterns against deployed manifests |
| AppView per-DID record cap | 100 records per (collection, tile) | AV-21 | Real-world abuse observation once an AppView is deployed; currently a judgment-call default |
| `max_per_tile` (operator-configured per-tile record threshold, batch pipeline and AppView) | 1000 | AV-11 | Deployed value observed in `oq-p2-5-serving-path-design.md`'s `config.yaml`; not independently justified in sources as a protocol-level default |
| AppView flush interval | 15 s | AV-14, AV-16 | Trade-off between write amplification and freshness; no measurement cited in sources |
| Manifest sharding size threshold | ~5 MB gzipped | MAN-8 | "Measure in pipeline validation" per source — not yet measured as of this document's date |
| SDK tile-cache budget | 50 MB | CACHE-4 | No blocking dependency; a policy default pending real device/browser usage data |
| Fetch concurrency cap | 8 | CACHE-5 | No blocking dependency identified |
| Default public instance (zero-config constructor target) | unset | DISC-5, SDK-18, §10.8 | Explicitly a governance question (source calls it "ATCF-shaped") — who operates it, which deployment, its bandwidth budget. The five-line bar (SDK-18) is blocked on this existing at all, not just on tuning it |
| Check-in record lexicon | unset | SDK-8, SDK-10 | Either draft `org.atgeo.checkin` or adopt/track a lexicon.community proposal; source recommends raising in the ATGeo working group before unilaterally minting one |
| Division/locality tileset prefetch size | expected single-digit MB gzipped | SDK-12 | To be measured in pipeline validation; if larger than expected, the documented contingency is per-continent z-prefix prefetching instead of the whole tileset |

Also open, not tied to a single numeric default:

- **Relay-scale filtering (§9.1, AV-1).** The firehose has no server-side
  collection filter, so `relay` mode means consuming the full stream to
  keep a trickle — a bandwidth commitment. Recorded as a known limitation,
  not solved; revisit only if a network-scale deployment is actually
  wanted. `pds` and `jetstream` modes are considered sufficient for now.
- **Manifest sharding — whether, not just when (MAN-8).** The `Coverage`
  abstraction is designed so either a sharded or unsharded answer fits
  without an SDK-breaking change, but which one ships is still open.
- **`REPO` hardcoding in gazetteer URIs (ENV-11).** The shipped producer
  hardcodes a single `repo` value (`places.atgeo.org`) into every record's
  `uri`. Correct for a single canonical deployment; becomes a required
  configuration parameter the day a second gazetteer deployment exists.
  Noted, not solved, in the source design.
- **Legacy `export_tiles()` / envelope duplication.** The pipeline
  historically had two envelope-writing code paths; consolidating onto one
  (`garganorn/envelope.py`) was done, but deleting the now-redundant legacy
  function entirely is tracked as separate follow-up cleanup, not part of
  the envelope protocol itself.

---

## 12. Editor's notes (ambiguities encountered)

These are points where the sources are silent, internally inconsistent, or
where I could not verify a claim against the cited evidence without going
beyond in-scope sources. None are resolved here; they are recorded per the
task's instruction not to invent or settle protocol decisions.

1. **Manifest sharding is described only in narrative prose, not as a
   worked example.** `atgeo-appview-sdk-design.md` §1.3 states the fallback
   shape (`manifest/{qk4}.json`, "same shape") but no source document gives
   a concrete example manifest-shard document or specifies whether a
   sharded deployment has a top-level index file listing the shards, or how
   a consumer discovers that sharding is in effect versus fetching
   `current/manifest.json` directly. MAN-8 above is written narrowly to
   avoid inventing that mechanism.

2. **`TooManyTiles` vs. the `getCoverage` XRPC lexicon's `BboxTooLarge`.**
   The design doc (§3.2) states `TooManyTiles` "is the old `BboxTooLarge`,
   now a client-side politeness limit." But the shipped
   `garganorn/lexicon/getCoverage.json` still defines a server-side
   `BboxTooLarge` XRPC error (and a `BboxTooPrecise` error not mentioned
   anywhere in the SDK design at all). It is unclear whether: (a) the
   server-side `getCoverage` errors are meant to be retired now that
   coverage is computed client-side from a fetched manifest, (b) they
   coexist with client-side `TooManyTiles` as a defense-in-depth pair, or
   (c) the shipped `getCoverage.json` lexicon is itself stale relative to
   the SDK design and due for revision. I did not reconcile this — §10.3
   above specifies only the client-side `TooManyTiles`/`ERR-2` behavior the
   design doc is explicit about, and does not attempt to normatively
   describe `getCoverage`'s server-side error contract, which is arguably
   in scope for a "complete" protocol spec but not resolvable from the
   given sources without a decision.

3. **`org.atgeo.searchRecords` lexicon status.** The SDK design doc's
   opening section states this design "assumes [the pipeline restructure's]
   outcomes (static tiles, static manifest, `searchRecords` removed)." But
   `garganorn/lexicon/searchRecords.json` still exists in the shipped
   lexicon directory, fully defined, including a JW/trigram-era shape
   (`distance_m`, a `#record` def distinct from the tile envelope's
   `{uri,cid,value}`). I have treated `searchRecords` as **not** part of
   the normative protocol surface this document specifies (consistent with
   the design doc's stated assumption that it is removed), and have not
   included it in §10's API surface. This is a live conflict between the
   design doc's premise and shipped code that should be resolved by
   deleting the stale lexicon file or by an explicit decision that it
   survives for some other reason — I did not decide which.

4. **`getPlace`'s `ref` shape vs. the `strongRef`/`#ref` distinction.**
   §3.2 of the design doc says `getPlace(ref)` takes "a strong ref
   `{uri, cid?}` or rkey+collection." Read literally against SDK-9 (the
   design doc's own emphatic statement that `strongRef` is unusable for
   gazetteer places because it requires `at-uri`), this is confusing: does
   `getPlace`'s `{uri, cid?}` form mean a literal `com.atproto.repo.strongRef`
   object (which per SDK-9 cannot address a gazetteer place), or does it
   mean "a strongRef-*shaped* object" that in practice carries an https URI
   for gazetteer places despite the lexicon's `at-uri` format constraint? I
   have written SDK-3 to describe the shape generically
   ("strong-ref-shaped") without asserting which lexicon type validates it,
   because the source text does not disambiguate this itself. This is
   plausibly the same underlying tension SDK-9 addresses for the *write*
   path but the design doc never states the read-path resolution
   explicitly.

5. **Level 0 (continent) and `borough` (55) have zero producer
   instances currently.** `phase2b-design.md` treats `borough: 55` as
   "normative; absent from current Overture" and explains it is included
   so a future release emitting it maps cleanly. Level 0 (continent) is
   stated as having no producer entry "not present in divisions" with no
   further comment on whether any producer is ever expected to emit it, or
   whether it exists purely as documented insertion-room / a placeholder
   for a data source that does not yet exist in this ecosystem. I have
   included both in LVL-2's table as specified but flag that "normative
   but currently unpopulated" is a slightly unusual conformance state this
   document does not have separate testing guidance for (e.g. must a
   conformance vector exercise a `level: 0` or `level: 55` record even
   though no real producer currently emits one?).

6. **Whether `MAN-8`'s shard threshold applies per-collection or globally,
   and whether the pipeline has actually measured against it yet.** The
   source text says "measure in pipeline validation; if gzipped manifests
   exceed ~5 MB the fallback is..." — phrased as a future action, not a
   completed one. I could not find evidence in the read sources that this
   measurement has been performed or that any manifest currently exceeds
   the threshold. Treated as still-open in §11.

7. **AppView `qk17` "current assignment" column name vs. tile-zoom
   variability (TILE-3).** The informative SQLite schema (§9.2) names a
   column `tile_qk` as "current assignment," which is consistent with the
   split-ratchet monotone assignment (AV-11), but the schema and prose
   never state whether a *consumer* reading AppView-served tiles needs any
   different handling than reading batch-pipeline tiles, given that the
   AppView's assignment strategy is explicitly *not* the same algorithm as
   the batch pipeline's global assignment (§9.4 vs. the batch pipeline's
   unspecified-in-this-document global algorithm). TILE-3 already tells a
   consumer not to assume a fixed zoom, which appears sufficient, but the
   two producers' differing *strategies* (global vs. streaming-monotone)
   are never asserted to be wire-indistinguishable beyond that — I have
   not asserted indistinguishability beyond what TILE-3 already covers.

8. **AV-20's byte-identity claim only covers the replay/CAR-rebuild CI
   check, and a path-(b) history-dependence gap is unresolved.** Design
   §2.6 states "a CI test rebuilds from a fixture CAR set and byte-compares
   tiles," and `phase2b-design.md` §6.7 describes byte-comparisons performed
   under an injected fixed timestamp for the batch pipeline's determinism
   check. Neither source states that byte-identity holds for AV-19's path
   (b) (enumerate-repos-and-fetch-CARs, then tail from the *then-current*
   cursor) when that path is exercised against **live**, evolving
   production state rather than a fixed fixture CAR set. The AppView's tile
   assignment is history-dependent — AV-12's split ratchet is one-directional
   and depends on the order records were observed and when splits crossed
   `max_per_tile`, not only on the current record set — so a rebuild that
   replays a different event history (e.g. after a cursor gap forced a
   path-(b) rebuild that skipped some now-superseded intermediate states)
   can legitimately produce a different, still-valid tile/split layout for
   the same current data. I have not asserted byte-identity beyond the CI
   fixture-CAR-set scenario the sources actually describe; whether some
   weaker invariant (e.g. record-set-identical, split-topology-may-differ)
   is what actually licenses the "no canonical state" claim for real
   deployments is not stated in the sources and is not decided here.

9. **MAN-1 and DISC-3's `{base}/current/manifest.json` default conflicts
   with the deployed OQ-P2-5 serving path.** `atgeo-appview-sdk-design.md`
   §1.3 and the `org.atgeo.tiles.service` lexicon's `manifestPath` default
   both specify the manifest at `{base}/current/manifest.json`. But
   `oq-p2-5-serving-path-design.md` (Change A/B, the deployed slug-aware
   route) resolves a collection's `base_url` to already point *inside*
   `<source>/tiles/current` (e.g.
   `https://places.atgeo.org/tiles/overture-place`), so the real manifest
   under that deployment is at `{base}/manifest.json` — appending
   `current/manifest.json` on top double-applies `current` and 404s. This
   means the spec's own MAN-1 default and DISC-3's `manifestPath` default
   are unusable as literally stated against places.atgeo.org and any other
   deployment following the OQ-P2-5 serving-path pattern. I have not
   resolved which source is authoritative (whether the design doc's manifest
   path is stale relative to the deployed serving path, or the deployed
   serving path is itself a deviation that should be corrected) — that is a
   protocol/deployment decision, not something this document decides.

10. **AV-8's takedown-gates-backfill obligation is SHOULD, not MUST, per the
    source, though strengthening it may be desirable.** Design §2.3 states
    "Takedown state should also gate backfill" — a SHOULD-level statement.
    An operator-compliance rationale (an AppView continuing to backfill
    records for a DID under takedown would work against the takedown's
    intent) plausibly justifies strengthening this to MUST, but the source
    does not make that decision, and this document does not make it either.
    AV-8 above is written at SHOULD to match the source; a future revision
    could propose MUST as a protocol amendment.

11. **Coverage-intersection semantics (bbox→tile-set intersection against a
    manifest's `quadkeys` list, including behavior at mixed zoom levels and
    across the antimeridian) are undefined in both this document and the
    cited sources.** `coverage.json`'s row in VEC-2 names the test surface
    ("bbox→tile set against sample manifests, incl. antimeridian bboxes,
    empty coverage, over-limit") but neither `atgeo-appview-sdk-design.md`
    nor `phase2b-design.md` states the actual intersection algorithm: how a
    bbox spanning the antimeridian (crossing ±180°) is split or wrapped
    before intersection, or how intersection is computed against a
    `quadkeys` list containing tiles at mixed zoom levels (a manifest is not
    guaranteed to be single-zoom — TILE-3). The conformance vectors
    (`coverage.json`) are the only place this behavior would actually be
    pinned down, but the vector corpus itself is out of scope for the
    sources reviewed here (it lives in a separate `atgeo-conformance`
    repository not among this document's sources). Separately, RANK-2's
    "distance ascending (only when the query carries a region/centroid)"
    tiebreak is similarly underspecified by the sources: neither source
    states the distance metric (great-circle vs. planar), reference point
    (device location vs. query centroid), or units. I have not invented
    either the coverage-intersection algorithm or the distance-ranking
    metric; both are recorded here as open rather than specified.

12. **What counts as one "run" is undefined for a continuously-flushing
    streaming producer, and ENV-7 is in unresolved tension with AV-14/AV-10
    for the AppView.** ENV-7 (`phase2b-design.md` §5 `P1`) requires every
    tile of one producer run to carry an identical `generated_at`, equal to
    the manifest's `generated_at` for that same run, and bans per-flush or
    per-record stamping. MAN-5 restates the same run-scoped identity
    requirement for the manifest. For a batch producer, "run" is
    unambiguous — one pipeline execution, one `generated_at`. But AV-14
    rewrites only DIRTY tiles on each flush interval (default 15 s,
    `[PROVISIONAL]`, §11) — a continuously-running AppView never has a
    single moment where all tiles are (re)written together — and AV-10
    requires the AppView to persist its cursor and resume from it
    transparently across a restart, with a full rebuild only triggered when
    a cursor gap exceeds the upstream's replay window. Neither
    `atgeo-appview-sdk-design.md` §2.5 (flush loop) nor `phase2b-design.md`
    §B.4/`P1` (the ENV-7 amendment itself) states what "run" means for this
    producer shape. At least two readings are each independently consistent
    with the literal requirement text, and would produce diverging
    conforming implementations:
    (a) **frozen-for-lifetime/rebuild-epoch value** — `generated_at` is
    fixed once, at AppView process start or at the start of a rebuild epoch
    (AV-19), and every flush (AV-14) — including flushes of tiles untouched
    by that flush's dirty set — is required to keep stamping that same
    frozen value indefinitely, so ENV-7's "identical value across the run"
    holds trivially because the run *is* the whole lifetime/epoch; or
    (b) **rewrite-all-tiles-per-flush** — each flush interval mints a new
    `generated_at` and is itself treated as a "run" boundary, which would
    require the AppView to rewrite the *entire* coverage set (not just
    AV-14's dirty tiles) on every flush to keep every tile's `generated_at`
    identical and current, effectively voiding the dirty-tile-only
    optimization AV-14 specifies. A third possibility — that ENV-7 simply
    does not apply to the AppView, or applies only loosely — is not
    supported by the requirement text, which states no AppView exemption.
    I have not chosen between these readings; ENV-7 and AV-14 are left as
    specified in the sources, with this note recording that an
    implementation must pick one to be interoperable, and that neither
    source states which.

13. **Two further source silences, recorded but not decided.** (a) ERR-1
    names `StaleManifest` and `ManifestUnavailable` as required distinct
    error types, but no source (`atgeo-appview-sdk-design.md` §3.2 or
    elsewhere) defines the raise conditions for either — what staleness
    threshold trips `StaleManifest`, or what failure modes (network error,
    5xx, malformed JSON, timeout) map to `ManifestUnavailable` versus some
    other error — and neither appears with defined coverage in the VEC-2
    vector-file table (`envelope.json` covers parsing/version-rejection,
    not these two conditions specifically). (b) ENV-3 states a consumer
    MUST reject a document whose `atgeo` value is an unknown major version,
    but no source states required consumer behavior for a payload that is
    otherwise well-formed JSON but is missing the `atgeo` field entirely
    (as opposed to present-but-unrecognized) — whether that is treated the
    same as an unknown-version rejection, some other error, or is
    unspecified/implementation-defined. Both are recorded here as open
    rather than specified.

No conflicts were found between `docs/phase2b-design.md` and the shipped
`garganorn/envelope.py` / `garganorn/levels.py` — both shipped modules
implement their respective design documents' decisions exactly as
specified (envelope field set, `cid: null` non-computation, run-scoped
`generated_at` handling delegated to the caller, the stride-5 level
vocabulary with no-ELSE CASE and fail-loud validation). See item 3 above
for the one shipped-lexicon-vs-design-doc conflict found
(`searchRecords.json`).

---

## 13. Change log

- **1.0-draft** (2026-07-11): Initial normative specification, transcribed
  from `docs/atgeo-appview-sdk-design.md` (as amended by
  `docs/phase2b-design.md` §5), `docs/oq-p2-5-serving-path-design.md`, and
  `docs/org.atgeo.tiles.service.json`. No new protocol decisions made in
  producing this document; see §12 for points left unresolved rather than
  decided here.
- **1.0-draft, review round 1 fixes** (2026-07-11): Corrected stale
  cross-section references left over from an earlier section numbering;
  rescoped AV-20's byte-identity claim to the replay/CAR-rebuild CI check
  and recorded the path-(b) history-dependence gap (Editor's note 8);
  flagged the MAN-1/DISC-3 manifest-path conflict against the deployed
  OQ-P2-5 serving path (Editor's note 9); trimmed CACHE-6 to its source
  obligation; restored AV-18's unconditional announcement-record
  publication; documented SDK-8's `precision?` parameter; corrected AV-4's
  field-agnostic address/geo/hthree sibling check; downgraded AV-8 to
  SHOULD to match the source (Editor's note 10); fixed the VEC-2 table's
  NORM-2 row to `normalize.json`; added missing `[PROVISIONAL]` tags at
  DISC-5, SDK-8, and SDK-12 so every §11 register row is tagged in its
  defining requirement; added a missing MUST to RANK-1; resolved two
  normative-language-in-informative-text findings (VEC-6 promoted to
  normative, AV-2 kept as prose) and removed LVL-4's requirement ID as
  genuinely informative (renumbering LVL-5/LVL-6 down to LVL-4/LVL-5);
  recorded coverage-intersection and RANK-2 distance-metric silence
  (Editor's note 11); reconciled MAN-9/MAN-11's required-vs-absent
  `immutable` tension; capitalized two lowercase normative verbs; defined
  `max_per_tile` at AV-11 with a new §11 register row; aligned DISC-3's
  `kind` field to the lexicon's open `knownValues`; added envelope.py's
  no-percent-encoding rule to ENV-11; restored the corpus's hand-written
  adversarial-case provenance (VEC-1) and the AppView's full metrics list
  (informative note after AV-21).
- **1.0-draft, review round 2 fixes** (2026-07-11): Recorded the
  undefined-"run"-boundary tension between ENV-7's run-scoped
  `generated_at` requirement and the AppView's continuous dirty-tile-only
  flush loop (AV-14) plus cursor-resume behavior (AV-10), naming the two
  divergent conforming readings without deciding between them, and added
  pointer warnings at ENV-7 and AV-14 (Editor's note 12); corrected LVL-4's
  informative note from "pre-CTAS" to "pre-artifact-write" to match the
  shipped check's actual position in `stages.py`; trimmed AV-15's
  parenthetical to the one example the source design supports; unified the
  ENV-11 URI-template placeholder to `{host}` (matching ENV-10); added a
  preamble carve-out noting AV-2 and AV-17 are explicitly informative, not
  requirements; clarified that the §11 `[PROVISIONAL]` tag lives in each
  row's primary defining requirement, with secondary listed requirements
  left untagged; restored design §3.4's "documented, not hidden" privacy-
  default obligation to CACHE-6 without reintroducing the round-1 invented
  MUST NOTs; recorded two further source silences — ERR-1's undefined
  `StaleManifest`/`ManifestUnavailable` raise conditions and vector
  coverage, and undefined consumer behavior for a payload missing the
  `atgeo` field entirely (Editor's note 13); added missing `(§11)` pointers
  to the ERR-2, CACHE-4, and CACHE-5 `[PROVISIONAL]` tags; removed SDK-13's
  normative assertion that the AppView is itself TypeScript, restating the
  shared-fixtures point as an informative note citing design §2.7.
