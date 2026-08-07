# ATGeo tile format and XRPC reference

What a client fetching from an ATGeo gazetteer receives: the tile files, the
record envelope inside them, the place record shape, the containment level
vocabulary, and the XRPC methods the server answers.

This document describes the intended format and interface. Almost all of it is
implemented today, and citations point at the implementing code or the test
that pins it, so a statement can be checked and will fail visibly when it stops
being true. Where the code has not caught up — a field still written that is
going away, a method still served that is being removed — the intent is stated
here and the gap is recorded under [Divergences from the
code](#divergences-from-the-code). Nothing is asserted that isn't either
implemented or decided.

Not covered here, because none of it exists: the streaming AppView, the client
SDKs, service discovery, and the conformance-vector corpus. Those are designs,
and they live in `atgeo-appview-sdk-design.md`.

---

## Tiles

A tile is a set of place records whose locations fall inside one quadkey cell,
stored as a gzipped JSON file.

**Quadkey scheme.** Web-Mercator (Bing) quadkeys over WGS84 longitude and
latitude. Digits encode `0=NW, 1=NE, 2=SW, 3=SE`, with y increasing southward
(`covering.py:46`, `:31`). Latitude is clamped to ±85.05112877980659
(`covering.py:25`). Zoom is the length of the quadkey string.

The pipeline derives quadkeys with DuckDB's `ST_QuadKey(lon, lat, 17)` at
import (`sql/foursquare_import.sql:19`, `sql/osm_import.sql:148`) and decodes
them in Python with `quadkey_to_bbox` (`stages.py:662-677`). The two agree:
`test_quadtree_functions.py:83-91` round-trips a point through the DuckDB
encoder and asserts the Python-decoded bbox contains it. A SQL twin of the
decoder matching to 1e-9 lives in `sql/qk_env_macro.sql`.

**Tile URLs are opaque.** A client never constructs one. It calls
`getCoverage` for a region, receives a list of URLs, fetches all of them, and
concatenates the `records` arrays into one result set. The on-disk path scheme
is a serving detail described under [Serving layout](#serving-layout); nothing
a client does depends on it, and it can change without breaking a client.

Because each record belongs to exactly one tile, concatenation needs no
deduplication — two tiles returned for the same region, even at different
zooms, hold disjoint record sets.

The only stability a tile URL owes anyone is consistency within a single
release of a collection: every URL `getCoverage` hands out for that release
resolves to that release's bytes. Across releases the URLs may change
completely, because a client always asks again rather than remembering.

**Compression.** Tiles are gzipped on disk with `mtime=0` so identical input
produces identical bytes (`stages.py:1648`). The server sends those bytes
verbatim with `Content-Encoding: gzip` and `Content-Type: application/json`
(`__main__.py:120-121`, pinned by `test_app.py:255-259`). It does not
negotiate: gzip is sent regardless of the request's `Accept-Encoding`.

**Zoom and assignment.** Tiles exist at any zoom from 6 to 17
(`stages.py:1319`); the range is fixed and not configurable. Assignment is
adaptive — a record goes in the coarsest tile holding no more than
`max_per_tile` records, falling back to zoom 17 when no zoom qualifies
(`stages.py:1409-1417`). `max_per_tile` defaults to 1000 (`stages.py:1319`,
`config.yaml.example:40`).

A consequence: tiles at different zooms coexist in one collection, and a
zoom-6 tile and a zoom-9 tile may cover overlapping ground
(`test_integration_quadtree.py:458-479`). A client must not assume a fixed
zoom.

**One record, one tile.** Each record belongs to exactly one tile per
collection. The assignment table has one row per input record
(`stages.py:1416-1423`), asserted by `test_tile_assignment.py:79`. A record
reaching two tiles means a duplicate primary key upstream, which the export
audit detects and reports as an error (`stages.py:1433-1438`).

**Caching.** Tiles are served with `Cache-Control: public, max-age=<ttl>` when
a TTL is configured, deliberately without `immutable`
(`__main__.py:122-126`, `test_app.py:281-284`).

That omission is forced by the current URL shape, not by the design. Tiles are
served through a `current` symlink repointed on each pipeline run
(`stages.py:1699-1707`), so one URL can return different bytes after a later
run, and a cache honoring `immutable` would serve stale tiles for the full
`max_age` window. Since URLs need only be consistent within a release, a
run-stamped path would make every tile URL permanently immutable instead.

---

## Record envelope

A tile payload is one JSON object with these top-level keys, in order
(`envelope.py:51-60`):

| key | meaning |
|---|---|
| `collection` | NSID of the collection carried in this tile |
| `attribution` | string; producers emit a URL, but the content is unconstrained |
| `generated_at` | RFC 3339 UTC timestamp, `Z`-suffixed, seconds precision |
| `records` | array of record objects |

There is no envelope version field. Lexicon schemas are upgrade-only, so the
evolution rule already governing the records inside a tile governs the tile
too. The code still writes one — see [Divergences from the
code](#divergences-from-the-code).

`generated_at` is run-scoped: one timestamp is taken at the start of a pipeline
run, used to name the run directory, and stamped identically on every tile of
that run (`stages.py:1553-1555`). `test_stages.py:379-399` asserts every tile
in a run carries the same value. It is not a per-tile or per-flush time, so
differences between tiles never indicate relative freshness.

Each element of `records` has exactly three keys — `uri`, `cid`, `value`
(`envelope.py:39`, asserted by `test_envelope.py:124` and
`test_stages.py:218`).

### `uri`

`https://{host}/{collection}/{rkey}` — a dereferenceable URL where a GET
returns the bare record value (`envelope.py:16-25`, `__main__.py:101-108`).
The `rkey` is the post-transform key, so an OSM record's rkey carries its
`node:`/`way:`/`relation:` prefix. Characters legal in a URI path segment are
served raw: the colon is not percent-encoded (`test_envelope.py:87-88`). This
is the same form `getRecord` returns, so tile URIs and XRPC-resolved URIs agree
by construction.

Gazetteer records never use `at://`. The `at://` scheme addresses ATProto
repository data, and ATProto has no per-record "signed by a DID" mechanism
outside MST inclusion in a signed commit. A gazetteer record minting an `at://`
URI it cannot resolve or verify would be dishonest about what it is. The URI
scheme is therefore a provenance signal: `https://` means the record is not
repository-backed and not CID-verifiable.

Recorded as considered and out of scope: a pipeline stage could batch-build a
real MST and signed commit over the gazetteer, earning `at://` honestly. It is
inadvisable today because shared relay infrastructure assumes account-sized
repos, and a 100M-record repo rewriting monthly is a crawl-load citizenship
problem. Revisit if the ecosystem grows bulk-repo conventions.

### `cid`

Always the literal `null` for gazetteer records, present rather than omitted
(`envelope.py:39`, `test_envelope.py:134`). It is never computed by any means.

A real ATProto CID requires canonical DAG-CBOR encoding, which bans floats — a
constraint gazetteer records only meet by accident, since raw Overture
`attributes` structs can contain them. Computing a hash nobody can verify
against a signed commit is a caching nicety, not a security property, and is
not worth the encoding-audit cost.

### `value`

The record payload, embedded verbatim as the JSON string DuckDB's `to_json()`
produced. The envelope writer splices it in rather than re-encoding, which
avoids a per-record parse/serialize round trip and preserves DuckDB's UTF-8
exactly with no `ensure_ascii` escaping (`envelope.py:31-35`, `:51-61`).
`test_envelope.py:156-185` asserts round-trip fidelity including non-ASCII.

---

## Place records

The `value` of a place record as actually emitted (`test_export.py:164-192`):

| field | type | notes |
|---|---|---|
| `$type` | string | `org.atgeo.place` |
| `rkey` | string | record key, post-transform |
| `name` | string | primary name |
| `importance` | integer | 0–100 |
| `locations` | array | union of `community.lexicon.location.{geo,hthree,address,bbox}` |
| `variants` | array | `{name, type?, language?}` |
| `attributes` | object | source-specific; divisions carry `{subtype, level}` |
| `relations` | object | `{within: [...]}` |

`importance` is computed at import as a blend of local density and name rarity
— `round(60*least(density/norm,1) + 40*least(idf/norm,1))` for places
(`foursquare_import.sql:45-48`), and a population-weighted variant for
divisions (`overture_division_import.sql:105-112`). The XRPC layer hoists it
out of `value` into the response wrapper (`server.py:116`, `:268`).

`relations.within` is a list of `{rkey}` objects naming the containing
divisions, emitted already sorted by containment level ascending, then by
boundary id (`sql/compute_containment.sql:58-59`; serving path
`boundaries.py:49-54`). The level itself is not carried in the relation — it
is the producer's sort key only, so the ordering is the information a consumer
gets.

---

## Containment levels

Division containment uses this vocabulary rather than any source dataset's
native admin-level field. Overture's `admin_level` is explicitly excluded: it
is OSM-inherited and semantically inconsistent across countries.

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

Exactly as coded in `LEVEL_VOCAB` (`levels.py:28-39`), with the key set and
every value asserted by `test_levels.py:79-99`.

The stride-5 numbering follows the WoF descent locality → borough → macrohood
→ neighborhood → microhood, so a macrohood sorts above its neighborhoods and a
microhood nests inside its neighborhood. Gaps are deliberate insertion room.

`borough` (55) is mapped but unpopulated — current Overture division data
contains no borough subtype (`test_levels.py:84-87`). There is no level 0;
continent has no producer entry (`levels.py:24-25`).

**Unmapped subtypes fail the import.** The generated SQL `CASE` has no `ELSE`
branch (`levels.py:67-73`), so an unmapped subtype yields `NULL`. Two checks
run after the CTAS and before any artifact is written: an existence check
listing offending subtypes (`stages.py:874-885`) and a `count(*) WHERE level IS
NULL = 0` assertion (`stages.py:891-899`). Either raises. `test_levels.py:198-231`
asserts both fire and that no `places.parquet` is written when they do.

This table was drawn from documented Overture subtypes. It should be confirmed
against `SELECT DISTINCT subtype` on a current division parquet before being
trusted, with any extras brought back as an amendment rather than silently
mapped.

---

## XRPC methods

Two methods, both at `/xrpc/<nsid>`. Every error returns HTTP 400 with a body
of `{"error": <name>, "message": ...}`.

Together they define the whole query surface: ask which tiles cover a region,
then fetch those tiles. There is no server-side text search and no record
enumeration. A caller can name a region no finer than roughly a kilometre, or
name a record it already knows the key for — neither shape lets a query
describe a point.

### `org.atgeo.getCoverage`

Which tiles cover a bounding box.

Parameters: `collection` (required), `bbox` (required, comma-separated
`minLon,minLat,maxLon,maxLat`).

Returns `{"tiles": [...]}` — a sorted list of fully-formed, opaque tile URLs
(`quadtree.py:131`). The caller fetches every URL returned and concatenates the
records; there is no pagination, no ranking, and no partial-result protocol.
Filtering and ordering are the client's, applied after the merge.

Errors, in evaluation order:

- `BboxTooPrecise` — any bbox coordinate carrying more than two decimal places.
  Checked before parsing (`server.py:200-211`). This is the precision floor,
  and it is enforced by the server rather than left to client courtesy: a
  caller cannot ask a question finer than roughly a kilometre, so no query can
  resolve to a device's position. `test_get_coverage.py:140-214` covers
  scientific notation, trailing zeros, and bare `37.`.
- `InvalidBbox` — malformed or out-of-range coordinates.
- `CollectionNotFound` — unknown collection, or a collection whose tile run is
  incomplete (`__main__.py:40-49`, `test_app.py:447-455`).
- `BboxTooLarge` — the box covers more than `max_coverage_tiles` tiles, default
  50 (`quadtree.py:132-133`, `server.py:45`).

Coverage is answered from `manifest.duckdb`: the server caches every distinct
`tile_qk` at boot (`quadtree.py:117-124`) and intersects the bbox against each
tile's bounds. `bboxes_intersect` treats touching edges and corners as
intersecting and handles bboxes crossing the antimeridian by wrap detection
(`stages.py:680-707`).

### `com.atproto.repo.getRecord`

One record by key.

Parameters: `repo`, `collection`, `rkey` (all required).

Returns `{"uri", "attribution", "importance"?, "value", "_query"}`. For
tile-backed collections the record is read out of the tile file itself: the
rkey is resolved to a tile via `manifest.duckdb`, and that tile is opened and
scanned (`tile_reader.py:30-56`), with a 256-entry LRU cache on parsed tiles.
Containment relations are attached as `record["relations"]["within"]`
(`server.py:88-110`).

Errors: `CollectionNotFound`, `RecordNotFound`.

The same record is available over plain HTTP at `/{collection}/{rkey}`, which
returns the bare value with no envelope (`__main__.py:101-108`) — this is what
makes the `uri` in a tile dereferenceable.

Lexicon documents are served at `GET /<nsid>` (`__main__.py:76-81`), outside
the XRPC surface.

---

## Divergences from the code

The code has not caught up to three decisions. Each is decided, not open.

**`org.atgeo.searchRecords` is being removed.** It is currently registered
(`server.py:39`) and implemented (`server.py:232-284`), taking raw
`latitude`/`longitude` at arbitrary precision and explicitly exempt from the
`BboxTooPrecise` check (`test_get_coverage.py:220-226`). While it is served,
the precision floor described above holds on `getCoverage` and not on
`searchRecords`. Removal takes the route, the method, its tests
(`test_server.py:100-209`), and `lexicon/searchRecords.json`.

**`com.atproto.repo.listRecords` is being removed.** It currently serves only
`com.atproto.lexicon.schema`, enumerating the server's own lexicon documents,
and raises `CollectionNotFound` for everything else (`server.py:130-134`).
Lexicon documents remain reachable at `GET /<nsid>`, so removal costs no
capability. It takes the route, the method, and `lexicon/listRecords.json`.

**The `atgeo` version key is being removed.** `envelope.py:13` defines
`ATGEO_VERSION = 1` and writes it into every tile payload (`:52`), into
`manifest.json` (`:74`), and into the `manifest.duckdb` metadata table
(`stages.py:632-633`). It originated in `atgeo-appview-sdk-design.md` §1.2 as a
proposed addition to the pipeline format. Removal touches those four write
sites and roughly eight test assertions (`test_envelope.py:57, 216, 227, 307,
319-325, 335`; `test_stages.py:202, 277-284`) which currently assert the
five-key set. Nothing reads it.

---

## Open inconsistencies

Unlike the above, these are not decided.

**Emitted records do not validate against `place.json`, and nothing checks
that they do.** There is no lexicon-conformance harness in `tests/`; the shape
tests hardcode expected keys independently of the lexicon file. They already
disagree in both directions. Emitted but undeclared: `importance`, `$type`,
`collection`. Declared but never produced: `same_as`, `published_at`,
`relation.name`, and the entire `#ref` def.

**`org.atgeo.place#ref` is unreferenced.** No code in `garganorn/` uses it. It
is the mechanism the SDK design's check-in write path depends on, so it is
declared ahead of a consumer that does not exist yet.

**`getRecord.json:27-31` declares a `cid` parameter the method does not
accept.** lexrpc passes unknown query parameters through as keyword arguments,
so a request carrying `?cid=` reaches a method that has no such argument.

**`config.yaml.example:9` sets `tiles.max_per_tile`, which nothing reads.**
Only the `pipeline:` section is loaded (`quadtree.py:141-145`); the effective
key is `pipeline.max_per_tile` at `config.yaml.example:40`.

**Antimeridian handling is implemented but untested.** The wrap branches in
`bboxes_intersect` (`stages.py:687-707`) and the two-lobe split in
`bbox_to_quadkeys` (`covering.py:68-78`) have no test coverage.

---

## Serving layout

Informative — how a deployment is arranged, not a contract a client depends on.

Within a run directory, a tile file is at `{qk[:6]}/{qk}.json.gz`
(`stages.py:1646-1650`) — the first six quadkey characters are a directory, the
full quadkey names the file. Quadkeys shorter than six characters do not occur,
since assignment starts at zoom 6. Clients never see this structure; they get
whole URLs from `getCoverage`.

Each collection's tiles live under `{output_dir}/{source}/tiles/`, with one
directory per pipeline run named for its UTC timestamp, and a `current`
symlink pointing at the newest complete run (`quadtree.py:34`,
`stages.py:1554-1557`, `:1699-1707`). A collection's configured `base_url`
resolves inside that symlink and must end with the collection's slug, which
`create_app` enforces at startup (`__main__.py:32-35`).

`manifest.json` is written last in a run and serves as its completeness
marker: the server refuses to serve a collection whose run lacks one
(`__main__.py:40-49`), the next run deletes run directories without one
(`stages.py:1530-1545`), and the freshness gate keys on its mtime
(`stages.py:1517-1528`). Its contents are not read by anything. Two complete
runs are retained (`stages.py:1709-1719`).
