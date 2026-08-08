# The ATGeo spatial AppView

A sidecar that consumes the ATProto firehose and serves live location records
as ATGeo tiles. Same tile format and same XRPC methods as the batch gazetteer,
so a client cannot tell which kind of producer it is talking to.

Nothing here is built. This is the design; `atgeo-spec.md` is normative for
anything on the wire, and where the two disagree, the spec wins.

It was extracted from an older document that braided a protocol spec, a client
SDK design, and this sidecar into one. The protocol half is now
`atgeo-spec.md`, the SDK half is `atgeo-client-sdk.md`, and what survived of
the rest is here.

## What it is for

The gazetteer answers "what is at this place" from imported reference data
rebuilt on a monthly cadence. It has no answer for "who checked in here an
hour ago," and it shouldn't — a batch pipeline is the wrong machine for
records that appear and vanish in seconds.

The AppView is that machine. It watches repos, indexes the location records it
finds, and serves them through the same interface. A co-op running a PDS can
run one next to it and have its members' check-ins queryable by any ATGeo
client, with no coordination with anyone else.

Two properties are load-bearing:

**It holds no canonical state.** Everything in the index is derived from
records that live in their authors' repos. The index can be dropped and
rebuilt from a firehose replay or from repo CARs, and rebuilding is tested,
not assumed. This is what licenses the claim that running one is a low-stakes
decision, and it mirrors the pipeline's reproducibility.

**Tombstones work.** A deleted check-in disappears from the next tile flush.
This is the structural difference from gazetteer data, where records only
change when a new release is built, and it is the thing most likely to be
quietly broken by an optimization.

Non-goals, stated because they get proposed:

- Private location data. ATProto repos are public and published locations are
  public. The AppView mitigates nothing about that; precision is the author's
  choice, made at write time, in a client.
- Routing, geocoding of freeform addresses, map rendering.
- Moderation and labeling. Compose with existing ATProto labeling. Nothing
  here conflicts with it and nothing here implements it, beyond the
  minimum abuse controls below.

## Discovery

A client should find tile services through ATProto identity rather than
configuration. Two mechanisms, and they do different jobs.

**A DID document service entry** on the operator's DID says where:

```json
{ "id": "#atgeo_tiles",
  "type": "AtgeoTileService",
  "serviceEndpoint": "https://tiles.example.org" }
```

**Announcement records** of type `org.atgeo.tiles.service` in the operator's
repo, one per served collection, say what:
`{collection, baseUrl, kind: "gazetteer" | "live"}`. They are enumerable via
`com.atproto.repo.listRecords`, so a client holding only a DID can discover
everything an operator serves.

The division of labour keeps the `at://` namespace honest. A DID document may
declare a non-PDS service freely, without implying repository hosting. The
announcement records live in a real, account-sized repo on a real PDS and
therefore legitimately carry `at://` URIs. Identity anchors the service; bulk
data stays out of the `at://` namespace.

The draft lexicon is `org.atgeo.tiles.service.json`, in this directory. This
overlaps with the collection-and-service-metadata proposal in
`planned-features.md`; whichever is designed first should absorb the other
rather than both shipping.

## Upstream and filtering

Config selects the firehose source:

- **`pds`** — `com.atproto.sync.subscribeRepos` against a single PDS. Co-op
  scale, and the intended first deployment.
- **`relay`** — the same endpoint against a relay, for network scale. Recorded
  but not solved: the firehose has no server-side collection filter, so relay
  scale means drinking the entire stream to keep a trickle. That is a
  bandwidth commitment, not a compute one.
- **`jetstream`** — a filtered JSON feed with `wantedCollections`. Cheap, but
  it inserts a trusted filter operator and gives up the signed-commit
  verification path. Acceptable per deployment, explicitly.

Watched collections are a config list of NSIDs. For each, config names which
lexicon fields carry location objects, as a list of paths, so a new
check-in-shaped lexicon is onboarded by editing config rather than code.

## Index

**SQLite, not DuckDB.** The workload is single-row upserts and deletes at
firehose rate, which is precisely what DuckDB's columnar engine is worst at —
D3 in `design-constraints.md`: every mutation is a table rewrite — and
precisely what SQLite is built for. DuckDB stays the batch tool. SQLite is the
streaming tool.

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

Deriving `qk17` depends on the location type: `geo` is direct; `hthree` uses
the H3 cell centroid, with the record keeping the cell and the centroid being
index-only; a record carrying only an `address` is indexed only if it has a
`geo` or `hthree` sibling, because geocoding is a non-goal. Records that fail
lexicon validation are counted and skipped, never partially indexed.

## Event handling

- **`#commit` create/update** — validate, derive `qk17`, assign a tile, upsert
  the row, and mark both the old and new tile dirty. They differ only if the
  record moved.
- **`#commit` delete** — remove the row, mark the tile dirty.
- **`#account`** (deactivated, deleted, taken down) — delete every row for the
  DID and mark the affected tiles dirty. Takedown state should also gate
  backfill.
- **`#identity`** — no-op. The index keys on DID; handles never enter it.
- **Cursor** — persist `seq` in the same transaction as the batch that
  consumed it, and resume from it on restart. A cursor gap beyond the
  upstream's replay window triggers a rebuild.

## Tile assignment

The batch pipeline picks each record's zoom globally: the coarsest tile
holding no more than `max_per_tile` records. Streaming cannot do a global
assignment, so the AppView uses a monotone rule instead. A record lands in the
tile at the collection's current zoom for its quadkey prefix. When a tile
exceeds `max_per_tile`, the flush loop splits it — reassigning its records one
zoom deeper, writing the four children, and dropping the parent from the
coverage set.

Tiles never merge. This is a deliberate one-way ratchet on the theory that
live datasets grow, and it is also an abuse surface, addressed below. A split
is an index-and-flush operation over roughly `max_per_tile` rows, inside one
transaction.

## Serving

**The AppView serves `org.atgeo.getCoverage`, not a manifest.** This is the
one place the extracted design has been changed rather than trimmed. The
original assumed clients hold a manifest and intersect it locally; the
gazetteer does not distribute one and the SDK is built on `getCoverage`, so an
AppView serving only a manifest could not be talked to by any ATGeo client.
Same lexicon, same errors, same coarse-bbox privacy floor.

Every `flush_interval` — default 15 seconds — the loop drains `dirty_tiles`,
rewrites each tile from the index (`SELECT ... WHERE collection = ? AND
tile_qk = ? ORDER BY rkey`, in the envelope `atgeo-spec.md` describes),
writes to a temp file and renames, and updates the coverage set if it changed.
Write amplification is one gzip of roughly 100 KB per dirty tile per interval,
which is negligible at any plausible check-in rate.

Eventual consistency of one flush interval is the freshness contract.

### Keeping tile URLs immutable

`atgeo-spec.md` states that a tile URL never serves two different byte
streams. The gazetteer earns this with run-stamped paths and keep-two
retention. An AppView rewriting a tile every fifteen seconds cannot stamp
whole runs, but it does not have to: `getCoverage` returns whole opaque URLs,
so each tile can carry its own stamp from the flush that last wrote it, and
tiles of different ages can coexist in one coverage response. Old versions are
retired after a short retention window.

This keeps the immutability invariant intact across both producer types, which
matters more than it looks — it is what lets one SDK cache policy work against
both. `getCoverage` responses themselves are short-lived, with `max-age` at
the flush interval rather than the gazetteer's hour.

*This resolution is new here and has not been reviewed.* The alternative is
amending the spec so immutability becomes a per-producer property that clients
must discover, which costs more than stamping does.

A deployment also publishes its announcement records, and its service entry if
it controls the DID. Identity is part of deployment, and it is what makes "run
a geo sidecar next to your PDS" discoverable rather than bilateral.

### Record provenance

AppView records differ from gazetteer records in exactly one way, and it is
the interesting one: they carry a genuine `at://` URI and a commit-verified
CID, so a client can fetch and verify the record at its origin PDS. Gazetteer
records carry an `https://` URI and a null CID because no signed commit
includes them, and minting an `at://` URI they cannot resolve would promise a
verifiability they cannot deliver.

Uniformity is in processing, not provenance. A client that wants to know
whether a record is repository-backed can tell from the URI scheme, which is a
signal it deserves rather than a leak to smooth over.

One conflict to resolve before this ships: `org.atgeo.tilePayload` types `cid`
as nullable but describes it as "always null; tile records have no computed
CID." That is true of the gazetteer and false of an AppView. The type is
already right; only the description needs widening.

## Rebuild and backfill

`--rebuild` drops the index and either replays the firehose from cursor zero
where the upstream retains that history, or enumerates repos and fetches CARs
via `com.atproto.sync.getRepo`, indexes the matching records, and then tails
from the current cursor.

Rebuildability is an invariant, not a feature. A CI test should rebuild from a
fixture CAR set and byte-compare the resulting tiles.

## Abuse resistance

The split ratchet is a denial-of-service surface. Tiles never merge, so one
account writing thousands of records into a single zoom-17 cell forces
permanent subdivision and bloats the tileset for everyone. Three mitigations,
all config, all enforced at index time:

- **A per-DID record cap** per collection and tile, defaulting to 100.
  Over-cap creates are dropped and counted rather than indexed, and the
  counter is a metric, so abuse is visible.
- **The watched-collections config, which doubles as the primary gate.** The
  AppView only ever indexes explicitly configured lexicons, so spam via a new
  collection requires operator action before it matters.
- **A DID denylist**, applied at event time and retroactively. Adding a DID
  deletes its rows and dirties the affected tiles, down the same path as
  `#account` deletion.

Deeper moderation — label-based filtering, community allowlists, reputation —
is left as a knob rather than designed here: the event handler exposes a
single `admit(event) -> bool` hook where such policies compose. The
user-generated-places moderation problem is real and unsolved, and these caps
are the minimum that keeps an unsolved policy problem from becoming an
infrastructure problem.

## Implementation notes

TypeScript on Node. `@atproto/sync` and `@atproto/repo` give firehose
consumption, CAR parsing, and commit verification off the shelf, and the
AppView can share envelope fixtures with the reference SDK. Python would match
Garganorn but would mean reimplementing firehose plumbing, which is not worth
it.

Footprint target: idles in tens of megabytes, deployable as a single container
with one volume for SQLite and tiles. This is deliberately a thing a co-op
member could run, and it is a design constraint rather than an aspiration —
anything that pushes it toward a cluster has broken the premise.

Metrics: events consumed, records indexed, records skipped as invalid, dirty
flushes, tombstones applied, cursor lag. Log counters; no APM dependency.

## Open questions

- **The check-in record lexicon.** Whether to draft `org.atgeo.checkin` or
  adopt a lexicon.community proposal. The AppView is lexicon-agnostic by
  configuration, so this does not block it, but nothing can be demonstrated
  without a concrete answer. Worth raising in the ATGeo working group before
  minting one unilaterally.
- **Relay-scale filtering.** Revisit if a network-scale deployment is actually
  wanted. Until then the PDS and Jetstream modes cover real use.
- **Tile stamping under a fifteen-second flush.** The retention window, and
  whether per-tile stamps churn CDN cache keys faster than they help.
- **Whether the coverage index needs its own storage.** The gazetteer keeps
  one in DuckDB alongside the tiles; the AppView could serve `getCoverage`
  straight from the SQLite index, which is one fewer artifact to keep
  consistent but puts a read path in front of the write path.
