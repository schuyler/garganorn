# Place Identity and Contract Redesign

A design to remove garganorn's `com.atproto.repo.getRecord`, give every
gazetteer place a portable identity (`urn:atgeo:{dataset}:{collection}:{id}`),
and let a tileset carry many `(dataset, collection)` pairs.

**Status:** design, decisions taken, nothing implemented. Baseline: garganorn
`1e84549`, atgeo-sdk-js `b1cf173` (local only, no remote), atgeo-web
untouched. Every question the proposal left open is resolved below, with the
reasoning; "Resolved questions" indexes them.

**What a reviewer should do:** attack the design. Look for contradictions
between sections, requirements the design does not meet, places where a
decision rests on a fact that is not established (see "Not verified"), and
cases the identity scheme cannot express.

## Why

`garganorn/server.py` registers one handler, `get_record`, under
`com.atproto.repo.getRecord`. It branches on the `collection` string over
three unrelated things:

1. place records (tile collections): `{uri, source, license, importance?, value, _query}`, HTTP(S) `uri`
2. collection metadata (private pseudo-collection `org.atgeo.collection`): `{uri, source, license, value}`
3. lexicon schemas (`com.atproto.lexicon.schema`): `{uri, value}`, `at://` uri, no attribution

`garganorn/lexicon/getRecord.json` reuses the protocol-owned NSID with a
stricter contract than the canonical lexicon (it requires `source` and
`license`, adds `importance`, loosens `uri` to `format: uri`). The schema
branch's output is valid against the canonical lexicon and invalid against
garganorn's fork, so lexrpc output validation rejects it. Confirmed over
HTTP against a local server:

```
GET /xrpc/com.atproto.repo.getRecord?repo=did:web:localhost&collection=com.atproto.lexicon.schema&rkey=org.atgeo.getCoverage
-> 400 InvalidRequest: ... output with value {'uri': 'at://did:web:localhost/com.atproto.lexico…': missing required property source
```

The bug is a symptom. Garganorn instances are not atproto repos: there are no
commits, no signing keys, and `did.json` has no verification method. Standard
lexicon resolution (`@atproto/lexicon-resolver`) calls
`com.atproto.sync.getRecord`, verifies a commit signature, and checks an MST
proof, so the schema branch can never serve a real resolver. Lexicon
authority belongs to the NSID's DNS domain (`_lexicon.<authority>` TXT), not
to each server.

Separately, the product docs (`atgeo-web/src/usage.md`, `faq.md`; `usage.md`
and `faq.md` are definitive, `api.md` is stale) say two things the current
contract does not deliver:

- lookup output "can be incorporated directly into AT Protocol records without modification" (`usage.md:40`);
- every reference place traces to its origin dataset through a globally unique key.

Users should be able to write `org.atgeo.place` records into their own
repos. The place value therefore has to be a valid record, identify its
origin without a server, and contain nothing relative to one producer's
build.

## Requirements

Tags: S stated by the owner, M from existing docs or memory, I inferred
(vetoable).

- **R1 (S)** Users can write `org.atgeo.place` records into their own repos; gazetteer place values embed unmodified. AC: a tile record's value and a place-fetch record's value each validate as an `org.atgeo.place` record with an atproto-valid key type.
- **R2 (S)** A place value is usable if its origin server goes away.
  - **R2a** Provenance: the value carries an `origin` URI whose dataset segment plainly names the upstream source. Licence and attribution travel with the *tile* (see "Attribution"), not with each record. AC: every gazetteer value has an `origin` URN; every tile links the licence and attribution of every `(dataset, collection)` it contains.
  - **R2b (M)** Two places from different datasets or collections with the same upstream ID are distinguishable by value alone. AC: `origin` differs.
  - **R2c** No producer- or build-relative data in the place. `importance` is not part of the place. AC: no `importance` in the place lexicon or in exported values.
- **R3 (I)** `importance` remains available per record in tiles and from place-fetch, outside the value, so client ranking and the summary band are unchanged. AC: SDK ranking tests pass reading importance from outside the value.
- **R4 (S)** Garganorn implements no `com.atproto.*` method and serves no lexicon claiming a `com.atproto.*` NSID. AC: `/xrpc/com.atproto.repo.getRecord` returns method-not-found; no `com.atproto.*` file in `garganorn/lexicon/`.
- **R5 (S)** Single-place fetch is an `org.atgeo.*` method whose lexicon requires the view fields; no DID is needed first. AC: the call passes lexrpc output validation with no `repo` param; SDK `getPlace` makes no `describeService` call.
- **R6 (S)** Metadata is served by validated XRPC queries; the tileset descriptor also has a plain-GET alias whose path cannot be mistaken for a place path. AC: query and alias return the same document; XRPC output validates.
- **R7 (M)** A place `uri` returned by place-fetch is dereferenceable on the serving host. Tiles bake in no gazetteer base URL. AC: tile records carry no `uri`; place-fetch builds `uri` from the server's `public_base`.
- **R8 (S)** `did.json` stops advertising `#atproto_pds` and advertises `#atgeo_tiles` / `AtgeoTileService` as specified in `atgeo-appview-design.md`.
- **R9 (S)** Every `org.atgeo.*` lexicon passes `goat lex lint` so it can be published under the `atgeo.org` authority. AC: lint passes. This task writes the publication runbook (`_lexicon.atgeo.org` TXT record, `goat lex publish` and `goat lex check-dns`); the owner runs it separately. `community.lexicon.location.*` are not published by atgeo.
- **R10 (I)** Every garganorn XRPC success path is tested through lexrpc output validation, not by calling handlers directly. The SDK's fake server enforces the same shapes.
- **R11 (M)** The SDK's real-server e2e (Node and browser) passes against a locally rebuilt tileset.
- **R12 (I)** Docs describe the result: garganorn `atgeo-spec.md`, `atgeo-client-sdk.md`, `explored-and-discarded.md`, `design-constraints.md` (licensing), `README.md`; SDK `README.md`; atgeo-web `place.md` and `api.md`.
- **R13 (M)** No backward compatibility, migration or deprecation period. Nothing is deployed to users.
- **R14 (S)** The SDK's default search spans a whole dataset; a collection filter is optional. Nothing else makes sense for OSM, whose node, way and relation collections are one tileset and one body of data. AC: a search with no collection filter returns records from every collection the dataset's tiles carry.
- **Out of scope (I):** the check-in lexicon, AppView, `org.atgeo.tiles.service` announcement records, honouring `cid`, and any dedup relation beyond keeping the existing `same_as` definition (see "Identity").

Several items above are decisions in a requirement's clothing (R2a's
field, R7's `public_base` mechanism, R8, R9's runbook, R12). They stay here
so the record is complete; the requirements gate of each tranche strips
them to outcomes and reattaches the mechanisms as acceptance criteria, per
`.claude/CLAUDE.md`.

## Current state

Paths are under `garganorn/` unless noted. File and line references come
from a read-only survey; the items marked ✓ were independently checked
against the baseline.

- Three source classes in `database.py:103-127`: OverturePlaces (`org.atgeo.places.overture.place`, key `overture_place`, pk `id`), OpenStreetMap (`org.atgeo.places.osm`, key `osm`, pk `rkey`), OvertureDivisions (`org.atgeo.places.overture.division`, key `overture_division`, pk `id`). Each carries `source_url`, `license_url`, `attribution`. Registered in `stages.py:32-35` and `quadtree.py:36`. No "dataset" concept exists in code.
- Each source is one pipeline and one tileset: `output/<source_key>/tiles/<timestamp>/` with `<qk[:6]>/<qk>.json.gz` tiles, `manifest.duckdb` (`record_tiles(rkey, tile_qk)`, `metadata(source, collection, generated_at)`), `collection.json`, and `manifest.json` written last. The server loads one `TileManifest` per collection (`__main__.py:25-53`); config is `tiles.collections.<NSID>: {slug, manifest, tiles_dir, base_url, source, license}`. Tiles are served at `/tiles/<slug>/<path>`.
- OSM rkeys: the import creates `rkey = osm_type || osm_id` (`n123`); the export SQL rewrites it to `node:123`, `way:123`, `relation:123` (`sql/osm_export_tiles.sql:9-22`, duplicated in `stages.py:786-794`). That rewrite is the only place an rkey is taken apart.
- Overture rkeys are GERS ids, unchanged (`rkey = p.id`). Places and divisions are separate imports, containment computations and tilesets. Division records span multiple tiles; `record_tiles` can hold several rows per rkey. Divisions are the `within` target for every source.
- ✓ `relations.within` entries are objects `{rkey, name, level}` (lexicon def `relation`, `rkey` required with `format: record-key`); the exporters build `rkey` as `'${collection_prefix}:' || boundary_id` (`sql/compute_containment.sql:41`, `sql/division_containment.sql:41`), default prefix `org.atgeo.places.overture.division`. Nothing parses them back. The lexicon also defines `relations.same_as` with the same `relation` shape; no exporter emits it.
- ✓ Tile payload (`envelope.py`, `lexicon/tilePayload.json`): top-level `collection, source, license, generated_at, records`. `wrappedRecord` is `{uri, cid: null, value}` with `cid` nullable. The `uri` is `{base}/{collection}/{rkey}` with `REPO = "places.atgeo.org"` hardcoded (`stages.py:37`), whereas `server.record_uri` uses the server's `public_base`.
- `getCoverage` takes a required `collection` (format nsid), looks up one manifest, and returns `{tiles: [urls], summary?}`. `describeService` returns `{did, collections: [{collection, metadata?}]}` where `metadata` is `{base}/org.atgeo.collection/{nsid}`.
- `org.atgeo.collection` metadata has `key: "nsid"` and requires `collection, source, license, generatedAt, recordCount, extent, locationTypes`; `attribution`, `containmentLevels`, `categories`, `attributes` are optional.
- ✓ `lexicon/place.json` types the key `record-key`, which is not a valid lexicon key type (valid: `tid`, `nsid`, `literal:*`, `any`). The value has an `rkey` property (`format: record-key`) duplicating the record's key. `importance` is a place property, hoisted out of `value` by `getRecord` and merged back by the SDK; tiles leave it in. The lexicon has no `collection` property, although `place.md` and `usage.md` call `collection` + `rkey` the global id.
- ✓ SDK: `collection` is a plain string with no multi-collection search (default `org.atgeo.places.overture.place`, `src/client.ts:17`). It is used in cache keys, `getCoverage` params, `getPlace`, `getCollectionInfo`. `cache.ts` keys tiles by URL and looks records up with `find(collection, rkey)` on `value.rkey`; `merge.ts` dedupes on `value.rkey` alone; `rank.ts` tie-breaks on `uri`. The SDK never parses rkeys or URIs.
- ✓ Garganorn's unit tests call the server's `get_record()` handler directly at 15 sites, all in `test_server.py`, and so skip lexrpc output validation, which is how the lexicon mismatch went unnoticed. `test_tile_reader.py` and `test_audit_export.py` call `TileBackedCollection.get_record()`, the tile reader's method of the same name, which this design keeps. The SDK fake's `getRecord` omits `source` and `license`.
- ✓ lexrpc's `format: uri` validator requires a scheme and some content after it and nothing else, so a URN validates. `goat` is installed with `lex lint`, `lex parse`, `lex publish` and `lex check-dns`.

## Design

### Identity

A gazetteer place is identified by three parts:

- **dataset**: the upstream publisher's domain, lowercase and without a host prefix: `openstreetmap.org`, `overturemaps.org`. The exact string for each dataset is fixed in its tileset descriptor and the spec, so `www.openstreetmap.org` never appears.
- **collection**: a dataset-defined sub-division. OSM: `node`, `way`, `relation`. Overture: `place`, `division`, and the other themes as they are added. The ATGeo working group is the registrar of these names; they appear as data in tileset descriptors and as a list in the spec, and nowhere else (see "Resolved questions", Q6).
- **id**: the upstream's own identifier within `(dataset, collection)`: an OSM numeric id, an Overture GERS id.

Canonical form, an ATGeo URN:

```
urn:atgeo:{dataset}:{collection}:{id}
urn:atgeo:openstreetmap.org:way:123456
urn:atgeo:overturemaps.org:division:08f2a1...
```

The dataset and collection segments contain no colons; `id` is the remainder
after the third colon. `urn:atgeo:` means "ATGeo's mapping of this upstream's
records", not a claim of upstream endorsement. The `atgeo` namespace is not
registered with IANA, so the spec must not claim RFC 8141 conformance;
registration is deferred.

Dataset names are domains because the alternatives lose something. NSID names
(`org.atgeo.places.osm`) only mattered while garganorn collections had to be
atproto collection NSIDs. A flat registry slug (`openstreetmap`) makes ATGeo a
naming authority and needs a guess at each upstream's preferred name
(Overture calls itself "Overture Maps", not "Overture"). A domain names the
source plainly, needs no registry, and lets another service name a new
upstream the same way. The cost is that identity is tied to a domain string
that the upstream could someday abandon; the URN stays a name, not a
location, so it survives that.

**What the URN is for.** AT-URIs identify a record held in a repo. Upstream
records are not ours and there are far too many to place in PDSes, so a
synthetic AT-URI would claim a repo that does not exist. An upstream's own
URL (`https://www.openstreetmap.org/way/123456`) is not ours to define and has
multiple spellings. A permalink resolver on `atgeo.org` would make one domain
the resolver for every record, which rules out federated gazetteer services as
peers. A URN is a name only, independent of any server; a client finds a
record by asking the gazetteer services it knows.

**Who parses a URN.** The URN's structure is normative for datasets an
ATGeo gazetteer serves. Clients construct URNs from parts they hold and
compare them as strings, including matching on a constructed prefix (every
`way` in `openstreetmap.org` shares the prefix
`urn:atgeo:openstreetmap.org:way:`). Only a gazetteer serving the dataset
decomposes a URN, to resolve it. This is why `getPlace` takes the URN whole
rather than three parts, and why the SDK filters a dataset's tiles by
collection with a prefix test rather than by splitting each `origin`.

**Upstream ids are only as stable as the upstream makes them.** Overture
replaced every GERS id with a UUID in release `2025-06-25.0` and published no
changelog for that release; ids captured before it do not resolve against
current data. Later releases ship a per-feature changelog. A URN therefore
names an id within an upstream release, and the tileset descriptor records
which release each pair came from (see "Tilesets"). The URN itself does not
carry the release: a name that changed with every extract would be useless
as an identity, and two services serving different extracts of one dataset
are distinguished by their descriptors, not by the names of their records.

**Community gazetteers** that hold records in repos should use AT-URIs for
those records. A community that republishes OSM places into a PDS keeps the
URN as the origin of the copy and gets an AT-URI for the copy itself.

### The `origin` field and references

- `origin` is a new optional property on `org.atgeo.place`, `format: uri`.
- Gazetteer-exported values always carry a URN origin. Because the property is optional in the lexicon, output validation cannot enforce this; the export tests assert it (R2a's acceptance criterion).
- A user-authored place has no `origin`. Absence means "this record is its own origin", not "unknown". Its identity is its `at://` URI.
- A user's copy of a gazetteer place keeps the dataset URN as origin; the copier's DID does not become the origin. An edited copy keeps it too: `origin` names what the record describes, not an assertion that the content is unchanged.
- A reference to a place is the `relation` object `{uri, name, level}`, `uri` required with `format: uri`, `name` and `level` unchanged from today. The same definition serves `relations.within` and `relations.same_as`. A `uri` is any valid URI. Producer preference: an ATGeo URN (where ATGeo defines one for the source), then an AT-URI, then any other URI. The lexicon cannot enforce the preference; the spec states it.
- `same_as` stays in the lexicon for future expansion. No exporter emits it and no SDK code reads it. It exists so that the relation a community gazetteer would use to record that two URIs name one entity is already defined with the shape every other reference uses.
- Consumers compare references and origins as exact strings and never normalize. The same entity may appear under several URIs (`urn:atgeo:openstreetmap.org:way:123456` and `https://www.openstreetmap.org/way/123456`); recording that equivalence is `same_as`'s job, for community gazetteers, not this task's.

### Record keys

- The `org.atgeo.place` lexicon uses `key: "any"`. A TID would require minting timestamps and would discard the upstream id; `any` lets a user key a copy however they like.
- The value's `rkey` property is removed. It duplicated the record's key, and once users may re-key a copy it would go stale on copy, which breaks R1's "unmodified" and R2c. `origin` is its replacement.
- Tile records carry no rkey at all (and no `uri`, see R7). A copier chooses the key; the gazetteer's choice is not visible to clients and not part of the contract.
- Inside garganorn, the manifest's `record_tiles` keys on `{dataset}:{collection}:{id}`, the URN without its prefix. Because the collection is in the key, OSM node 5 and way 5 do not collide and the manifest needs no extra column. Every character is legal in an `any` rkey (`[A-Za-z0-9._:~-]`, 1 to 512 characters, not `.` or `..`, per the atproto record-key spec at atproto.com/specs/record-key) provided real upstream ids stay in that set; the first tranche scans the real ids once at export time and records the result. No permanent assertion: OSM ids are integers and GERS ids are UUIDs, and a guard for a condition that cannot occur is machinery nothing reaches.
- Two copies of the same upstream place under different rkeys are both valid. Deduplication groups on `origin`.

### Tilesets and the tileset descriptor

A tileset is one pipeline's output. It relates many-to-many to
`(dataset, collection)` pairs:

- OSM: one tileset carries `(openstreetmap.org, node|way|relation)`.
- Overture: one tileset per theme: `(overturemaps.org, division)` and `(overturemaps.org, place)`. Places and divisions come from separate pipelines with different tile characteristics (divisions are multi-tile and the `within` target for everything else), so merging them would merge pipelines for no gain.
- A collection could also span several tilesets (regional shards); nothing in the model forbids it.

A **tileset descriptor** replaces today's `collection.json`:

```
{slug, generated_at, extent, pairs: [pair, ...]}
pair: {dataset, collection, release, record_count, location_types,
       source, license, attribution,
       containment_levels?, categories?, attributes?}
```

`release` is the upstream's own version string: an Overture release id
(`YYYY-MM-DD.N`) or an OSM planet timestamp. It is there because Overture's
ids have churned between releases, so two services answering one URN
differently need a way to show why. `source`, `license` and `attribution`
are URLs. The optional vocabularies are the ones `org.atgeo.collection`
carries today, moved per pair. Exact property names are settled in the first
tranche's design phase; the invariant is that the descriptor says which
pairs the tileset carries and under which upstream release.

The **tile header** can no longer carry a single `collection`. It carries
the tile's generation time, the record array, and an `attribution` list:
one `pair` object per `(dataset, collection)` present in the tile, minus the
counts and vocabularies. One lexicon definition serves both the descriptor
and the header, so the two cannot drift.

Each tile record is `{value, importance}`. There is no `uri` (R7): identity
is `value.origin`, and a dereferenceable URL is per server, built by
place-fetch from its own `public_base`. There is no `cid`: it was always
null, nothing read it, and it only made sense while the tile record imitated
an atproto record view, which without `uri` it no longer does.

Every copy of a multi-tile record must remain byte-identical (a current
invariant in `design-constraints.md`). Record bytes are unchanged by the
header; only headers differ between tiles. `importance` is per record and
identical in every copy, so moving it outside the value preserves the
invariant.

### Attribution and licensing in tiles

Each tile carries, for every `(dataset, collection)` present in that tile,
one entry with the source URL, the licence URL (the licence text itself, for
example the ODbL text) and an attribution URL. An OSM tile's entry links
`https://www.openstreetmap.org/copyright` as the attribution. Entries are
keyed by `(dataset, collection)` because licences vary inside a dataset.

**Overture's attribution page** (`docs.overturemaps.org/attribution`, raw
HTML read 2026-10-13) lists sources per theme as "Data from X. Available
under Y". Divisions: ODbL, "© OpenStreetMap contributors", plus geoBoundaries,
Esri Community Maps and LINZ under CC BY 4.0. Places: Meta, Microsoft,
PinMeTo, Krick, RenderSEO, DAC and BrightQuery under CDLA Permissive 2.0;
Foursquare "Copyright 2024 Foursquare Labs, Inc. All rights reserved",
Apache 2.0, with a note that the data was transformed to the Overture schema
and a link to a NOTICE.txt; AllThePlaces under CC0 1.0. The page states no
per-record requirement for any source. The Overture pairs therefore point
their `attribution` URL at that page, which carries the Foursquare notice
and the NOTICE link, and tile records for places carry no attribution
field. Overture place records' own `sources` array, which the export already
passes through in `attributes`, stays as-is under the data-as-is rule.

**ODbL 1.0** (raw text read from opendatacommons.org). The notice duties
attach to the database conveyed, not to individual records:

- §1.0 "Convey" means enabling a person to make or receive copies; it "does not include interaction with a user through a computer network… where no transfer of a copy… occurs". Tiles transfer copies.
- §1.0 "Substantial": "substantial in terms of quantity or quality or a combination of both. The repeated and systematic Extraction or Re-utilisation of insubstantial parts of the Contents may amount to the Extraction or Re-utilisation of a Substantial part."
- §4.2: on Publicly Conveying the database, a derivative, or the database in a collective database, include "a copy of this License… or its Uniform Resource Identifier (URI) with the Database or Derivative Database"; keep intact copyright, Database Right and licence notices; "If it is not possible to put the required notices in a particular file due to its structure, then You must include the notices in a location (such as a relevant directory) where users would be likely to look for it."
- §4.3: a publicly used Produced Work needs a notice "reasonably calculated to make any Person… aware that Content was obtained from the Database… and that it is available under this License"; the example is "Contains information from DATABASE NAME, which is made available here under the Open Database License (ODbL)", with hyperlinks to the database and to the licence text.
- §4.4: a publicly used Derivative Database must be under the ODbL, a later similar version, or a compatible licence; §4.5 limits share-alike for Collective Databases and Produced Works.

**OSM attribution guidelines** (osmfoundation.org, fetched through a
summarizing tool, so quotes are not guaranteed verbatim): for databases,
attribution to "OpenStreetMap" plus the ODbL text or a link must be part of
the database, "within the data or metadata" is an accepted location, and the
attribution should make clear the data is under the ODbL, for example by
linking "OpenStreetMap" to openstreetmap.org/copyright. The guidelines
contain no rule for a lone record separated from its database; the only
size threshold they state (under 10,000 m² or fewer than 100 features) is for
static images.

**Design position.** The tile header is "within the data or metadata" and
carries the licence URI for everything it contains. A record copied out of a
tile into a user's repo keeps `origin`, whose dataset segment plainly names
the source, but no licence link. The reasoning: the ODbL's duties attach to
conveying a substantial part of the database. The tile is garganorn's
conveyance and carries the notices. A user's repo is the user's conveyance,
and the user's duties are theirs. Nothing is stamped into copies, and
`usage.md:40`'s "without modification" holds literally. The owner holds this
position; neither the licence nor the guidelines say so directly, it is not
legal advice, and it should be re-examined if it matters to a deployment.

### Server API

All provisional names. All methods are `org.atgeo.*` queries with lexicons
that lexrpc validates on output.

- **Remove** `com.atproto.repo.getRecord` and its lexicon. Lexicon schemas are served by `GET /{nsid}` (already supported) or not at all; the repo-style schema branch goes.
- **`org.atgeo.getPlace`** takes one parameter, `uri`, the URN, and returns a `placeView` `{uri, importance?, record}` where `record` validates as `org.atgeo.place`, `uri` is dereferenceable on this server, and importance is outside the record. One parameter rather than three because a client's natural input is the `origin` it already holds, and three typed parameters would force every client to decompose a URN, which only the serving gazetteer may do. A client that holds the parts builds the URN, which is construction, not parsing.
- **`getCoverage`** takes `dataset` plus an optional `collection` and resolves, through the tileset descriptors, to the tilesets carrying that pair; the output is still a flat list of tile URLs plus the optional `summary` flag. Tiles can contain records the caller did not ask for, so the SDK filters by collection client-side with a prefix test on `origin`, and only when a filter was asked for (R14).
- **`describeTileset`** takes a tileset `slug` and returns its descriptor.
- **`describeService`** advertises the instance's `did` and its tilesets, each with the `(dataset, collection)` pairs it carries. This is the discovery mechanism for federation: a client holding a URN asks each service it knows whether it serves that pair. There is no separate dataset query; a dataset view is derivable from these two documents, and a third lexicon would buy nothing.
- **Plain-GET aliases.** The tileset descriptor is also served at `/tilesets/{slug}`. The three-segment place alias `{base}/{dataset}/{collection}/{id}` replaces today's two-segment `/{collection}/{rkey}` route for single-place dereferencing; its route requires a dot in the first segment, since a dataset is a domain, so any dotless first segment is free for aliases and the two cannot collide. The service document needs no alias: `did.json` and the XRPC query cover it.
- **`did.json`** drops `#atproto_pds` and advertises `#atgeo_tiles` / `AtgeoTileService` per `atgeo-appview-design.md`.
- `format: nsid` constraints on dataset/collection params and the `collection.json` key go away (dataset is a domain string; collection is a short string).

### Place lexicon

`org.atgeo.place` changes: `key: any`; add optional `origin` (`format: uri`);
remove `importance` and `rkey`; `relation` becomes `{uri, name, level}` with
`uri` required; `same_as` stays; the description states the absence rule and
the URI preference order. `org.atgeo.collection` as a record type goes; its
content becomes the `pair` object definition shared by the descriptor and
the tile header.

### Export SQL

The three `*_export_tiles.sql` files change: emit `origin` as a URN, emit no
`uri` and no `rkey` in the tile record, move `importance` outside the value,
and write the `{dataset}:{collection}:{id}` manifest key. The `within`
construction in `compute_containment.sql` and `division_containment.sql`
emits `{uri: urn, name, level}`. Keep the byte-identical-copies invariant
and the `strip_json_nulls` rule. The first tranche also scans real ids for
rkey-legal characters once and records the result.

### Lexicon publication

This task makes every `org.atgeo.*` lexicon pass `goat lex lint` and writes
a runbook for the `_lexicon.atgeo.org` TXT record, `goat lex publish` and
`goat lex check-dns`. It does not publish.

## Tranches

Every tile-format change above lands in one rebuild, because a planet
rebuild takes hours and needs a go-ahead. The descriptor's property names,
the tile record shape and the relation shape are settled before the export
stage runs.

0. **Remove `getRecord`.** The handler, the forked lexicon, the schema branch, and the tests that call the handler directly. Small, independently reviewable, and it satisfies R4 on its own. It leaves no single-place fetch until tranche 1, which is harmless pre-release with the SDK local-only; the SDK's fake server keeps the SDK's tests green meanwhile. Doing it first means the identity tranche never has to keep a doomed endpoint working against a changing tile shape.
1. **Identity.** All of garganorn: lexicons, export SQL, manifest, descriptor, tile reader, server API, `did.json`, tests through lexrpc validation, lint and the publication runbook, with one local rebuild for its own e2e. Identity and API stay together because splitting them would leave the old API serving the new tile shape as throwaway work.
2. **SDK.** Against tranche 1's local tileset: types, cache lookup by origin, merge and rank on `origin`, dataset-wide default search, fake server, e2e.
3. **Docs.** After the SDK, so the spec describes what shipped.

Then the planet rebuild and deploy.

## Alternatives rejected

- **Make `source`/`license` optional in the forked `getRecord.json`.** Minimal, but leaves a fork of a protocol NSID and loses the lexicon-level guarantee that place records carry attribution.
- **Invent `source`/`license` URLs for schema documents.** Fabricates attribution.
- **Keep `getRecord` for schemas only, canonical lexicon.** Garganorn has no repo; the branch can never serve a standard resolver.
- **TID keys.** Gazetteer ids are upstream ids; a minted timestamp discards them.
- **A client-visible rkey convention.** Considered as `{dataset}:{collection}:{id}` in every tile record. Nothing reads identity from a key once `origin` exists, a copier re-keys anyway, and a key in the value goes stale on copy. The convention survives only as the manifest's lookup key.
- **Typed origin object `{dataset, collection, id}` instead of a URI.** A URI lets a place point at a gazetteer record, a repo record, or any other URI with one field type.
- **`getPlace` with `dataset`, `collection`, `id` parameters, or both forms.** Forces clients to decompose URNs; both forms is API surface for no gain.
- **A dataset query (`describeDataset`).** Derivable from `describeService` plus the descriptors; a third lexicon for nothing.
- **A published lexicon of each dataset's collection vocabulary.** Would make `org.atgeo` the normative source for what OSM's record types are called. The working group is the registrar, but as a small volunteer group it records names as data in descriptors and the spec, not as a schema.
- **A per-record attribution field for Overture places.** Overture's page requires none; the tile conveys the collection, so the collection-level notice list is the right unit.
- **A `collection` field beside `importance` on tile records.** Would let the SDK filter without touching `origin`, but duplicates what the URN prefix already says.
- **A release segment in the URN.** A name that changes with every extract is no identity; the descriptor carries the release instead.
- **A permanent rkey-charset assertion at export.** Guards a condition the upstream id formats cannot produce.
- **Synthetic AT-URIs under a service DID**, **upstream URLs** and **a permalink resolver on `atgeo.org`**: see "What the URN is for". ARK identifiers need an authority registration and a resolver ecosystem; not investigated further. Content hashes identify a value, not a place.
- **Prior art: Beacon Bits** (`app.beaconbits.beacon`, lexicon.garden): `tid` rkey (the record is a check-in), required `venueUri` string such as `osm://node/123` with copied display fields. It keeps place identity in a field, not the key, but as an unregistered scheme string; a commenter on the thread suggested a URN instead.

## Impact

**garganorn:** `server.py`, `__main__.py` (routes, config keys), `lexicon/` (delete `getRecord.json`; change `place.json`, `tilePayload.json`, `getCoverage.json`, `collection.json`, `describeService.json`; add `getPlace` and `describeTileset`), `envelope.py`, `stages.py` (manifest, descriptor, tile payload, `REPO` constant), `quadtree.py`, `tile_reader.py`, `database.py`, the three export SQL files, the two containment SQL files, `did.json` generation, `config.yaml.example`, and the tests that call `get_record` directly plus `test_get_coverage.py`, `test_app.py`, `test_config.py`. Docs listed in R12.

**atgeo-sdk-js:** `src/client.ts` (dataset-wide search, `getPlace(uri)`), `cache.ts` (`find` by origin; tile keying by URL is unchanged), `types.ts`, `merge.ts` (dedupe on `origin`), `rank.ts` (tie-break on `origin`), `README.md`, `scripts/e2e.ts`, `scripts/e2e.browser.ts`, `test/helpers/fakeServer.ts` and the tests that use it; `fixtures/` changes only through the fake.

**atgeo-web:** `place.md`, `api.md`.

## Resolved questions

Each answer's reasoning is in the section named.

1. **`getPlace` parameters:** one `uri` param carrying the URN. Server API.
2. **Descriptor and metadata queries:** descriptor `{slug, generated_at, extent, pairs}`; `describeTileset` and `describeService` only, no dataset query. Tilesets; Server API.
3. **Tile record shape:** `{value, importance}`; `cid` dropped. Tilesets.
4. **Overture places attribution:** per collection, none per record; the pair's attribution URL is Overture's page. Attribution.
5. **A lone record's attribution:** the URN is adequate provenance; nothing stamped into copies; the owner holds the position. Attribution.
6. **Collection vocabulary:** data in descriptors and a list in the spec; no lexicon. Identity.
7. **Alias paths:** `/tilesets/{slug}`; place routes require a dot in the first segment. Server API.
8. **Extract dates:** `release` per pair in the descriptor. Tilesets.
9. **Who parses a URN:** servers decompose, clients construct and prefix-match. Identity.
10. **`value.rkey`:** removed. Record keys.
11. **`same_as`:** kept, same `relation` shape, not emitted. The `origin` field and references.
12. **Default search scope:** the whole dataset. R14.
13. **Sequencing:** four tranches, `getRecord` removal first. Tranches.

## Not verified

- The "Current state" items without ✓ come from a read-only survey; their file and line references were not independently checked.
- Real upstream data was not scanned: rkey character validity rests on construction (OSM `[a-z]+:[0-9]+`, Overture GERS ids as UUIDs). The scan happens in tranche 1. The plain-GET route `/<collection>/<path:rkey>` was not tested against other routes.
- OSM's attribution guidelines were read through a fetch tool that summarizes pages; the ODbL text and Overture's attribution page were read raw. Overture's GERS documentation and release notes were read through the summarizing tool; the `2025-06-25.0` id replacement is stated in Overture's own release notes but the quotes are not guaranteed verbatim.
- The atproto reference linter (`goat lex lint`) has not yet been run on any `org.atgeo.*` lexicon; lexrpc's `uri` validator was read and accepts a URN.
- No legal review. Registration requirements for a URN namespace were not investigated.
