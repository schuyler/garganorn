# Place Identity and Contract Redesign

A design to remove garganorn's `com.atproto.repo.getRecord`, give every
gazetteer place a portable identity (`urn:atgeo:{dataset}:{collection}:{id}`),
and let a tileset carry many collections. A **collection** is a
`(dataset, collection)`, e.g. `(openstreetmap.org, way)`.

**Status:** decisions taken. Tranches 0 and 1 are implemented and their
lexicons frozen in `garganorn/lexicon/`; tranches 2 and 3 are not done.
Baseline: garganorn `1e84549`, atgeo-sdk-js `b1cf173` (local only, no remote), atgeo-web
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
  - **R2c** No producer- or build-relative data in the place. `importance` is not part of the place. AC: no `importance` in the place lexicon or in exported values. Derived relations such as `within` are part of the place's description, not build-relative data: they assert real-world fact, usually settled, even though a producer computes them against its own division release. R2c covers ranking and build-parameter data, of which `importance` is the instance.
- **R3 (I)** `importance` remains available per record in tiles and from place-fetch, outside the value, so client ranking and the summary band are unchanged. AC: SDK ranking tests pass reading importance from outside the value.
- **R4 (S)** Garganorn implements no `com.atproto.*` method and serves no lexicon claiming a `com.atproto.*` NSID. AC: `/xrpc/com.atproto.repo.getRecord` returns method-not-found; no `com.atproto.*` file in `garganorn/lexicon/`.
- **R5 (S)** Single-place fetch is an `org.atgeo.*` method whose lexicon requires the view fields; no DID is needed first. AC: the call passes lexrpc output validation with no `repo` param; SDK `getPlace` makes no `describeService` call.
- **R6 (S)** Metadata is served by validated XRPC queries; the tileset descriptor also has a plain-GET alias whose path cannot be mistaken for a place path. AC: query and alias return the same document; XRPC output validates.
- **R7 (M)** A place `uri` returned by place-fetch is dereferenceable on the serving host. Tiles bake in no gazetteer base URL. AC: no tile record carries a gazetteer `uri`; place-fetch builds `uri` from the server's `public_base`.
- **R8 (S)** `did.json` stops advertising `#atproto_pds` and advertises `#atgeo_tiles` / `AtgeoTileService` as specified in `atgeo-appview-design.md`.
- **R9 (S)** Every `org.atgeo.*` lexicon passes `goat lex lint` so it can be published under the `atgeo.org` authority. AC: lint passes. This task writes the publication runbook (`_lexicon.atgeo.org` TXT record, `goat lex publish` and `goat lex check-dns`); the owner runs it separately. `community.lexicon.location.*` are not published by atgeo.
- **R10 (I)** Every garganorn XRPC success path is tested through lexrpc output validation, not by calling handlers directly. The SDK's fake server enforces the same shapes.
- **R11 (M)** The SDK's real-server e2e (Node and browser) passes against a locally rebuilt tileset.
- **R12 (I)** Docs describe the result: garganorn `atgeo-spec.md`, `atgeo-client-sdk.md`, `explored-and-discarded.md`, `design-constraints.md` (licensing), `README.md`; SDK `README.md`; atgeo-web `place.md` and `api.md`.
- **R13 (M)** No backward compatibility, migration or deprecation period. Nothing is deployed to users.
- **R14 (S)** The SDK's default search spans a whole dataset; a collection filter is optional. Nothing else makes sense for OSM, whose node, way and relation collections are one tileset and one body of data. AC: a search with no collection filter returns records from every collection the dataset's tiles carry. For Overture, the SDK's built-in default is the `place` collection, with divisions on request. An unfiltered Overture search would span places and divisions: the combined tile count meets the cap sooner, so the summary band would trigger at smaller areas, and division importance would be ranked against place importance although the two come from different formulas. This is why tilesets and collections are not isomorphic: a dataset's collections can be one body of data (OSM) or several kinds (Overture), and the SDK's default follows the kind, not the dataset.
- **Out of scope (I):** the check-in lexicon, AppView, `org.atgeo.tiles.service` announcement records, honouring `cid`, and any dedup relation beyond keeping the `sameAs` definition (see "Identity").

Several items above are decisions in a requirement's clothing (R2a's
field, R7's `public_base` mechanism, R8, R9's runbook, R12). They stay here
so the record is complete. "Requirements by tranche" below restates them as
outcomes with the mechanisms as acceptance criteria, per
`.claude/CLAUDE.md`, and records which lists the owner has approved.

## Baseline state

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
after the fourth colon (two in `urn:atgeo:`, one after the dataset, one
after the collection). `urn:atgeo:` means "ATGeo's mapping of this upstream's
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
rather than three parts, why the SDK filters a dataset's tiles by
collection with a prefix test rather than by splitting each `origin`, and
why SDK results identify a place by `origin` rather than by a per-server
URL: a cache hit has no server to name, and a client that needs a URL
calls `getPlace` with the URN.

**Upstream ids are only as stable as the upstream makes them.** Overture
replaced every GERS id with a UUID in release `2025-06-25.0` and published no
changelog for that release; ids captured before it do not resolve against
current data. Later releases ship a per-feature changelog. A URN therefore
names an id within an upstream release, and the tileset descriptor records
which release each collection came from (see "Tilesets"). The URN itself does not
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
- A user's copy of a gazetteer place keeps the dataset URN as origin; the copier's DID does not become the origin. An edited copy keeps it too: `origin` names what the record describes, not an assertion that the content is unchanged. A copy of a repo record that has no origin takes that record's AT-URI.
- A reference to a place is the `relation` object `{uri, name, type}`, `uri` required with `format: uri`, `name` and `type` optional. The same definition serves `relations.within` and `relations.sameAs`. A `uri` is any valid URI. Producer preference: an ATGeo URN (where ATGeo defines one for the source), then an AT-URI, then any other URI. The lexicon cannot enforce the preference; the spec states it.
- `type` is the related place's type in its own dataset's vocabulary (for Overture divisions, the `subtype`, e.g. `region`). Nesting is carried by the order of `within`, broadest to most specific. ATGeo defines no type vocabulary: administrative names and their nesting are not universal, and an integer rank is a producer-chosen build parameter, like `importance`, that two producers would emit differently for one place and that readers would mistake for OSM `admin_level`. Each tileset publishes its own vocabulary and order as `containmentTypes` (see "Tilesets").
- `sameAs` stays in the lexicon for future expansion. No exporter emits it and no SDK code reads it. It exists so that the relation a community gazetteer would use to record that two URIs name one entity is already defined with the shape every other reference uses.
- Consumers compare references and origins as exact strings and never normalize. The same entity may appear under several URIs (`urn:atgeo:openstreetmap.org:way:123456` and `https://www.openstreetmap.org/way/123456`); recording that equivalence is `sameAs`'s job, for community gazetteers, not this task's.

### Record keys

- The `org.atgeo.place` lexicon uses `key: "any"`. A TID would require minting timestamps and would discard the upstream id; `any` lets a user key a copy however they like.
- The value's `rkey` property is removed. It duplicated the record's key, and once users may re-key a copy it would go stale on copy, which breaks R1's "unmodified" and R2c. `origin` is its replacement.
- Tile records carry no rkey at all. A record with an `origin` carries no `uri` either (see R7); a record without one, such as a copy of a repo record, carries its own `uri`. A copier chooses the key; the gazetteer's choice is not visible to clients and not part of the contract.
- Inside garganorn, the manifest's `record_tiles` keys on `{dataset}:{collection}:{id}`, the URN without its prefix. Because the collection is in the key, OSM node 5 and way 5 do not collide and the manifest needs no extra column. The key is a DuckDB lookup string and nothing else: no atproto record key is ever formed from it, so the record-key character rules do not apply to it.
- Two copies of the same upstream place under different rkeys are both valid. Deduplication groups on `origin`.

### Tilesets and the tileset descriptor

A tileset is one pipeline's output. It relates many-to-many to
collections:

- OSM: one tileset carries `(openstreetmap.org, node|way|relation)`.
- Overture: one tileset per theme: `(overturemaps.org, division)` and `(overturemaps.org, place)`. Places and divisions come from separate pipelines with different tile characteristics (divisions are multi-tile and the `within` target for everything else), so merging them would merge pipelines for no gain.
- A collection could also span several tilesets (regional shards); nothing in the model forbids it.

A **tileset descriptor** (`tileset.json`, `org.atgeo.defs#tileset`) describes
one tileset:

```
tileset: {slug, publishedAt?, extent?, collections: [collectionInfo, ...], references?: [collectionAttribution, ...]}
collectionAttribution: {dataset, collection, release?, source?, licenses?, attribution?}
collectionInfo: collectionAttribution + {recordCount?, locationFormats?,
                containmentTypes?, categories?, attributes?}
```

Only what a reader cannot do without is required: `slug` and a non-empty
`collections` on a tileset, `dataset` and `collection` on a collection, and
`records` on a tile. A user can author a place with nothing but `$type` and
`name`, and publish tiles of their own records with no `importance` and no
upstream dataset. Garganorn's own export always emits every optional
attribution field, `recordCount`, `publishedAt` and `importance`, which its
tests enforce; the upstream licence premise binds garganorn's tiles, and the
shared schema does not impose it on every producer.

The **slug** is part of the tileset: fixed when it is built, stored in the
descriptor, and the name under which every gazetteer serves it. Its
character set (lowercase letters, digits, hyphens) is a convention.
`publishedAt` is when the tileset was created; it is identical in every tile
header. A place's `updatedAt` is the data provider's date for the place's
data, a different thing.

`release` is the upstream's own version string: an Overture release id
(`YYYY-MM-DD.N`) or an OSM replication timestamp. It is there because
Overture's ids have churned between releases, so two services answering one
URN differently need a way to show why. `source` and `attribution` are URLs.
`licenses` is a list of licence-text URLs, one per licence the collection's
data is under, because a collection can be under several: Overture divisions
are ODbL plus CC BY 4.0, Overture places are under three. `references` lists
the collections whose data the tileset copies into its records but does not
carry; a collection appears under `collections` or `references`, never both.
The OSM and Overture place tilesets reference `(overturemaps.org, division)`,
because `within` copies a division's name and URN into their records; the
division tileset carries it. The division release matters for the same churn
reason, and `within` URNs resolve only if every theme in an instance was
built from the same Overture release. That is a build constraint, not a
mechanism: the `release` fields make a mismatch visible, and T1-R12's
criterion assumes the match. `locationFormats` lists the `$type` of the
location objects a collection's records carry. `containmentTypes` lists the
`within` types the collection's records carry, broadest first, so each
tileset publishes its own vocabulary and order (as ISO 19112 gazetteers
declare their location types); it is absent when records carry none.
`categories` and `attributes` are the observed category values with counts
and attribute keys. All of these are per collection.

The **tile header** carries no single collection. It carries `publishedAt`,
the record array, and an `attribution` list: one `collectionAttribution`
object for every collection the tileset carries or references, the same list
in every tile of the tileset. Computing it per tile would buy smaller headers
at the cost of a per-tile pass and a rule nobody can check by eye; one list
per tileset meets R2a by construction. Garganorn builds each `collectionInfo`
from the collection's `collectionAttribution` plus its statistics, so the
descriptor and the header cannot drift.

Each tile record is `{value, uri?, importance?}`. Identity is `value.origin`;
a dereferenceable URL is per server, built by place-fetch from its own
`public_base` (R7). `uri` is the record's own URI, for a record with no
origin (an app publishing tiles of `at://` repo records); records with an
origin omit it. Such a producer names its own collection, conventionally
`(its domain, a collection name)`, and its records match no collection
filter. There is no `cid`: nothing would read it, and it only made sense
while the tile record imitated an atproto record view.

The wrapper exists to hold `importance` outside the value, and the owner
accepted that grudgingly, so the reason is recorded. Importance is producer
state: this build's formula ranks this place among the places from the same
source in this build. A second gazetteer serving the same dataset would
compute a different number for the same origin, so two services would
disagree on the bytes of one record. In a user-authored or copied record
the property is undefined, since there is no producer. And it goes stale on
copy for the same reason `value.rkey` would have. Keeping it inside the
value would weaken the one promise the redesign makes, that a value embeds
unmodified and is the same everywhere. The wrapper itself is not new depth:
the baseline tile record is `{uri, cid, value}`, and this design drops
`cid` and keeps `uri` only for records with no origin.

Every copy of a multi-tile record must remain byte-identical (a current
invariant in `design-constraints.md`). Record bytes are unchanged by the
header, which is itself the same in every tile of a tileset; only the
record arrays differ between tiles. `importance` is per record and
identical in every copy, so moving it outside the value preserves the
invariant.

### Attribution and licensing in tiles

Each tile carries, for every `(dataset, collection)` its tileset carries or
references, one entry with the source URL, the licence-text URLs (for
example the ODbL text) and an attribution URL. An OSM tile's three OSM
entries (node, way, relation) each link
`https://www.openstreetmap.org/copyright` as the attribution, and its
division entry carries the ODbL and CC BY 4.0 texts. Entries are keyed by
`(dataset, collection)` because licences vary inside a dataset, and carry a
list because they vary inside a collection too.

**Overture's attribution page** (`docs.overturemaps.org/attribution`, raw
HTML read 2026-10-13) lists sources per theme as "Data from X. Available
under Y". Divisions: ODbL, "© OpenStreetMap contributors", plus geoBoundaries,
Esri Community Maps and LINZ under CC BY 4.0. Places: Meta, Microsoft,
PinMeTo, Krick, RenderSEO, DAC and BrightQuery under CDLA Permissive 2.0;
Foursquare "Copyright 2024 Foursquare Labs, Inc. All rights reserved",
Apache 2.0, with a note that the data was transformed to the Overture schema
and a link to a NOTICE.txt; AllThePlaces under CC0 1.0. The page states no
per-record requirement for any source. The Overture collections therefore point
their `attribution` URL at that page, which carries the Foursquare notice
and the NOTICE link, and tile records for places carry no attribution
field. Overture place records' own `sources` array, which the export already
passes through in `attributes`, stays under the data-as-is rule, with its
per-source `confidence` cast to a string (see "Export SQL").

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
- **`getTiles`** takes `dataset` plus an optional `collection` and resolves, through the tileset descriptors, to the tilesets carrying that collection; the output is a flat list of tile URLs plus the optional `overview` flag. When a dataset spans several tilesets the answer is one combined list, and the tile cap and the `overview` flag apply to that combined list, because the client's cost is the total download. Tiles can contain records the caller did not ask for, so the SDK filters by collection client-side with a prefix test on `origin`, and only when a filter was asked for (R14). A record with no `origin` matches no collection filter; it is still returned when no filter is given.
- **`describeTileset`** takes a tileset `slug` and returns its descriptor.
- **`describeService`** advertises the instance's `did` and its tilesets, each with the collections it carries. This is the discovery mechanism for federation: a client holding a URN asks each service it knows whether it serves that collection. There is no separate dataset query; a dataset view is derivable from these two documents, and a third lexicon would buy nothing.
- **Plain-GET aliases.** The tileset descriptor is also served at `/tilesets/{slug}`. The three-segment place alias `{base}/{dataset}/{collection}/{id}` dereferences a single place; its route requires a dot in the first segment, since a dataset is a domain, so any dotless first segment is free for aliases and the two cannot collide. The service document needs no alias: `did.json` and the XRPC query cover it.
- **`did.json`** drops `#atproto_pds` and the `alsoKnownAs` `at://` entry, both of which claim an atproto identity the instance does not have, and advertises `#atgeo_tiles` / `AtgeoTileService` per `atgeo-appview-design.md`.
- `dataset` is a domain string and `collection` a short string; neither has `format: nsid`.

### Place lexicon

`org.atgeo.place`: `key: any`; optional `origin` (`format: uri`); no
`importance` or `rkey`; `relation` is `{uri, name?, type?}` with `uri`
required; `updatedAt`, the data provider's date for the place's data;
`relations.sameAs`; `name` is the only required property. The description
states the absence rule and the URI preference order. The collection
metadata is the `collectionAttribution` and `collectionInfo` definitions in
`org.atgeo.defs` (see "Tilesets").

### Export SQL

The three `*_export_tiles.sql` files change: emit `origin` as a URN, emit no
`rkey` and no `uri` (every garganorn record has an origin), move `importance` outside the value,
and write the `{dataset}:{collection}:{id}` manifest key. The `within`
construction in `compute_containment.sql` and `division_containment.sql`
emits `{uri: urn, name, type}`, ordered so that `within` runs broadest first. Keep the byte-identical-copies invariant
and the `strip_json_nulls` rule.

Value content is bound by the atproto data model, which has no float type.
Every non-integer number is emitted as a string, as the top-level Overture
`confidence` already is (`::DECIMAL(4,3)::VARCHAR`). The one passthrough
that violated this at baseline is `sources: p.sources` in the Overture place
export: `sources[].confidence` is a DOUBLE. It is cast element-wise. The fixture nulls `sources`, which is why the
violation never surfaced; the fixture gains a non-null `sources` array so
that T1-R1's criterion runs on data of the real shape.

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

## Requirements by tranche

Tags: **S** stated by the owner, **M** from memory or existing docs, **I**
inferred and vetoable, **O** open. Each item is an outcome; the mechanisms
the design fixes appear as acceptance criteria. Premises binding every
tranche, not items to satisfy: the upstream licences; the user-safety
principle (no nearest-neighbour or full-text search over the network);
R13.

**Approval status.** Tranches 0, 1 and 2: approved by the owner
2026-10-13. Tranche 3: provisional until tranche 1 is accepted. The
lexicons tranche 1 ships are the contract tranches 2 and 3 bind to.

The approved requirement text below keeps the names it was approved under.
At the tranche 1 design gate the owner renamed `getCoverage` to `getTiles`,
`summary` to `overview`, `same_as` to `sameAs`, and "pair" to
"collection" (`collectionRef`, `collectionAttribution`,
`collectionInfo`); the descriptor's `containmentLevels` is `containmentTypes`
and `locationTypes` is `locationFormats`. A tile record is
`{value, uri?, importance?}`: `uri` is present only for a record with no
`origin`. Read the acceptance criteria with those names.

### Tranche 0: remove the atproto record surface (approved)

`get_record` backs three things: the XRPC method, the plain-GET
`/<collection>/<rkey>` route (places, collection metadata and lexicon
schemas), and the `metadata` URL `describeService` emits. Removing the
handler removes all three.

- **T0-R1 (S)** Garganorn serves no `com.atproto.*` method and no lexicon under a `com.atproto.*` NSID, so that it stops presenting itself as an atproto repo it is not. AC: `GET /xrpc/com.atproto.repo.getRecord?...` returns lexrpc's method-not-found response; no file in `garganorn/lexicon/` has an `id` starting `com.atproto.`; the string `com.atproto` appears nowhere under `garganorn/`.
- **T0-R2 (I)** Every URL the service document advertises resolves, so that a client following `describeService` never reaches a 404. The `metadata` link leaves with its target in this tranche; tranche 1 replaces it with the tileset documents. AC: for every entry in `describeService` output, a GET on each URL it contains returns 200.
- **T0-R3 (I)** Lexicon documents stay fetchable, so that the one schema-serving path that works (`GET /{nsid}`) survives the removal of the repo-style one. AC: `GET /org.atgeo.getCoverage` returns the lexicon JSON; `GET /com.atproto.lexicon.schema/org.atgeo.getCoverage` returns 404.
- **T0-R4 (I)** Tests that exercised the handler keep what they assert about record content, so that coverage of tile-backed lookup does not leave with the endpoint. The handler is called directly at 15 sites, all in `test_server.py`; `test_app.py` exercises the plain-GET route; the tile-reader, export and audit tests use the tile reader's own `get_record` and are untouched. AC: no test calls the server handler; assertions about record content in `test_server.py` move to the tile reader or to the XRPC methods that remain, and only assertions about the removed envelope (`uri`, `source`, `license`, `_query`) and the removed routes are dropped; the full suite is green.
- **Out of scope:** any replacement fetch method, the place alias, the descriptor, SDK changes. The SDK's `getPlace` and `getCollectionInfo` break against a tranche-0 server by design; the SDK's fake keeps its tests green, and e2e does not run until tranche 2.

### Tranche 1: identity, export, descriptor, server API (approved)

- **T1-R1 (S)** A gazetteer place value is a valid `org.atgeo.place` record a user can write into their own repo unmodified, so that lookup output is usable as atproto data. AC: every tile record's `value` and every `getPlace` response's `record` pass `goat lex validate` against the lexicon; the lexicon's key type is one atproto accepts; no value contains a float, checked on a fixture whose Overture `sources` array is non-null.
- **T1-R2 (S)** A place value states its origin dataset, collection and upstream id by value alone, distinguishes same-id places from different datasets or collections, and carries nothing build- or producer-relative, so that the value survives its server. AC: every exported value has `origin` equal to `urn:atgeo:{dataset}:{collection}:{id}` with the dataset string the descriptor states; no value has a top-level `importance`, `rkey` or `uri` property; a test shows an OSM node and way with the same numeric id have different origins.
- **T1-R3 (I)** Per-record importance stays available to clients from tiles and from place-fetch, outside the value, so that client ranking and the summary band are unchanged. AC: a tile record is `{value, importance}`; `getPlace` output carries importance outside `record`; a fixture's exported importance values equal baseline's.
- **T1-R4 (S)** A single place is fetchable by its URN with no prior discovery call, so that a client holding an `origin` can resolve it. AC: `org.atgeo.getPlace?uri=urn:...` passes lexrpc output validation with no `repo` parameter; an unknown URN yields an XRPC error, not a 500; `GET /{dataset}/{collection}/{id}` returns the same record.
- **T1-R5 (S)** Every tileset says which `(dataset, collection)` pairs it carries and which it references, under which upstream release, with each pair's source, licences and attribution; every tile carries the licences and attribution of every pair its tileset carries or references, so that attribution travels with the data. AC: `describeTileset` output validates and lists carried and referenced pairs with `release`, `source`, `licenses` and `attribution`; `GET /tilesets/{slug}` returns the same document; an OSM tile's header carries node, way and relation entries each linking `openstreetmap.org/copyright` and an `(overturemaps.org, division)` entry listing the ODbL and CC BY 4.0 texts; the byte-identical-copies test still passes.
- **T1-R6 (S)** The service document says which datasets and tilesets the instance serves, and every URL in it resolves, so that federation discovery works from one call. AC: `describeService` lists tilesets with their pairs; a GET on every URL it contains returns 200.
- **T1-R7 (M)** A tileset is portable between hosts without a rebuild: tiles bake in no gazetteer host, coverage answers give absolute tile URLs from the serving configuration, and a place `uri` from place-fetch dereferences on the serving host. AC: no built tile contains the configured host string; every tile URL in a `getCoverage` answer is absolute and built from the configured tile base; `getPlace.uri` starts with the configured `public_base` and a GET on it returns the record.
- **T1-R8 (S)** `did.json` advertises a tile service, not a PDS, and claims no atproto identity. AC: the service list has `#atgeo_tiles` of type `AtgeoTileService` as `atgeo-appview-design.md` specifies, no `#atproto_pds`, and no `alsoKnownAs` entry with an `at://` value.
- **T1-R9 (S)** Every `org.atgeo.*` lexicon is publishable under `atgeo.org`, so that resolvers can find them by NSID. AC: `goat lex lint` and `goat lex parse` pass on every `org.atgeo.*` lexicon file (the `community.lexicon.location.*` copies fail lint with `[unlimited-string]`, are not ours to change, and are excluded); a runbook in `docs/` states the `_lexicon.atgeo.org` TXT record and the `goat lex publish` and `goat lex check-dns` commands; nothing is published by this tranche.
- **T1-R10 (I)** Every XRPC success path is proven through output validation, so that a lexicon mismatch cannot ship unnoticed again. AC: no test calls a handler method directly; each registered method has a test through the Flask client whose response lexrpc validated; a mutation removing a required output field from any handler turns a test red.
- **T1-R11 (S)** Coverage is answerable per dataset with an optional collection filter, so that one OSM tileset serves node, way and relation searches. AC: `getCoverage` with `dataset=openstreetmap.org` returns the OSM tileset's tiles; adding `collection=way` returns the same tiles; an unknown dataset yields an XRPC error. (The approved text names `getCoverage`; the owner renamed the method to `getTiles` at this tranche's design gate.)
- **T1-R12 (I)** References between places are URIs in the shape the lexicon defines, so that containment survives a copy. AC: every `relations.within[].uri` in exported values is the URN of a division present in the served data; no exported relation carries `rkey`.
- **T1-R13** Withdrawn. It required a scan of upstream ids for record-key-legal characters, but no atproto record key is ever formed from the manifest key, so the scan checked a property nothing depends on. The number is kept so references stay stable.
- **T1-R14 (M)** No compatibility shim, dual format, or deprecation path. AC: no code path reads or writes the old tile shape.
- **T1-R15 (S)** The lexicons require only what a consumer needs to read a place or a tile, so that users and apps can author their own places and publish their own tiles. AC: every required property in `place`, `tilePayload` and `defs` is one a reader cannot do without; a minimal user-authored place (`$type`, `name`) validates; a tile whose records carry no `importance` and come from no upstream dataset validates; no lexicon requires a property only a gazetteer build can produce.
- **Docs in this tranche:** garganorn `README.md`, `design-constraints.md`, and `pipeline-artifacts.md` where the change touches them. `atgeo-spec.md` and `atgeo-client-sdk.md` belong to tranche 3.
- **Out of scope:** the SDK; publishing lexicons; the planet rebuild and deploy; `same_as` emission.

### Tranche 2: SDK (approved)

- **T2-R1 (S)** The default search spans a whole dataset, with an optional collection filter, so that an OSM search sees nodes, ways and relations together. AC: against the fake server with a tile holding all three, a search with no filter returns all three kinds and `collection: "way"` returns only ways.
- **T2-R2 (S)** Callers receive the gazetteer value unmodified plus importance outside it; ranking and the summary band are unchanged. AC: the returned place deep-equals the tile value; ranking tests pass reading importance from the wrapper.
- **T2-R3 (S)** `getPlace(uri)` resolves a URN with no discovery call first, and results identify a place by `origin` rather than by a server URL. AC: the fake records no `describeService` call before `getPlace`; a cache hit and a network answer return the same shape, found by origin.
- **T2-R4 (I)** Merging and ordering key on origin, so that a multi-tile record appears once and ordering is deterministic. AC: two tiles holding the same division record merge to one hit; a tie-break test orders by origin.
- **T2-R5 (I)** Tileset and service metadata are reachable through the new documents and the old collection-info path is gone. AC: typed calls for `describeService` and `describeTileset`; no reference to `org.atgeo.collection` or `getRecord` in `src/`.
- **T2-R6 (I)** The fake server enforces the server's shapes, so that SDK tests cannot pass against a response the server would never send. AC: the fake validates its responses against lexicon JSON copied from garganorn; removing a required field from a fake response fails a test.
- **T2-R7 (M)** Node and browser e2e pass against a local tranche-1 server. AC: both scripts exit 0.
- **T2-R8 (I)** The SDK README describes the shipped behaviour. AC: every example in it runs against the fake.
- **T2-R9 (S)** The SDK's developer-facing surface does not change meaningfully without the owner's confirmation, so that the SDK keeps abstracting the user-safety mechanics and a developer never has to get them right; it stays as easy to use in development as it is today. AC: every change to an exported name, signature, option or default is listed in the tranche design and approved before Red; the README's examples from before the tranche still run with at most the changes on that list; no new developer-facing option concerns tile fetching, caching or search locality. Changes already expected on the list: the `collection` option becomes `dataset` with an optional collection filter, `getPlace` takes a URN, results identify by `origin`, and the collection-info call becomes tileset and service calls.
- **Out of scope:** reading `sameAs`; querying more than one service.

### Tranche 3: client-facing docs (provisional)

- **T3-R1 (I)** The client-facing docs describe the shipped contract and nothing else: garganorn `atgeo-spec.md`, `atgeo-client-sdk.md`, `explored-and-discarded.md` (kept; sole record of client-side ranking), atgeo-web `place.md`, `api.md`, `usage.md`, `faq.md`. AC: every example request and response validates against the shipped lexicons; every NSID and route named exists on the server; `getRecord`, `org.atgeo.collection` and `record-key` appear nowhere in them.
- **T3-R2 (I)** The two product claims the redesign was built to make true are true as written: output incorporable without modification, and every place traceable to its origin dataset by a globally unique key. AC: each claim cites the mechanism that delivers it.
- **T3-R3 (O)** What becomes of this document once implemented. Memory says a planning doc is deleted once implemented and its facts and choices salvaged into living docs. The owner asked for a full record of the decisions and their rationale. Options: salvage the rationale into `design-constraints.md` and `explored-and-discarded.md`, then delete; or keep it as an architecture decision record, which the global rules allow when the record stops a process repeating a mistake. Decided at this tranche's requirements gate.
- **Out of scope:** the Kotlin and Swift port text in the spec beyond what the TypeScript SDK's behaviour dictates.

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
- **A per-tile attribution list.** Smaller headers, but a per-tile pass and a correctness rule (which collections does this tile contain, counting data copied in through `within`?) that nobody can check by eye. One list per tileset meets R2a by construction.
- **One licence URL per collection.** Cannot express Overture divisions (ODbL plus CC BY 4.0) or places (three licences); pointing it at the attribution page instead, as the baseline config does, puts no licence URI with the data, which ODbL §4.2 asks for.
- **Synthetic AT-URIs under a service DID**, **upstream URLs** and **a permalink resolver on `atgeo.org`**: see "What the URN is for". ARK identifiers need an authority registration and a resolver ecosystem; not investigated further. Content hashes identify a value, not a place.
- **An integer containment `level` on relations.** A producer-chosen rank that two producers would emit differently for one place; see "The `origin` field and references".
- **Prior art: Beacon Bits** (`app.beaconbits.beacon`, lexicon.garden): `tid` rkey (the record is a check-in), required `venueUri` string such as `osm://node/123` with copied display fields. It keeps place identity in a field, not the key, but as an unregistered scheme string; a commenter on the thread suggested a URN instead.

## Impact

**garganorn:** `server.py`, `__main__.py` (routes, config keys), `lexicon/` (delete `getRecord.json`, `coverageResult.json` and `collection.json`; change `place.json`, `tilePayload.json`, `describeService.json`; `getCoverage.json` becomes `getTiles.json`; add `defs`, `getPlace` and `describeTileset`), `envelope.py`, `stages.py` (manifest, descriptor, tile payload, `REPO` constant), `quadtree.py`, `tile_reader.py`, `database.py`, the three export SQL files, the two containment SQL files, `did.json` generation, `config.yaml.example`, and the tests that call `get_record` directly plus `test_get_coverage.py`, `test_app.py`, `test_config.py`. Docs listed in R12.

**atgeo-sdk-js:** `src/client.ts` (dataset-wide search, `getPlace(uri)`), `cache.ts` (`find` by origin; tile keying by URL is unchanged), `types.ts`, `merge.ts` (dedupe on `origin`), `rank.ts` (tie-break on `origin`), `README.md`, `scripts/e2e.ts`, `scripts/e2e.browser.ts`, `test/helpers/fakeServer.ts` and the tests that use it; `fixtures/` changes only through the fake.

**atgeo-web:** `place.md`, `api.md`.

## Resolved questions

Each answer's reasoning is in the section named.

1. **`getPlace` parameters:** one `uri` param carrying the URN. Server API.
2. **Descriptor and metadata queries:** descriptor `{slug, publishedAt, extent, collections, references}`, the slug fixed with the tileset; `describeTileset` and `describeService` only, no dataset query. Tilesets; Server API.
3. **Tile record shape:** `{value, uri?, importance?}`, `uri` only for a record with no origin; `cid` dropped. Tilesets.
4. **Overture places attribution:** per collection, none per record; the collection's attribution URL is Overture's page. Attribution.
5. **A lone record's attribution:** the URN is adequate provenance; nothing stamped into copies; the owner holds the position. Attribution.
6. **Collection vocabulary:** data in descriptors and a list in the spec; no lexicon. Identity.
7. **Alias paths:** `/tilesets/{slug}`; place routes require a dot in the first segment. Server API.
8. **Extract dates:** `release` per collection in the descriptor. Tilesets.
9. **Who parses a URN:** servers decompose, clients construct and prefix-match. Identity.
10. **`value.rkey`:** removed. Record keys.
11. **`sameAs`:** kept, same `relation` shape, not emitted. The `origin` field and references.
12. **Default search scope:** the whole dataset. R14.
13. **Sequencing:** four tranches, `getRecord` removal first. Tranches.
14. **Tile attribution granularity:** one list per tileset, covering carried and referenced collections, the same in every tile. Tilesets.
15. **Licences per collection:** a list of licence-text URLs. Tilesets; Attribution.
16. **Floats:** no value contains one; `sources[].confidence` is cast and the fixture carries a non-null `sources`. Export SQL.
17. **Two-tileset coverage:** one combined list; the cap and `overview` apply to it. Server API.
18. **Derived relations and R2c:** `within` is part of the place's description; R2c covers ranking and build-parameter data. Requirements, R2c.
19. **Overture default search:** the `place` collection, divisions on request; OSM searches the whole dataset. Requirements, R14.
20. **Containment:** relations carry `type` in the dataset's own vocabulary, not an integer level; the descriptor lists `containmentTypes` broadest first. The `origin` field and references; Tilesets.

## Not verified

- The "Baseline state" items without ✓ come from a read-only survey; their file and line references were not independently checked.
- The plain-GET route `/<collection>/<path:rkey>` was not tested against other routes.
- The Overture place export also passes `names` and `brand` through raw. Overture's shared schemas may carry a linear-referencing `between` (a list of doubles) in those structs; the fixtures omit it and for point places it should be null, so no reachable input is known. The float criterion under T1-R1 is what catches it if one exists.
- OSM's attribution guidelines were read through a fetch tool that summarizes pages; the ODbL text and Overture's attribution page were read raw. Overture's GERS documentation and release notes were read through the summarizing tool; the `2025-06-25.0` id replacement is stated in Overture's own release notes but the quotes are not guaranteed verbatim.
- lexrpc's `uri` validator was read and accepts a URN.
- No legal review. Registration requirements for a URN namespace were not investigated.
