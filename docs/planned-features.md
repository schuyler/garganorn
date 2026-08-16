# Planned features

New feature ideas that haven't been designed or scoped yet. Add new
sections here rather than starting another file.

Each section below is its own proposal with its own status. Nothing in this
document is scoped for implementation until its section says so.

## Collection and service metadata

Status: proposed, not started. No design has been reviewed.

There is no way to ask a gazetteer what it serves. `atgeo-spec.md` puts it
plainly — "Ask `getCoverage`; a collection it doesn't recognize is a
collection the deployment doesn't serve" — which is discovery by guessing an
NSID and reading an error. And there is no way to ask what a collection *is*:
`source` and `license` arrive in every tile header, so a client learns them
only by fetching a tile in a region it already knows about, and everything
else about a collection is undocumented at runtime.

Two pieces, probably:

**Collection metadata.** What a client needs before it can decide whether a
collection is usable: source and license without fetching a tile; the
attribute and category vocabulary, which is the source's own and differs
between collections; which members of the location union the collection
actually emits; which containment levels its records carry; spatial extent,
record count, and when it was last built.

**Service metadata.** The list of collections a deployment serves, so a
client can enumerate instead of guess. `org.atgeo.tiles.service.json`, the
draft lexicon sitting in this directory, sketches an adjacent version of this
as announcement records in an operator's repo; whether that or a plain XRPC
method is the right shape is part of what needs designing.

Worth being clear about what this does *not* solve. Place categories and
attribute shapes are not standardized across sources, and describing two
incompatible vocabularies does not reconcile them. An application that does
anything with attributes will still pick one collection and stay there.
Metadata makes that choice informed; it doesn't make it unnecessary.

Open questions: whether this is one XRPC method or two; whether it rides on
the existing lexicon-serving route (`GET /{nsid}`) rather than being new
surface; how a category vocabulary gets described in a way a client can act
on rather than merely display; and whether any of it is worth building before
a second gazetteer deployment exists to be discovered.

## Two introductory tutorials, in both directions

Status: proposed, not started. No design has been reviewed.

Two audiences will show up at this project, and each finds a different half
of it baffling. Neither is served by the existing documentation, which is
written for people who already accept both sets of premises.

**Geospatial developers arriving at AT Protocol** need to know why the data
looks the way it does. The worked example: coordinates are decimal strings,
not JSON numbers, and every geospatial developer's first instinct is that
this is a mistake. It isn't — AT Protocol's data model has no float type at
all, because records are content-addressed and floats do not reliably
round-trip to identical bytes across architectures, so a re-encoded record
would hash differently and break its own CID. The spec's recommended
workaround for anything that needs a float is exactly what the location
lexicons do: encode it as a string. That one answer opens onto the rest —
what a lexicon is, why records have URIs instead of IDs, what an NSID and a
DID are, and why there is no `/search` endpoint to call.

**AT Protocol developers arriving at geospatial** need the opposite. Why a
bounding box and not a radius; what a quadkey is and why tiles come at mixed
zooms; why longitude comes before latitude; that Web Mercator's projection
breaks down at the poles, and why garganorn's quadkeys reach them anyway;
that the antimeridian is a real place where naive coordinate comparisons
break; what `importance` means and why it isn't comparable across
collections; and the difference between "near me" and "inside this thing,"
which is distance versus containment and wants different data.

Open questions: whether these are documents in this repo, posts on
atgeo.org, or the READMEs of the SDK and a demo app; how much can be carried
by a worked example instead of prose; and whether the AT Protocol half is
better contributed upstream, since none of it is specific to this gazetteer.

## Summary tiles for region-less search

Status: decided and scoped for implementation, not implemented — see
`summary-tiles-design.md`, which resolves this section's open questions,
records the measurements, and carries the implementation plan.

A client cannot search for "Kyoto" without already knowing roughly where
Kyoto is. `getCoverage` takes a bounding box, so finding a place requires
locating it first, and tiles therefore have no answer for the ordinary
gazetteer question of typing a place name into a box. This is the one real
capability the tile architecture gave up when `searchRecords` was removed,
and `docs/atgeo-client-sdk.md` records region-less search as out of scope
pending this section.

The idea: publish a small, coarse tileset holding only the records worth
finding without a region — high-importance divisions and places, at a low
zoom, complete enough to resolve a name to a rough location. A client
fetches it once (or is shipped with it), searches it locally to turn "Kyoto"
into a bounding box, and then runs an ordinary `searchPlaces` inside that
box. Two tiers, same tile format, same matching rules, no new server method
and no new privacy surface: the coarse tier is a static asset every client
holds identically, so fetching it reveals nothing about what the user is
looking for.

An earlier SDK design proposed something adjacent — prefetch the whole
division tileset and run a locality tier over it — which does not work,
because enumerating a tileset requires the manifest and the gazetteer does
not distribute one. A purpose-built summary tileset is the same idea without
that dependency, and much smaller: the division tileset is every division on
earth, where the useful set for name resolution is a few tens of thousands of
records.

Open questions: what goes in it (importance threshold? population floor?
divisions only, or notable places too?); how big it is gzipped, which decides
whether it ships in the SDK or is fetched on first use; whether one global
file or a handful of continental ones; how a client learns it exists and
where it lives, given there is no manifest and no discovery mechanism; and
whether the coarse tier's ranking can reuse the ordinary matching rules
unchanged or needs its own, since resolving a name to a region is a different
question from ranking results within one.

## Division-in-division containment is never computed

Status: designed, not implemented. The design below is the whole of it; no
separate document.

Every division record ships `relations: {}`, by construction rather than by
configuration. Divisions are the source that *produces* `boundaries.duckdb`
for the other sources, so `run_pipeline` is invoked for `overture_division`
without a `boundaries_db` argument, and `compute_containment` treats that as
its documented empty-containment degradation: it writes the `{"empty": true}`
marker and no parquet. Place and OSM records get real `relations.within`;
divisions never have.

The fix is a join, not geometry. Every record in the Overture divisions
parquet carries `hierarchies` — a list of ancestor chains as `(division_id,
subtype, name)` structs, each chain ordered broadest-first and ending at the
record itself — which `garganorn/sql/overture_division_import.sql` currently
drops. Verified against release 2026-07-22.0: 4,655,003 rows, none with a
null `hierarchies`, none with more than one chain. A division's
`relations.within` is therefore: flatten `hierarchies`, deduplicate, drop
the record itself, keep only ancestors present in the imported set (the
import's filters — `is_land`, a bbox-scoped build — can drop an ancestor,
and an emitted rkey must resolve via `getRecord`), map `division_id` to the
collection-qualified rkey, derive `level` from the chain member's `subtype`
exactly as the import does for the record's own, and order level-ascending
then id. Flatten-plus-deduplicate is identical to taking the single chain
today and stays correct if a future release ships per-perspective chains.
Written in the same `containment/` parquet shape `compute_containment`
produces, the result flows through `stage_export` unchanged — noting that
export joins containment on `(place_id, tile_qk)` and divisions are
multi-tile-referenced, so the rows must fan out across every tile that
references the division, via `tile_references.parquet`.

The geometric route — feeding the division pipeline its own
`boundaries.duckdb` — was considered and rejected: a representative point is
the wrong containment test for divisions (a region's point lands in exactly
one county without the region being inside it, and every division's point is
inside itself), so it would need level-filtering machinery the hierarchy
join makes unnecessary, at covering-probe cost the join doesn't pay. The
geometric machinery stays untouched and is still required for places and
OSM, whose records carry no division references. Where the join runs —
inside the import stage or as a division-specific containment writer — is an
implementation choice.

`atgeo-spec.md` says containment "reaches every level in the vocabulary,
country through microhood ... wherever the source data supports it" and does
not carve out division records, so the current behavior is also a spec-scrub
item until this ships.

## Maritime divisions

Status: proposed, not started. No design has been reviewed.

`garganorn/sql/overture_division_import.sql`'s division import filters on
`is_land=true`, which drops bays, straits, and seas from the division
collection entirely. This is a completeness question, not a data-quality
one — those are real Overture divisions, just not land ones — and it was
deliberately left open rather than decided: is a body of water a useful
containment answer for a client, and if so, does it change tile
assignment or containment-name derivation (see `design-constraints.md`'s
"A record may be referenced by more than one tile")?

## Fold the OSM parquet extraction into the pipeline

Status: proposed, not started. No design has been reviewed.

`scripts/extract-osm-parquet.sh` filters `planet.osm.pbf` with `osmium
tags-filter` and converts the result to parquet, but it lives entirely
outside `garganorn.quadtree`: nothing in the pipeline invokes it, checks its
freshness, or warns when it's stale. Its own cache check (PBF mtime plus a
`filter-selectors.txt` sidecar recording the selector list) is sound, but
nothing wires that check into `quadtree`'s own `--force`, which only
bypasses the pipeline's own stage-level freshness gates. On 2026-08-16 this
let a full `quadtree all --force` run start against a parquet cache dated
March 2026 — five months stale relative to the OSM whitelist expansion
(`39d7c14`) that had just changed the extraction script's selectors — with
nothing in the pipeline's own logs or exit status distinguishing that run
from a correct one.

The idea: have `quadtree` check the extraction script's own freshness
markers before the OSM import stage and either shell out to it automatically
or fail loudly with instructions, rather than silently proceeding on
whatever parquet happens to be on disk.

Timing: the re-extraction triggered by the 2026-08-16 incident, run
standalone against the full planet PBF (91.8 GB, `planet.osm.pbf` dated
2026-03-27), took 1h08m40s — 64m00s for the two `tags-filter` passes plus
`osmium merge`, 4m40s for the `osm-pbf-parquet` conversion (1.82B elements),
producing a 36 GB parquet cache.

Open questions: whether `quadtree` should invoke the script directly
(pulling `osmium`/`osm-pbf-parquet` into the pipeline's dependency surface)
or just check freshness and refuse to proceed; whether the check belongs in
`stage_import` or earlier, before Overture stages run for nothing; and
whether a bbox-scoped build (no full planet PBF on disk) should skip the
check entirely.

## OSM relations are never imported

Status: proposed, not started. No design has been reviewed.

`scripts/extract-osm-parquet.sh` passes only node and way selectors to
`osmium tags-filter`, so relations never reach the parquet — the
`type=relation` partition holds zero rows — and `osm_import.sql` has no
relation pipeline. A feature mapped only as a multipolygon or boundary
relation is invisible no matter what the whitelist says. This bites tags
already whitelisted — `leisure=park` has 41K relations and
`leisure=nature_reserve` 43K, which is where the largest named parks and
reserves live — and it caps any future `natural=water` import (955K water
relations).

Fixing it is a pipeline change, not a filter tweak: relations carry member
lists rather than node refs, so centroid derivation needs member
resolution, potentially recursive, and the osmium filter and parquet must
be regenerated with relation selectors.

Open questions: whether a centroid over member-way nodes is an acceptable
location for a large multipolygon; whether super-relations are worth
resolving; and what fraction of relation-only places actually carry names,
which neither the parquet (no rows) nor taginfo (no per-type name splits)
can answer.
