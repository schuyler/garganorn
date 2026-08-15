# Planned features

New feature ideas that haven't been designed or scoped yet. Add new
sections here rather than starting another file.

Each section below is its own proposal with its own status. Nothing in this
document is scoped for implementation until its section says so.

## Serve tiles uncompressed; let the transport layer own compression

Status: proposed, not started. No design has been reviewed.

Tiles are stored gzipped on disk (`.json.gz`) and served that way today:
`garganorn/__main__.py`'s `/tiles/<slug>/<path:tile_path>` route
unconditionally sets `Content-Encoding: gzip` on the response, regardless of
what the client's `Accept-Encoding` says. Caddy currently decompresses this in transit for every request
(see `knowledge/server_infrastructure.md`'s tile-serving note) — the
backend always emits gzip, and the reverse proxy always undoes it.

The idea: keep gzip as the on-disk storage format (compact, cheap to
produce once at export time), but have the app decompress before
responding — serve plain JSON with no `Content-Encoding` header — and let
Caddy (the transport layer) apply standard HTTP compression negotiation
based on each client's actual `Accept-Encoding`. Storage-format and
wire-format are currently the same decision; they shouldn't be.

Open questions: where the decompression happens (in the Flask route vs.
letting Caddy re-encode from a plain source); whether CDN/cache layers in
front of Caddy benefit from a properly negotiated `Content-Encoding` in a
way they don't today; whether this is worth doing before the project has
real traffic to negotiate for.

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

Status: proposed, not started. No design has been reviewed.

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

## Maritime divisions

Status: proposed, not started. No design has been reviewed.

`sql/overture_division_import.sql`'s division import filters on
`is_land=true`, which drops bays, straits, and seas from the division
collection entirely. This is a completeness question, not a data-quality
one — those are real Overture divisions, just not land ones — and it was
deliberately left open rather than decided: is a body of water a useful
containment answer for a client, and if so, does it change tile
assignment or containment-name derivation (see `design-constraints.md`'s
"A record may be referenced by more than one tile")?

## Expand the OSM import tag whitelist

Status: researched; requirements approved 2026-08-15; design not
started. The whitelist in `osm_import.sql`'s
`filtered` CTE was audited on 2026-08-15 against OSM's Map Features
taxonomy (with taginfo and Overpass counts) and against the planet parquet
cache; this section records the result. This is a data-completeness
question, not a data-quality one — it doesn't touch how existing records
are scored or deduplicated, only which categories of real-world named
places reach the pipeline.

The whitelist itself held up. None of its 84 value-restricted entries is
deprecated on the wiki, and its exclusions block genuine junk — 153K named
`man_made=survey_point`, 1.06M named `railway=rail` track segments. The
amenity exclude list is sound; the one tempting entry, `amenity=shelter`,
is safe because named mountain shelters carry `tourism=*`, which imports.

What it misses, ranked by named elements. Counts for keys already in the
parquet are planet nodes+ways; keys the osmium filter drops (`landuse`,
`waterway`, `power`, `boundary`, and the rest) are global taginfo counts:

| candidate | named | note |
|---|---|---|
| `place=locality` | 1.8M | the canonical "named place, no population" gazetteer tag; with `isolated_dwelling` 653K, `farm` 244K, `islet` 128K, `city_block` 85K |
| `natural=water` | 652K | named lakes and ponds; plus `wood` 95K, `wetland` 55K, `cliff` 37K, `strait` and `peninsula` (~97% named) |
| `landuse=cemetery` | 269K | new key; also `industrial` 372K, `quarry` 60K, `allotments` 72K, `military` 42K, `winter_sports` (whole ski resorts) |
| `waterway=dam`, `=waterfall` | 78K / 35K | new key |
| `man_made=bridge` | 65K | plus `wastewater_plant` 30K, `water_works` 18K, `pumping_station` 14K, `adit`+`mineshaft` 16K |
| `historic` additions | ~100K | `wayside_shrine` 46K, `tomb` 17K, `citywalls` 7.5K, plus `monastery`, `battlefield`, `aircraft` (52–88% named) |
| `power=plant` | 59K | new key |
| long tail | ~70K | `boundary=national_park`/`protected_area`/`aboriginal_lands`, `highway=services`/`rest_area`/`trailhead`, `aeroway=helipad` 15K, `emergency=ambulance_station` 14K, `railway=yard` (96% named), `telecom=data_center` |

Two cost tiers. Adding values under keys already imported is a SQL-only
change (which no freshness gate notices — a rebuild needs `--force`).
Adding a key means re-running `osmium tags-filter` over the ~90GB planet
PBF and regenerating the parquet, because `filtered.osm.pbf` carries only
the current fourteen keys. Military installations need no new key:
`military=base` is 99% co-tagged `landuse=military`, and `military=airfield`
is 80% covered by `aeroway=aerodrome` already.

Relations are excluded from this expansion at every tier: the import has no
relation pipeline at all (next section), which leaves roughly a million
named-candidate relations — 41K `leisure=park`, 43K `leisure=nature_reserve`,
955K `natural=water`, plus most national parks and protected areas —
invisible until that separate, larger change.

Two decisions closed with the audit. **Transit stops stay out**: stations,
halts and tram stops remain the transit granularity; `highway=bus_stop`,
`public_transport=platform`/`stop_position` and `railway=platform`/`stop`
(3.7M named elements, 87% mutual overlap) are excluded.

**Named buildings come in.** In Manhattan, 2,831 of 5,301 named buildings
(53%) carry no whitelisted key, and that unreachable set is the landmark
skyline — Seagram Building, MetLife Building, General Motors Building;
the Chrysler Building imports today only because someone tagged it
`tourism=yes`. The landmark class lives in `building=yes`/`tower`/
`commercial`/`apartments`, so a value allowlist cannot capture it without
`yes`; the rule is subtractive instead: named buildings import except the
small-residential/outbuilding values `house`, `detached`,
`semidetached_house`, `terrace`, `garage`, `garages`, `shed`, `hut`,
`barn`, `greenhouse`, `static_caravan`, `roof`. A rural-England sample
puts what the drop-list removes at ~18% of unreachable named buildings
(private house names — "Meadow Cottage"); the residue that stays includes
campus blocks and generic labels ("Club House", "Pavillion"), which rank
down by importance rather than by tag.

Requirements (approved 2026-08-15): named elements of the definite-in
categories above, plus buildings per the subtractive rule, are findable in
`org.atgeo.places.osm`, scored by the existing importance formula
unchanged (new categories receive IDF scores from the data as today).
Borderline candidates (`natural=tree`/`stone`, the `leisure` long tail,
`barrier=toll_booth`, `power=substation`) are decided at design time by
the audit's criteria: a discrete place, commonly named, not already
reachable through another imported tag. Exclusions are settled: transit
stops, relations, `leisure=pitch`, the small-residential building values.
Accept: a spot-list — a named cemetery, waterfall, dam, locality, lake,
power plant, and the Seagram Building — resolves after a rebuild, and no
existing category loses records.

Pipeline facts a design must work with. `osmium tags-filter` has no
conjunction, so a bare `building` selector would pull all 705M buildings
into `filtered.osm.pbf`; buildings need a chained filter instead — planet
→ `building` → `name` — whose output (~4M named buildings plus their way
nodes) converts to its own parquet. The other new keys fit the existing
filter as value-restricted selectors (`w/landuse=cemetery,...`), keeping
its output bounded. Either way the planet re-filter runs once per
snapshot, costing tens of minutes of osmium I/O. Build cost scales at
worst linearly with records: containment busy-time per record is 18.7µs
on the 74M-record Overture collection versus 24.7µs on 27.3M-record OSM
(2026-08-15 build), and within a run the per-record cost falls with
density — 532µs/record in ~100-record prefixes down to 8.5µs in the
1.03M-record hottest prefix, a fixed ~0.1s per-batch overhead dominating —
so building records land in the cheapest buckets. Estimated impact: the
OSM containment stage (674s busy) grows by 40–90s, import gains one small
parquet scan (~35M rows next to today's 1.8B), export is per-record, and
the ~2h45m planet build grows by single-digit minutes. Tile payload in
building-dense cells grows too; that is bytes, not time.

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
