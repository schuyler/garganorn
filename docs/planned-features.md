# Planned features

Tracked separately from `cleanup-punchlist.md` (converges to empty — every
item there brings already-decided code/docs into line) and
`performance-improvements.md` (optimizes code that already works correctly).
This document holds new feature ideas that haven't been designed or scoped
yet. Add new sections here rather than starting another file.

Each section below is its own proposal with its own status. Nothing in this
document is scoped for implementation until its section says so.

## Serve tiles uncompressed; let the transport layer own compression

Status: proposed, not started. No design has been reviewed.

Tiles are stored gzipped on disk (`.json.gz`) and served that way today:
`garganorn/__main__.py`'s `/tiles/<slug>/<path:tile_path>` route
unconditionally sets `Content-Encoding: gzip` on the response
(`__main__.py:112-125`), regardless of what the client's `Accept-Encoding`
says. Caddy currently decompresses this in transit for every request
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

`atgeo-appview-sdk-design.md` §3.2 proposed something adjacent — prefetch the
whole division tileset and run a locality tier over it — which does not work,
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

## Audit OSM's Map Features against the import tag whitelist

Status: proposed, not started. No design has been reviewed.

`garganorn/sql/osm_import.sql`'s `filtered` CTE (nodes) whitelists specific
tags to decide what counts as a "place": `amenity` (with an exclude list),
`shop`, `tourism`, `leisure` (specific values), `office`, `craft`,
`healthcare`, `historic` (specific values), `natural` (specific values),
`man_made` (specific values), `aeroway`, `railway`, `public_transport`,
`place`. This list was built by hand, not derived from OSM's canonical tag
taxonomy.

The idea: go through OSM's Map Features wiki page category by category and
check the whitelist against it, to catch categories of legitimately-named,
findable POIs that aren't being imported for no better reason than nobody
thought to add them. This is a data-completeness question, not a
data-quality one — it doesn't touch how existing records are scored or
deduplicated, only whether an entire category of real-world named places is
being silently excluded before it ever reaches the pipeline.
