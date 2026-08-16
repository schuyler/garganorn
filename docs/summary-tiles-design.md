# Summary tiles for region-less search — design

Status: draft, not yet reviewed. Elaborates the planned-features section of
the same name. This document is a planning artifact: once the feature ships,
its settled rationale moves to `design-constraints.md` and the rest is
deleted.

Settled premises, from requirements:

- No name, query, or search intent crosses the wire; only collections of
  records (`tile-privacy-design.md`).
- Discovery rides the existing query surface: a `getCoverage` request whose
  bbox exceeds the tile limit returns summary-band references instead of the
  `BboxTooLarge` error.
- The content cut is top-N per collection by importance alone. Importance is
  comparable only within a collection; cross-dataset skew is accepted, and
  queries are always collection-filtered, so no normalization or quota
  machinery is added.
- The summary band is hierarchically tiled so an overlarge bbox gets a slice
  of the world rather than the whole thing. There is no special-cased
  storage: coarse tiles are ordinary tiles at low zooms, and the query
  answers with a limited number of zoom levels within the bbox.
- Protocol and SDK document updates are out of scope for this tranche; a
  complete spec scrub happens after all desired features are in.

## Shape: a coarse band inside each collection's tileset

Each collection's tileset gains low-zoom tiles holding only that
collection's top-N records by importance. There is no combined
cross-collection tileset: `getCoverage` is already per-collection, so a
client resolving a name against divisions and places queries each
collection's summary band and resolves against each independently — the
SDK's rule that there is no merged ranking across collections applies to
summary results as much as any others.

A client with no region knowledge asks `getCoverage` for the world and gets
the collection's whole summary pyramid — a bounded, cacheable set of
references. A client with partial knowledge ("somewhere in Japan") asks for
that region; if the bbox still exceeds the tile limit, it gets the summary
slice covering it.

## The cut

Top-N per collection, ranked by `importance` descending, ties broken by
`id` for determinism. N is a producer parameter like the normalization
constants — not protocol — and is chosen by measurement (see Measurements
pending). The requirements floor is N ≥ ~1000 per collection.

One structural risk needs the measurement before the cut is final: within
the divisions collection, non-locality subtypes top out at importance 40 by
formula (`design-constraints.md`, "Importance scoring varies by entity
type"), so a pure top-N could crowd countries and regions out with
high-scoring localities. If the by-subtype distribution shows that, the
candidate remedy is to include the non-locality subtypes unconditionally —
they are a few thousand records worldwide — and spend N on localities. That
choice waits on the numbers.

## Tiling

Summary assignment reuses the existing algorithm — coarsest tile holding at
most `max_per_tile` records, on the record's `qk17` prefix — run over only
the top-N set, in its own zoom band: z1 down to a floor no deeper than z5.
z0 is excluded because its quadkey is the empty string, which degenerates
the `qk[:6]` path and URL expressions; the cost is four z1 tiles where one
z0 tile would do. The floor cannot reach z6, because the regular band
starts there and the two bands would collide on the same quadkey (and
therefore the same filename); key length shorter than six characters is
also what marks a tile as summary-band at query time. The exact top and
floor within z1–z5 are measurement questions. With N in the tens of thousands and `max_per_tile`
1000, the band is a few dozen tiles, most of them coarse.

Divisions keep their multi-tile reference rule: a division is referenced
from every summary tile its geometry overlaps, as
`stage_division_tile_references` already does for the regular grid.
Single-assignment by representative point would make a slice query miss a
division whose point falls outside the slice — the France problem. Places
are points and stay single-assigned.

**Layout.** There is no special-cased storage: summary tiles use the
ordinary layout, and short quadkeys slot into it without a new convention.
The export partitions on `left(tile_qk, 6)` and the URL builder slices
`qk[:6]`; for a key shorter than six characters both expressions yield the
whole key, so a z4 tile lands at `<qk>/<qk>.json.gz` beside the regular
`<qk[:6]>/<qk>.json.gz` grid and round-trips through the same serving
route. What changes is documentation, not code shape: `tile_reader` and the
export both state a zoom ≥ 6 invariant that becomes "zoom 6–17, plus the
summary band's short keys". The compatibility policy ("Backwards
compatibility with prior tile/record formats is a non-goal") covers the
layout addition; the server-then-re-export deploy order still holds.

## Serving

A summary tile is an ordinary tile in every respect — layout, payload
format, export view, manifest entry, serving route. What distinguishes the
band is only which records it holds (the top-N cut rather than all of
them) and which zooms it occupies.

The manifest needs no new tables or flags: band membership is key length,
so `TileManifest` partitions its loaded quadkeys once at startup.
`get_tiles_for_bbox` first answers from the regular band as today; when
that exceeds `max_tiles`, it returns the summary-band tiles intersecting
the bbox instead of raising. `BboxTooLarge` stays defined in the lexicon
and is still raised when a collection has no summary band, so deployments
without one keep today's behavior and no client-breaking change ships
ahead of the spec scrub.

This changes what an answer means: today every coverage answer is complete
for its bbox, and an oversized query now receives an answer that is
deliberately incomplete — the notable subset. A client can tell which it
got from the references themselves: summary keys are shorter than six
characters. The spec scrub owes this sentence to the client contract; no
new response surface is needed.

The summary answer is the whole band's covering slice, so its worst case —
a world bbox — is the whole pyramid, and the pyramid must fit the response
budget. That couples the parameters: N per collection is bounded by roughly
the band's tile capacity, `max_per_tile` times the tile count the budget
allows. This is the deliberate starting simplification. The recorded
alternative, if measurement wants N larger than the budget permits: descend
the band a level at a time, accumulating intersecting tiles against a soft
cumulative limit, so a world query gets only the top of the pyramid while a
smaller-but-still-oversized bbox reaches deeper levels within the same
budget. That variant decouples N from the response budget at the cost of
depth logic in the serving path; it is not built until the coupling
actually binds.

Bounded and immutable-cacheable, the summary answer preserves the origin-
protection role `BboxTooLarge` plays per `tile-privacy-design.md`
("Separate from max-tiles") by construction rather than by refusal.

## Records

Summary-band copies are byte-identical to regular-band copies: same export
views, filtered to the top-N ids. `design-constraints.md`'s multi-tile rule
makes this mandatory — dedup-by-rkey silently drops divergent copies — and
it forecloses a slimmed summary record format. The fat Overture place
records cost size here; that cost is part of what the size measurement
prices.

**Dependency: division name variants.** Division records export a
hardcoded empty `variants` list — the derivation was never built
(`pipeline-artifacts.md`). A summary band without division variants cannot
resolve "München", "Firenze", or "京都". The Overture source columns
(`names.common`, `names.rules`) are already read into the import, and the
Overture-places variant expression ports directly. This is its own work
item, sequenced before or alongside implementation; the feature's
acceptance depends on it.

## Client use

Resolution reuses the SDK's matching rules as written — normalize, match
name and variants in tiers, rank by tier then importance — with one
adaptation: there is no query region, so the distance tiebreak drops out
and the order is tier, importance descending, uri. Turning a matched record
into a `searchPlaces` region: a division's bbox location is used directly;
a place point gets a fixed pad. Client-side; specified in the SDK document
during the later spec scrub, not here.

## Privacy

The bbox a client sends is the same surface `getCoverage` already exposes,
at the same 0.01° floor; the summary band adds no finer signal. A
world-bbox fetch is identical for every client. Fetching a regional slice
reveals a coarse region of interest — no more than any regular `getCoverage`
call reveals today.

## Measurements pending

Blocked on the build currently occupying garganorn-1. Each number decides
something specific:

| Measurement | Decides |
| --- | --- |
| Importance distribution per collection, by subtype within divisions | N per collection; whether non-locality divisions need unconditional inclusion |
| Record count and gzipped size at candidate N (~1k / 10k / 50k per collection) | Final N; whether a world fetch is cheap enough to be the SDK's default first act |
| Per-level tile counts and gzipped tile sizes across candidate bands within z1–z5 | Band top and floor; summary `max_per_tile`; whether the whole-pyramid-fits-the-budget coupling binds at the chosen N |
| Fat-place-record share of summary size | Whether place inclusion pressure argues for a smaller places N |

## Out of scope for this tranche

Implementation, the division-variants derivation, and all updates to
`atgeo-spec.md`, `atgeo-client-sdk.md`, and the lexicon JSON files, which
wait for the post-features spec scrub.
