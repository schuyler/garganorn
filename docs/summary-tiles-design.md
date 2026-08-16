# Summary tiles for region-less search — design

Status: decided and scoped for implementation. All measurements are in and
every parameter below is settled; the Implementation plan section is the
work order. This document is a planning artifact: once the feature ships,
its settled rationale moves to `design-constraints.md` and the rest is
deleted, along with the planned-features section it elaborates.

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
- A world-zoom division query returns the most important cities worldwide,
  ranked by the importance formula. Within divisions, `subtype='locality'`
  is the only subtype the density+population formula ranks densely, so
  cities are the natural content of the top-N; the unconditional set below
  exists to rescue the subtypes cities crowd out, and rides on top of N
  rather than spending it, so the city count never shrinks when that set
  changes.
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
constants — not protocol. **N = 10,000 for every collection**, chosen from
the measurements: at 10k each collection's whole pyramid fits the coverage
budget with room to spare, while 50k blows it for all three (see
Measurements below).

The crowd-out risk is confirmed by measurement: non-locality subtypes top
out at importance 40 by formula (`design-constraints.md`, "Importance
scoring varies by entity type"), and below N≈50,000 almost no non-locality
division survives a pure importance cut. The settled remedy: **records with
`subtype IN ('country', 'region', 'dependency')` — 4,191 worldwide — are
included unconditionally, additively on top of N**, deduplicated against
the top-N. County and localadmin are deliberately excluded: they appear at
z6+ in the regular band, and localadmin alone is large enough to pressure
the tile budget. The additive form means the division band holds ~10k
cities plus every country and region on earth.

## Tiling

Summary assignment reuses the existing algorithm — coarsest tile holding at
most `max_per_tile` records, on the record's `qk17` prefix — run over only
the top-N set, in its own zoom band: z1 down to a floor no deeper than z5.
z0 is excluded because its quadkey is the empty string, which degenerates
the `qk[:6]` path and URL expressions; the cost is four z1 tiles where one
z0 tile would do. The floor cannot reach z6, because the regular band
starts there and the two bands would collide on the same quadkey (and
therefore the same filename); key length shorter than six characters is
also what marks a tile as summary-band at query time. **The band is the
full z1–z5**: the endpoints were forced (z0's empty quadkey, z6's filename
collision), and at N = 10,000 the whole pyramid fits the budget, so there
is nothing to buy by trimming. At `max_per_tile` 1000 the band is a few
dozen tiles per collection, most of them coarse; z5 is a hard floor, so z5
tiles may exceed `max_per_tile` — tiling cannot split past it, and the
serving path must not treat that as an error.

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
route. What changes is documentation, not code shape: the zoom ≥ 6
assumption — a `tile_reader` comment, and the `min_zoom=6` /
`export_partition_zoom=6` parameter defaults — becomes "zoom 6–17, plus the
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
actually binds. Measurement settled this: at N = 10,000 the coupling does
not bind (every collection's pyramid is well under `max_coverage_tiles:
50`), so the fixed band ships and the descent variant stays unbuilt.

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

## Measurements

Run 2026-08-16, read-only against the garganorn-1 global rebuild (deployed
config `max_per_tile: 1000`, `max_coverage_tiles: 50`), JSON built from the
real export SQL and `garganorn.envelope` serialization. What each number
decided:

- **Importance distribution** — p50/p90/max: overture_place 42/55/94, osm
  29/50/94, overture_division 0/21/74. Non-locality division subtypes cap
  at 24–40 by formula. Below N≈50,000 almost no non-locality division
  survives a pure cut → confirmed the unconditional-inclusion remedy.
- **Gzipped size at candidate N** (1k/10k/50k per collection):
  overture_place 196KB/2.1MB/10.2MB, osm 91KB/0.8MB/4.2MB,
  overture_division 73KB/0.7MB/3.5MB. A 10k world fetch is a few MB per
  collection — cheap enough to be the SDK's default first act.
- **Simulated z1–z5 tiling** — at N=10,000 every collection's whole pyramid
  is 18–29 tiles, under the 50-tile budget; at N=50,000 every collection
  exceeds it (54/88/96), and lowering `max_per_tile` makes it worse because
  z5 is a hard floor (z5 tiles hold 600–1,650 records regardless of the
  cap) → N=10,000, budget machinery untouched.
- **Division tiling re-measured with the additive unconditional set**
  (top-10k ∪ country/region/dependency = 14,178 records, 13 overlapping;
  multi-tile references included, 1.05× duplication): 39 leaf tiles
  (z1–z5: 1/5/11/14/8), under the 50-tile budget with 11 of headroom;
  1.11MB gzip for the whole pyramid. One z5 tile holds 2,437 records —
  the expected hard-floor overflow. → the additive set does not disturb
  N=10,000.
- **Fat-place share** — avg raw record bytes: overture_place 2,354.6, osm
  943.5, overture_division 392.5. Accepted; no smaller places N.

Two caveats the numbers carry: division sizes are a floor, because every
division record on this build has empty `relations` (see planned-features,
"Division-in-division containment is never computed" — separate work, not
part of this tranche); and the per-collection gzip figures for divisions
undercount multi-tile duplication by ~8%.

## Implementation plan

Settled parameters: N = 10,000 per collection; unconditional division
subtypes `('country', 'region', 'dependency')`, additive on top of N;
band z1–z5; rank by `importance` descending, ties by `id` ascending;
`max_per_tile` and `max_coverage_tiles` unchanged.

1. **Pipeline.** Select each collection's summary set (top-N; for
   divisions, union the unconditional subtypes) and run summary tile
   assignment over it in the z1–z5 band — same coarsest-fit algorithm, own
   band. Divisions get multi-tile references from covering overlap, as
   `stage_division_tile_references` does for the regular grid. Export rides
   the existing views filtered to the summary ids; short keys need no
   layout change (`left(tile_qk, 6)` and `qk[:6]` yield the whole key).
2. **Invariant wording.** The zoom ≥ 6 assumption lives in a `tile_reader`
   comment and in the `min_zoom=6` and `stage_export` partition defaults;
   the wording becomes "zoom 6–17, plus the summary band's short keys".
3. **Serving.** `TileManifest` partitions its loaded quadkeys by key
   length at startup. `get_tiles_for_bbox` answers from the regular band
   as today; when the result exceeds `max_tiles` it returns the
   summary-band tiles intersecting the bbox instead of raising.
   `BboxTooLarge` still raises for collections without a band.
4. **Config.** `overture_division` is absent from `tiles.collections` in
   the deployed `/opt/garganorn/config.yaml` — only `overture_place` and
   `osm` are servable via `getCoverage` today. The deployed file renders
   from `config.yaml.j2` in the `atgeo-server-config` repo, so the entry
   lands there; the division summary band is unreachable without it.
5. **Tests first** (the suite exists), per the ordinary pipeline. The
   z5-overflow behavior (tiles above `max_per_tile`) and the
   summary-fallback-vs-`BboxTooLarge` fork are the two behaviors most worth
   locking in red.

Out of scope, restated: all updates to `atgeo-spec.md`,
`atgeo-client-sdk.md`, and the lexicon JSON files wait for the
post-features spec scrub, which owes the client contract one sentence —
an oversized answer is deliberately incomplete, marked by sub-six-character
keys. Division containment is its own planned-features item.

On completion: move the settled rationale into `design-constraints.md`,
then delete this document and the planned-features section it elaborates.
