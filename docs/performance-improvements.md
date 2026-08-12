# Performance improvements

Proposed performance work on code that already works correctly —
open-ended, one section per idea, each with its own status. This doc
holds performance-improvement ideas generally, not just containment; add
new sections here rather than starting another file. Nothing here is
scoped for implementation until its section says so.

## Containment computation: polygon tiling, and correct division tile assignment

Status: two of the three shippable units below have shipped;
overlap-tile-references remains. What survives here is the evidence the
design rests on — the two problem statements, the requirements both
designs are checked against, and the Discovery numbers that pin `V`, the
depth cap, and the reference zoom. The designs themselves live in
[fragment-containment-design.md](fragment-containment-design.md) and
[overlap-tile-references-design.md](overlap-tile-references-design.md),
which are authoritative wherever this document and they disagree.

This section retires when overlap-tile-references ships. The rest of the
file does not — the disk-writes section below is independent of it.

### Problem: containment performance

Log analysis of the 2026-08-08 build (`compute-containment.log`,
`tile-build.log`) shows `compute_containment`'s per-batch cost is
near-linear in candidate count `n` in aggregate (log-log slope 0.71-0.75 for
n≥1000, both the overture_place and osm runs) — but a consistent set of
~20 quadtree cells run **10-38x slower than their own `n` predicts**,
independently, in both runs. Every one of them falls on a complex coastline
or archipelago: Chesapeake Bay, Cuba/Bahamas, Nova Scotia/Gulf of St.
Lawrence, Maine coast, BC fjords, Norway fjords, Finland lakes, Hawaii,
Indonesia, Sicily, Río de la Plata. `032010` (Chesapeake/DC-MD-VA-DE) is the
worst outlier in both runs (~23-35x) and also a top-10 cell by raw `n`.

This is the same mechanism `compute_containment.sql`'s own header already
documents for the Nunavut case (~200k vertices): the edge-arm join's
`ST_Contains`-style test costs scale with boundary polygon **vertex count**,
not candidate point count. Right now every candidate point tested against a
boundary pays that boundary's full vertex count, no matter how far from the
complex part of the coastline the point actually is.

### Problem: division tile assignment is a correctness bug

A division is indexed by a single point. `overture_division_import.sql`
computes `qk17` from the geometry's interior point
(`ST_QuadKey(ST_X(ma.interior_point), ST_Y(ma.interior_point), 17)`), the
same point containment tests, so the point is at least guaranteed to lie
inside the division. Fragment containment depends on that agreement: its
edge arm joins a covering leaf by `qk17` and then tests a fragment clipped
to that leaf, so a `qk17` naming a tile the point isn't in matches nothing.

What remains wrong is the single point itself. The z17 depth is not what
ships — `stage_tile_assignment` truncates `qk17` to the coarsest prefix
holding ≤ `max_per_tile` records, so the assigned tile can be any zoom
from 6 to 17 — but whatever the depth, a division is discoverable only
through the one tile containing that point. A client's bbox query misses
the division the user is standing in whenever that tile doesn't intersect
the query bbox, which for any division meaningfully larger than its
assigned tile is the common case. Only real geometry overlap fixes it,
which is what `overlap-tile-references` is for.

Divisions **do** go through the record-density coarsest-fitting-zoom
assigner. `stage_import`'s dispatch to `stage_division_import` returns
early for the *import* stage only; `run_pipeline` (`quadtree.py`) then
calls `stage_tile_assignment` — the Python twin of
`compute_tile_assignments.sql` — unconditionally for every source,
divisions included, feeding `qk17` into the `max_per_tile` splitter. The
`qk17` column is the mechanism's *input*, not the whole mechanism.

### Requirements

What "correct division tile assignment" and "faster containment" have to
mean, stated so the design below (and its eventual implementation) can be
checked against something:

1. **No false negatives (discoverability).** For every client bbox query
   B and every division D whose geometry intersects B, at least one tile
   referencing D intersects B. This is the invariant the midpoint
   mechanism violates.
2. **Bounded false positives.** A division may be referenced in a tile
   its geometry doesn't actually reach — a reference derived from the
   division's bbox rather than its geometry can do that — but never in a
   tile its bbox doesn't touch. False positives cost tile bytes, not
   correctness; clients filter by geometry or bbox anyway.
3. **Containment parity, bounded.** Point-in-division answers computed
   against fragments must equal answers computed against the whole
   polygon everywhere except within a narrow band around fragment
   seams, where disagreement is accepted at a bounded, measured rate.
   The band sits orders of magnitude below the source data's own
   positional accuracy, so in-band flips are noise beneath the data's
   noise floor; disagreement outside the band is a bug with no
   tolerance. Exact parity is deliberately not required; each design
   doc's own verification section states the checks that replace it.
4. **Tile records are unchanged in shape.** One record per division per
   referencing tile, carrying the whole-division bbox. Fragments are
   internal to the build and never appear in tile JSON. All copies of a
   division's record must be byte-identical across tiles — the spec's
   dedup-by-rkey rule silently drops all but one copy, so any per-tile
   divergence is data loss.
5. **One-time cost.** Decomposition runs where the covering already
   runs (`stage_covering`), never per containment batch, and re-runs
   only when its inputs change (same `artifact_fresh` discipline as
   every other stage).
6. **Bounded memory.** Decomposition works fragment-by-fragment,
   level-by-level, the way the covering loop already does — never
   whole-polygon × whole-grid in one operation. `ST_Intersection` over
   polygons as large as ~200k vertices is exactly the unbounded,
   whole-relation shape of work that has exhausted memory before (see
   [design-constraints.md](design-constraints.md)'s notes on unbounded
   complex-state aggregation and `ST_Union_Agg` as a memory-pressure
   point).

### Shippable units

Referred to by these names — not by phase numbers. Ordinal labels rot
as the plan flexes, invite invented sub-numbering, and become
impenetrable to the supervising developer; whether a named unit has
shipped can be checked against the repo.

- **representative-candidate-point** — shipped. Division candidates'
  containment point is `ST_PointOnSurface` of the division's geometry
  rather than the bbox midpoint, so it is guaranteed to lie inside the
  division.
- **fragment-containment** — shipped. The performance fix: edge cells
  recurse on fragment vertex count rather than to a fixed depth, and
  each edge leaf stores its own clipped geometry, so a candidate point
  is tested against a fragment instead of a whole polygon. It also moved
  division `qk17` onto the interior point, since testing a fragment
  clipped to the joined tile requires the point to be in that tile. See
  [fragment-containment-design.md](fragment-containment-design.md).
- **overlap-tile-references** — not started. The correctness fix:
  division-to-tile references derived from real geometry overlap at
  reference zoom z4, membership only, no stored fragments; replaces
  `stage_tile_assignment` for the division source. See
  [overlap-tile-references-design.md](overlap-tile-references-design.md).

### Discovery

Prerequisite for overlap-tile-references and fragment-containment; ran
against `boundaries.duckdb` and the covering artifact on `atgeo-1` from
the 2026-08-08 build — the same build behind `compute-containment.log`/
`tile-build.log` in the repo root, which turned out to already be the
fresh build log this step needed. Three numbers, now pinned:

1. **Reference zoom: z4.** Cells-touched-per-division at z4 has p99 =
   1.67, p999 = 2.12; only Antarctica (71 cells) breaches the client's
   `max_tiles=50` cap, and total tile-reference duplication stays under
   1.2%. Every zoom from z5 up puts Russia and/or Canada over the cap.
   Antarctica is a guardrail case (below), not a reason to go coarser.
   z4 dominates the z4–z7 candidate range outright — it's both the
   shallowest option and the only one under the cap — so there's no
   real tradeoff between zoom depth and tile bloat to weigh here.
2. **Unbounded per-tile division counts: accepted.** Divisions-per-cell
   at z4 is heavily clustered — one SE-Asia-region cell (`1202`) holds
   130,269 of 617,734 divisions (p50 across hit cells is 12) — geographically
   concentrated bloat, not diffuse growth. (This measures covering-touch
   at a fixed z4, which is an upper bound: divisions small enough to fit
   one cell are placed at their own size-fit zoom instead, so real
   per-tile counts run lower.) The implausible-referencing-count
   guardrail below is the
   intended backstop, not a `max_per_tile`-style cap.
3. **Fragment capacity: `V = 5000`, depth cap `16`.** `ST_NPoints` over
   all divisions: p50 = 123, p99 = 6,217, max = 345,467. The ~20
   known-slow cells' cost is driven by a handful of hyper-complex
   single boundaries (130k-345k vertices) tested at full size against
   every candidate point. Prototyping the adaptive stopping rule shows
   fragment count falling monotonically as `V` grows (23,891 fragments
   at `V = 500` down to 2,244 at `V = 5000`); `V = 500` fails to
   converge even at the prototype's z18 depth cap. `V = 5000` cuts
   worst-case per-point test cost 40-70x for the worst cells and fully
   resolves by z15 — three levels past today's `COVER_MAX_ZOOM = 12`,
   with one level of headroom under the depth cap. Storage isn't what
   a tighter `V` would buy: total stored vertices across all fragments
   barely moves with `V` (3.88M at `V = 500` vs 4.05M at `V = 5000`,
   +4.5%) — the real cost of going tighter is fragment count and
   recursion depth, both minimized by `V = 5000` while it still clears
   the fix's own target (the 10-38x slowdowns the original log
   analysis measured) with margin to spare.

### Open questions

- [ ] Guardrail: log (or fail on) divisions whose referencing-cell
      count is implausible for their level, so a garbage geometry
      (`known-data-quality-issues.md` documents them) cannot fan out
      into hundreds of tiles silently. Concrete candidates from
      Discovery: Antarctica (71 cells at z4) and a level-50 division
      reaching 27 cells at z7 against a level-50 median of 1.

## Which pipeline disk writes earn their keep

Status: one open question (the import sort), one settled note kept as
context (the export staging write). Neither is scoped for implementation.

The bar is that spill stays bounded — a few dozen GB is fine, hundreds
is not.

### The import sort by qk17

`stage_import` writes the places parquet with `ORDER BY qk17 NULLS LAST`
(`stages.py`, and the division equivalent). Sorting the full wide places
table is the pipeline's other whole-dataset sort.

Its usual justification is DuckDB zone-map pruning, which requires the
filtered column to be sorted. But a sweep of every `qk17` predicate in
the repo finds exactly one range filter — the per-batch
`WHERE qk17 >= … AND qk17 <= …` in `compute_containment` — and it runs
against `places_slim`, a temp table that is itself built `ORDER BY
p.qk17`. Every other `qk17` use is `left(qk17, N)` inside a `GROUP BY`,
a join key, or an `IS NOT NULL`: full-scan work that gains nothing from
sort order. **No query prunes the places parquet by a `qk17` range.**

Dropping the sort would move `places_slim`'s build from a cheap re-sort
of sorted input to a real sort — but of a four-column projection, not
the wide table.

To measure before acting: sorting by `qk17` clusters spatially adjacent
rows, which plausibly improves parquet compression on `names` and
`addresses`. That is the one real benefit, and it wants a number rather
than an argument.

### The export staging write

The batched export writes a staging parquet of the payload, estimated at
25–40 GiB. A few dozen GB is within budget, so this is recorded as
understood rather than as work to do.

Zero is reachable for `overture_place` and `osm` by filtering both sides
of the export join on a quadkey prefix range instead of materialising:
for those sources `tile_qk` is always a prefix of the place's own
`qk17`, so both sides prune by zone map and nothing extra is written.
It does not generalise to divisions, whose tile references come from the
covering artifact rather than their own `qk17`, so a places-side `qk17`
filter would drop them silently. That buys back a few dozen GB at the
cost of a second export mechanism — not a trade worth making unless
something else motivates it.
