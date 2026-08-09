# Performance improvements

Proposed performance work on code that already works correctly —
open-ended, one section per idea, each with its own status. This doc
holds performance-improvement ideas generally, not just containment; add
new sections here rather than starting another file. Nothing here is
scoped for implementation until its section says so.

## Containment computation: polygon tiling, and correct division tile assignment

Status: design settled in the 2026-08-09 walkthrough, recorded below as
three named shippable units. representative-candidate-point has
shipped. Discovery (below) has pinned all three numbers, so
overlap-tile-references and fragment-containment are also unblocked for
implementation. (History:
scoping 2026-08-08 merged this with the division tile assignment
problem once both needed the same mechanism; a rigor pass 2026-08-09
corrected the description of the current tile-assignment path and added
the Requirements; the walkthrough then settled the rest.)

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

`overture_division_import.sql` computes, per division:

```sql
-- qk17 placed at the geometry centroid for tile assignment
ST_QuadKey(
    (ST_XMin(ma.geometry) + ST_XMax(ma.geometry)) / 2.0,
    (ST_YMin(ma.geometry) + ST_YMax(ma.geometry)) / 2.0, 17
) AS qk17,
```

Two distinct bugs, both from indexing an area by a single point:
1. The comment claims "centroid"; the computation is the bbox midpoint,
   which is not the centroid and for crescent-shaped, multipart, or
   overseas-territory geometries (Norway, Chile, Indonesia) is not inside
   the division at all.
2. The whole assignment is derived from that one point. The z17 depth is
   not what ships — `stage_tile_assignment` truncates `qk17` to the
   coarsest prefix holding ≤ `max_per_tile` records, so the assigned
   tile can be any zoom from 6 to 17 — but whatever the depth, a
   division is discoverable only through the single tile containing its
   bbox midpoint. A client's bbox query misses the division the user is
   standing in whenever that one tile doesn't intersect the query bbox,
   which for any division meaningfully larger than its assigned tile is
   the common case.

Correction to what an earlier revision of this section claimed: divisions
**do** go through the record-density coarsest-fitting-zoom assigner.
`stage_import`'s dispatch to `stage_division_import` returns early for
the *import* stage only; `run_pipeline` (`quadtree.py`) then calls
`stage_tile_assignment` — the Python twin of
`compute_tile_assignments.sql` — unconditionally for every source,
divisions included, feeding the midpoint `qk17` into the `max_per_tile`
splitter. The `qk17` column is the mechanism's *input*, not the whole
mechanism.

### Requirements

What "correct division tile assignment" and "faster containment" have to
mean, stated so the design below (and its eventual implementation) can be
checked against something:

1. **No false negatives (discoverability).** For every client bbox query
   B and every division D whose geometry intersects B, at least one tile
   referencing D intersects B. This is the invariant the midpoint
   mechanism violates.
2. **Bounded false positives.** A division may be referenced in a tile
   its geometry doesn't actually reach (Tier B works from the bbox), but
   never in a tile its bbox doesn't touch. False positives cost tile
   bytes, not correctness — clients filter by geometry or bbox anyway.
3. **Containment parity, bounded.** Point-in-division answers computed
   against fragments must equal answers computed against the whole
   polygon everywhere except within a narrow band around fragment
   seams, where disagreement is accepted at a bounded, measured rate.
   The band sits orders of magnitude below the source data's own
   positional accuracy, so in-band flips are noise beneath the data's
   noise floor; disagreement outside the band is a bug with no
   tolerance. Exact parity is deliberately not required — see
   Verification for the checks that replace it.
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
   polygons as large as ~200k vertices is exactly the shape of work
   that has exhausted memory before (D6, D9).

### Solution

**Pre-split boundary polygons against the quadtree grid and use the
result twice**: bound the edge-arm test's cost to a fragment's vertex
count instead of the whole polygon's, and derive every division's tile
references from real overlap instead of one bbox-derived point.

**The decomposition kernel already exists: it is the covering loop.**
`covering_seed.sql` and `covering_level.sql` already perform per-level
quadtree decomposition: clip the parent level's already-clipped
fragment (not the original polygon) to each child cell, emit cells the
geometry fully contains as interior, expand the rest ×4, recurse to
`cover_max_zoom` — and then discard the edge-leaf geometry, keeping
membership only. Fragment containment is three modifications to that
loop, not a new subsystem, and the one-time decomposition cost lands
where the covering already runs (`stage_covering`), fitting the
existing artifact pipeline without restructuring `boundaries.duckdb`:

1. **Adaptive stopping rule.** Recurse an edge cell only while its
   fragment's vertex count exceeds a capacity `V`, up to a depth cap.
   This is the record-density tile assigner's shape of knob — (`V`,
   depth cap) playing the role of (`max_per_tile`, `max_zoom`) — for
   the same reason, driven by vertex count instead of record count:
   resolution goes where the complexity is. Chesapeake and the fjords
   recurse deep; a simple inland polygon stops splitting immediately
   and its fragment set is itself. One uniform mechanism, so there is
   no "what triggers tiling" threshold and no second code path in the
   edge arm.
2. **Persist edge-leaf geometry** instead of discarding it. Fragment
   identity and attribution are inherited, not designed: covering rows
   are already keyed `(boundary_id, tile_qk)`, and the edge arm
   already joins to `bnd.places` by `boundary_id` for rkey/name/level
   — for every source's containment run, not just divisions'. Where
   the geometry physically lives (inline in the covering parquet, a
   sidecar file, or `boundaries.duckdb`) is an implementation-time
   decision sized by Discovery output; it cannot reopen the key or the
   attribution.
3. **Variable-depth edge arm.** Edge cells then exist at multiple
   zooms, so the edge arm joins per-zoom — the same per-level UNION
   ALL pattern the interior arms already use.

**Fragment tests use covers-plus-dedup.** Splitting introduces interior
seams that were never boundaries of the original polygon, and
`ST_Contains` excludes boundaries: a point exactly on a seam would be
inside the whole division yet inside neither fragment. Fragments are
therefore tested with `ST_Covers`, and matches deduplicated per
boundary id before the `within` list aggregation, because a seam point
covers-matches two fragments of the same division and would otherwise
put the same rkey in the list twice.

**Degenerate fragments are already handled.** The covering loop drops
zero-area clips (`ST_Area(geom) > 0` in both the seed and level SQL).
Keep that filter; it is what prevents a border running exactly along a
grid line from producing line/point fragments and spurious references.

**Web-Mercator range: clip and accept.** Quadtree cells end at ±85.05°;
geometry beyond that intersects no cell at any zoom and is silently
excluded from decomposition and references. Accepted deliberately — a
tile that cannot exist cannot be queried.

**Tile references decouple from fragment depth.** How deep fragments go
is internal and adaptive; which tiles reference a division is
client-facing policy at fixed zooms:

- **Tier A — larger than one cell at the reference zoom.** Reference
  the division in every cell its geometry overlaps at the *reference
  zoom* (a policy constant pinned by Discovery) — obtainable by
  truncating the division's existing covering to that zoom, so this
  needs no new geometry computation. Real overlap replaces the bbox
  midpoint, which fixes Norway/Chile/Indonesia.
- **Tier B — fits within one cell at the reference zoom.** Placement
  zoom is the deepest zoom at which the division's bbox is no larger
  than one cell in each axis — a *size* test, not a containment test:
  quadtree cell edges at z1 lie on 0° longitude and the equator, so a
  wholly-contains rule would assign a London borough straddling the
  prime meridian to z0. Reference the (at most 4) cells the bbox
  touches at that zoom. Sub-localities land here, far deeper than the
  reference zoom. The cell-touch computation must use the two-lobe D7
  logic `covering.py` already implements (`min_longitude >
  max_longitude`), or Fiji gets referenced across the entire Pacific;
  note the import-side bbox filter currently *drops* ±180-crossers
  (design-constraints D7), so test data must be constructed, not
  found.
- **Straddling needs no special case.** A division touching a cell
  boundary at its tier's placement zoom is simply referenced in each
  cell it touches, same mechanism in both tiers.
- **Replaces the record-density tile assigner for divisions.**
  `run_pipeline` calls `stage_tile_assignment` for every source (see
  the correction in the problem statement above), so this design must
  explicitly skip or fork that stage for `overture_division` and
  produce the division-to-tile artifact from geometry instead.
  Dropping the splitter also drops `max_per_tile` for divisions —
  per-tile division counts become bounded only by geometry. Whether
  that is acceptable is Discovery item 2, answered with data, not
  assumed.

**Schema consequence**: today `tile_assignments.parquet` holds exactly
one `(place_id, tile_qk)` row per division. A division mapping to N
tiles needs a one-to-many structure — likely N rows in the same
artifact, since the export and containment joins already key on
`place_id` — and `stage_tile_assignment`'s duplicate-place_id error
check must become division-aware rather than simply deleted.

**Dedup is already declared.** The normative dedup-by-rkey statement
landed in `atgeo-spec.md` and `atgeo-client-sdk.md` on 2026-08-08,
ahead of this work, following the same declare-ahead pattern as
published_at/same_as/cid. Nothing left to write when this ships — but
there is something to *verify*: dedup-by-rkey assumes all copies of a
division's record are identical (Requirement 4 above).

Already shipped, ahead of the tile-assignment fix, because they didn't
depend on it: `relations.within` entries now carry `name` and `level`
(`compute_containment.sql`), and containment now reaches every level in
`LEVEL_VOCAB` (country through microhood, not just locality and coarser —
the `WHERE level <= 50` filter in `stages.py` had no principled reason to
exist once sub-localities were in scope, and is gone).

### Shippable units

Referred to by these names — not by phase numbers. Ordinal labels rot
as the plan flexes, invite invented sub-numbering, and become
impenetrable to the supervising developer; whether a named unit has
shipped can be checked against the repo. The design is reviewed once
(this document); the units ship separately. Ordering is a dependency
stated in prose: fragment-containment extends the covering machinery
that overlap-tile-references reads, so it lands last and reworks
nothing that shipped before it.

- **representative-candidate-point** — shipped. Division candidates'
  containment point is now `ST_PointOnSurface` of the division's
  geometry (computed once, in `overture_division_import.sql`'s
  `merged_areas_interior` CTE) instead of the bbox midpoint, guarded by
  a fail-loud import assertion (`_assert_interior_points`, a standalone
  helper following the `_assert_unique_key` convention) that the point is `ST_Within` the
  geometry for every shape `ST_Union_Agg` can produce, MULTIPOLYGON
  included. `_coord_exprs`'s `overture_division` branch reads the new
  `interior_lon`/`interior_lat` columns; `qk17` and tile assignment are
  untouched. A single interior point still can't perfectly represent an
  area's containment, but admin hierarchies are nested enough that it's
  correct in all non-pathological cases, and it's categorically better
  than a point that may not be inside the division at all. Shipped
  alone, with no dependency on the other two units, so `within`-relation
  diffs in the next build are attributable to this change and nothing
  else.
- **overlap-tile-references** — the correctness fix. Division-to-tile
  references from real overlap (Tier A / Tier B above; reference zoom
  z4), membership only, no stored fragments; replaces
  `stage_tile_assignment` for the division source. Implementation-level
  design, reviewed through four rounds: see
  [overlap-tile-references-design.md](overlap-tile-references-design.md).
- **fragment-containment** — the performance fix. The three
  covering-loop modifications above: adaptive stopping rule (`V =
  5000`, depth cap 16), persisted edge-leaf geometry, variable-depth
  edge arm. Storage placement decided during implementation.

### Verification

Exact parity is the wrong bar and is not required (Requirement 3).
Epsilon-scale disagreement at fragment seams is strictly below the
source data's positional noise floor — Overture boundaries are
generalized geometries with accuracy measured in tens of meters at
best. The dangerous failures are structural: a dropped fragment, a
misattributed boundary_id, a lost D7 lobe. Those are binary and
geographically clustered — a single lost fragment silently strips
relations from every point in half a province — and clustering makes
them invisible to spot checks. The checks target them directly:

- **Structural invariants — exact, cheap, no reference run needed.**
  Per-division mass balance: area of the fragment union equals area of
  the whole polygon within floating-point tolerance (catches dropped
  and duplicated fragments). Every division retains at least one tile
  reference.
- **Sampled behavioral diff with distance triage — one-time, at
  migration.** Run old and new containment on a stratified sample (the
  ~20 known-slow cells, seam and antimeridian synthetics, a random
  background sample) and diff the match sets. Triage every
  disagreement by distance to the nearest boundary: within a narrow
  band, accepted and counted against a rate threshold; outside the
  band, a failure with no tolerance. The triage rule is what separates
  "the data was ambiguous anyway" from "we broke the join."
- **Synthetic unit tests — recurring.** Seam points, grid-aligned
  borders, antimeridian lobes. Also assert the edge arm's D7 OR-branch
  is structurally unreachable for decomposed boundaries — fragments
  have single-lobe bboxes, so decomposition partly *retires* D7, and
  the suite should prove it.

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
   at a fixed z4, an upper bound on Tier B's actual per-tile counts,
   which place each division at its own size-fit zoom — real counts run
   lower.) The implausible-referencing-count guardrail (below) is the
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

- [ ] Storage placement for persisted edge-leaf geometry —
      implementation-time, sized by the pinned `V`/depth cap above.
      Key and attribution are inherited from the covering and are not
      reopenable here.
- [ ] Guardrail: log (or fail on) divisions whose referencing-cell
      count is implausible for their level, so a garbage geometry
      (`known-data-quality-issues.md` documents them) cannot fan out
      into hundreds of tiles silently. Concrete candidates from
      Discovery: Antarctica (71 cells at z4) and a level-50 division
      reaching 27 cells at z7 against a level-50 median of 1.

### Evidence

Full cell-by-cell numbers (prefix, n, duration, actual÷predicted) live in
the 2026-08-08 log-analysis session, not reproduced here. Re-running that
analysis against a fresh build log is now Discovery item 3, not a
standalone instruction.

A candidate second cost driver was investigated and ruled out — whether
same-level boundary overlap density (as opposed to per-boundary vertex
complexity) also drives the slow cells above. It doesn't, and while
investigating it, a data characteristic surfaced that isn't performance
work at all (source boundaries duplicated under two different IDs). Both
now live in `known-data-quality-issues.md`.
