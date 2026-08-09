# Performance improvements

Proposed performance work on code that already works correctly —
open-ended, one section per idea, each with its own status. This doc
holds performance-improvement ideas generally, not just containment; add
new sections here rather than starting another file. Nothing here is
scoped for implementation until its section says so.

## Containment computation: polygon tiling, and correct division tile assignment

Status: scoped, not started. No design has been reviewed. Scoping
conversation 2026-08-08 merged this with the division tile assignment and
containment names problem (formerly tracked separately) once both turned
out to need the same mechanism — see Solution below. No longer purely a
performance nice-to-have: it also fixes a real correctness bug (next
section), which settles the old "is it worth doing yet" question.
Rigor pass 2026-08-09: corrected the description of the current
tile-assignment path (divisions do go through the record-density
splitter), added a Requirements statement, fixed a contradiction in the
Tier B placement rule, and expanded the open questions with
implementation hazards found by reading the code.

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
3. **Containment parity.** Point-in-division answers computed against
   fragments must equal answers computed against the whole polygon, for
   every candidate point — including points lying exactly on fragment
   seams and points in either lobe of a D7 antimeridian boundary. (Seams
   are the sneaky part; see open questions.)
4. **Tile records are unchanged in shape.** One record per division per
   referencing tile, carrying the whole-division bbox. Fragments are
   internal to the build and never appear in tile JSON. All copies of a
   division's record must be byte-identical across tiles — the spec's
   dedup-by-rkey rule silently drops all but one copy, so any per-tile
   divergence is data loss.
5. **One-time cost.** Decomposition runs at import (or the boundaries
   build), never per containment batch, and re-runs only when its inputs
   change (same `artifact_fresh` discipline as every other stage).
6. **Bounded memory.** Decomposition is per-division × per-cell
   `ST_Intersection` over polygons as large as ~200k vertices — exactly
   the shape of work that has exhausted memory before (D6, D9). It must
   run over bounded partitions like everything else.

### Solution

**Pre-split large/geometrically-complex boundary polygons against a coarse
global grid at import time** — a one-time cost paid once per boundary —
and use the resulting fragments for two purposes at once: bound the
containment edge-arm test's cost to a fragment's vertex count instead of
the whole polygon's, and give every division a correct set of tile
references instead of one bbox-derived point. Divisions are referenced in
every tile they overlap, computed from real geometry, not the bounding
box. Two tiers, separated by one test (no level-based gating needed — a
sub-locality's polygon is never large enough to trigger the first tier):

- **Tier A — not wholly contained within one cell at the decomposition
  zoom.** Intersect the polygon against the grid, store one fragment per
  overlapping cell, reference the division in each cell whose fragment is
  non-empty. Fixes Norway/Chile/Indonesia for free, since real geometry
  replaces the bbox, and bounds the edge-arm test to that fragment's
  vertex count.
- **Tier B — fits within one cell at that zoom.** Placement zoom is the
  deepest zoom at which the division's bbox is no larger than one cell
  in each axis — a *size* test, not a containment test. Reference the
  division in each of the (at most 4) cells its bbox touches at that
  zoom. This is where sub-localities land, generally much deeper than
  the Tier A decomposition zoom. The rule deliberately isn't "deepest
  tile that wholly contains the bbox": quadtree cell edges at z1 lie on
  0° longitude and the equator, so under a wholly-contains rule a London
  borough straddling the prime meridian would be assigned to z0. Size-fit
  plus touched-cells gives the same division a deep zoom in ≤4 tiles.
- **Straddling needs no special case.** A division touching a cell
  boundary at its tier's placement zoom is simply referenced in each cell
  it touches, same mechanism in both tiers.
- **Replaces the record-density tile assigner for divisions.** An
  earlier revision claimed the two don't collide; that was a misreading
  (see the correction in the problem statement above). `run_pipeline`
  calls `stage_tile_assignment` for every source, so this design must
  explicitly skip or fork that stage for `overture_division` and produce
  the division-to-tile artifact from geometry instead. Dropping the
  splitter also drops `max_per_tile` for divisions — per-tile division
  counts become bounded only by how many divisions overlap a cell.
  Probably fine (divisions are sparse relative to places), but check the
  worst cell (dense sub-locality regions, e.g. Jakarta's kelurahan)
  before accepting it silently.

**Schema consequence**: today `tile_assignments.parquet` holds exactly
one `(place_id, tile_qk)` row per division. A division mapping to N
tiles needs a one-to-many structure — either N rows in the same artifact
(the export and containment joins already key on `place_id`, so fan-out
may Just Work) or a separate division-to-tile table. Not designed here;
note that `stage_tile_assignment` currently *errors* on duplicate
place_ids in its output, so whichever shape is chosen, that check has to
become division-aware rather than simply deleted.

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

### Open questions

- [ ] What triggers tiling a polygon — a vertex-count threshold, or the
      observed slowdown-vs-`n` ratio, or both?
- [ ] Decomposition zoom: reuse the existing qk17 / `partition_zoom=6`
      quadtree partitioning already used for containment batching, or
      something independent? Same parameter for both the performance and
      tile-assignment uses — resolve once, here.
- [ ] Does decomposition produce and store the clipped fragment geometry
      (needed for the containment-cost fix) or only cell membership
      (sufficient for tile-reference correctness alone)? A
      cell-membership-only version is a smaller first cut if the
      correctness fix is wanted before the performance fix — confirm
      whether that staging is acceptable or whether the two should ship
      together.
- [ ] What structure holds the division-to-tile mapping (table shape,
      where it's built relative to `stage_division_import` and
      `compute_containment`)?
- [ ] Seam semantics. `ST_Contains` excludes the boundary, and splitting
      introduces interior seams that were never boundaries of the
      original polygon: a candidate point lying exactly on a seam is
      inside the whole division but contained by *neither* fragment
      under `ST_Contains` — or by *both* under `ST_Covers`. Either way
      naive parity breaks. Likely answer: covers-plus-dedup (test
      fragments with `ST_Covers`, dedupe matches per division id before
      the `within` list aggregation, since duplicate matches would put
      the same rkey in the list twice). Whatever is chosen, the parity
      test must include seam points deliberately, not by luck.
- [ ] Degenerate fragments. `ST_Intersection` of a polygon with a cell
      can return GeometryCollections, and a division whose border runs
      exactly along a grid line yields line/point pieces. "Non-empty
      fragment" must mean *positive area* (extract polygonal components),
      or grid-aligned borders produce spurious tile references and edge
      fragments that no point can ever match.
- [ ] Clipping creates new vertices under floating point. A point close
      enough to a seam can land on the other side of it relative to the
      unsplit polygon. Parity testing should bound seam-adjacent
      disagreement (or snap/quantize deliberately), not assert exact
      equality and flake.
- [ ] Web-Mercator range. Quadtree cells end at ±85.05°; geometry beyond
      that (Antarctica) intersects no cell at any zoom, so Tier A
      decomposition silently discards it. Probably acceptable — a tile
      that can't exist can't be queried — but decide it explicitly.
- [ ] Same root bug, different limb: `_coord_exprs` gives division
      *candidates* their containment point from the same bbox midpoint,
      so Norway's own `within` relations are computed at a point that
      may not be in Norway. This design fixes discoverability only.
      Decide whether the candidate point is in scope (an
      `ST_PointOnSurface`-style representative point at import is cheap
      and orthogonal to tiling) or a tracked follow-up — but don't let
      it silently ride.
- [ ] Fragment identity vs. boundary identity. `cov` and the edge arm
      join on `boundary_id`, and `relations` rkey/name/level come from
      `bnd.places` by that id — for *every* source's containment run,
      not just divisions'. Fragments need their own rows for
      covering/edge-arm purposes while carrying the parent division id
      for rkey attribution. Decide the fragment table's key and how
      `stage_covering` regenerates against it.
- [ ] Tier B cell-touch computation must handle D7 bboxes
      (`min_longitude > max_longitude`) with the two-lobe logic
      `covering.py` already uses, or Fiji gets referenced in the cells
      spanning the entire Pacific. Note the import-side bbox filter
      currently *drops* ±180-crossers (design-constraints D7), so test
      data has to be constructed, not found.
- [ ] Guardrail for the no-gating assumption. "A sub-locality is never
      large enough to trigger Tier A" is a claim about Overture data
      quality, and `known-data-quality-issues.md` already documents
      garbage boundaries; a junk geometry would silently fan out into
      hundreds of tiles. Log (or fail on) divisions whose
      referencing-cell count is implausible for their level.
- [ ] Parity/correctness testing: tiled/multi-tile division output must
      match a from-scratch geometric answer — same containment and
      discoverability, not just "doesn't crash." D7 (antimeridian
      OR-logic in `compute_containment.sql`) must be preserved through
      decomposition, not just the single-lobe case. Note decomposition
      partly *retires* D7 for Tier A: each fragment has a sane
      single-lobe bbox, so the OR-logic branch stops being reachable for
      decomposed boundaries — the parity suite should assert that, too.
- [ ] Where does the one-time decomposition cost land — `stage_division_
      import`, or the `boundaries.duckdb` build? Whichever it is has to
      fit the existing artifact pipeline without restructuring
      `boundaries.duckdb`.
- [ ] Sub-locality inclusion (already shipped, see above) roughly doubled
      the level vocabulary considered by `bnd.places` — worth a
      size/cost sanity check against the evidence below before
      implementing the rest, not a blocker.

### Evidence

Full cell-by-cell numbers (prefix, n, duration, actual÷predicted) live in
the 2026-08-08 log-analysis session, not reproduced here — re-run the same
analysis against a fresh build log before implementing, since this data is a
snapshot of one build.

A candidate second cost driver was investigated and ruled out — whether
same-level boundary overlap density (as opposed to per-boundary vertex
complexity) also drives the slow cells above. It doesn't, and while
investigating it, a data characteristic surfaced that isn't performance
work at all (source boundaries duplicated under two different IDs). Both
now live in `known-data-quality-issues.md`.
